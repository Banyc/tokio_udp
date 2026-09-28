# The tokio_udp receive-path gate

`tokio_udp` is the socket layer beneath `udp_listener`, `rtp` and the proxy
chain, so a stall here surfaces as a multi-second interactive stall at the top.
Its readiness handling is a state machine, and the half of it no completed
operation can reach is *cancellation*: a `recv` / `recv_from` / `recv_buf` /
`recv_buf_from` / `try_recv` future that is dropped mid-poll must leave the
socket in a state a correct implementation cannot leave, because `tokio`'s
readiness is a latch and the drop lands inside the window where the latch is
armed.

This file is the authoritative scope of that gate. `cargo test -p tokio_udp`
silently skips every `#[ignore]`d test, so the opt-in tier is named in the
`gate-manifest` block below; the checker (`netem-tools check-gate`,
per-crate mode) re-derives the set from the compiled test binary and fails when
the manifest and reality disagree. Run it from this crate's checkout:

```sh
netem-tools check-gate \
  --crate . tokio_udp tests GATE.md
```

Everything else in the crate is the `--lib` target's 30 always-run unit tests,
which are correctness tests rather than perf rows: they are outside this
manifest and are not cost-declared here.

## The invariant the default tier asserts

> Whenever a datagram is available the socket is readable and a read makes
> progress; when the queue is drained the cached event has been dropped, so
> `readable` parks instead of answering off an event whose datagram is already
> gone; and a receive future dropped at any point has consumed nothing from the
> kernel.

Three tests hold that in the tier that always runs:

* `cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced`
  stages the cancellation window rather than sampling it. The receive is polled
  once against an empty socket (parked, no syscall performed), the peer then
  sends, the arrival's event is *awaited* — which is what proves the driver's
  publish has already landed — and only then is the future dropped, so the
  `readable` after the drop is a property and not a race: a park there can only
  mean the drop consumed the wakeup. The round then requires the datagram to be
  delivered whole, from its own sender, and the final `readable` requires the
  drained socket to stop claiming readable.
* `cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram`
  is the other half: a cancellable receive has two legal outcomes for the single
  poll it is given — complete, having performed the syscall and committed the
  datagram it took, or stay parked, having consumed nothing. The illegal third
  (a syscall performed into a future that is then dropped) is asserted against
  in both directions, with `recv` and `recv_from` driven separately because they
  are separate paths through the backend.
* `cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram`
  parks three concurrent receives on one socket, drops them all mid-poll before
  releasing four senders (so every run exercises the window, rather than only
  the runs whose schedule produces it), then cycles the readers through a
  single-poll drop, a short-`timeout` drop, `try_recv` and an armed
  `readable`-then-drop. It asserts the law a cancellation bug breaks: each of
  the 40 datagrams is committed to exactly one caller, from its own sender, and
  a fresh datagram arriving after the drain is still announced and delivered.

## The opt-in soak

`cancellation::cancelled_receive_soak_conserves_every_datagram` runs the same
schedule for many cycles, one fresh set of receive futures per cycle, with the
accounting closed and the queue required empty between cycles. Every cycle is
bounded: a cycle that never completes is counted, printed and fatal as a
**hang**; a cycle that completes but exceeds 250 ms is counted and printed as
**late**, which is a host-latency observation and never a catch. Tier
**standard** (`#[ignore]`d, asserting), declared below with its cost and cells.

```text
CARGO_TARGET_DIR=/Users/charliesmith/code/tmp/it48_tokio_udp_target \
  cargo test --release -p tokio_udp --locked --offline \
  --test cancellation -- --ignored --nocapture
```

`TOKIO_UDP_SOAK_CYCLES` sets the cycle count (default 300; the per-cycle
datagram count is fixed so the accounting stays comparable run to run). The
sockets are bound once for the whole soak and reused, because a fresh pair per
cycle would be tens of thousands of binds and this host refuses binds with
`EADDRNOTAVAIL` under that churn — an artifact indistinguishable from the loss
under test. No cycle can therefore lose a datagram to a refused bind, and the
loss counter only moves when a receive failed to commit one.

## The loopback delay this socket layer adds

A deployed client reports a 190 ms **minimum** round trip where the harness's
clean arm reports tens of milliseconds. A floor is a cost paid by essentially
every datagram, so it is measured directly: `tests/loopback_delay.rs` runs a
one-datagram-in-flight ping-pong on loopback — no datagram can queue behind
another — and compares this crate's readiness path against a plain
`std::net::UdpSocket` carrying the same echo. Syscall and copy counts are
properties of the paths, stated rather than counted per datagram because macOS
offers no unprivileged tracer: `send`/`recv*` issue one `sendmsg` and one
`recvmsg` per datagram with two user/kernel copies, `std`'s
`send_to`/`recv_from` issue one `sendto` and one `recvfrom` with two, and the
concatenating vectored fallback adds a third userspace copy.

Measured on the tree this file is committed with (`--release`, best of three):
this crate's median 0.040 ms against `std`'s 0.023 ms — **0.017 ms of added cost
over a blocking thread pair, 0.009 % of the field's floor** — with p99 0.081 ms.
The concatenating fallback measures p50 0.032 ms but p99 1.03 ms, so the extra
copy is an allocator cost in the tail rather than a toll on every datagram. The
bound is a tripwire at 2 ms on the median, two orders of magnitude below the
field number.

The same file pins the one thing about this crate's socket that is knowingly
unsized: `bind` calls `socket(2)` and `bind(2)` and sets **no** buffer size, so
both directions run at the kernel defaults. Measured here: receive 786 896 B and
send 9 216 B, identical to a plain `std` socket. That is a **loss ceiling, not a
floor** — a socket buffer never delays a datagram it accepts, it can only drop
one — and the burst arm measures the shape: 9 000 datagrams of 1 200 B offered
with no reader keep 638 at the default size and 3 404 once the receive buffer is
set to 4 MiB. On Linux an unsized socket's receive queue comes from
`net.core.rmem_default` — `SK_RMEM_DEFAULT = _SK_MEM_OVERHEAD * _SK_MEM_PACKETS`
with `_SK_MEM_OVERHEAD = SKB_TRUESIZE(256)` (`include/net/sock.h:3056-3059`),
212 992 B on x86_64 — and a datagram is refused once the truesize-accounted queue
would exceed it (`net/ipv4/udp.c:1669-1679`), which is charged as
`UDP_MIB_RCVBUFERRORS` (`net/ipv4/udp.c:2313-2316`), so the ceiling prices offered
*load* — the opposite mechanism to a floor. The buffer sizes are therefore left to
the caller: the value that matters is a path bandwidth-delay product, which the
transport that knows its own send rate owns and a socket layer does not. The
section below measures what that ceiling costs and gives the caller the surface
to act on it.

## The raw receive buffer: what the ceiling costs, and the sizing surface it needs

The arm above records that the default buffer is a *loss ceiling* rather than a
floor. `tests/rcvbuf_cliff.rs` measures what that ceiling costs **at the rates
this product offers** — the interactive lane's 256 B / 5 ms ≈ 51 KB/s and
256 B / 25 ms ≈ 10 KB/s, and the bulk lane's 1 MiB/s, the rate the M3 arm offers
— and decides the sizing question the earlier arm deliberately left open.

All four arms were measured with `cargo test --release --locked --offline --test
rcvbuf_cliff -- --ignored --nocapture --test-threads=1` on the tree this file is
committed with, at load averages **1.8–3.6 on 10 cores** (`uptime`).

### A live reader drains every rate the product offers

With the reader draining from the first millisecond, the default refuses nothing
at any rate the product produces, at either product size (`OFFER_SWEEP` rows):

| size | nominal rate | lane | offered | kept | refused | achieved |
| --- | --- | --- | --- | --- | --- | --- |
| 256 B | 10 KB/s | interactive 256 B / 25 ms | 12 | 12 | 0 | 10 240 B/s |
| 256 B | 51.2 KB/s | interactive 256 B / 5 ms | 61 | 61 | 0 | 52 053 B/s |
| 256 B | 1 MiB/s | bulk (M3) | 1 234 | 1 234 | 0 | 1 053 013 B/s |
| 1 200 B | 51.2 KB/s | interactive 256 B / 5 ms | 13 | 13 | 0 | 52 000 B/s |
| 1 200 B | 1 MiB/s | bulk (M3) | 262 | 262 | 0 | 1 048 000 B/s |

The arm also reports 8 MiB/s and 64 MiB/s, both of which also lose nothing (9 878
and 79 178 datagrams of 256 B offered, 2 111 and 16 738 of 1 200 B); they are
reported and not asserted, because above 1 MiB/s the limiter on this host is the
drain loop rather than the buffer and conflating the two would make the arm lie
about which one moved. So the buffer is **not implicated at any offered rate the
product produces while its reader is running** — a buffer is filled by the offer
and emptied by the reader, and a reader that keeps up never lets it fill.

### The cliff is a stall duration per lane, not a rate

That is the whole shape of the mechanism: `rate x stall` is what the queue has to
absorb, so the cliff is the stall duration at which the accumulation exceeds the
budget. Measured at the bulk lane's 1 MiB/s against the `linux-default` budget
(212 992 B), with the reader stalling and then draining:

| datagrams accumulated in the stall | bytes | refused |
| --- | --- | --- |
| 166 (190 ms) | 199 KB | **0** |
| 192 (220 ms) | 230 KB | **20** |
| 437 (500 ms) | 524 KB | **264** |

and against this host's own default (786 896 B) and a 4 MiB control, both
refused **nothing** at every stall to 500 ms. At the **interactive** cadence the
`linux-default` budget refused **nothing** at every stall the arm walks, up to
500 ms — 102 datagrams of 256 B is 26 KB against a 208 KiB budget, an eightfold
margin. So: the interactive lane is not implicated at any stall the field's
190 ms floor can produce, and the bulk lane's cliff sits between 190 ms and
220 ms of reader stall at the M3 arm's own offered rate. On this host's truesize
accounting that is arithmetically 212 992 B / 1 200 B = **177 datagrams**, which
at 873 datagrams/s is **203 ms**; on the deployed target's tighter accounting it
is **~105–132 ms** (derived below). The field's floor is 190 ms. The cliff is *at*
it here and *below* it there.

### What a refusal costs

`a_refused_datagram_costs_a_path_round_trip` stages a 190 ms round trip in
userspace (two relays, one one-way delay each) and runs a bounded transfer over
it, with the receive buffer the only place a datagram can be lost:

| receive buffer | offered | arrived first pass | refused | completed |
| --- | --- | --- | --- | --- |
| 786 896 B (host default) | 699 | 699 | 0 | 297 ms (1.57 x RTT) |
| 212 992 B (`linux-default`) | 699 | 463 | **236** | 490 ms (2.58 x RTT) |
| 4 MiB (path BDP) | 699 | 699 | 0 | 297 ms (1.57 x RTT) |

**Recovery cost = 490 ms - 297 ms = 193 ms = 1.01 x the path's round trip.** A
refused datagram is not lost work, it is work deferred by one round trip, and on
the field's path a transfer that has to recover ~236 of 699 datagrams finishes in
2.6 round trips where the same transfer against a sized buffer finishes in 1.6.
The offer is paced against wall clock, so the refused count moves by a couple of
datagrams between runs (two runs measured 236 and 238) and the recovery cost does
not (193 ms and 192 ms).
That is the shape of the field's maxima: 1063 ms and 3205 ms are 5.6 and 16.9
base round trips — multiples of the path, which is what a refusal recovered by a
retransmission produces.

### The kernel's bounds, read from the checkout

* The doubling. `SO_RCVBUF` stores **twice** the accepted value, to account for
the `skb` overhead charged to the receive queue:
  `WRITE_ONCE(sk->sk_rcvbuf, max_t(int, val * 2, SOCK_MIN_RCVBUF))`,
  `net/core/sock.c:987` (in `__sock_set_rcvbuf`, `:967`), with
  `SOCK_MIN_RCVBUF = TCP_SKB_MIN_TRUESIZE` (`include/net/sock.h:2604`).
* The `rmem_max` clamp. `SO_RCVBUF` is passed through
  `min_t(u32, val, READ_ONCE(sysctl_rmem_max))`, `net/core/sock.c:1375` — above
the sysctl the request is **silently** reduced, so the size a socket runs with is
  `getsockopt(SO_RCVBUF)`'s answer (`v.val = READ_ONCE(sk->sk_rcvbuf)`,
  `net/core/sock.c:1771-1773`) and never the argument that was passed.
  `net.core.rmem_max` defaults to `4 << 20` (`net/core/sock.c:286`,
  `Documentation/admin-guide/sysctl/net.rst:228`), and the default is settable
  down to `SOCK_MIN_RCVBUF` (`net/core/sysctl_net_core.c:752-756`, `:36`).
* The default. `sk->sk_rcvbuf = READ_ONCE(sysctl_rmem_default)`
  (`net/core/sock.c:3707`) with `sysctl_rmem_default = SK_RMEM_DEFAULT`
  (`net/core/sock.c:289`, `include/net/sock.h:3059`) = 212 992 B on x86_64.
* The refusal. `if (rmem + size > rcvbuf) { ... goto drop; }` where
  `rcvbuf = READ_ONCE(sk->sk_rcvbuf)` and `size = skb->truesize`,
  `net/ipv4/udp.c:1669-1679`; the drop is charged `UDP_MIB_RCVBUFERRORS` and
  `UDP_MIB_INERRORS` at `net/ipv4/udp.c:2313-2316` and counted per socket by
  `numa_drop_add` (`:2319`). `sk->sk_rcvbuf` is quoted for the UDP forward
  threshold at `net/ipv4/udp.c:2892`.

The truesize accounting is why the Linux budget is the **tighter** one, and it is
**derived rather than measured** here. On this host 212 992 B holds 172 datagrams
of 1 200 B (`CAPACITY` row), i.e. it charges ~1 238 B per 1 200 B datagram — an
upper bound on Linux's, because Linux charges `skb->truesize`, which is
`ksize(head allocation) + SKB_DATA_ALIGN(sizeof(struct sk_buff))`
(`net/core/skbuff.c:393-396`, with `SKB_TRUESIZE(X) = X +
SKB_DATA_ALIGN(sizeof(struct sk_buff)) +
SKB_DATA_ALIGN(sizeof(struct skb_shared_info))`, `include/linux/skbuff.h:273-275`)
and so includes the slab-rounded head plus the `struct sk_buff` itself. The
in-tree arithmetic for this exact budget is already on record — `wmem_default is
212992 and overhead is 640 bytes per packet (256 skb, 64 headroom, 320 shared
info)` (`drivers/net/ethernet/intel/ixgbe/ixgbe_main.c:2829-2831`) — which puts a
1 200 B datagram at ~1 840 B and the budget at **~115 datagrams**, and slab
rounding to the next allocation class (2 048 B of head plus a 256 B `sk_buff`)
puts it at ~92. So the deployed receive budget holds **~92–115 datagrams** of
1 200 B, or 110–138 KB of payload, against this host's measured 172, and the
Linux cliff at the bulk lane's 873 datagrams/s is **~105–132 ms** of reader
stall — *below* the field's 190 ms floor. Every depth measured in this file is an
upper bound on the deployed target's, never a lower one.

### The decision

Both defaults are finite and both are smaller than the field path's bandwidth-delay
product at any lane the product offers: at 1 MiB/s and a 190 ms round trip the
product is 199 KB, against 110–138 KB of payload in the `linux-default` receive
budget — the buffer cannot hold one round trip of the bulk lane, so a transfer
above ~0.55–0.7 MiB/s on that path loses to the kernel whatever the link allows. The
send side is the same shape: an unsized socket's send buffer bounds in-flight
bytes at `wmem_default`, so on a 190 ms path it caps throughput at
`212 992 / 190 ms ≈ 1.1 MB/s` independently of the receive side.

**This layer therefore exposes the sizing**, and leaves the default alone. Two
halves, each with a reason:

* `set_recv_buffer_size` / `recv_buffer_size` and `set_send_buffer_size` /
  `send_buffer_size` are now public on `UdpSocket` (Unix backend on `socket2`,
  the non-Unix fallback through a `socket2::SockRef` view of the same
  descriptor, and the parity shim instantiates both). A caller that knows a path
  bandwidth-delay product can now act on it through this crate's own API; the
  only way before was `async_fd()`, which is Unix-only and hands back the backend.
  The getters read the kernel, so the request-not-setting trap above — "we set
  4 MiB and landed at 213 KiB" — is observable by the caller rather than silent.
  On this host a 64 KiB request reads back 65 536 B, and a **1 GiB** request reads
  back 8 388 608 B: the clamp is the host ceiling and the getter reports it.
* The default stays the kernel's. A socket layer knows neither its path's round
  trip nor its transport's send rate, so any constant it chose would be a guess
  applied to every socket; and the interactive lane — the lane the field's client
  actually runs — is **not implicated at all**, since its own offered rate cannot
  fill the deployed budget in 500 ms of stall. The value that matters is a path
  product, and the transport that owns those two numbers is where it belongs.

Wiring the setters into that transport is the consuming change and is **not**
made here, because `rtp` sits above this crate and is owned elsewhere.

Each arm prints its own rows and asserts its own sanity. The assertions are: no
refusals at a live reader at the mandate rates; the default queue is finite and a
4 MiB request buys depth (the "the size did not reach the socket" tripwire); the
`linux-default` budget refuses nothing at the interactive cadence and the 4 MiB
control refuses nothing at any stall; the 4 MiB control needs no repair over the
emulated path while the deployed budget does, and that repair costs at least half
a round trip; and the default tier's setter arm makes a setter that does not reach
the kernel and a getter that echoes the request both red.

## Vacuity: the injections this gate is graded against

Each injection is a transient edit to `src/platform/unix.rs`, reverted before
the next; every row below was reproduced on the tree that carries these tests.

| injection | change | result |
|---|---|---|
| `IZ` — never clear cached readiness | `nonblocking` runs the operation bare, so a `WouldBlock` never drops the event it was armed with | **red**: `a_receive_dropped_while_parked_…` fails on "the queue was drained, so the cached readiness event must have been dropped", as do the crate's two pre-existing arms `tests::a_would_block_try_recv_stops_claiming_the_socket_is_readable` and `platform::unix::tests::a_failing_operation_drops_the_event_it_was_armed_with`. The other three tests stay green, which is correct: a stale latch costs a spurious wakeup, and the conservation, single-poll and soak assertions are about datagrams and completions, not about the latch's value. |
| `B` — lost datagram on cancel | `recv_uninit`/`recv_from_uninit` await `yield_now()` between the syscall and the completion | **red**: `a_receive_dropped_mid_poll_has_consumed_no_datagram` fails on its first round ("a single poll that did not complete the future had already consumed the datagram"), `concurrent_cancelled_and_timed_out_receives_…` hangs with 16 of 40 committed, the soak loses 14–20 of 32 per cycle and reports every one as a hang, and the crate's pre-existing `tests::recv_buf_commits_in_the_poll_that_reads_the_datagram` fails too. |
| `C` — readiness cleared too eagerly on cancel | a guard on the receive future performs the crate's own clear when the future is dropped before completing | **red**: `a_receive_dropped_while_parked_…` fails on its first round ("dropping the parked receive consumed the readiness event the arrival had already published"). The lib tier stays green, which is the point: this direction is only observable through the cancellation the new test stages. |

Restored, all four tests are green, and the pristine run is green. Each
injection was reverted with the file byte-identical to the committed tree
(`jj diff` empty) before the next was applied.

## Opt-in manifest

Each line is `target::test_name = tier`, and the set must equal the set of
non-`support` tests `cargo test -p tokio_udp --test cancellation -- --list
--ignored` reports. The default tier is defined by the absence of `#[ignore]`,
so the three tests this gate exists for are recorded as required-default: a
test silently re-ignored leaves the gate's own property unasserted.

```gate-manifest
cancellation::cancelled_receive_soak_conserves_every_datagram = standard
rcvbuf_cliff::a_live_reader_drains_every_rate_the_product_offers = full
rcvbuf_cliff::the_receive_capacity_in_datagrams_at_the_product_sizes = full
rcvbuf_cliff::a_reader_stall_is_what_overflows_the_receive_buffer = full
rcvbuf_cliff::a_refused_datagram_costs_a_path_round_trip = full
```

```gate-default-required
cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced
cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram
cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram
loopback_delay::tokio_udp_adds_no_floor_over_a_plain_std_udp_socket
loopback_delay::the_socket_buffers_are_left_at_the_kernel_defaults
loopback_delay::a_burst_past_the_default_receive_buffer_is_a_loss_ceiling_not_a_floor
lib::tests::the_buffer_setters_report_what_the_kernel_accepted
```

The asserting set is every `standard`/`full` scenario plus every required
entry; the `standard` soak asserts (that is what its injections check), so it
is asserting and not report-only.

```gate-asserting
cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced
cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram
cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram
cancellation::cancelled_receive_soak_conserves_every_datagram
loopback_delay::tokio_udp_adds_no_floor_over_a_plain_std_udp_socket
loopback_delay::the_socket_buffers_are_left_at_the_kernel_defaults
loopback_delay::a_burst_past_the_default_receive_buffer_is_a_loss_ceiling_not_a_floor
rcvbuf_cliff::a_live_reader_drains_every_rate_the_product_offers
rcvbuf_cliff::the_receive_capacity_in_datagrams_at_the_product_sizes
rcvbuf_cliff::a_reader_stall_is_what_overflows_the_receive_buffer
rcvbuf_cliff::a_refused_datagram_costs_a_path_round_trip
lib::tests::the_buffer_setters_report_what_the_kernel_accepted
```

## Perf declaration and coverage

Costs are measured on the release gate build (`--release --locked --offline`),
best of five wall-clock runs of the test binary; the two ~5 ms rows sit at the
process-start floor, so their declared cost is what the suite pays for them.

```gate-perf-design
cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced = default | 0.04 | baseline | cancellation-latch@readiness=parked+concurrency=single+conservation=witness
cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram = default | 0.01 | orthogonal | cancellation-commit@readiness=armed+concurrency=single+conservation=witness
cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram = default | 0.01 | composite(readiness,concurrency,conservation,sustained) | cancellation-conservation@readiness=armed+concurrency=concurrent+conservation=bulk+sustained=one-shot
cancellation::cancelled_receive_soak_conserves_every_datagram = standard | 0.25 | composite(readiness,concurrency,conservation,sustained) | cancellation-conservation@readiness=armed+concurrency=concurrent+conservation=bulk+sustained=repeated
loopback_delay::tokio_udp_adds_no_floor_over_a_plain_std_udp_socket = default | 0.10 | composite(path,shape,reference) | socket-floor@path=readiness+shape=ping-pong+reference=plain-std-udp
loopback_delay::the_socket_buffers_are_left_at_the_kernel_defaults = default | 0.01 | composite(name,state) | socket-option@name=so_rcvbuf_and_so_sndbuf+state=unsized
loopback_delay::a_burst_past_the_default_receive_buffer_is_a_loss_ceiling_not_a_floor = default | 0.15 | composite(path,load,size) | socket-loss@path=receive-queue+load=burst+size=default-vs-4MiB
rcvbuf_cliff::a_live_reader_drains_every_rate_the_product_offers = full | 3.2 | composite(load,rate,size) | socket-offer@load=live-reader+rate=sweep+size=product
rcvbuf_cliff::the_receive_capacity_in_datagrams_at_the_product_sizes = full | 2.2 | composite(name,state,size) | socket-capacity@name=so_rcvbuf+state=host-default-and-1MiB-and-4MiB-and-over-ceiling+size=product, socket-capacity@name=so_rcvbuf+state=linux-default+size=product
rcvbuf_cliff::a_reader_stall_is_what_overflows_the_receive_buffer = full | 10.2 | composite(stall,size,rate,budget) | socket-stall@stall=sweep+size=product+rate=interactive-and-bulk+budget=host-default-and-linux-default-and-4MiB
rcvbuf_cliff::a_refused_datagram_costs_a_path_round_trip = full | 1.5 | composite(path,rtt,budget) | socket-repair@path=emulated-190ms-rtt+rtt=field-floor+budget=linux-default-and-path-bdp
```

The declared sums are `default` 0.32 s (0.06 s cancellation + 0.26 s loopback measurement, measured 0.10/0.01/0.15 s) and `standard` 0.25 s (measured 0.24 s at 300 cycles). The default tier of the *crate* grows by the ~0.26 s this measurement adds on top of an unchanged 0.58 s lib tier; the parked test's
whole cost is its negative bound, held at 25 ms because a stale latch answers in
microseconds and a shorter bound only makes the arm stronger.

```gate-budgets
default = 1
standard = 1
full = 60
perf = 60
baseline = cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced
drift = 0.5
drift_floor_s = 2.0
```

Every cell this gate does **not** claim, with the reason it is empty:

```gate-coverage-gaps
cancellation-latch@readiness=error-only = no new arm; the pending-`SO_ERROR` wakeup is asserted by the pre-existing `tests::recv_surfaces_a_pending_so_error` and the `RECV_INTEREST` pin, and cancellation of an error-armed receive is not separately staged because read readiness is folded with error readiness on this host, so the arm would not attribute.
cancellation-conservation@scale=thousands-of-datagrams = the soak's per-cycle datagram count is fixed at 32 to keep the accounting comparable across runs; aggregate scale is bought with cycles, not per-cycle size, so nothing here claims what a single cycle looks like at a larger window.
cancellation-conservation@transport=shaped-link = this crate has no impairment instrument; loss, delay, reordering and rate shaping live in `netem_test` and the `rtp`/`rtp_mux` scenarios that compose this socket, not in a test that binds loopback directly.
cancellation-conservation@pillar=multicast-or-broadcast = the conservation law is asserted on connected and unconnected unicast sockets only; the multicast surface (`join_multicast_v4`, `set_multicast_loop_v4`) is exercised by the pre-existing option tests, and no cancellation arm claims it.
cancellation-commit@readiness=armed+conservation=bulk = the armed single-poll arm is asserted at the witness scale (one datagram per round) and inside the concurrent row's reader cycle; there is no separate armed-only bulk arm, because the concurrent row's readers already take that shape.
cancellation-commit@outcome=parked-with-datagram-queued = the parked arm is exercised whenever the arming wait returns before the platform has delivered the datagram (loopback delivery, not the readiness event, is what decides), so it is opportunistic on a correct implementation; staging it deterministically would mean reaching into the backend's own readiness state, which is what the lib-tier pin does instead.
cancellation-latch@tier=full = the soak is `standard`; the `full` budget is declared so a heavier row cannot be added without one, not because a `full` row is missing.
socket-floor@transport=impaired = this crate has no impairment instrument; loss, delay, reordering and rate shaping live in `netem_test` and the `rtp`/`rtp_mux` scenarios that compose this socket, not in a test that binds loopback directly.
socket-floor@shape=pipelined = the floor arm keeps one datagram in flight so nothing can queue behind anything else; pipelining depths are measured in `udp_listener`'s dispatch sweep, which composes this socket.
socket-loss@host=linux = the buffer sizes this arm reads are the host's, and the Linux default quoted above is read from the kernel source rather than measured on a Linux host, so the *magnitudes* of the two platforms' ceilings are not compared here.
socket-offer@host=linux = `rcvbuf_cliff`'s live-reader sweep runs on macOS and measures this host's 786 896 B default, which is 3.7x the `linux-default` budget the deployed target gets; the `linux-default` rows in the stall and repair arms are the ones that stand in for it, and they too are measured on this host's truesize accounting, so a refusal here is one Linux would refuse and a non-refusal is not a clearance.
socket-repair@stack=rtp = `rtp` was off limits for this measurement, so the repair is a bounded in-crate model — a gap report and a bulk second pass over the emulated path — and no repair ladder, RTO schedule, FEC scheme or congestion response of the composing transport is exercised. The mechanism (a refusal is recovered one round trip later) is measured; the transport's own recovery time is not.
socket-stall@shape=consumer-stall = the stall is a reader that stops reading, which is the shape a scheduling stall or a busy consumer produces; it is not a network outage, and the arrival process at the receiver is loopback's, so this arm says nothing about reordering or an arrival pattern compressed by a bottleneck queue.
socket-capacity@host=linux = the Linux depth is derived rather than measured; the derivation is stated with its arithmetic and its two citations in the section above, and every measured depth is an upper bound on it.
socket-offer@lane=multiplexed = the sweep drives one socket, so several flows sharing one receiver is the composing transport's question and not a cell this crate can attribute.
socket-repair@shape=consumer-stall = the refusal is staged by the reader's own stall on the emulated path, so this is a scheduling-stall shape rather than an arrival burst compressed by a bottleneck queue, and it says nothing about reordering.
socket-capacity@stack=rtp = the setters are exposed but nothing in this crate calls them, so no cell here claims that a composed transport sizes its sockets; the default tier's arm claims only that a request reaches the kernel and that the getter reports what the kernel accepted.
socket-floor@metric=syscall-count = macOS offers no unprivileged syscall tracer, so the syscall and copy counts are stated from the code paths and differenced by cost rather than counted per datagram.
```

The `gate-perf-guard-helpers` block is empty: this crate has no `perf`-tier
scenario, so no report-only body can reach an asserting helper.

```gate-perf-guard-helpers
```

## The env-scaled opt-in surface

`TOKIO_UDP_SOAK_CYCLES` is the one surface of this crate that no `#[ignore]`
set can show: whoever invokes the test reads it in-process, and no script here
sets it, so it is invisible to every other block and the declaration below is
its only record. The checker's `gate-env-tier` block enforces it in both
directions — every variable the sources read must be declared, and every
declared variable must be read — and refuses the `-` no-runner marker for a
variable a script does set (`netem-tools check-gate`). The row's
`total` is **derived** from the surface's own variable
(`TOKIO_UDP_SOAK_CYCLES*32`, the per-cycle datagram count being the fixed
`SENDERS*PER_SENDER`) rather than restated beside it; its `wall` is
**measured** — this crate's own best-of-five wall clock of the test binary at
the default 300 cycles (0.27, 0.32, 0.31, 0.25, 0.28 s), the same 0.25 s the
`standard` perf row above declares — and its `bound` is the rule-of-three
detection limit that cycle count buys, stated in full under "Detection limit".

```gate-env-tier
soak-cycle-scale = TOKIO_UDP_SOAK_CYCLES | - | the opt-in cancellation soak's cycle count, read in-process by whoever invokes the test and set by no script of this crate, defaulting to 300 when unset: it sizes how many times the soak replays one fixed 32-datagram schedule family on a socket pair bound once and reused, so aggregate exposure is bought with cycles and never with per-cycle size; the arm it scales is asserting and not report-only — every cycle asserts each of its 32 datagrams is committed exactly once, that none is left queued after the accounting closes, and that no cycle failed to complete within its 2 s hang bound, with a 250 ms overrun counted and printed as a host-latency late and never as a catch — and its tier is the harness's `standard` for an asserting opt-in (`tests/cancellation.rs:675`), whose `#[ignore]` reason names the default tier's bounded form rather than this tier name | cancellation-conservation@readiness=armed+concurrency=concurrent+conservation=bulk+sustained=repeated, cancellation-conservation-rate@metric=rule-of-three+unit=cycle | TOKIO_UDP_SOAK_CYCLES=300,total=TOKIO_UDP_SOAK_CYCLES*32,wall=0.25s,bound=1.0e-2/cycle
```

## Detection limit

A zero-hit soak run of `N` cycles at 32 datagrams excludes a per-cycle
datagram-loss rate above ~3/N at 95 % (the rule of three) — under 1 % per cycle
at the default 300 cycles, and the denominator that matters is larger than the
cycle count: each cycle exposes **3 parked receive futures plus every
opportunistic cancellation its readers take**, measured and printed at the end
of every run. Across the recorded 300-cycle runs the printed denominators are
~2 500–3 500 parked drops, ~100–160 timed-out receives, ~1 800–3 000 `try_recv`
and ~2 000–3 800 `readable` observations. The cycles share one socket, one
runtime and one host and replay one schedule family, so they are replications
rather than independent draws: zero hits support an order-of-magnitude
exclusion for this schedule, not a rate. Host-capacity failures are not
catches — the soak binds no socket after the first three, and a lost datagram
always shows up as a missing tag in the accounting rather than as an error.
