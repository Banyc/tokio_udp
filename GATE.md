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
`gate-manifest` block below; the checker (`netem_test/tools/check-gate.py`,
per-crate mode) re-derives the set from the compiled test binary and fails when
the manifest and reality disagree. Run it from the `netem_test` checkout, so it
finds the shared tooling:

```sh
python3 ../netem_test/tools/check-gate.py \
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
`net.core.rmem_default` (`SKB_TRUESIZE(256) * 256`, `net/core/sock.h:3056-3059`)
and a datagram is dropped with `UDP_MIB_RCVBUFERRORS` once the truesize-accounted
queue exceeds it (`net/core/sock.h:1153-1158`, `net/ipv4/udp.c:2310-2324`), so
the ceiling prices offered *load* — the opposite mechanism to a floor. The
buffer sizes are therefore left to the caller: the value that matters is a path
bandwidth-delay product, which the transport that knows its own send rate owns
and a socket layer does not.

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
```

```gate-default-required
cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced
cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram
cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram
loopback_delay::tokio_udp_adds_no_floor_over_a_plain_std_udp_socket
loopback_delay::the_socket_buffers_are_left_at_the_kernel_defaults
loopback_delay::a_burst_past_the_default_receive_buffer_is_a_loss_ceiling_not_a_floor
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
variable a script does set (`netem_test/tools/check-gate.py`). The row's
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
