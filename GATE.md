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
```

The asserting set is every `standard`/`full` scenario plus every required
entry; the `standard` soak asserts (that is what its injections check), so it
is asserting and not report-only.

```gate-asserting
cancellation::a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced
cancellation::a_receive_dropped_mid_poll_has_consumed_no_datagram
cancellation::concurrent_cancelled_and_timed_out_receives_conserve_every_datagram
cancellation::cancelled_receive_soak_conserves_every_datagram
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
```

The declared sums are `default` 0.06 s and `standard` 0.25 s (measured 0.03 s
and 0.24 s at 300 cycles). The default tier of the *crate* grows by the ~0.03 s
this target costs on top of an unchanged 0.58 s lib tier; the parked test's
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
```

The `gate-perf-guard-helpers` block is empty: this crate has no `perf`-tier
scenario, so no report-only body can reach an asserting helper.

```gate-perf-guard-helpers
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
