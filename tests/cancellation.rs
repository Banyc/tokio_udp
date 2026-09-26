//! Cancellation and readiness-latch tests for the receive path.
//!
//! `tokio`'s readiness is a latch: an interest the driver has observed ready
//! keeps reporting so until a receive observes `WouldBlock` and drops the event
//! it was armed with. The default tier of this file pins both directions of
//! that latch under cancellation, because a stale "readable" claim parks no one
//! (it wastes a poll) while a latch dropped too eagerly strands a datagram that
//! is already queued — the second shape is a multi-second interactive stall.
//!
//! A cancelled receive must be invisible to the kernel: for a datagram socket
//! the syscall for one datagram runs inside a single poll and the future then
//! completes, so a future that has not completed has performed no syscall, and
//! dropping it must leave the datagram neither consumed nor duplicated, leave
//! the next datagram's source address its own, and leave readiness neither stuck
//! claiming a drained socket nor cleared in front of queued data.
//!
//! Three tests run in the default tier (each bounded, each vacuity-checked
//! against the injections recorded in `GATE.md`):
//!
//! * `a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced`
//!   stages the exact cancellation window rather than sampling it, and asserts
//!   both directions of the latch.
//! * `concurrent_cancelled_and_timed_out_receives_conserve_every_datagram`
//!   races three concurrent receive futures in the four forms a cancellation bug
//!   can hide in, released only once every reader has parked so that the
//!   cancellation is exercised on every run rather than whenever the schedule
//!   happens to produce it.
//! * `cancelled_receive_soak_conserves_every_datagram` is the opt-in soak; it is
//!   `#[ignore]`d so the default tier stays cheap, and its tier, cost, coverage
//!   cells and detection limit are declared in `GATE.md`.

use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;

/// The payload of every datagram these tests send: a sender tag, a per-sender
/// sequence number whose uniqueness makes a duplicate detectable, and two
/// constant bytes that make a truncated or reordered datagram detectable.
const DATAGRAM_LEN: usize = 4;

/// Every datagram in a cycle is `[sender, seq, 0x5A, 0xA5]`; a datagram that
/// arrives with a different tail was not sent by this test's sender loop.
const DATAGRAM_TAIL: [u8; 2] = [0x5A, 0xA5];

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn datagram(sender: u8, seq: u8) -> [u8; DATAGRAM_LEN] {
    [sender, seq, DATAGRAM_TAIL[0], DATAGRAM_TAIL[1]]
}

fn decode(buf: &[u8; DATAGRAM_LEN]) -> (u8, u8) {
    assert_eq!(
        buf[2..],
        DATAGRAM_TAIL,
        "a datagram arrived with a foreign tail, so its payload is not one this test sent"
    );
    (buf[0], buf[1])
}

/// Poll `future` exactly once and drop it. `Some` when that single poll
/// completed; `None` when the future was dropped while it was still parked —
/// the cancellation these tests exist to drive, and the only way a receive
/// future reaches a drop without having performed a syscall.
async fn poll_once_dropping<F: std::future::Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    match std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await {
        std::task::Poll::Ready(value) => Some(value),
        std::task::Poll::Pending => None,
    }
}

/// Wait, without a timer, until `counter` reaches `target`. Used to release the
/// senders only after every reader has parked its first receive, which is what
/// makes the cancellation window part of every run instead of a race the
/// schedule wins or loses. The caller bounds this wait.
async fn until(counter: &AtomicUsize, target: usize) {
    while counter.load(Ordering::SeqCst) < target {
        tokio::task::yield_now().await;
    }
}

/// One datagram a receive committed: its sender tag, its per-sender sequence
/// number, and the source address the receive reported — `None` for the
/// synchronous `try_recv`, which has no source-address form.
type Committed = (u8, u8, Option<SocketAddr>);

/// The accounting a run shares across its readers: every datagram committed,
/// whether or not the committing receive knew the source address.
type Accounting = Arc<Mutex<Vec<Committed>>>;

/// How many cancellations of each form a run actually exercised. A zero-hit
/// soak is only as strong as this denominator: it is the number of receive
/// futures that were really dropped while parked (and so really were in the
/// window a lost-datagram bug lives in), not the number of loop iterations.
#[derive(Default)]
struct Progress {
    /// Receive futures dropped after a single poll that returned `Pending`.
    parked_drops: AtomicU64,
    /// Receive futures cancelled by a short `timeout`.
    timed_out: AtomicU64,
    /// Synchronous `try_recv` calls, which can consume a datagram and clear the
    /// cached event without any future existing at all.
    try_recv: AtomicU64,
    /// Bare `readable()` observations, which only observe the latch.
    readable: AtomicU64,
}

/// One concurrent reader. It first parks a receive on the socket **before any
/// sender is released** and drops it mid-poll — the cancellation window, made
/// deterministic by [`until`] in the caller — then cycles through the receive
/// forms a cancellation bug can hide in: a future dropped after a single poll,
/// a future cancelled by a short `timeout`, a `try_recv` that can clear the
/// cached event, and a bare `readable()` await that only observes it. Every
/// datagram it commits is recorded, until `wanted` datagrams are shared across
/// all readers.
///
/// A reader records a datagram only when a poll committed it, so the shared
/// record is an exact accounting: a datagram whose syscall ran but whose future
/// was then dropped appears in no record, and the missing tag is the defect.
async fn cancel_reader(
    server: Arc<UdpSocket>,
    got: Accounting,
    wanted: usize,
    seed: u32,
    probe: Duration,
    progress: Arc<Progress>,
    parked: Arc<AtomicUsize>,
) {
    let mut buf = [0u8; DATAGRAM_LEN];
    assert!(
        poll_once_dropping(server.recv_from(&mut buf))
            .await
            .is_none(),
        "the socket must be empty before the senders are released, so every reader's first \
         receive parks and is then dropped mid-poll"
    );
    progress.parked_drops.fetch_add(1, Ordering::Relaxed);
    parked.fetch_add(1, Ordering::SeqCst);

    let mut step = seed;
    loop {
        if got.lock().unwrap().len() >= wanted {
            return;
        }
        let committed = match step % 4 {
            0 => match poll_once_dropping(server.recv_from(&mut buf)).await {
                Some(result) => {
                    let (n, src) = result.unwrap();
                    assert_eq!(n, DATAGRAM_LEN, "a receive returned a partial datagram");
                    let (sender, seq) = decode(&buf);
                    Some((sender, seq, Some(src)))
                }
                None => {
                    progress.parked_drops.fetch_add(1, Ordering::Relaxed);
                    None
                }
            },
            1 => match tokio::time::timeout(probe, server.recv_from(&mut buf)).await {
                Ok(result) => {
                    let (n, src) = result.unwrap();
                    assert_eq!(n, DATAGRAM_LEN, "a receive returned a partial datagram");
                    let (sender, seq) = decode(&buf);
                    Some((sender, seq, Some(src)))
                }
                Err(_elapsed) => {
                    progress.timed_out.fetch_add(1, Ordering::Relaxed);
                    None
                }
            },
            2 => match server.try_recv(&mut buf) {
                Ok(n) => {
                    progress.try_recv.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(n, DATAGRAM_LEN, "a receive returned a partial datagram");
                    let (sender, seq) = decode(&buf);
                    Some((sender, seq, None))
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                Err(e) => panic!("try_recv: {e}"),
            },
            _ => {
                let _ = tokio::time::timeout(probe, server.readable()).await;
                progress.readable.fetch_add(1, Ordering::Relaxed);
                None
            }
        };
        if let Some(datum) = committed {
            got.lock().unwrap().push(datum);
        }
        step = step.wrapping_add(1);
        tokio::task::yield_now().await;
    }
}

/// Verify an accounting collected by [`cancel_reader`]: every expected datagram
/// arrived exactly once, and every datagram whose source address was observable
/// came from its own sender. Returns the tags that never arrived (empty on
/// success) so a caller can report a loss rather than only assert it.
fn conserve(
    got: &[Committed],
    senders: u8,
    per_sender: u8,
    sender_addrs: &[SocketAddr],
) -> Vec<(u8, u8)> {
    let mut seen = std::collections::BTreeMap::new();
    for (sender, seq, src) in got {
        assert!(
            seen.insert((*sender, *seq), *src).is_none(),
            "datagram {sender}:{seq} was delivered twice, so a receive committed one datagram to two callers"
        );
        if let Some(src) = src {
            assert_eq!(
                *src, sender_addrs[*sender as usize],
                "datagram {sender}:{seq} reported a source address that is not its own sender"
            );
        }
    }
    (0..senders)
        .flat_map(|sender| (0..per_sender).map(move |seq| (sender, seq)))
        .filter(|tag| !seen.contains_key(tag))
        .collect()
}

/// A receive future that is dropped while parked must leave a datagram arriving
/// afterwards exactly where it was: the drop consumes nothing from the kernel,
/// and it must not consume the readiness event the arrival published either.
///
/// The window is *staged*, not sampled, because in production it is a handful of
/// instructions wide. The receive is polled once against an empty socket, so it
/// is parked with its interest registered and has performed no syscall; the peer
/// then puts the datagram on the wire; the future is dropped **without a second
/// poll**. The datagram must still be announced (`readable` returns instead of
/// parking behind a consumed event) and still be deliverable (`recv_from`
/// returns it, from its own sender).
///
/// The drain at the end of every round is the other direction of the same latch,
/// and it is race-free *because of* the order above: awaiting `readable` is what
/// proves the driver's event for that arrival has landed, so by the time the
/// datagram has been consumed and `try_recv` reports `WouldBlock` there is no
/// publish still in flight for the clear to lose to. A latch left claimed by an
/// empty socket would make every later reader poll through a spurious wakeup;
/// the final assertion is that it was dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced() {
    const ROUNDS: u8 = 8;
    const BOUND: Duration = Duration::from_secs(5);
    /// Long enough that a genuinely parked `readable` cannot be woken inside it
    /// on a busy host, short enough not to dominate the default tier's cost. A
    /// latch left claimed by the drain below answers in microseconds, so this
    /// bound is not what decides that arm.
    const NOT_READABLE: Duration = Duration::from_millis(150);

    let server = UdpSocket::bind(loopback()).await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let peer = UdpSocket::bind(loopback()).await.unwrap();
    let peer_addr = peer.local_addr().unwrap();

    let mut buf = [0u8; DATAGRAM_LEN];
    for round in 0..ROUNDS {
        let payload = datagram(0, round);
        assert!(
            poll_once_dropping(server.recv_from(&mut buf))
                .await
                .is_none(),
            "round {round}: the socket must start empty, so the receive parks in this poll"
        );
        // The datagram arrives after the receive registered its interest and
        // before the future is dropped: the cancellation window.
        peer.send_to_vectored(&[IoSlice::new(&payload)], &server_addr)
            .await
            .unwrap();
        tokio::time::timeout(BOUND, server.readable())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "HANG: round {round}: the arrival's readiness event was consumed by the dropped \
                     receive, so a reader parks in front of a datagram that is already queued"
                )
            })
            .unwrap();
        let (n, src) = tokio::time::timeout(BOUND, server.recv_from(&mut buf))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "HANG: round {round}: the datagram was consumed by the receive future that was \
                     dropped while parked, so a correct reader never sees it"
                )
            })
            .unwrap();
        assert_eq!(
            &buf[..n],
            &payload,
            "round {round}: the late datagram must arrive whole and unmodified"
        );
        assert_eq!(
            src, peer_addr,
            "round {round}: the next datagram's source must be its own sender"
        );
        assert!(
            matches!(
                server.try_recv(&mut buf),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
            ),
            "round {round}: the dropped receive must not have duplicated or requeued its datagram"
        );
    }
    assert!(
        tokio::time::timeout(NOT_READABLE, server.readable())
            .await
            .is_err(),
        "the queue was drained, so the cached readiness event must have been dropped: a latch that \
         keeps claiming readable with an empty socket makes every later reader spin or return a \
         spurious wakeup"
    );
}

/// The datagram-conservation law under cancellation, with several receive
/// futures parked on one socket at once. Four senders put 40 uniquely tagged
/// datagrams on the wire while three concurrent readers cycle through a
/// single-poll drop, a short-timeout drop, `try_recv` and `readable()`. The
/// senders are released only after all three readers have parked and dropped a
/// receive on the empty socket, so every run exercises the cancellation window
/// rather than only the runs whose schedule happens to produce it.
///
/// The law is a property a correct implementation cannot violate: every
/// datagram sent is committed to exactly one caller, from its own sender. The
/// run is bounded, and a reader that never reaches the committed count is
/// reported as a **HANG** naming the count and the bound — a cancelled receive
/// that consumed a datagram without committing it would park every reader
/// forever on a socket the accounting says still owes datagrams, which is
/// distinct from a read that merely completes late. Vacuity: with the syscall
/// and the commit split across an await point (injection B in `GATE.md`) the
/// dropped futures consume datagrams and this run hangs.
///
/// The latch direction is asserted by
/// `a_receive_dropped_while_parked_leaves_the_late_datagram_queued_and_announced`
/// rather than here: after readers that consume opportunistically, a driver
/// event for a datagram one of them already took can still land, which leaves a
/// spurious `readable` on a drained socket — a wasted wakeup, not a stall — and
/// asserting it in this shape would be a race with the driver rather than a
/// property of the crate. What replaces it here is the direction that *is*
/// airtight: a fresh datagram arriving after the drain must still be announced
/// and delivered inside the bound, so the clear that `WouldBlock` performed
/// cannot have taken a later arrival's wakeup with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn concurrent_cancelled_and_timed_out_receives_conserve_every_datagram() {
    const SENDERS: u8 = 4;
    const PER_SENDER: u8 = 10;
    const TOTAL: usize = SENDERS as usize * PER_SENDER as usize;
    const READERS: usize = 3;
    const PROBE: Duration = Duration::from_millis(1);
    const BOUND: Duration = Duration::from_secs(5);

    let server = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
    let server_addr = server.local_addr().unwrap();

    let mut sender_addrs = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..SENDERS {
        let socket = UdpSocket::bind(loopback()).await.unwrap();
        sender_addrs.push(socket.local_addr().unwrap());
        peers.push(Arc::new(socket));
    }

    let got: Accounting = Arc::new(Mutex::new(Vec::new()));
    let progress = Arc::new(Progress::default());
    let parked = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for seed in 0..READERS as u32 {
        tasks.spawn(cancel_reader(
            Arc::clone(&server),
            Arc::clone(&got),
            TOTAL,
            seed,
            PROBE,
            Arc::clone(&progress),
            Arc::clone(&parked),
        ));
    }
    // Release the senders only once every reader is parked in a receive it has
    // already been dropped out of; otherwise all 40 datagrams can be queued
    // before the first cancellation and the arm under test never runs.
    tokio::time::timeout(BOUND, until(&parked, READERS))
        .await
        .unwrap_or_else(|_| panic!("HANG: a reader never parked a receive on the empty socket"));
    for (sender, peer) in peers.iter().enumerate() {
        let peer = Arc::clone(peer);
        tasks.spawn(async move {
            for seq in 0..PER_SENDER {
                let payload = datagram(sender as u8, seq);
                peer.send_to_vectored(&[IoSlice::new(&payload)], &server_addr)
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
    }

    if tokio::time::timeout(BOUND, tasks.join_all()).await.is_err() {
        let seen = got.lock().unwrap().len();
        panic!(
            "HANG: {READERS} concurrent receives never delivered {TOTAL} datagrams within {BOUND:?} \
             (committed {seen}); a cancelled or timed-out receive consumed a datagram without \
             committing it, so every reader is parked on a socket the accounting says still owes one"
        );
    }

    let collected = got.lock().unwrap().clone();
    let missing = conserve(&collected, SENDERS, PER_SENDER, &sender_addrs);
    assert!(
        missing.is_empty(),
        "{} datagrams were sent but never committed (lost by a cancelled or timed-out receive): \
         {missing:?}",
        missing.len()
    );
    assert_eq!(
        parked.load(Ordering::SeqCst),
        READERS,
        "each reader must have dropped one parked receive before the senders were released; if none \
         did, this run never exercised cancellation"
    );
    assert!(
        matches!(
            server.try_recv(&mut [0u8; DATAGRAM_LEN]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
        ),
        "accounting says the queue is drained, so a datagram must not still be waiting"
    );

    // Liveness across the clear: the `WouldBlock` just observed dropped the
    // cached event, and that must not have taken a later arrival's wakeup with
    // it. A lost wakeup leaves this read parked in front of a datagram that is
    // already queued.
    let probe = [0xF0, 0x0D, 0xBE, 0xEF];
    peers[0]
        .send_to_vectored(&[IoSlice::new(&probe)], &server_addr)
        .await
        .unwrap();
    let mut buf = [0u8; DATAGRAM_LEN];
    let (n, src) = tokio::time::timeout(BOUND, server.recv_from(&mut buf))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "HANG: a datagram that arrived after the drain was never delivered, so the clear \
                 that observing `WouldBlock` performed consumed a later arrival's wakeup"
            )
        })
        .unwrap();
    assert_eq!(&buf[..n], &probe);
    assert_eq!(
        src, sender_addrs[0],
        "the probe's source must be its own sender"
    );
}

/// The opt-in soak: the default tier's conservation law run for many cycles,
/// with several concurrent receive futures parked on one socket, dropped
/// mid-poll and raced against short timeouts, `try_recv` and `readable()`,
/// interleaved with senders on other sockets and with the `try_recv`
/// `WouldBlock` that drops the cached readiness event.
///
/// Run it with the cycle count as the only knob:
///
/// ```text
/// CARGO_TARGET_DIR=/Users/charliesmith/code/tmp/it48_tokio_udp_target \
///   cargo test --release -p tokio_udp --locked --offline \
///   --test cancellation -- --ignored --nocapture
/// TOKIO_UDP_SOAK_CYCLES=5000 …   # default 300
/// ```
///
/// Each cycle's sockets are bound once for the whole soak and reused: a fresh
/// pair per cycle would be tens of thousands of binds, and this host refuses a
/// bind with `EADDRNOTAVAIL` under that churn — an artifact that would be
/// indistinguishable from the loss under test. So no cycle can lose a datagram
/// to a refused bind, and the loss counter only moves when a receive failed to
/// commit one. The per-cycle schedule is fresh (new futures, new datagrams) but
/// the socket, the runtime and the host are shared, so the cycles are
/// replications of one schedule family rather than independent draws; the
/// detection limit that buys is stated in `GATE.md`.
///
/// Every cycle is bounded. A cycle that never completes is a **hang** (counted,
/// reported and fatal); a cycle that completes but takes longer than `LATE` is
/// counted and reported as **late**, which is a host-latency observation rather
/// than a correctness one and is never reported as a catch.
#[ignore = "opt-in soak; the default tier runs the bounded form — see GATE.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn cancelled_receive_soak_conserves_every_datagram() {
    const SENDERS: u8 = 2;
    const PER_SENDER: u8 = 16;
    const TOTAL: usize = SENDERS as usize * PER_SENDER as usize;
    const READERS: usize = 3;
    const READERS_SEEDED: u32 = READERS as u32;
    const CYCLE_HANG: Duration = Duration::from_secs(2);
    const LATE: Duration = Duration::from_millis(250);
    const PROBE: Duration = Duration::from_millis(1);

    let cycles: u64 = match std::env::var("TOKIO_UDP_SOAK_CYCLES") {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("TOKIO_UDP_SOAK_CYCLES={value:?} is not a cycle count")),
        Err(_) => 300,
    };

    // Bound once and reused for every cycle (see the doc comment).
    let server = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
    let server_addr = server.local_addr().unwrap();
    let mut peers = Vec::new();
    let mut sender_addrs = Vec::new();
    for _ in 0..SENDERS {
        let socket = UdpSocket::bind(loopback()).await.unwrap();
        sender_addrs.push(socket.local_addr().unwrap());
        peers.push(Arc::new(socket));
    }

    let progress = Arc::new(Progress::default());
    let mut hangs = 0u64;
    let mut lates = 0u64;
    let mut worst_cycle = Duration::ZERO;
    let mut committed_total = 0usize;

    for cycle in 0..cycles {
        let got: Accounting = Arc::new(Mutex::new(Vec::new()));
        let parked = Arc::new(AtomicUsize::new(0));
        let mut tasks = JoinSet::new();
        for seed in 0..READERS_SEEDED {
            tasks.spawn(cancel_reader(
                Arc::clone(&server),
                Arc::clone(&got),
                TOTAL,
                seed,
                PROBE,
                Arc::clone(&progress),
                Arc::clone(&parked),
            ));
        }

        let started = Instant::now();
        if tokio::time::timeout(CYCLE_HANG, until(&parked, READERS))
            .await
            .is_err()
        {
            hangs += 1;
            println!(
                "HANG: cycle {cycle}: only {} of {READERS} readers parked a receive on the empty \
                 socket within {CYCLE_HANG:?}",
                parked.load(Ordering::SeqCst)
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            continue;
        }
        for (sender, peer) in peers.iter().enumerate() {
            let peer = Arc::clone(peer);
            tasks.spawn(async move {
                for seq in 0..PER_SENDER {
                    let payload = datagram(sender as u8, seq);
                    peer.send_to_vectored(&[IoSlice::new(&payload)], &server_addr)
                        .await
                        .unwrap();
                    tokio::task::yield_now().await;
                }
            });
        }

        let outcome = tokio::time::timeout(CYCLE_HANG, tasks.join_all()).await;
        let elapsed = started.elapsed();
        worst_cycle = worst_cycle.max(elapsed);

        let collected = got.lock().unwrap().clone();
        // The queue must be empty between cycles whatever the outcome was: a
        // datagram left over is one this cycle's accounting never saw.
        let leftover = match server.try_recv(&mut [0u8; DATAGRAM_LEN]) {
            Ok(_) => true,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
            Err(e) => panic!("cycle {cycle}: try_recv: {e}"),
        };
        let missing = conserve(&collected, SENDERS, PER_SENDER, &sender_addrs);
        if outcome.is_err() {
            hangs += 1;
            println!(
                "HANG: cycle {cycle} did not complete within {CYCLE_HANG:?} (committed {}/{} \
                 datagrams, {} never committed, leftover {leftover})",
                collected.len(),
                TOTAL,
                missing.len()
            );
            continue;
        }
        assert!(
            missing.is_empty(),
            "cycle {cycle}: {} datagrams were sent but never committed (lost by a cancelled or \
             timed-out receive): {missing:?}",
            missing.len()
        );
        assert!(
            !leftover,
            "cycle {cycle}: a datagram was left queued after the accounting closed, so one was \
             neither committed nor lost by a followable path"
        );
        committed_total += collected.len();
        if elapsed > LATE {
            lates += 1;
        }
    }

    // Liveness across the clear, on the idle socket: the last `WouldBlock`
    // dropped the cached event, and a fresh arrival must still be announced and
    // delivered. (The opposite direction — a latch stuck claiming readable —
    // is asserted by the parked-drop test, which is sequential and therefore
    // race-free; see its doc comment for why this shape cannot assert it.)
    let probe = [0xF0, 0x0D, 0xBE, 0xEF];
    peers[0]
        .send_to_vectored(&[IoSlice::new(&probe)], &server_addr)
        .await
        .unwrap();
    let mut buf = [0u8; DATAGRAM_LEN];
    let mut arrived = false;
    for _ in 0..3 {
        if let Ok(Ok((n, _src))) = tokio::time::timeout(LATE, server.recv_from(&mut buf)).await {
            assert_eq!(&buf[..n], &probe);
            arrived = true;
            break;
        }
    }
    assert!(
        arrived,
        "HANG: a datagram that arrived after the final drain was never delivered, so a clear \
         consumed a later arrival's wakeup"
    );

    let progress = &progress;
    println!(
        "tokio_udp cancellation soak: {cycles} cycles, {committed_total} datagrams conserved, {} \
         parked drops, {} timed-out receives, {} try_recv, {} readable observations, {lates} late \
         cycles, worst cycle {worst_cycle:?}, {hangs} hangs",
        progress.parked_drops.load(Ordering::Relaxed),
        progress.timed_out.load(Ordering::Relaxed),
        progress.try_recv.load(Ordering::Relaxed),
        progress.readable.load(Ordering::Relaxed),
    );
    assert_eq!(
        hangs, 0,
        "{hangs} of {cycles} cycles never completed within {CYCLE_HANG:?}: a cancelled or timed-out \
         receive consumed a datagram without committing it"
    );
}
