//! What the raw UDP receive buffer costs at the rates this product offers.
//!
//! The deployed client reports a **190 ms minimum** round trip with maxima of
//! **1063 ms and 3205 ms**, where the harness's clean arm reports tens of
//! milliseconds. A received-datagram buffer cannot produce a *floor* — a buffer
//! never delays a datagram it accepts, it can only refuse one — so an earlier
//! arm closed that question and left the buffers at the kernel defaults. What
//! that left open is the other half: the default buffer is a **loss ceiling**,
//! and a reliable transport pays a refusal back as a retransmission, which over
//! a 190 ms path is the shape of the field's maxima.
//!
//! The arms here answer three questions with numbers:
//!
//! 1. **At the rate the product offers, does the default buffer drop anything?**
//!    `a_live_reader_drains_every_rate_the_product_offers` offers a known rate
//!    for a known window with the reader draining from the first millisecond and
//!    reports offered / kept / refused, at the interactive lane's own cadences
//!    (256 B / 5 ms ≈ 51 KB/s, 256 B / 25 ms ≈ 10 KB/s) and at the bulk lane's
//!    (1 MiB/s, the rate the harness's M3 arm offers).
//! 2. **Where is the cliff, and what is it a function of?** A buffer is filled by
//!    the offered rate and emptied by the reader, so an overflow is a **rate ×
//!    stall** product and the cliff is a *stall duration per lane*, never a bare
//!    rate. `the_receive_capacity_in_datagrams_at_the_product_sizes` measures the
//!    depth a stall has to outlast; `a_reader_stall_is_what_overflows_the_receive_buffer`
//!    stalls the reader for a known duration at a known rate and reports the
//!    refusals, including for an explicit `linux-default` budget.
//! 3. **What does a refusal cost?** `a_refused_datagram_costs_a_path_round_trip`
//!    runs a bounded transfer over a **userspace-delayed relay** with a known
//!    190 ms round trip: the sender retransmits anything unacknowledged after one
//!    round trip, the receive buffer is the only place a datagram can be lost,
//!    and the same load is replayed against a buffer sized to the path's
//!    bandwidth-delay product.
//!
//! # The `linux-default` row, and why every row names its budget
//!
//! This host is macOS: an unsized socket gets `net.inet.udp.recvspace` =
//! **786 896 B**, and a request is capped by `kern.ipc.maxsockbuf` = 8 MiB. The
//! deployed artifact is linux-musl, where an unsized receive queue comes from
//! `net.core.rmem_default` — `_SK_MEM_OVERHEAD * 256` with
//! `_SK_MEM_OVERHEAD = SKB_TRUESIZE(256)` (`include/net/sock.h:3056-3059`),
//! **212 992 B** on x86_64 — and where a refused datagram is charged
//! `UDP_MIB_RCVBUFERRORS` (`net/ipv4/udp.c:2313-2316`). The Linux budget is
//! smaller in bytes *and* accounted in `skb->truesize`, so this host's own
//! default is the **weaker** instrument: every row therefore carries an explicit
//! `Some(212_992)` arm standing in for the deployed default. What that arm cannot
//! reproduce is the truesize accounting itself, so the Linux depth measured here
//! is an **upper bound** on Linux's.
//!
//! # What these arms cannot measure
//!
//! * **Loopback is not a path.** No jitter, no reordering, no independent loss;
//!   the relay in the third arm emulates a *delay* and nothing else, so it
//!   cannot show what a real path's queueing does to the arrival process.
//! * **`rtp` was off limits.** This crate sits below it, so no repair ladder, RTO
//!   schedule, FEC scheme or congestion response is exercised. The third arm
//!   models the *mechanism* — a refusal is recovered one round trip later — with
//!   its own toy retransmitter, and measures nothing about rtp's recovery.
//!
//! Each arm prints its own rows and asserts its own sanity, so an arm that
//! measured nothing cannot be read as a clean arm. They are opt-in (`#[ignore]`,
//! the harness's `standard` tier): a rate sweep is seconds of wall clock and the
//! crate's `default` tier budget is one second.

use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio_udp::UdpSocket;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// The receive-buffer budget an unsized Linux socket gets, in bytes:
/// `_SK_MEM_OVERHEAD * 256` with `_SK_MEM_OVERHEAD = SKB_TRUESIZE(256)`
/// (`include/net/sock.h:3056-3059`), which is 212 992 on x86_64.
const LINUX_DEFAULT_RCVBUF: usize = 212_992;

/// A receive buffer that holds a whole bandwidth-delay product at the field's
/// own 190 ms round trip and the bulk lane's 1 MiB/s — 199 KB — with room to
/// spare, plus room for a burst arriving at several times that rate.
const PATH_BDP_RCVBUF: usize = 4 << 20;

/// How the receiver's reader behaves while the offer runs.
#[derive(Clone, Copy, Debug)]
enum Drain {
    /// Draining from the first millisecond: the shape of a live consumer.
    Live,
    /// Not reading until this long after the offer starts: a scheduling stall,
    /// which is what lets the queue accumulate at all.
    After(Duration),
    /// Never reading during the offer; the queue is drained afterwards. This is
    /// the pure capacity probe.
    Never,
}

/// One measurement: what was offered, what the kernel kept, and the receive
/// buffer the socket was actually running with.
#[derive(Debug, Clone, Copy)]
struct Offered {
    offered: usize,
    kept: usize,
    recv_buf: usize,
}

impl Offered {
    fn lost(&self) -> usize {
        self.offered - self.kept
    }
}

/// Offer `size`-byte datagrams at `rate_bps` for `window`, paced on a 1 ms tick
/// so a rate below one datagram per tick is still honoured, and count what the
/// kernel keeps. `rate_bps == 0.0` offers as fast as the sender can.
async fn measure(
    size: usize,
    rate_bps: f64,
    window: Duration,
    drain: Drain,
    rcvbuf: Option<usize>,
) -> Offered {
    let rx = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
    if let Some(bytes) = rcvbuf {
        rx.async_fd().get_ref().set_recv_buffer_size(bytes).unwrap();
    }
    let recv_buf = rx.async_fd().get_ref().recv_buffer_size().unwrap();
    let rx_addr = rx.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let kept = Arc::new(AtomicUsize::new(0));
    let reader = match drain {
        Drain::Never => None,
        Drain::Live | Drain::After(_) => {
            let stall = match drain {
                Drain::After(d) => d,
                _ => Duration::ZERO,
            };
            Some(tokio::spawn({
                let rx = Arc::clone(&rx);
                let stop = Arc::clone(&stop);
                let kept = Arc::clone(&kept);
                async move {
                    if !stall.is_zero() {
                        tokio::time::sleep(stall).await;
                    }
                    let mut buf = [0u8; 2048];
                    while !stop.load(Ordering::Relaxed) {
                        // A 1 ms idle tick bounds the spin on an empty queue;
                        // with datagrams available the loop drains at full speed.
                        match tokio::time::timeout(Duration::from_millis(1), rx.recv_from(&mut buf))
                            .await
                        {
                            Ok(Ok((n, _))) => {
                                assert_eq!(n, size, "a survivor was truncated");
                                kept.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(Err(_)) => break,
                            Err(_) => {}
                        }
                    }
                }
            }))
        }
    };

    let tx = UdpSocket::bind(loopback()).await.unwrap();
    let payload = vec![0x5Au8; size];
    let offered = if rate_bps == 0.0 {
        let start = Instant::now();
        let mut sent = 0usize;
        while start.elapsed() < window {
            match tx
                .send_to_vectored(&[IoSlice::new(&payload)], &rx_addr)
                .await
            {
                Ok(_) => sent += 1,
                // A UDP send buffer that cannot be filled on loopback (documented
                // in this crate's backend): stop rather than spin, and report the
                // offer actually made.
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("the burst failed: {e}"),
            }
        }
        sent
    } else {
        let start = Instant::now();
        let mut sent = 0usize;
        loop {
            if start.elapsed() >= window {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            let ideal = (rate_bps * start.elapsed().as_secs_f64() / size as f64).floor() as usize;
            while sent < ideal {
                match tx
                    .send_to_vectored(&[IoSlice::new(&payload)], &rx_addr)
                    .await
                {
                    Ok(_) => sent += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("the offer failed: {e}"),
                }
            }
        }
        sent
    };

    // Settle: let the last datagrams reach the queue and the drain loop notice
    // them, so `kept` is "what the kernel kept" and not "what the reader raced".
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.store(true, Ordering::Relaxed);
    if let Some(reader) = reader {
        reader.await.unwrap();
    } else {
        let mut buf = [0u8; 2048];
        let mut drained = 0usize;
        loop {
            match rx.try_recv(&mut buf) {
                Ok(n) => {
                    assert_eq!(n, size, "a survivor was truncated");
                    drained += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("the post-offer drain failed: {e}"),
            }
            assert!(drained < 1_000_000, "the drain did not terminate");
        }
        kept.store(drained, Ordering::Relaxed);
    }

    let kept = kept.load(Ordering::Relaxed);
    assert!(offered > 0, "an arm that offered nothing measured nothing");
    assert!(
        kept <= offered,
        "conservation: kept {kept} > offered {offered}"
    );
    Offered {
        offered,
        kept,
        recv_buf,
    }
}

/// **Question 1.** A live reader drains every rate the product offers, so the
/// default buffer refuses nothing at any of them.
///
/// The assertion is the product-relevant half: at the interactive lane's two
/// cadences and at the M3 bulk arm's offered rate, a live reader must lose
/// **zero** datagrams. The two higher tiers are reported and not asserted,
/// because above them the limiter becomes this host's drain loop, and conflating
/// the two would make the arm lie about which one moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
#[ignore = "costs ~1 s of paced offer; the `standard` tier, declared in GATE.md"]
async fn a_live_reader_drains_every_rate_the_product_offers() {
    const WINDOW: Duration = Duration::from_millis(300);
    const RATES: [(f64, &str); 5] = [
        (10.0 * 1024.0, "interactive 256B/25ms"),
        (51.2 * 1024.0, "interactive 256B/5ms"),
        (1.0 * 1024.0 * 1024.0, "bulk 1MiB/s (M3)"),
        (8.0 * 1024.0 * 1024.0, "bulk 8MiB/s"),
        (64.0 * 1024.0 * 1024.0, "bulk 64MiB/s"),
    ];
    println!("OFFER_SWEEP window={WINDOW:?} reader=draining-from-t0");
    for size in [256usize, 1200] {
        for (rate, label) in RATES {
            let arm = measure(size, rate, WINDOW, Drain::Live, None).await;
            let achieved = arm.offered as f64 * size as f64 / WINDOW.as_secs_f64();
            println!(
                "OFFER_SWEEP size={size:<5} nominal={rate:>10.0}B/s {label:<24} offered={:<6} \
                 kept={:<6} lost={:<4} achieved={achieved:>11.0}B/s recvbuf={}B",
                arm.offered,
                arm.kept,
                arm.lost(),
                arm.recv_buf,
            );
            assert!(arm.kept <= arm.offered, "conservation");
            if rate <= 1.0 * 1024.0 * 1024.0 {
                assert_eq!(
                    arm.lost(),
                    0,
                    "a live reader lost {} datagram(s) at size {size} rate {rate}B/s: the default \
                     receive buffer ({}B) is refusing datagrams at a rate the product offers",
                    arm.lost(),
                    arm.recv_buf,
                );
            }
        }
    }
}

/// **Question 2a, the depth.** With no reader at all, a burst fills the receive
/// queue until the kernel refuses; the survivors are the queue's capacity in
/// datagrams, at the two product sizes and at four receive-buffer budgets. The
/// `linux-default` row is the budget an unsized Linux socket gets, and is the
/// row that matters for the deployed target.
///
/// The asserted property is the one a sizing decision turns on, and is kept
/// weak enough to be a property rather than a host measurement: the default is
/// **finite** (a 9 000-datagram burst must not all survive) and sizing the
/// buffer buys **depth**. A regression that silently stopped applying the size —
/// the "we set 4 MiB and landed at 213 KiB" failure — turns the second red.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
#[ignore = "costs ~1 s of burst per row; the `standard` tier, declared in GATE.md"]
async fn the_receive_capacity_in_datagrams_at_the_product_sizes() {
    const BURST: Duration = Duration::from_millis(200);
    let rows: [(Option<usize>, &str); 5] = [
        (None, "host-default"),
        (Some(LINUX_DEFAULT_RCVBUF), "linux-default"),
        (Some(1 << 20), "1MiB"),
        (Some(PATH_BDP_RCVBUF), "4MiB (path BDP @1MiB/s,190ms)"),
        (Some(64 << 20), "64MiB (over this host's ceiling)"),
    ];
    println!("CAPACITY burst={BURST:?} reader=none");
    for size in [256usize, 1200] {
        let mut capacities = Vec::new();
        for (want, label) in rows {
            let arm = measure(size, 0.0, BURST, Drain::Never, want).await;
            capacities.push(arm.kept);
            println!(
                "CAPACITY size={size:<5} want={:<11} effective={:<9} offered={:<6} kept={:<6} \
                 lost={:<6} payload_kept={:<9} label={label}",
                want.map_or("none".to_string(), |v| v.to_string()),
                arm.recv_buf,
                arm.offered,
                arm.kept,
                arm.lost(),
                arm.kept * size,
            );
            assert!(
                arm.offered > arm.kept,
                "size {size} {label}: the burst did not exhaust the queue ({}/{} kept), so this row \
                 is not a capacity measurement",
                arm.kept,
                arm.offered
            );
        }
        assert!(
            capacities[0] < 9_000,
            "size {size}: the host default kept {} datagrams of a 9 000-datagram burst",
            capacities[0]
        );
        assert!(
            capacities[3] > capacities[0],
            "size {size}: a 4 MiB receive buffer kept {} datagrams against the default's {} — the \
             size is not reaching the socket",
            capacities[3],
            capacities[0]
        );
        assert!(
            capacities[4] >= capacities[3],
            "size {size}: a request above the ceiling kept {} against 4 MiB's {}",
            capacities[4],
            capacities[3]
        );
    }
}

/// **Question 2b, the cliff.** A reader that stalls for `D` at rate `R` lets
/// `R x D` datagrams accumulate, so the refusal is `max(0, R x D - capacity)` and
/// the cliff is a *stall duration per lane*. The rows walk `D` from below any
/// plausible capacity to the field's own 190 ms floor, at the interactive
/// cadence and at the bulk arm's offered rate, and carry three budgets: this
/// host's default, the `linux-default` budget that stands in for the deployed
/// target, and a 4 MiB path-BDP size as the control.
///
/// The asserted pair is what makes a refusal attributable to the buffer: at the
/// **interactive** cadence the `linux-default` budget loses nothing even across a
/// stall that contains a whole field round trip, while at the **bulk** rate a
/// stall of the field's own floor loses from that budget and loses nothing from
/// the size chosen for the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
#[ignore = "costs ~10 s of paced offer; the `standard` tier, declared in GATE.md"]
async fn a_reader_stall_is_what_overflows_the_receive_buffer() {
    const STALLS: [Duration; 5] = [
        Duration::from_millis(20),
        Duration::from_millis(50),
        Duration::from_millis(190),
        Duration::from_millis(220),
        Duration::from_millis(500),
    ];
    // The interactive cadence the mandate arms offer, and the M3 bulk arm's.
    let lanes: [(usize, f64, &str); 2] = [
        (256, 51.2 * 1024.0, "interactive 256B/5ms"),
        (1200, 1.0 * 1024.0 * 1024.0, "bulk 1MiB/s (M3)"),
    ];
    let budgets: [(Option<usize>, &str); 3] = [
        (None, "host-default"),
        (Some(LINUX_DEFAULT_RCVBUF), "linux-default"),
        (Some(PATH_BDP_RCVBUF), "4MiB"),
    ];
    println!("STALL_CLIFF (offer window = stall + 100ms drain)");
    for (size, rate, label) in lanes {
        for stall in STALLS {
            let window = stall + Duration::from_millis(100);
            let nominal = (rate * stall.as_secs_f64() / size as f64).round() as usize;
            let mut linux_lost = 0usize;
            let mut sized_lost = 0usize;
            let mut linux_offered = 0usize;
            for (want, tag) in budgets {
                let arm = measure(size, rate, window, Drain::After(stall), want).await;
                println!(
                    "STALL_CLIFF {label:<24} stall={:>6.1}ms offered={:<6} in_stall={:<5} kept={:<6} \
                     lost={:<6} recvbuf={:<9} arm={tag}",
                    stall.as_secs_f64() * 1e3,
                    arm.offered,
                    nominal,
                    arm.kept,
                    arm.lost(),
                    arm.recv_buf,
                );
                assert!(arm.kept <= arm.offered, "conservation");
                if tag == "linux-default" {
                    linux_lost = arm.lost();
                    linux_offered = arm.offered;
                }
                if tag == "4MiB" {
                    sized_lost = arm.lost();
                }
            }
            // The control half: a buffer sized past the stall's accumulation
            // must not refuse, so the default's refusals are attributable to the
            // buffer rather than to the host.
            assert_eq!(
                sized_lost,
                0,
                "a 4 MiB buffer lost {sized_lost} datagram(s) at size {size} rate {rate}B/s after a \
                 {}ms stall ({} datagrams accumulate), so this arm is not measuring the buffer",
                stall.as_secs_f64() * 1e3,
                nominal,
            );
            // The product-relevant half: at the interactive cadence, a stall
            // containing a whole field round trip must lose nothing from the
            // deployed budget.
            if size == 256 {
                assert_eq!(
                    linux_lost,
                    0,
                    "the linux-default budget lost {linux_lost} of {linux_offered} datagram(s) at the \
                     interactive cadence after a {}ms stall",
                    stall.as_secs_f64() * 1e3,
                );
            }
        }
    }
}

/// **Question 3.** What a refusal costs.
///
/// A two-way path with a known round trip is staged in userspace: each direction
/// runs through a relay that holds every datagram for one one-way delay, so an
/// RTT of 190 ms — the field's own floor — is emulated. A bounded transfer then
/// runs over it: the sender offers a known amount of data, the reader stalls
/// long enough for the receive queue to accumulate more than a budget can hold,
/// and every datagram the queue refused is detected as a gap and repaired one
/// **round trip** later. The relay drops nothing and loopback loses nothing, so
/// the receive buffer is the only place a datagram can be lost, and the repair's
/// duration is the cost this arm is measuring.
///
/// Three budgets are run: this host's default, the `linux-default` budget that
/// stands in for the deployed unsized socket, and a 4 MiB path-BDP control. A
/// refusal is measured as an **absence** — the offered set minus what arrived —
/// so nothing about the transport's own detection is assumed; the detection
/// latency is a stated constant (`HALF`) applied identically to every arm, so it
/// cancels out of the difference between arms.
///
/// The asserted property is the mechanism: from a budget too small for the
/// path's bandwidth-delay product the transfer needs **repairs** and its last
/// datagram arrives a **round trip** later than the same load against a buffer
/// that holds the BDP, which needs none. What this is not is a measurement of
/// `rtp`: the repair here is a bulk second pass triggered by an out-of-band gap
/// report, with no congestion control, no FEC and no repair ladder, and `rtp`
/// was off limits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
#[ignore = "costs ~2 s of delayed transfer; the `standard` tier, declared in GATE.md"]
async fn a_refused_datagram_costs_a_path_round_trip() {
    // The field's own floor, split evenly across the two directions.
    const RTT: Duration = Duration::from_millis(190);
    const HALF: Duration = Duration::from_millis(95);
    const SIZE: usize = 1200;
    // 4 MiB/s over a 200 ms window is 700 datagrams: the reader's stall begins
    // only after the path's own one-way delay, so ~120 ms of arrivals (503 KB)
    // accumulate against the `linux-default` budget before it wakes, which is
    // twice that budget and an eighth of the 4 MiB control.
    const RATE: f64 = 4.0 * 1024.0 * 1024.0;
    const WINDOW: Duration = Duration::from_millis(200);
    /// How long the reader stalls, measured from the receiver task's start. It
    /// exceeds the forward one-way delay on purpose: the queue only accumulates
    /// what arrives after the offer begins and before the reader wakes, so a
    /// stall shorter than the delay stages nothing and the arm proves nothing.
    const STALL: Duration = Duration::from_millis(215);
    /// How long after the offer ends the gap report is issued. Stated and
    /// identical across arms, so it cancels out of the comparison; it stands in
    /// for a transport's own loss detection, which this crate does not have.
    const DETECT: Duration = HALF;
    const ARRIVE_MARGIN: Duration = Duration::from_millis(30);

    /// One direction of the emulated path: receive on its own socket, forward to
    /// `dst` after `delay`.
    async fn relay(delay: Duration, dst: SocketAddr) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let sock = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
        let addr = sock.local_addr().unwrap();
        let handle = tokio::spawn({
            let sock = Arc::clone(&sock);
            async move {
                let mut buf = [0u8; 2048];
                loop {
                    let (n, _) = match sock.recv_from(&mut buf).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let payload = buf[..n].to_vec();
                    let sock = Arc::clone(&sock);
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = sock.send_to_vectored(&[IoSlice::new(&payload)], &dst).await;
                    });
                }
            }
        });
        (addr, handle)
    }

    /// What the receiver has seen, shared with the arm's own control flow.
    #[derive(Default)]
    struct Seen {
        flags: Vec<bool>,
        count: usize,
        all_at: Option<Instant>,
    }

    #[derive(Debug, Clone, Copy)]
    struct Transfer {
        total: usize,
        arrived_first_pass: usize,
        refused: usize,
        repaired: usize,
        recv_buf: usize,
        /// When the receiver had every sequence, measured from the moment the
        /// offer started. Subtracting the one-way delay gives the cost a refusal
        /// added on top of the path's own latency.
        time_to_all: Duration,
    }

    async fn transfer(rcvbuf: Option<usize>) -> Transfer {
        let receiver = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
        if let Some(bytes) = rcvbuf {
            receiver
                .async_fd()
                .get_ref()
                .set_recv_buffer_size(bytes)
                .unwrap();
        }
        let recv_buf = receiver.async_fd().get_ref().recv_buffer_size().unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        let sender = UdpSocket::bind(loopback()).await.unwrap();
        let sender_addr = sender.local_addr().unwrap();

        let (fwd_addr, fwd) = relay(HALF, receiver_addr).await;
        let (_rev_addr, rev) = relay(HALF, sender_addr).await;
        let _ = sender_addr;

        let total = (RATE * WINDOW.as_secs_f64() / SIZE as f64).round() as usize;
        let start = Instant::now();

        // The receiver: wait out the stall, then record every sequence it sees.
        let seen = Arc::new(std::sync::Mutex::new(Seen {
            flags: vec![false; total + 1],
            count: 0,
            all_at: None,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let rx = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            let seen = Arc::clone(&seen);
            let stop = Arc::clone(&stop);
            async move {
                tokio::time::sleep(STALL).await;
                let mut buf = [0u8; 2048];
                while !stop.load(Ordering::Relaxed) {
                    match tokio::time::timeout(
                        Duration::from_millis(1),
                        receiver.recv_from(&mut buf),
                    )
                    .await
                    {
                        Ok(Ok((n, _))) if n >= 8 => {
                            let seq = u64::from_be_bytes(buf[..8].try_into().unwrap()) as usize;
                            let mut seen = seen.lock().unwrap();
                            if seq >= 1 && seq < seen.flags.len() && !seen.flags[seq] {
                                seen.flags[seq] = true;
                                seen.count += 1;
                                if seen.count == total && seen.all_at.is_none() {
                                    seen.all_at = Some(Instant::now());
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        });

        // The offer: a known amount of data, paced over the window.
        let mut payload = [0x5Au8; SIZE];
        let offer_start = Instant::now();
        let mut sent = 0usize;
        loop {
            if offer_start.elapsed() >= WINDOW {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            let ideal = (RATE * offer_start.elapsed().as_secs_f64() / SIZE as f64).floor() as usize;
            while sent < ideal && sent < total {
                payload[..8].copy_from_slice(&((sent + 1) as u64).to_be_bytes());
                let _ = sender
                    .send_to_vectored(&[IoSlice::new(&payload)], &fwd_addr)
                    .await;
                sent += 1;
            }
        }
        assert_eq!(
            sent, total,
            "the offer was backpressured before it was complete"
        );

        // Detection: what the receiver holds once the whole offer has had time to
        // arrive (the last datagram is sent at `WINDOW` and crosses one one-way
        // delay). `DETECT` stands in for the transport's own loss detection and is
        // applied to every arm, so it cancels out of the comparison between arms.
        let detect_at = start + WINDOW + HALF + DETECT;
        if let Some(wait) = detect_at.checked_duration_since(Instant::now()) {
            tokio::time::sleep(wait).await;
        }
        let arrived_first_pass = seen.lock().unwrap().count;
        let missing: Vec<u64> = {
            let seen = seen.lock().unwrap();
            (1..=total)
                .filter(|seq| !seen.flags[*seq])
                .map(|seq| seq as u64)
                .collect()
        };

        // The repair: one bulk second pass, whose every datagram pays the path's
        // one-way delay again — the whole cost of a refusal.
        for seq in &missing {
            payload[..8].copy_from_slice(&seq.to_be_bytes());
            let _ = sender
                .send_to_vectored(&[IoSlice::new(&payload)], &fwd_addr)
                .await;
        }
        if !missing.is_empty() {
            tokio::time::sleep(2 * HALF + ARRIVE_MARGIN).await;
        } else {
            // Nothing to repair: wait the same wall clock so the two arms are
            // compared over the same window.
            tokio::time::sleep(2 * HALF + ARRIVE_MARGIN).await;
        }
        stop.store(true, Ordering::Relaxed);
        rx.await.unwrap();
        fwd.abort();
        rev.abort();
        let (all_at, awaited_total) = {
            let seen = seen.lock().unwrap();
            (seen.all_at, seen.flags.len() - 1)
        };
        assert_eq!(
            awaited_total, total,
            "the sequence space must match the offer"
        );

        // A repair that did not fill every gap means the arm did not complete, and
        // a completion time read from it would be a lie.
        let completed = {
            let seen = seen.lock().unwrap();
            (1..=total).all(|seq| seen.flags[seq])
        };
        assert!(
            completed,
            "the transfer never completed: {} of {total} sequences arrived even after the repair",
            arrived_first_pass
        );
        Transfer {
            total,
            arrived_first_pass,
            refused: total - arrived_first_pass,
            repaired: missing.len(),
            recv_buf,
            time_to_all: all_at.expect("a completed transfer has an all-at instant") - start,
        }
    }

    let mut arms = Vec::new();
    for (want, tag) in [
        (None, "host-default"),
        (Some(LINUX_DEFAULT_RCVBUF), "linux-default"),
        (Some(PATH_BDP_RCVBUF), "4MiB/path-BDP"),
    ] {
        let arm = transfer(want).await;
        println!(
            "RT_COST arm={tag:<15} recvbuf={:<9}B offered={:<5} arrived={:<5} refused={:<5} \
             repaired={:<5} time_to_all={:>7.0}ms ({:.2} x 190ms RTT, minus one one-way delay = \
             {:.1}ms, minus the {}ms detection constant = {:.1}ms)",
            arm.recv_buf,
            arm.total,
            arm.arrived_first_pass,
            arm.refused,
            arm.repaired,
            arm.time_to_all.as_secs_f64() * 1e3,
            arm.time_to_all.as_secs_f64() / RTT.as_secs_f64(),
            (arm.time_to_all - HALF).as_secs_f64() * 1e3,
            DETECT.as_secs_f64() * 1e3,
            (arm.time_to_all - HALF - DETECT).as_secs_f64() * 1e3,
        );
        // Structural: a transfer that lost nothing to the queue completed as soon
        // as the offer's last datagram crossed one one-way delay; one that lost
        // something cannot have completed before the repair's own forward leg.
        if arm.refused == 0 {
            assert!(
                arm.time_to_all <= WINDOW + 2 * HALF,
                "a transfer with no refusal still needed {:?} to complete, which is longer than the \
                 offer plus the path's one one-way delay: this arm blames the buffer for something \
                 else on the path",
                arm.time_to_all
            );
        } else {
            assert!(
                arm.time_to_all >= WINDOW + DETECT + HALF,
                "a transfer with {} refusals completed in {:?}, before the repair could have crossed \
                 the path",
                arm.refused,
                arm.time_to_all
            );
        }
        arms.push(arm);
    }
    let host = arms[0];
    let linux = arms[1];
    let sized = arms[2];
    println!(
        "RT_COST accumulation={:.0}B = {:.0}B/s x ({}ms stall - {}ms one-way delay); linux-default \
         holds {}B = {:.0} datagrams of {SIZE}B, so a refusal is unavoidable; path_bdp={:.0}B; \
         recovery cost = linux-default time_to_all - control time_to_all = {:.0}ms = {:.2} x RTT",
        RATE * (STALL - HALF).as_secs_f64(),
        RATE,
        STALL.as_secs_f64() * 1e3,
        HALF.as_secs_f64() * 1e3,
        linux.recv_buf,
        linux.recv_buf as f64 / SIZE as f64,
        RATE * RTT.as_secs_f64(),
        (linux.time_to_all - sized.time_to_all).as_secs_f64() * 1e3,
        (linux.time_to_all - sized.time_to_all).as_secs_f64() / RTT.as_secs_f64(),
    );
    // The control: a buffer holding the path's BDP refuses nothing and therefore
    // needs no repair, so the other arms' refusals are the buffer's and not some
    // other loss on the path.
    assert_eq!(
        sized.refused, 0,
        "the 4 MiB control refused {} datagram(s), so this arm's losses are not attributable to the \
         receive buffer",
        sized.refused
    );
    // The mechanism: the deployed undefended default (the `linux-default` budget,
    // and this host's smaller-margin default) refuses datagrams and pays them back
    // a round trip later, so the same load finishes later than against a buffer
    // that holds the path.
    assert!(
        linux.refused > 0,
        "the linux-default budget refused nothing from {:.0}B accumulated against {:.0}B, so this \
         arm staged no refusal for the deployed target",
        RATE * STALL.as_secs_f64(),
        linux.recv_buf as f64
    );
    assert!(
        host.refused <= linux.refused,
        "this host's larger default refused {} against linux-default's {}: a larger buffer cannot \
         refuse more, so the budget the arm set is not the one it measured",
        host.refused,
        linux.refused
    );
    assert!(
        linux.time_to_all >= sized.time_to_all + HALF,
        "the linux-default arm finished in {:?} against the sized control's {:?}: a refusal cost less \
         than the half round trip the repair's forward leg is",
        linux.time_to_all,
        sized.time_to_all
    );
    assert!(
        linux.time_to_all <= sized.time_to_all + RTT + Duration::from_millis(30),
        "the linux-default arm finished {:?} later than the sized control: a refusal is costing more \
         than the round trip the detection constant plus the repair's forward leg are",
        linux.time_to_all - sized.time_to_all
    );
}
