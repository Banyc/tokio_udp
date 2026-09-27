//! Loopback measurement of the per-datagram delay this socket layer adds.
//!
//! The product's field report is a **190 ms minimum** round trip on a deployed
//! build while the clean harness arm reports tens of milliseconds. A *floor* is
//! a systematic cost paid by essentially every datagram, not a tail, and this
//! socket layer is the lowest thing under `udp_listener`/`rtp` that could pay
//! one: an unsized kernel buffer, a per-datagram allocation, a readiness wait
//! that only fires on a timer, or an extra copy.
//!
//! Three properties are measured here, and each prints its own numbers:
//!
//! * **The floor.** A one-datagram-in-flight ping-pong on loopback has no queue
//!   to wait in, so its round trip *is* the per-datagram cost of the path. It is
//!   compared against a plain `std::net::UdpSocket` doing the same thing, so an
//!   added constant shows up as a difference rather than as a bare number. A
//!   layer that added 190 ms would be unmissable; the assertion is a tripwire at
//!   a few milliseconds, three orders of magnitude below the field floor, so it
//!   fails loudly if such a cost is ever introduced.
//! * **The socket options this crate does not set.** The Unix backend calls
//!   `socket(2)` and `bind(2)` and sizes *no* buffer, so both directions run at
//!   whatever the kernel defaults are. That is a load-dependent loss risk, not a
//!   floor, and the burst arm states which with numbers instead of an argument.
//! * **The scale of an extra copy.** The concatenating vectored fallback is the
//!   only path in this stack that copies a datagram a third time; its measured
//!   round trip is what that copy is worth here.
//!
//! Syscall and copy counts are properties of the code paths, stated rather than
//! counted by the kernel (macOS offers no unprivileged tracer):
//!
//! | path | syscalls/datagram | user<->kernel copies/datagram |
//! | --- | --- | --- |
//! | `tokio_udp::UdpSocket::send` / `recv*` | 1 `sendmsg` + 1 `recvmsg` | 2 |
//! | `std::net::UdpSocket::send_to` / `recv_from` | 1 `sendto` + 1 `recvfrom` | 2 |
//! | `tokio::net::UdpSocket` multi-buffer `send_to_vectored` | 1 `sendto` (concatenated) | 3 |

use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_udp::UdpSocket;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(
        !sorted.is_empty(),
        "no samples: the percentile is undefined"
    );
    assert!(
        (0.0..=1.0).contains(&p),
        "a percentile outside 0..=1 is a caller error"
    );
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx]
}

/// A round-trip distribution. Constructing one from zero samples panics, so an
/// arm that measured nothing cannot be read as a fast arm.
#[derive(Debug)]
struct Dist {
    samples: usize,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl Dist {
    fn from(mut values: Vec<Duration>) -> Self {
        assert!(
            !values.is_empty(),
            "an arm that collected no samples has measured nothing"
        );
        values.sort();
        Self {
            samples: values.len(),
            p50: percentile(&values, 0.50),
            p99: percentile(&values, 0.99),
            max: *values.last().expect("non-empty"),
        }
    }

    fn line(&self, label: &str) -> String {
        format!(
            "DELAY_ARM {label:<26} n={:<7} p50={:>8.3}ms p99={:>8.3}ms max={:>8.3}ms",
            self.samples,
            ms(self.p50),
            ms(self.p99),
            ms(self.max),
        )
    }
}

/// One-datagram-in-flight ping-pong over `tokio_udp`: the round trip of a
/// datagram that never queues behind another one.
async fn pingpong_tokio_udp(samples: usize) -> Dist {
    let server = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
    let server_addr = server.local_addr().unwrap();
    let echo = tokio::spawn({
        let server = Arc::clone(&server);
        async move {
            let mut buf = [0u8; 64];
            loop {
                let (n, src) = server.recv_from(&mut buf).await.unwrap();
                let dst: &[u8] = &buf[..n];
                server
                    .send_to_vectored(&[IoSlice::new(dst)], &src)
                    .await
                    .unwrap();
            }
        }
    });

    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(server_addr).await.unwrap();
    let mut buf = [0u8; 64];
    let mut rtts = Vec::with_capacity(samples);
    for i in 0..samples {
        let payload = (i as u64).to_be_bytes();
        let sent_at = Instant::now();
        assert_eq!(client.send(&payload).await.unwrap(), payload.len());
        let n = tokio::time::timeout(Duration::from_secs(5), client.recv(&mut buf))
            .await
            .expect("the echo never came back")
            .expect("recv failed");
        assert_eq!(
            &buf[..n],
            &payload,
            "the echo did not carry its own datagram"
        );
        rtts.push(sent_at.elapsed());
    }
    echo.abort();
    Dist::from(rtts)
}

/// The same ping-pong over a plain `std::net::UdpSocket`, one blocking thread
/// per direction. This is the comparison the field question needs: if
/// `tokio_udp`'s readiness handling added a floor, it would show as a gap
/// against this arm, which pays a thread park/unpark instead of a readiness
/// wakeup.
fn pingpong_std_udp(samples: usize) -> Dist {
    let server = std::net::UdpSocket::bind(loopback()).unwrap();
    let server_addr = server.local_addr().unwrap();
    // A read timeout, not a blocking read: on macOS a `recv_from` parked in
    // another thread is not woken by closing the socket, so a blocking echo
    // thread would never observe the stop flag and `join` would hang the test.
    server
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let echo = std::thread::spawn({
        let stop = Arc::clone(&stop);
        move || {
            let mut buf = [0u8; 64];
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let (n, src) = match server.recv_from(&mut buf) {
                    Ok(v) => v,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        continue;
                    }
                    Err(_) => return,
                };
                if server.send_to(&buf[..n], src).is_err() {
                    return;
                }
            }
        }
    });

    let client = std::net::UdpSocket::bind(loopback()).unwrap();
    client.connect(server_addr).unwrap();
    let mut buf = [0u8; 64];
    let mut rtts = Vec::with_capacity(samples);
    for i in 0..samples {
        let payload = (i as u64).to_be_bytes();
        let sent_at = Instant::now();
        client.send(&payload).unwrap();
        let n = client.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], &payload);
        rtts.push(sent_at.elapsed());
    }
    // The sampler is done, so the echo thread can be stopped and joined: the
    // timeout above is what lets it observe the flag.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(client);
    echo.join().expect("the std echo thread panicked");
    Dist::from(rtts)
}

/// Three-buffer `send_to_vectored` on `tokio::net::UdpSocket`: the concatenating
/// fallback this crate replaces on Unix, and the one path here that copies a
/// datagram a third time.
async fn pingpong_concatenating_fallback(samples: usize) -> Dist {
    let server = Arc::new(tokio::net::UdpSocket::bind(loopback()).await.unwrap());
    let server_addr = server.local_addr().unwrap();
    let echo = tokio::spawn({
        let server = Arc::clone(&server);
        async move {
            let mut buf = [0u8; 64];
            loop {
                let (n, src) = server.recv_from(&mut buf).await.unwrap();
                let dst: &[u8] = &buf[..n];
                server.send_to(dst, src).await.unwrap();
            }
        }
    });

    let client = tokio::net::UdpSocket::bind(loopback()).await.unwrap();
    client.connect(server_addr).await.unwrap();
    let mut buf = [0u8; 64];
    let mut rtts = Vec::with_capacity(samples);
    for i in 0..samples {
        let payload = (i as u64).to_be_bytes();
        // The concatenation `default_send_to_vectored` performs for a socket
        // with no vectored syscall: three slices into one temporary. It is the
        // third copy per datagram the table above prices.
        let concat: Vec<u8> = [&payload[..3], &payload[3..6], &payload[6..]]
            .into_iter()
            .flat_map(|part| part.iter().copied())
            .collect();
        assert_eq!(concat, payload);
        let sent_at = Instant::now();
        client.send(&concat).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(5), client.recv(&mut buf))
            .await
            .expect("the echo never came back")
            .unwrap();
        assert_eq!(&buf[..n], &payload);
        rtts.push(sent_at.elapsed());
    }
    echo.abort();
    Dist::from(rtts)
}

/// The floor this socket layer adds over a plain `std::net::UdpSocket`.
///
/// The bound is a tripwire, not a target: the field floor is 190 ms, the clean
/// harness arm reports tens of milliseconds, and this asserts the *added* cost
/// of the readiness handling is under 2 ms at the median. A regression that
/// introduced a fixed per-datagram wait — a timer-driven poll, a hot-path
/// allocation, a lost wakeup — lands here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn tokio_udp_adds_no_floor_over_a_plain_std_udp_socket() {
    // 500 samples put the p99 at the fifth-largest observation and keep the
    // whole target inside the crate's declared `default` tier budget.
    const SAMPLES: usize = 500;

    let tokio_arm = pingpong_tokio_udp(SAMPLES).await;
    let std_arm = tokio::task::spawn_blocking(move || pingpong_std_udp(SAMPLES))
        .await
        .expect("the std arm panicked");
    let concat_arm = pingpong_concatenating_fallback(SAMPLES).await;

    for line in [
        tokio_arm.line("tokio_udp/connected"),
        std_arm.line("std/blocking-threads"),
        concat_arm.line("tokio-net/3-buffer-fallback"),
    ] {
        println!("{line}");
    }
    println!(
        "DELAY_SYSCALLS tokio_udp 2/datagram (sendmsg+recvmsg), std 2/datagram (sendto+recvfrom), \
         concatenating fallback 2/datagram plus one userspace copy"
    );

    // Sanity: both arms measured, and a round trip is a positive duration. A
    // zero median would mean the clock and the syscall are the same thing,
    // which is not a measurement of a socket.
    assert_eq!(tokio_arm.samples, SAMPLES, "the tokio arm lost samples");
    assert_eq!(std_arm.samples, SAMPLES, "the std arm lost samples");
    assert_eq!(concat_arm.samples, SAMPLES, "the fallback arm lost samples");
    assert!(
        tokio_arm.p50 > Duration::ZERO,
        "a zero round trip is not one"
    );
    assert!(std_arm.p50 > Duration::ZERO, "a zero round trip is not one");
    let added = tokio_arm
        .p50
        .checked_sub(std_arm.p50)
        .unwrap_or(Duration::ZERO);
    println!(
        "DELAY_FLOOR tokio_udp_p50_minus_std_p50={:.3}ms (tokio_udp p50 {:.3}ms, std p50 {:.3}ms); \
         the field floor this is being read against is 190ms",
        ms(added),
        ms(tokio_arm.p50),
        ms(std_arm.p50),
    );
    assert!(
        added < Duration::from_millis(2),
        "tokio_udp's median round trip exceeds a plain std socket's by {:.3}ms: a per-datagram \
         floor has appeared in the readiness path (tokio_udp {:.3}ms, std {:.3}ms)",
        ms(added),
        ms(tokio_arm.p50),
        ms(std_arm.p50),
    );
    assert!(
        tokio_arm.p99 < Duration::from_millis(20),
        "the tokio_udp p99 round trip is {:.3}ms on loopback: a per-datagram stall has appeared",
        ms(tokio_arm.p99)
    );
}

/// The Unix backend sizes no send or receive buffer, so both directions run at
/// the kernel defaults. This pins that as a *measured fact* rather than a code
/// reading: if a sizing is added — or removed — the numbers here change, and the
/// burst arm below is the one that says whether it mattered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn the_socket_buffers_are_left_at_the_kernel_defaults() {
    let ours = UdpSocket::bind(loopback()).await.unwrap();
    let ours_recv = ours.async_fd().get_ref().recv_buffer_size().unwrap();
    let ours_send = ours.async_fd().get_ref().send_buffer_size().unwrap();

    let plain = std::net::UdpSocket::bind(loopback()).unwrap();
    let plain = socket2::Socket::from(plain);
    let plain_recv = plain.recv_buffer_size().unwrap();
    let plain_send = plain.send_buffer_size().unwrap();

    println!(
        "BUFFER_DEFAULTS tokio_udp recv={ours_recv} send={ours_send}; \
         std/socket2(default) recv={plain_recv} send={plain_send}"
    );

    // Sanity: an unsized socket still has *a* buffer, so a zero here would mean
    // the option read failed rather than that the buffer is absent.
    assert!(
        ours_recv > 0 && ours_send > 0,
        "a socket always has buffers"
    );
    assert!(
        plain_recv > 0 && plain_send > 0,
        "the std socket always has buffers"
    );

    // The measured claim: this crate sizes neither buffer, so what the kernel
    // gives a freshly created socket is what it uses.
    assert_eq!(
        (ours_recv, ours_send),
        (plain_recv, plain_send),
        "tokio_udp no longer takes the kernel's default buffer sizes; the burst arm below must be \
         re-read against the new sizes"
    );
}

/// A burst larger than the receive buffer, delivered while nobody reads, is
/// dropped by the kernel. The same burst is replayed against a socket explicitly
/// sized to 4 MiB, so the difference between the arms is the buffer's
/// contribution and nothing else.
///
/// The property this asserts is the one that distinguishes a loss *ceiling* from
/// a *floor*: the default-sized socket loses datagrams from a burst the sized
/// socket keeps more of, which means the default buffer prices offered load, not
/// every datagram. A buffer that caused a floor would slow every datagram at
/// every rate, and the ping-pong arm above measures that it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn a_burst_past_the_default_receive_buffer_is_a_loss_ceiling_not_a_floor() {
    const DATAGRAM: usize = 1_200;
    const BURST: usize = 9_000;

    /// Returns `(offered, kept, effective_recv_buffer)`.
    async fn burst(size: Option<usize>) -> (usize, usize, usize) {
        let receiver = UdpSocket::bind(loopback()).await.unwrap();
        if let Some(bytes) = size {
            receiver
                .async_fd()
                .get_ref()
                .set_recv_buffer_size(bytes)
                .unwrap();
        }
        let effective = receiver.async_fd().get_ref().recv_buffer_size().unwrap();
        let addr = receiver.local_addr().unwrap();

        let sender = UdpSocket::bind(loopback()).await.unwrap();
        let payload = [0x5Au8; DATAGRAM];
        let mut offered = 0usize;
        for _ in 0..BURST {
            match sender
                .send_to_vectored(&[IoSlice::new(&payload)], &addr)
                .await
            {
                Ok(_) => offered += 1,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
        // Let in-flight loopback delivery settle before counting survivors, so
        // the count is "what the kernel kept" and not "what the reader raced".
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut buf = [0u8; DATAGRAM];
        let mut kept = 0usize;
        loop {
            match receiver.try_recv(&mut buf) {
                Ok(n) => {
                    assert_eq!(n, DATAGRAM, "a survivor was truncated");
                    kept += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
        (offered, kept, effective)
    }

    let (default_offered, default_kept, default_size) = burst(None).await;
    let (sized_offered, sized_kept, sized_size) = burst(Some(4 << 20)).await;
    println!(
        "BUFFER_BURST default size={default_size}B offered={default_offered} kept={default_kept} \
         lost={} ; sized size={sized_size}B offered={sized_offered} kept={sized_kept} lost={}",
        default_offered - default_kept,
        sized_offered - sized_kept,
    );

    // Sanity: the burst was genuinely offered, and nothing was invented. An arm
    // that sent nothing, or that counted more survivors than datagrams, is not a
    // measurement of a buffer.
    assert_eq!(
        default_offered, BURST,
        "the sender was backpressured, so the default-size burst was not offered"
    );
    assert_eq!(
        sized_offered, BURST,
        "the sender was backpressured, so the sized burst was not offered"
    );
    assert!(default_kept <= default_offered, "conservation");
    assert!(sized_kept <= sized_offered, "conservation");

    // The measured claim: at the kernel default this burst overflows the receive
    // queue, so the default buffer prices offered load.
    assert!(
        default_kept < default_offered,
        "the burst did not overflow the default receive buffer ({default_offered} offered, \
         {default_kept} kept): raise BURST before reading this arm as exonerating the default"
    );
    // A larger buffer cannot keep fewer datagrams; if the host clamps the resize
    // the printed sizes show it and the two arms are then the same measurement.
    assert!(
        sized_kept >= default_kept,
        "a larger receive buffer kept fewer datagrams ({sized_kept} vs {default_kept})"
    );
}
