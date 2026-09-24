// ---------------------------------------------------------------------------
// Unix implementation — sendmsg(2) via socket2::Socket + AsyncFd
// ---------------------------------------------------------------------------

use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::SocketAddr;

use socket2::{MsgHdr, SockAddr};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

/// Readiness interests awaited for an asynchronous receive.
///
/// `ERROR` is awaited alongside `READABLE` because a connected socket's
/// pending `SO_ERROR` — for example the `ECONNREFUSED` an ICMP
/// port-unreachable queues on the socket — is delivered as an error
/// readiness event that is *not* folded into read readiness on every
/// platform: `mio`'s epoll selector maps `EPOLLERR` to a distinct
/// readiness bit, so awaiting only `READABLE` can park the receive forever
/// instead of surfacing the error. tokio's own `UdpSocket` awaits
/// `Interest::READABLE | Interest::ERROR` for exactly this reason, and the
/// receive closure consumes the error on its next call. Send readiness is
/// deliberately left as `WRITABLE` only, matching tokio.
const RECV_INTEREST: Interest = Interest::READABLE.add(Interest::ERROR);

/// The bounded backoff schedule for the macOS `sendmsg` `EWOULDBLOCK` retry
/// loop: `BACKOFFS_US[i]` is the sleep before retry number `i + 1`. The loop
/// consults [`would_block_retry_micros`], the single decision authority, so
/// the schedule and its exhaustion boundary are machine-checked without
/// needing a real full-send-buffer socket (which macOS cannot produce — a
/// UDP peer whose receive queue is full drops the datagram instead of
/// backpressuring the sender; see the tests below).
const BACKOFFS_US: [u64; 5] = [1_000, 2_000, 4_000, 8_000, 16_000];

/// The retry decision for one `EWOULDBLOCK` outcome: the sleep (micros)
/// before the next attempt, or `None` when the bounded retry budget is
/// exhausted and the error must be returned to the caller. Pure, so the
/// budget math is exhaustively testable.
fn would_block_retry_micros(attempt: usize) -> Option<u64> {
    BACKOFFS_US.get(attempt).copied()
}
pub struct UdpSocket {
    inner: AsyncFd<socket2::Socket>,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<UdpSocket>();
};

impl std::fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpSocket").finish()
    }
}

impl std::os::fd::AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(self.inner.get_ref())
    }
}

impl UdpSocket {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
        socket.set_nonblocking(true)?;
        socket.bind(&SockAddr::from(addr))?;
        let inner = AsyncFd::new(socket)?;
        Ok(Self { inner })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .get_ref()
            .local_addr()?
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert socket address"))
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .get_ref()
            .peer_addr()?
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert peer address"))
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.get_ref().connect(&SockAddr::from(addr))
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.inner.get_ref().set_broadcast(on)
    }

    pub fn broadcast(&self) -> io::Result<bool> {
        self.inner.get_ref().broadcast()
    }

    pub fn set_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.inner.get_ref().set_ttl_v4(ttl)
    }

    pub fn ttl_v4(&self) -> io::Result<u32> {
        self.inner.get_ref().ttl_v4()
    }

    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.inner.get_ref().set_multicast_loop_v4(on)
    }

    pub fn join_multicast_v4(
        &self,
        multi_addr: &std::net::Ipv4Addr,
        interface: &std::net::Ipv4Addr,
    ) -> io::Result<()> {
        self.inner
            .get_ref()
            .join_multicast_v4(multi_addr, interface)
    }

    pub fn leave_multicast_v4(
        &self,
        multi_addr: &std::net::Ipv4Addr,
        interface: &std::net::Ipv4Addr,
    ) -> io::Result<()> {
        self.inner
            .get_ref()
            .leave_multicast_v4(multi_addr, interface)
    }

    /// Low-level [`AsyncFd`] access for registering custom readiness
    /// interests.
    pub fn async_fd(&self) -> &AsyncFd<socket2::Socket> {
        &self.inner
    }

    fn try_send_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: Option<&SockAddr>,
    ) -> io::Result<usize> {
        let msg = match target {
            Some(addr) => MsgHdr::new().with_addr(addr).with_buffers(bufs),
            None => MsgHdr::new().with_buffers(bufs),
        };
        self.inner.get_ref().sendmsg(&msg, 0)
    }

    #[cfg(not(target_os = "macos"))]
    async fn send_vectored_when_writable(
        &self,
        bufs: &[IoSlice<'_>],
        target: Option<&SockAddr>,
    ) -> io::Result<usize> {
        self.inner
            .async_io(Interest::WRITABLE, |_| self.try_send_vectored(bufs, target))
            .await
    }

    #[cfg(target_os = "macos")]
    async fn send_vectored_with_bounded_backoff(
        &self,
        bufs: &[IoSlice<'_>],
        target: Option<&SockAddr>,
    ) -> io::Result<usize> {
        let mut attempt = 0;
        loop {
            match self.try_send_vectored(bufs, target) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    match would_block_retry_micros(attempt) {
                        Some(micros) => {
                            tokio::time::sleep(std::time::Duration::from_micros(micros)).await;
                            attempt += 1;
                        }
                        None => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn as_uninit(buf: &mut [u8]) -> &mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
        }
    }

    async fn recv_uninit(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        self.inner
            .async_io(RECV_INTEREST, |sock| sock.recv(buf))
            .await
    }

    async fn recv_from_uninit(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<(usize, SockAddr)> {
        self.inner
            .async_io(RECV_INTEREST, |sock| sock.recv_from(buf))
            .await
    }

    pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        #[cfg(target_os = "macos")]
        {
            self.send_vectored_with_bounded_backoff(bufs, None).await
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.send_vectored_when_writable(bufs, None).await
        }
    }

    pub async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &SocketAddr,
    ) -> io::Result<usize> {
        let addr = SockAddr::from(*target);
        #[cfg(target_os = "macos")]
        {
            self.send_vectored_with_bounded_backoff(bufs, Some(&addr))
                .await
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.send_vectored_when_writable(bufs, Some(&addr)).await
        }
    }

    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.send_vectored(&[IoSlice::new(buf)]).await
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv_uninit(Self::as_uninit(buf)).await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (n, addr) = self.recv_from_uninit(Self::as_uninit(buf)).await?;
        let addr = addr
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert source address"))?;
        Ok((n, addr))
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.inner.readable().await.map(|_| ())
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.inner.writable().await.map(|_| ())
    }

    /// Run one non-blocking operation, keeping `AsyncFd`'s cached readiness in
    /// step with the kernel.
    ///
    /// `AsyncFd` caches kernel readiness: once an interest has been observed
    /// ready it keeps reporting so until explicitly cleared. An operation that
    /// returns `WouldBlock` therefore leaves that cached readiness stale — a
    /// later `readable()`/`writable()` await would return immediately off the
    /// stale event instead of parking — so the event has to be cleared.
    ///
    /// The event that may be cleared is the one `AsyncFd::try_io` snapshots as
    /// the operation *starts*, never one read back once it has finished.
    /// `try_io` clears that snapshot via `ScheduledIo::set_readiness(Tick::Clear)`,
    /// which is a no-op if the driver has published a newer event in the
    /// meantime (the tick no longer matches). An event published while the
    /// operation runs has already woken whoever was parked for it; consuming it
    /// would strand that waiter — woken, re-polling, finding the cache empty —
    /// in front of the very datagram the event was announcing.
    ///
    /// `try_io` skips the operation entirely when nothing is cached, so fall
    /// back to a bare attempt then: a non-blocking call on a socket whose
    /// readiness has never been observed must still reach the kernel, and with
    /// nothing cached there is nothing to clear either (any event published from
    /// here on belongs to a waiter this call never observed).
    fn nonblocking<T>(
        &self,
        interest: Interest,
        op: impl FnOnce(&socket2::Socket) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut op = Some(op);
        let result = self.inner.try_io(interest, |socket| {
            op.take().expect("the operation runs at most once")(socket)
        });
        match op {
            Some(op) => op(self.inner.get_ref()),
            None => result,
        }
    }

    /// Attempt a non-blocking send. Returns `WouldBlock` if the kernel
    /// buffer is full.
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.nonblocking(Interest::WRITABLE, |socket| socket.send(buf))
    }

    /// Attempt a non-blocking send to a target address.
    pub fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        let addr = SockAddr::from(*target);
        self.nonblocking(Interest::WRITABLE, |socket| socket.send_to(buf, &addr))
    }

    /// Attempt a non-blocking receive. Returns `WouldBlock` if no data
    /// is available.
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.nonblocking(Interest::READABLE, |socket| {
            socket.recv(Self::as_uninit(buf))
        })
    }

    fn chunk_as_uninit(dst: &mut bytes::buf::UninitSlice) -> &mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(dst.as_mut_ptr() as *mut MaybeUninit<u8>, dst.len())
        }
    }

    /// Receive into a `BufMut` (bytes buffer). Returns the
    /// number of bytes read.
    pub async fn recv_buf(&self, buf: &mut impl bytes::BufMut) -> io::Result<usize> {
        let dst = buf.chunk_mut();
        let n = self.recv_uninit(Self::chunk_as_uninit(dst)).await?;
        unsafe { buf.advance_mut(n) };
        Ok(n)
    }

    /// Receive from any peer into a `BufMut`.
    pub async fn recv_buf_from(
        &self,
        buf: &mut impl bytes::BufMut,
    ) -> io::Result<(usize, SocketAddr)> {
        let dst = buf.chunk_mut();
        let (n, addr) = self.recv_from_uninit(Self::chunk_as_uninit(dst)).await?;
        unsafe { buf.advance_mut(n) };
        let addr = addr
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert source address"))?;
        Ok((n, addr))
    }

    pub fn try_clone_std(&self) -> io::Result<std::net::UdpSocket> {
        Ok(self.inner.get_ref().try_clone()?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{RECV_INTEREST, UdpSocket};
    use std::future::Future;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use tokio::io::Interest;

    /// Poll `future` to completion on the calling thread, parking it between
    /// polls. The test below has to wait for a readiness event the driver
    /// publishes from inside a synchronous operation, a position no `await`
    /// reaches. `budget` bounds that wait, so a publication that never arrives
    /// fails the test instead of hanging it.
    fn block_on<F: Future>(future: F, budget: Duration) -> Option<F::Output> {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        let deadline = Instant::now() + budget;
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
                return Some(value);
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            std::thread::park_timeout(deadline - now);
        }
    }

    /// Whether an `AsyncFd` readiness event for `interest` is cached. The probe
    /// does not consume the event: `try_io` only clears when the operation it
    /// runs reports `WouldBlock`.
    fn is_cached(socket: &UdpSocket, interest: Interest) -> bool {
        socket
            .async_fd()
            .try_io(interest, |_| -> io::Result<()> { Ok(()) })
            .is_ok()
    }

    /// The async receive must await `ERROR` as well as `READABLE`: a pending
    /// `SO_ERROR` is a distinct error readiness bit on some platforms, and a
    /// receive that awaits only `READABLE` parks forever there instead of
    /// surfacing the error. This pins the guarded property directly, so it
    /// fails on every platform if `ERROR` is dropped from the wait.
    #[test]
    fn recv_interest_surfaces_a_pending_error() {
        assert!(RECV_INTEREST.is_readable(), "recv must await READABLE");
        assert!(
            RECV_INTEREST.is_error(),
            "recv must await ERROR so a pending SO_ERROR wakes it"
        );
    }

    /// The bounded-backoff budget math (the single decision authority the
    /// macOS retry loop consults): attempts 0..4 get the documented sleep,
    /// attempt 5 exhausts the budget and must return `None` so the loop
    /// returns the `EWOULDBLOCK` error. Vacuity: shrink the schedule or the
    /// exhaustion bound and this fails naming the arm.
    #[test]
    fn would_block_retry_micros_covers_the_schedule_and_exhausts() {
        let expected = [1_000u64, 2_000, 4_000, 8_000, 16_000];
        for (attempt, want) in expected.iter().enumerate() {
            assert_eq!(
                super::would_block_retry_micros(attempt),
                Some(*want),
                "attempt {attempt} must sleep the documented backoff"
            );
        }
        assert_eq!(
            super::would_block_retry_micros(expected.len()),
            None,
            "the bounded retry budget must be exhausted after {} would-blocks",
            expected.len()
        );
    }

    /// A readiness event the driver publishes while a non-blocking operation
    /// runs must survive the clear that the operation's `WouldBlock` triggers:
    /// the event has already woken a waiter, and that waiter must still find the
    /// socket readable.
    ///
    /// In production the gap between the failing syscall and the clear is a
    /// handful of instructions, so the race is staged instead of sampled. The
    /// operation (1) consumes the datagram that armed the cached event, (2) fails
    /// a real syscall on an honestly empty queue — dropping that cached event —
    /// then (3) publishes a second datagram and waits, over the same readiness
    /// path the socket's own waiters use, for the driver to announce it. A clear
    /// armed with the event observed *before* the operation leaves the newer
    /// event cached; a clear armed with the event observed after the operation
    /// consumes it, and the reader parked behind it never sees the datagram.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn a_would_block_clear_keeps_an_event_published_while_the_operation_ran() {
        const BUDGET: Duration = Duration::from_secs(5);
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let socket = UdpSocket::bind(bind).await.unwrap();
        let peer = std::net::UdpSocket::bind(bind).unwrap();
        let addr = socket.local_addr().unwrap();
        let mut buf = [0u8; 16];

        // The datagram whose arrival makes the driver publish the event the
        // operation captures.
        peer.send_to(b"first", addr).unwrap();
        block_on(socket.readable(), BUDGET)
            .expect("the driver never published the first datagram")
            .unwrap();
        assert!(
            is_cached(&socket, Interest::READABLE),
            "READABLE must be cached before the operation runs"
        );

        let result: io::Result<usize> = socket.nonblocking(Interest::READABLE, |_| {
            // 1. Consume the datagram that armed the captured event.
            assert_eq!(socket.try_recv(&mut buf).unwrap(), 5);
            // 2. A real failing syscall on an honestly empty queue. Its
            //    `WouldBlock` is what this operation reports, and it drops the
            //    captured event so that a fresh waiter can park below.
            assert_eq!(
                socket.try_recv(&mut buf).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            // 3. Announce a second datagram while the operation is still
            //    running, and park until the driver publishes its event.
            peer.send_to(b"second", addr).unwrap();
            block_on(socket.readable(), BUDGET)
                .expect("the driver never published the second datagram")
                .unwrap();
            Err(io::ErrorKind::WouldBlock.into())
        });
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "the operation must report the failing syscall's error"
        );

        assert!(
            is_cached(&socket, Interest::READABLE),
            "the clear consumed a readiness event the driver published while the operation ran"
        );
        let n = tokio::time::timeout(BUDGET, socket.recv(&mut buf))
            .await
            .expect("the reader parked on a readiness event that was consumed")
            .unwrap();
        assert_eq!(&buf[..n], b"second");
    }

    /// A non-blocking operation that reports `WouldBlock` must drop the readiness
    /// event it was armed with, so a later `writable()`/`readable()` await parks
    /// instead of returning immediately off the stale cache.
    ///
    /// The socket-level `*_stops_claiming_*` tests can only drive that through the
    /// kernel where the kernel genuinely backpressures a UDP send, which macOS
    /// loopback never does; supplying the failing operation directly holds on every
    /// platform.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn a_failing_operation_drops_the_event_it_was_armed_with() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let socket = UdpSocket::bind(bind).await.unwrap();
        block_on(socket.writable(), Duration::from_secs(5))
            .expect("the driver never published WRITABLE")
            .unwrap();
        assert!(
            is_cached(&socket, Interest::WRITABLE),
            "WRITABLE must be cached before the operation runs"
        );

        let result: io::Result<usize> =
            socket.nonblocking(
                Interest::WRITABLE,
                |_| Err(io::ErrorKind::WouldBlock.into()),
            );
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "the operation's error must reach the caller"
        );
        assert!(
            !is_cached(&socket, Interest::WRITABLE),
            "a failing operation must drop the readiness event it was armed with"
        );
    }

    /// The other-error arm of the macOS backoff loop is the only arm a real
    /// socket can drive deterministically on this host: a zero-`iovec`
    /// `sendmsg(2)` fails immediately with `EMSGSIZE`, which is neither
    /// `EWOULDBLOCK` nor `EINTR`, so it must propagate straight back without
    /// consuming any retry budget. (The `EWOULDBLOCK` arms are not
    /// deterministically reachable on macOS: a UDP peer whose receive queue
    /// is full drops the datagram instead of backpressuring the sender, so a
    /// send buffer never fills hard enough to block — verified empirically;
    /// `EINTR` needs a signal racing a syscall, also non-deterministic.)
    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn send_vectored_propagates_a_non_blocking_error_without_backoff() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let sock = UdpSocket::bind(bind).await.unwrap();
        let err = sock
            .send_vectored(&[])
            .await
            .expect_err("an empty iovec list must fail");
        assert!(
            err.kind() != std::io::ErrorKind::WouldBlock
                && err.kind() != std::io::ErrorKind::Interrupted,
            "a non-blocking error must propagate without being retried: {err:?}"
        );
    }
}
