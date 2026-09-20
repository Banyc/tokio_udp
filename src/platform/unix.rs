// ---------------------------------------------------------------------------
// Unix implementation — sendmsg(2) via socket2::Socket + AsyncFd
// ---------------------------------------------------------------------------

use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::SocketAddr;

use socket2::{MsgHdr, SockAddr};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

fn would_block() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}

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

    /// Clear `AsyncFd`'s cached readiness for `interest` without doing any I/O.
    ///
    /// `AsyncFd` caches kernel readiness: once an interest has been observed
    /// ready it keeps reporting so until explicitly cleared. After a raw
    /// non-blocking sendmsg returns `EWOULDBLOCK`, that cached writable
    /// readiness is stale — the kernel is telling us the socket is *not*
    /// ready, yet a subsequent `writable()` await would still return
    /// immediately off the stale cache instead of blocking. Forcing the
    /// `try_io` closure to return `WouldBlock` makes `AsyncFd` drop the cached
    /// readiness, so the driver re-arms the interest and the next await only
    /// completes when the kernel reports the socket genuinely ready again.
    fn clear_readiness(&self, interest: Interest) {
        let _ = self
            .inner
            .try_io(interest, |_| -> io::Result<()> { Err(would_block()) });
    }

    /// Attempt a non-blocking send. Returns `WouldBlock` if the kernel
    /// buffer is full.
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.clear_readiness_on_would_block(Interest::WRITABLE, self.inner.get_ref().send(buf))
    }

    /// Attempt a non-blocking send to a target address.
    pub fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        let addr = SockAddr::from(*target);
        self.clear_readiness_on_would_block(
            Interest::WRITABLE,
            self.inner.get_ref().send_to(buf, &addr),
        )
    }

    /// Attempt a non-blocking receive. Returns `WouldBlock` if no data
    /// is available.
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.get_ref().recv(Self::as_uninit(buf));
        self.clear_readiness_on_would_block(Interest::READABLE, n)
    }

    fn clear_readiness_on_would_block<T>(
        &self,
        interest: Interest,
        result: io::Result<T>,
    ) -> io::Result<T> {
        if matches!(&result, Err(e) if e.kind() == io::ErrorKind::WouldBlock) {
            self.clear_readiness(interest);
        }
        result
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
    use std::net::SocketAddr;

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
