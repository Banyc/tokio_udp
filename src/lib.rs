//! A tokio-integrated UDP socket with zero-copy vectored sends via
//! `sendmsg(2)` on Unix, and a `tokio::net::UdpSocket` fallback on
//! non-Unix (Windows).

// ---------------------------------------------------------------------------
// Cross-platform public API
// ---------------------------------------------------------------------------

/// A tokio-integrated UDP socket.
///
/// On Unix the socket is backed by `socket2::Socket` + `AsyncFd` and uses
/// `sendmsg(2)` for zero-copy vectored sends. On other platforms it wraps
/// `tokio::net::UdpSocket` and falls back to concatenation for vectored
/// sends.
pub use imp::UdpSocket;

// ---------------------------------------------------------------------------
// Unix implementation — sendmsg(2) via socket2::Socket + AsyncFd
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod imp {
    use std::io::{self, IoSlice};
    use std::mem::MaybeUninit;
    use std::net::SocketAddr;
    use std::task::{Context, Poll, ready};

    use socket2::{MsgHdr, SockAddr};
    use tokio::io::unix::AsyncFd;

    pub struct UdpSocket {
        inner: AsyncFd<socket2::Socket>,
    }

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

        pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
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
        fn poll_send_vectored(
            &self,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
            target: Option<&SockAddr>,
        ) -> Poll<io::Result<usize>> {
            loop {
                match self.try_send_vectored(bufs, target) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                ready!(self.inner.poll_write_ready(cx))?.clear_ready();
            }
        }

        #[cfg(target_os = "macos")]
        async fn send_vectored_with_bounded_backoff(
            &self,
            bufs: &[IoSlice<'_>],
            target: Option<&SockAddr>,
        ) -> io::Result<usize> {
            const BACKOFFS_US: [u64; 5] = [1_000, 2_000, 4_000, 8_000, 16_000];
            let mut attempt = 0;
            loop {
                match self.try_send_vectored(bufs, target) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if attempt >= BACKOFFS_US.len() {
                            return Err(e);
                        }
                        tokio::time::sleep(std::time::Duration::from_micros(BACKOFFS_US[attempt]))
                            .await;
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
            let buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
            };
            loop {
                match self.inner.get_ref().recv(buf) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                ready!(self.inner.poll_read_ready(cx))?.clear_ready();
            }
        }

        fn poll_recv_from(
            &self,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<(usize, SockAddr)>> {
            let buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
            };
            loop {
                match self.inner.get_ref().recv_from(buf) {
                    Ok((n, addr)) => return Poll::Ready(Ok((n, addr))),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                ready!(self.inner.poll_read_ready(cx))?.clear_ready();
            }
        }

        pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            #[cfg(target_os = "macos")]
            {
                self.send_vectored_with_bounded_backoff(bufs, None).await
            }
            #[cfg(not(target_os = "macos"))]
            {
                std::future::poll_fn(|cx| self.poll_send_vectored(cx, bufs, None)).await
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
                std::future::poll_fn(|cx| self.poll_send_vectored(cx, bufs, Some(&addr))).await
            }
        }

        pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
            self.send_vectored(&[IoSlice::new(buf)]).await
        }

        pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            std::future::poll_fn(|cx| self.poll_recv(cx, buf)).await
        }

        pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            let (n, addr) = std::future::poll_fn(|cx| self.poll_recv_from(cx, buf)).await?;
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

        /// Attempt a non-blocking send. Returns `WouldBlock` if the kernel
        /// buffer is full.
        pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
            // socket2's Socket is set to non-blocking, so send returns
            // WouldBlock when the buffer is full.
            self.inner.get_ref().send(buf)
        }

        /// Attempt a non-blocking send to a target address.
        pub fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
            let addr = SockAddr::from(*target);
            self.inner.get_ref().send_to(buf, &addr)
        }

        /// Attempt a non-blocking receive. Returns `WouldBlock` if no data
        /// is available.
        pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            let buf: &mut [MaybeUninit<u8>] = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
            };
            self.inner.get_ref().recv(buf)
        }

        fn poll_recv_buf(
            &self,
            cx: &mut Context<'_>,
            dst: &mut bytes::buf::UninitSlice,
        ) -> Poll<io::Result<usize>> {
            let buf: &mut [MaybeUninit<u8>] = unsafe {
                std::slice::from_raw_parts_mut(dst.as_mut_ptr() as *mut MaybeUninit<u8>, dst.len())
            };
            loop {
                match self.inner.get_ref().recv(buf) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                ready!(self.inner.poll_read_ready(cx))?.clear_ready();
            }
        }

        fn poll_recv_buf_from(
            &self,
            cx: &mut Context<'_>,
            dst: &mut bytes::buf::UninitSlice,
        ) -> Poll<io::Result<(usize, SockAddr)>> {
            let buf: &mut [MaybeUninit<u8>] = unsafe {
                std::slice::from_raw_parts_mut(dst.as_mut_ptr() as *mut MaybeUninit<u8>, dst.len())
            };
            loop {
                match self.inner.get_ref().recv_from(buf) {
                    Ok((n, addr)) => return Poll::Ready(Ok((n, addr))),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                ready!(self.inner.poll_read_ready(cx))?.clear_ready();
            }
        }

        /// Receive into a `BufMut` (vectored bytes buffer). Returns the
        /// number of bytes read.
        pub async fn recv_buf(&self, buf: &mut impl bytes::BufMut) -> io::Result<usize> {
            let dst = buf.chunk_mut();
            let n = std::future::poll_fn(|cx| self.poll_recv_buf(cx, dst)).await?;
            // SAFETY: `n` bytes were written by the kernel into `dst`.
            unsafe {
                buf.advance_mut(n);
            }
            Ok(n)
        }

        /// Receive from any peer into a `BufMut`.
        pub async fn recv_buf_from(
            &self,
            buf: &mut impl bytes::BufMut,
        ) -> io::Result<(usize, SocketAddr)> {
            let dst = buf.chunk_mut();
            let (n, addr) = std::future::poll_fn(|cx| self.poll_recv_buf_from(cx, dst)).await?;
            // SAFETY: `n` bytes were written by the kernel into `dst`.
            unsafe {
                buf.advance_mut(n);
            }
            let addr = addr
                .as_socket()
                .ok_or_else(|| io::Error::other("failed to convert source address"))?;
            Ok((n, addr))
        }
    }

    unsafe impl Send for UdpSocket {}
    unsafe impl Sync for UdpSocket {}
}

// ---------------------------------------------------------------------------
// Non-Unix implementation — tokio::net::UdpSocket + concatenation fallback
// ---------------------------------------------------------------------------

#[cfg(not(unix))]
mod imp {
    use std::io::{self, IoSlice};
    use std::net::SocketAddr;

    pub struct UdpSocket {
        inner: tokio::net::UdpSocket,
    }

    impl std::fmt::Debug for UdpSocket {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("UdpSocket").finish()
        }
    }

    impl UdpSocket {
        pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
            let inner = tokio::net::UdpSocket::bind(addr).await?;
            Ok(Self { inner })
        }

        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.local_addr()
        }

        pub fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.peer_addr()
        }

        pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
            self.inner.connect(addr).into()
        }

        pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
            self.inner.set_broadcast(on)
        }

        pub fn broadcast(&self) -> io::Result<bool> {
            self.inner.broadcast()
        }

        pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
            self.inner.set_ttl(ttl)
        }

        pub fn ttl(&self) -> io::Result<u32> {
            self.inner.ttl()
        }

        pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            match bufs.len() {
                0 => Ok(0),
                1 => self.inner.send(&bufs[0]).await,
                _ => {
                    let total: usize = bufs.iter().map(|b| b.len()).sum();
                    let mut buf = Vec::with_capacity(total);
                    for b in bufs {
                        buf.extend_from_slice(b);
                    }
                    self.inner.send(&buf).await
                }
            }
        }

        pub async fn send_to_vectored(
            &self,
            bufs: &[IoSlice<'_>],
            target: &SocketAddr,
        ) -> io::Result<usize> {
            match bufs.len() {
                0 => Ok(0),
                1 => self.inner.send_to(&bufs[0], target).await,
                _ => {
                    let total: usize = bufs.iter().map(|b| b.len()).sum();
                    let mut buf = Vec::with_capacity(total);
                    for b in bufs {
                        buf.extend_from_slice(b);
                    }
                    self.inner.send_to(&buf, target).await
                }
            }
        }

        pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
            self.inner.send(buf).await
        }

        pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.recv(buf).await
        }

        pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            self.inner.recv_from(buf).await
        }

        pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
            self.inner.try_send(buf)
        }

        pub fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
            self.inner.try_send_to(buf, target)
        }

        pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.try_recv(buf)
        }

        pub async fn recv_buf(&self, buf: &mut impl bytes::BufMut) -> io::Result<usize> {
            self.inner.recv_buf(buf).await
        }

        pub async fn recv_buf_from(
            &self,
            buf: &mut impl bytes::BufMut,
        ) -> io::Result<(usize, SocketAddr)> {
            self.inner.recv_buf_from(buf).await
        }
    }

    unsafe impl Send for UdpSocket {}
    unsafe impl Sync for UdpSocket {}
}

/// Returns `true` on platforms where `sendmsg(2)` is available and
/// [`send_vectored`](UdpSocket::send_vectored) /
/// [`send_to_vectored`](UdpSocket::send_to_vectored) issue a single
/// zero-copy system call.
///
/// Returns `false` on platforms where the vectored methods fall back to
/// concatenating all buffers into a temporary `Vec<u8>` before calling
/// the kernel's single-buffer send path.
pub fn is_vectored_supported() -> bool {
    cfg!(unix)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::imp::UdpSocket;
    use std::net::SocketAddr;

    #[tokio::test(flavor = "multi_thread")]
    async fn send_recv_connected() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = UdpSocket::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let client = UdpSocket::bind(bind).await.unwrap();
        client.connect(server_addr).unwrap();

        tokio::spawn(async move {
            let mut buf = [0u8; 64];
            let (_n, peer) = server.recv_from(&mut buf).await.unwrap();
            let reply = b"pong";
            server
                .send_to_vectored(&[std::io::IoSlice::new(&reply[..])], &peer)
                .await
                .unwrap();
        });

        let msg = b"ping";
        client.send(msg).await.unwrap();
        let mut buf = [0u8; 64];
        let n = client.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"pong");
    }

    #[tokio::test]
    async fn send_to_vectored_recv_from() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = UdpSocket::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let client = UdpSocket::bind(bind).await.unwrap();
        let client_addr = client.local_addr().unwrap();

        let header = b"HDR:";
        let body = b"hello vectored";
        let iov = [std::io::IoSlice::new(header), std::io::IoSlice::new(body)];
        server.send_to_vectored(&iov, &client_addr).await.unwrap();

        let mut buf = [0u8; 64];
        let (n, src) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"HDR:hello vectored");
        assert_eq!(src, server_addr);
    }

    #[tokio::test]
    async fn send_vectored_two_buffers() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let a = UdpSocket::bind(bind).await.unwrap();
        let b = UdpSocket::bind(bind).await.unwrap();
        let b_addr = b.local_addr().unwrap();

        let parts = [
            std::io::IoSlice::new(b"hello "),
            std::io::IoSlice::new(b"world"),
        ];
        a.send_to_vectored(&parts, &b_addr).await.unwrap();

        let mut buf = [0u8; 32];
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello world");
    }

    #[tokio::test]
    async fn send_vectored_single_buffer_is_same_as_send() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let a = UdpSocket::bind(bind).await.unwrap();
        let b = UdpSocket::bind(bind).await.unwrap();

        let msg = b"single";
        a.send_to_vectored(&[std::io::IoSlice::new(&msg[..])], &b.local_addr().unwrap())
            .await
            .unwrap();

        let mut buf = [0u8; 32];
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], msg);
    }

    #[tokio::test]
    async fn set_and_read_ttl() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let sock = UdpSocket::bind(bind).await.unwrap();
        sock.set_ttl_v4(64).unwrap();
        assert_eq!(sock.ttl_v4().unwrap(), 64);
    }

    #[tokio::test]
    async fn set_and_read_broadcast() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let sock = UdpSocket::bind(bind).await.unwrap();
        sock.set_broadcast(true).unwrap();
        assert!(sock.broadcast().unwrap());
        sock.set_broadcast(false).unwrap();
        assert!(!sock.broadcast().unwrap());
    }
}
