use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::task::{ready, Context, Poll};

use socket2::{MsgHdr, SockAddr};
use tokio::io::unix::AsyncFd;

/// A UDP socket integrated with tokio's async reactor that supports true
/// zero-copy vectored sends via `sendmsg(2)`.
///
/// Where [`tokio::net::UdpSocket`] only accepts a single `&[u8]` per send,
/// this wrapper lets callers pass multiple `IoSlice` segments directly to
/// the kernel via `sendmsg`, avoiding intermediate concatenation.
pub struct UdpSocket {
    inner: AsyncFd<socket2::Socket>,
}

impl UdpSocket {
    /// Create a new UDP socket bound to `addr`.
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

    /// Return the local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .get_ref()
            .local_addr()?
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert socket address"))
    }

    /// Connect the socket to a remote peer.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.get_ref().connect(&SockAddr::from(addr))
    }

    /// Return the peer address if the socket is connected.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .get_ref()
            .peer_addr()?
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert peer address"))
    }

    // ── low-level poll methods ──────────────────────────────────────

    fn poll_send_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: Option<&SockAddr>,
    ) -> Poll<io::Result<usize>> {
        loop {
            let msg = match target {
                Some(addr) => MsgHdr::new().with_addr(addr).with_buffers(bufs),
                None => MsgHdr::new().with_buffers(bufs),
            };
            match self.inner.get_ref().sendmsg(&msg, 0) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            ready!(self.inner.poll_write_ready(cx))?.clear_ready();
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // SAFETY: MaybeUninit<u8> has the same layout as u8, so reinterpreting
        // a &mut [u8] as &mut [MaybeUninit<u8>] is valid. The recv system call
        // will write initialized bytes into it.
        let buf = unsafe {
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut MaybeUninit<u8>,
                buf.len(),
            )
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
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut MaybeUninit<u8>,
                buf.len(),
            )
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

    // ── async public API ────────────────────────────────────────────

    /// Send data to the connected peer from multiple buffers via
    /// `sendmsg(2)`. The socket must be connected.
    pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_send_vectored(cx, bufs, None)).await
    }

    /// Send data to `target` from multiple buffers via `sendmsg(2)`.
    pub async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &SocketAddr,
    ) -> io::Result<usize> {
        let addr = SockAddr::from(*target);
        std::future::poll_fn(|cx| self.poll_send_vectored(cx, bufs, Some(&addr))).await
    }

    /// Convenience: send a single buffer to the connected peer.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.send_vectored(&[IoSlice::new(buf)]).await
    }

    /// Receive from the connected peer into a single buffer.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_recv(cx, buf)).await
    }

    /// Receive from any peer (unconnected socket).
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (n, addr) = std::future::poll_fn(|cx| self.poll_recv_from(cx, buf)).await?;
        let addr = addr
            .as_socket()
            .ok_or_else(|| io::Error::other("failed to convert source address"))?;
        Ok((n, addr))
    }
}

// SAFETY: socket2::Socket wraps a raw fd, which is Send + Sync.
unsafe impl Send for UdpSocket {}
unsafe impl Sync for UdpSocket {}

#[cfg(test)]
mod tests {
    use super::*;

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
            server.send_to_vectored(&[std::io::IoSlice::new(&reply[..])], &peer)
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
        let iov = [IoSlice::new(header), IoSlice::new(body)];
        server.send_to_vectored(&iov, &client_addr).await.unwrap();

        let mut buf = [0u8; 64];
        let (n, src) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"HDR:hello vectored");
        assert_eq!(src, server_addr);
    }
}
