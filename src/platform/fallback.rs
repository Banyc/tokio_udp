// ---------------------------------------------------------------------------
// Non-Unix implementation — tokio::net::UdpSocket + concatenation fallback
// ---------------------------------------------------------------------------

use std::io::{self, IoSlice};
use std::net::SocketAddr;

pub struct UdpSocket {
    inner: tokio::net::UdpSocket,
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

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.connect(addr).await
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.inner.set_broadcast(on)
    }

    pub fn broadcast(&self) -> io::Result<bool> {
        self.inner.broadcast()
    }

    pub fn set_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.inner.set_ttl(ttl)
    }

    pub fn ttl_v4(&self) -> io::Result<u32> {
        self.inner.ttl()
    }

    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.inner.set_multicast_loop_v4(on)
    }

    pub fn join_multicast_v4(
        &self,
        multi_addr: &std::net::Ipv4Addr,
        interface: &std::net::Ipv4Addr,
    ) -> io::Result<()> {
        self.inner.join_multicast_v4(*multi_addr, *interface)
    }

    pub fn leave_multicast_v4(
        &self,
        multi_addr: &std::net::Ipv4Addr,
        interface: &std::net::Ipv4Addr,
    ) -> io::Result<()> {
        self.inner.leave_multicast_v4(*multi_addr, *interface)
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
        self.inner.try_send_to(buf, *target)
    }

    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.try_recv(buf)
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.inner.readable().await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.inner.writable().await
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

    pub fn try_clone_std(&self) -> io::Result<std::net::UdpSocket> {
        #[cfg(windows)]
        {
            use std::os::windows::io::{AsRawSocket, BorrowedSocket};
            let borrowed = unsafe { BorrowedSocket::borrow_raw(self.inner.as_raw_socket()) };
            Ok(std::net::UdpSocket::from(borrowed.try_clone_to_owned()?))
        }
        #[cfg(not(windows))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "socket clone unsupported on this platform",
            ))
        }
    }
}

/// The non-Unix backend is compiled into the test build on every host, so its
/// transitions stay checked where the default backend is `unix`: the temporary
/// concatenation that replaces `sendmsg(2)`, the `BufMut` receive paths, the
/// option surface, and the non-Windows answer for `try_clone_std`.
#[cfg(test)]
mod tests {
    use super::UdpSocket;
    use std::io::IoSlice;
    use std::net::{Ipv4Addr, SocketAddr};

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    /// With no `sendmsg(2)`, a vectored send concatenates every buffer into one
    /// temporary and hands the kernel a single datagram; a concatenation that
    /// kept only the first buffer would silently truncate the packet. The
    /// connected and unconnected paths are separate arms, and the empty list is
    /// a third: it reports zero without putting a datagram on the wire (the Unix
    /// `sendmsg` backend instead rejects an empty iovec list).
    #[tokio::test]
    async fn vectored_sends_carry_every_buffer_and_an_empty_list_sends_nothing() {
        let a = UdpSocket::bind(loopback()).await.unwrap();
        let b = UdpSocket::bind(loopback()).await.unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();
        assert!(format!("{a:?}").contains("UdpSocket"));

        let parts = [
            IoSlice::new(b"ab"),
            IoSlice::new(b"cd"),
            IoSlice::new(b"ef"),
        ];
        let mut buf = [0u8; 16];

        assert_eq!(a.send_to_vectored(&parts, &b_addr).await.unwrap(), 6);
        let (n, src) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"abcdef");
        assert_eq!(src, a_addr);

        assert_eq!(
            a.send_to_vectored(&[IoSlice::new(b"x")], &b_addr)
                .await
                .unwrap(),
            1
        );
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"x");

        assert_eq!(a.send_to_vectored(&[], &b_addr).await.unwrap(), 0);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), b.recv_from(&mut buf))
                .await
                .is_err(),
            "an empty iovec list must not put a datagram on the wire"
        );

        let c = UdpSocket::bind(loopback()).await.unwrap();
        c.connect(b_addr).await.unwrap();
        assert_eq!(c.peer_addr().unwrap(), b_addr);
        assert_eq!(c.send_vectored(&parts).await.unwrap(), 6);
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"abcdef");
        assert_eq!(c.send_vectored(&[IoSlice::new(b"y")]).await.unwrap(), 1);
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"y");
        assert_eq!(c.send_vectored(&[]).await.unwrap(), 0);
    }

    /// Every receive path commits the bytes it read — `recv`/`recv_buf` and
    /// `recv_from`/`recv_buf_from` must advance the caller's buffer, or the
    /// returned length points at bytes the buffer does not contain.
    #[tokio::test]
    async fn recv_paths_commit_the_bytes_and_try_operations_reach_the_peer() {
        let a = UdpSocket::bind(loopback()).await.unwrap();
        let b = UdpSocket::bind(loopback()).await.unwrap();
        let b_addr = b.local_addr().unwrap();
        let a_addr = a.local_addr().unwrap();
        a.connect(b_addr).await.unwrap();

        a.send(b"hello").await.unwrap();
        assert!(b.readable().await.is_ok());
        let mut buf = bytes::BytesMut::with_capacity(64);
        assert_eq!(b.recv_buf(&mut buf).await.unwrap(), 5);
        assert_eq!(&buf[..], b"hello");

        assert_eq!(a.try_send(b"one").unwrap(), 3);
        let mut raw = [0u8; 8];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match b.try_recv(&mut raw) {
                    Ok(n) => return n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        b.readable().await.unwrap();
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        })
        .await
        .expect("the datagram never became receivable");
        assert_eq!(n, 3);
        assert_eq!(&raw[..n], b"one");

        assert_eq!(b.try_send_to(b"two", &a_addr).unwrap(), 3);
        let n = a.recv(&mut raw).await.unwrap();
        assert_eq!(&raw[..n], b"two");

        b.send_to_vectored(&[IoSlice::new(b"world")], &a_addr)
            .await
            .unwrap();
        let mut buf = bytes::BytesMut::with_capacity(64);
        let (n, src) = a.recv_buf_from(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..], b"world");
        assert_eq!(src, b_addr);

        assert!(b.writable().await.is_ok());
    }

    /// The option surface delegates to the tokio socket, and `try_clone_std`
    /// only has an answer where a borrowed handle can be duplicated: Windows
    /// duplicates it, every other platform refuses rather than hand back a
    /// socket that is not the same one.
    #[tokio::test]
    async fn options_multicast_and_clone() {
        let sock = UdpSocket::bind(loopback()).await.unwrap();
        sock.set_ttl_v4(64).unwrap();
        assert_eq!(sock.ttl_v4().unwrap(), 64);
        sock.set_broadcast(true).unwrap();
        assert!(sock.broadcast().unwrap());
        sock.set_broadcast(false).unwrap();
        assert!(!sock.broadcast().unwrap());
        sock.set_multicast_loop_v4(true).unwrap();

        let group = Ipv4Addr::new(239, 255, 0, 1);
        sock.join_multicast_v4(&group, &Ipv4Addr::LOCALHOST)
            .unwrap();
        sock.leave_multicast_v4(&group, &Ipv4Addr::LOCALHOST)
            .unwrap();

        #[cfg(not(windows))]
        assert_eq!(
            sock.try_clone_std().unwrap_err().kind(),
            std::io::ErrorKind::Unsupported,
            "only the Windows backend can duplicate a borrowed socket handle"
        );
    }
}
