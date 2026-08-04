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
