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

mod imp;

/// Returns `true` on platforms where `sendmsg(2)` is available and
/// [`send_vectored`](UdpSocket::send_vectored) /
/// [`send_to_vectored`](UdpSocket::send_to_vectored) issue a single
/// zero-copy system call.
///
/// Returns `false` on platforms where the vectored methods fall back to
/// concatenating all buffers into a temporary `Vec<u8>` before calling
/// the kernel's single-buffer send path.
#[allow(dead_code)]
async fn every_platform_has_the_whole_api(
    socket: &UdpSocket,
    addr: std::net::SocketAddr,
    group: std::net::Ipv4Addr,
    buf: &mut [u8],
    bufs: &[std::io::IoSlice<'_>],
    growable: &mut Vec<u8>,
) -> std::io::Result<()> {
    let _: UdpSocket = UdpSocket::bind(addr).await?;
    let _: std::net::SocketAddr = socket.local_addr()?;
    let _: std::net::SocketAddr = socket.peer_addr()?;
    socket.connect(addr).await?;
    socket.set_broadcast(true)?;
    let _: bool = socket.broadcast()?;
    socket.set_ttl_v4(1)?;
    let _: u32 = socket.ttl_v4()?;
    socket.set_multicast_loop_v4(true)?;
    socket.join_multicast_v4(&group, &group)?;
    socket.leave_multicast_v4(&group, &group)?;
    socket.readable().await?;
    socket.writable().await?;
    let _: usize = socket.send(buf).await?;
    let _: usize = socket.send_vectored(bufs).await?;
    let _: usize = socket.send_to_vectored(bufs, &addr).await?;
    let _: usize = socket.recv(buf).await?;
    let _: (usize, std::net::SocketAddr) = socket.recv_from(buf).await?;
    let _: usize = socket.try_send(buf)?;
    let _: usize = socket.try_send_to(buf, &addr)?;
    let _: usize = socket.try_recv(buf)?;
    let _: usize = socket.recv_buf(growable).await?;
    let _: (usize, std::net::SocketAddr) = socket.recv_buf_from(growable).await?;
    let _: std::net::UdpSocket = socket.try_clone_std()?;
    Ok(())
}

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
        client.connect(server_addr).await.unwrap();

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

    #[tokio::test(flavor = "multi_thread")]
    async fn two_tasks_can_await_recv_on_one_socket() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = std::sync::Arc::new(UdpSocket::bind(bind).await.unwrap());
        let server_addr = server.local_addr().unwrap();
        let client = UdpSocket::bind(bind).await.unwrap();
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..2 {
            let server = server.clone();
            readers.spawn(async move {
                let mut buf = [0u8; 8];
                let (n, _) = server.recv_from(&mut buf).await.unwrap();
                buf[..n].to_vec()
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        for msg in [b"a", b"b"] {
            client
                .send_to_vectored(&[std::io::IoSlice::new(&msg[..])], &server_addr)
                .await
                .unwrap();
        }
        let mut got = Vec::new();
        for _ in 0..2 {
            let one = tokio::time::timeout(std::time::Duration::from_secs(5), readers.join_next())
                .await
                .expect("a task awaiting recv was never woken");
            got.push(one.unwrap().unwrap());
        }
        got.sort();
        assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_tasks_can_await_send_on_one_socket() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = UdpSocket::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = std::sync::Arc::new(UdpSocket::bind(bind).await.unwrap());
        client.connect(server_addr).await.unwrap();
        let mut senders = tokio::task::JoinSet::new();
        for msg in [b"a", b"b"] {
            let client = client.clone();
            senders.spawn(async move { client.send(&msg[..]).await.unwrap() });
        }
        for _ in 0..2 {
            tokio::time::timeout(std::time::Duration::from_secs(5), senders.join_next())
                .await
                .expect("a task awaiting send was never woken")
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_would_block_try_recv_stops_claiming_the_socket_is_readable() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = UdpSocket::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = UdpSocket::bind(bind).await.unwrap();
        client
            .send_to_vectored(&[std::io::IoSlice::new(b"x")], &server_addr)
            .await
            .unwrap();
        server.readable().await.unwrap();
        let mut buf = [0u8; 8];
        let mut got = 0;
        loop {
            match server.try_recv(&mut buf) {
                Ok(n) => {
                    assert_eq!(&buf[..n], b"x");
                    got += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(got, 1);
        let again =
            tokio::time::timeout(std::time::Duration::from_millis(200), server.readable()).await;
        assert!(
            again.is_err(),
            "readable() returned with nothing left to read"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn try_send_on_a_socket_the_driver_has_not_polled_yet_still_sends() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = UdpSocket::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = UdpSocket::bind(bind).await.unwrap();
        client.connect(server_addr).await.unwrap();
        assert_eq!(client.try_send(b"probe").unwrap(), 5);
        let mut buf = [0u8; 8];
        let n = server.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"probe");
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
