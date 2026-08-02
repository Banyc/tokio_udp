#[cfg(unix)]
mod unix;

#[cfg(not(unix))]
mod fallback;

#[cfg(unix)]
pub use unix::UdpSocket;

#[cfg(not(unix))]
pub use fallback::UdpSocket;
