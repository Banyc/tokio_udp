#[cfg(unix)]
mod unix;

#[cfg(any(not(unix), test))]
mod fallback;

#[cfg(unix)]
pub use unix::UdpSocket;

#[cfg(not(unix))]
pub use fallback::UdpSocket;
