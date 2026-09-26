#[cfg(unix)]
mod unix;

#[cfg(any(not(unix), test))]
mod fallback;

#[cfg(unix)]
pub use unix::UdpSocket;

#[cfg(not(unix))]
pub use fallback::UdpSocket;

#[cfg(test)]
mod tests {
    use super::UdpSocket;
    use std::marker::PhantomData;

    /// Two values of the same generic parameter type check only when the types
    /// are identical, so this turns a backend identity into a compile error
    /// rather than a silent substitution.
    fn assert_same_type<T>(_: PhantomData<T>, _: PhantomData<T>) {}

    /// The exported `UdpSocket` must *be* the backend that
    /// [`is_vectored_supported`](crate::is_vectored_supported) describes: the
    /// `sendmsg(2)` module on Unix, where the predicate reports `true` and the
    /// crate's contract is one zero-copy system call, and the concatenating
    /// fallback elsewhere. Without this the predicate is a self-comparison
    /// against `cfg!(unix)`: routing Unix through the fallback leaves it
    /// returning `true` while the crate no longer issues a vectored syscall.
    /// The Unix assertion running here also fixes *which* module this host's
    /// multi-buffer fidelity tests exercise.
    #[cfg(unix)]
    #[test]
    fn the_compiled_backend_is_the_unix_vectored_one() {
        assert_same_type::<UdpSocket>(PhantomData, PhantomData::<super::unix::UdpSocket>);
        assert!(
            crate::is_vectored_supported(),
            "the unix backend is the vectored one, so the predicate must be true"
        );
    }

    /// The non-Unix half of the same identity, compiled only where the fallback
    /// is the exported backend: no `sendmsg(2)`, so the predicate must be `false`
    /// and `UdpSocket` must be the concatenating module.
    #[cfg(not(unix))]
    #[test]
    fn the_compiled_backend_is_the_concatenating_fallback() {
        assert_same_type::<UdpSocket>(PhantomData, PhantomData::<super::fallback::UdpSocket>);
        assert!(
            !crate::is_vectored_supported(),
            "the fallback concatenates, so the predicate must be false"
        );
    }
}
