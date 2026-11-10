pub mod deflate;
pub mod inflate;

/// A request-local writer adapter which bounds each write to 32 KiB and checks
/// before and after accepted writes. In particular, `write_all()` retries of
/// `Interrupted` errors re-enter the check instead of spinning without control.
/// The inner writer must still bound individual blocking I/O calls itself.
pub struct ControlledWrite<'a, W> {
    inner: W,
    check: &'a dyn Fn() -> gix_error::Result,
}

impl<'a, W> ControlledWrite<'a, W> {
    /// Wrap `inner` with the borrowed request-local check.
    pub fn new(inner: W, check: &'a dyn Fn() -> gix_error::Result) -> Self {
        Self { inner, check }
    }
}

impl<W: std::io::Write> std::io::Write for ControlledWrite<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        (self.check)().map_err(std::io::Error::other)?;
        let n = self.inner.write(&bytes[..bytes.len().min(32 * 1024)])?;
        (self.check)().map_err(std::io::Error::other)?;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (self.check)().map_err(std::io::Error::other)?;
        self.inner.flush()?;
        (self.check)().map_err(std::io::Error::other)
    }
}
