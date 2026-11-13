//! Owned, request-local transport I/O with bounded cancellation polls.
//!
//! No fd flags, socket timeouts, global stdio locks or process interrupt flags
//! are changed. This reader never invents EOF on cancellation or timeout. Only
//! the protocol's command/single-pack parser can establish a receive boundary.

use crate::{Result, invalid, process};
use git_rs_core::{Control, StopReason};
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fs::{FileType, fstat},
    io::Errno,
    net::{RecvFlags, SendFlags, SocketType, recv, send, sockopt},
};
use std::{
    io::{self, Read},
    os::{
        fd::{AsFd, OwnedFd},
        unix::net::UnixStream,
    },
    sync::Arc,
    time::{Duration, Instant},
};

const QUANTUM: Duration = Duration::from_millis(10);
const CHUNK: usize = 32 * 1024;

struct State {
    operation: Arc<process::Operation>,
    control: Control,
}

/// Clones cancel the same bounded I/O, not the process or unrelated requests.
/// CPU admissions create this once and share its exact identity with the reader
/// and executor. The bridge can create one without constructing any CPU runtime.
#[derive(Clone)]
pub struct IoControl(Arc<State>);

impl IoControl {
    pub fn new(operation: Arc<process::Operation>) -> Result<Self> {
        operation.check()?;
        if operation
            .deadline()
            .saturating_duration_since(Instant::now())
            > Duration::from_secs(process::MAX_OPERATION_SECONDS)
        {
            return Err(invalid("transport requires a bounded operation"));
        }
        let control = Control::new(operation.deadline());
        Ok(Self(Arc::new(State { operation, control })))
    }

    pub fn cancel(&self) {
        self.0.control.cancel();
    }

    pub(crate) fn control(&self) -> &Control {
        &self.0.control
    }

    pub(crate) fn operation(&self) -> &Arc<process::Operation> {
        &self.0.operation
    }

    fn shares_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn check(&self) -> io::Result<()> {
        if self.0.operation.check().is_err() {
            self.cancel();
        }
        self.0.control.check().map_err(|reason| {
            let kind = match reason {
                StopReason::Deadline => io::ErrorKind::TimedOut,
                // Interrupted is deliberately NOT used: Read::read_exact and
                // std::io::copy retry it, which would hide cancellation forever.
                StopReason::Cancelled => io::ErrorKind::ConnectionAborted,
            };
            io::Error::new(kind, reason)
        })
    }

    fn ready(&self, fd: &impl AsFd, events: PollFlags) -> io::Result<()> {
        loop {
            self.check()?;
            let timeout = Timespec::try_from(self.0.control.remaining().min(QUANTUM))
                .map_err(io::Error::other)?;
            let mut descriptors = [PollFd::new(fd, events)];
            match poll(&mut descriptors, Some(&timeout)) {
                Ok(_) => {
                    self.check()?;
                    let ready = descriptors[0].revents();
                    if ready.contains(PollFlags::NVAL) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid transport fd",
                        ));
                    }
                    // HUP/ERR must be passed to read/recv/send to obtain EOF or
                    // the actual socket error, not mistaken for an idle poll.
                    if ready.intersects(events | PollFlags::HUP | PollFlags::ERR) {
                        return Ok(());
                    }
                }
                Err(Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// One owned stream socket or an EXCLUSIVELY READ pipe. Closing this value closes
/// only its owned descriptor; it never calls shutdown or closes process stdin.
///
/// A pipe must have no concurrent reader, including std::io::stdin().lock() or
/// another fd duplicate. This is a trusted transport-ownership precondition:
/// after poll indicates data, an exclusive pipe read returns available bytes.
/// Without exclusivity another reader could steal readiness and make read block.
/// Socket reads use per-call MSG_DONTWAIT and do not require that precondition.
/// Regular files, terminals/devices and datagrams are rejected, not assumed to
/// have bounded read latency. No O_NONBLOCK change is made on a shared fd.
pub struct ReceiveInput {
    fd: OwnedFd,
    kind: FileType,
    control: IoControl,
}

impl ReceiveInput {
    pub fn new(fd: OwnedFd, control: IoControl) -> io::Result<Self> {
        control.check()?;
        let kind = FileType::from_raw_mode(fstat(&fd)?.st_mode);
        match kind {
            FileType::Fifo => {}
            FileType::Socket if sockopt::socket_type(&fd)? == SocketType::STREAM => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "receive input requires a stream socket or exclusive pipe",
                ));
            }
        }
        Ok(Self { fd, kind, control })
    }

    /// SSH bridge only: duplicate stdin without closing it, locking it, changing
    /// its flags, or starting a thread. Replace the old stdin reader first;
    /// exclusivity is required for the lifetime of this duplicate.
    pub fn stdin(control: IoControl) -> io::Result<Self> {
        Self::new(io::stdin().as_fd().try_clone_to_owned()?, control)
    }

    pub(crate) fn belongs_to(&self, control: &IoControl) -> bool {
        self.control.shares_with(control)
    }

    /// Bridge forwarding on its existing transport thread, NOT a new CPU pool.
    /// Both idle input and a full backend send buffer remain cancellable. Merely
    /// cancelling reads around std::io::copy would leave a blocked write alive.
    /// The caller owns output and decides whether actual EOF warrants SHUT_WR.
    pub fn forward_to(mut self, output: &UnixStream) -> io::Result<u64> {
        let mut buffer = [0; CHUNK];
        let mut copied = 0;
        loop {
            let count = self.read(&mut buffer)?;
            if count == 0 {
                return Ok(copied);
            }
            let mut offset = 0;
            while offset < count {
                self.control.ready(output, PollFlags::OUT)?;
                match send(
                    output,
                    &buffer[offset..count],
                    SendFlags::DONTWAIT | SendFlags::NOSIGNAL,
                ) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(written) => {
                        self.control.check()?;
                        offset += written;
                        copied += written as u64;
                    }
                    Err(Errno::INTR | Errno::AGAIN) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
}

impl Read for ReceiveInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer.len().min(CHUNK);
        loop {
            self.control.ready(&self.fd, PollFlags::IN)?;
            let result = match self.kind {
                FileType::Socket => recv(&self.fd, &mut buffer[..limit], RecvFlags::DONTWAIT)
                    .map(|(count, _)| count),
                FileType::Fifo => rustix::io::read(&self.fd, &mut buffer[..limit]),
                _ => unreachable!("constructor checks descriptor kind"),
            };
            match result {
                Ok(count) => {
                    self.control.check()?;
                    return Ok(count);
                }
                Err(Errno::INTR | Errno::AGAIN) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IoControl, ReceiveInput};
    use crate::process::Operation;
    use rustix::{
        fs::fcntl_getfl,
        io::{Errno, ioctl_fionread},
        net::{SendFlags, send},
    };
    use std::{
        io::{self, Read, Write},
        os::{
            fd::AsFd,
            unix::net::{UnixDatagram, UnixStream},
        },
        sync::mpsc,
        time::{Duration, Instant},
    };

    #[test]
    fn idle_socket_deadline_is_an_error_not_a_fabricated_eof() {
        let (server, peer) = UnixStream::pair().unwrap();
        let operation = Operation::until(Instant::now() + Duration::from_secs(1));
        let mut input =
            ReceiveInput::new(server.into(), IoControl::new(operation).unwrap()).unwrap();
        let (returned, finished) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            returned.send(input.read(&mut [0])).unwrap();
        });
        let result = finished.recv_timeout(Duration::from_secs(3));
        // Also releases a broken uninterruptible reader before joining on test
        // failure, rather than leaving the test process with an owned I/O thread.
        drop(peer);
        reader.join().unwrap();
        let error = result
            .expect("deadline did not stop idle read")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn cancellation_interrupts_read_exact_without_closing_or_changing_the_protected_pipe() {
        let (protected, writer) = rustix::pipe::pipe().unwrap();
        let flags = fcntl_getfl(&protected).unwrap();
        let control = IoControl::new(Operation::new(5)).unwrap();
        let mut input = ReceiveInput::new(
            protected.as_fd().try_clone_to_owned().unwrap(),
            control.clone(),
        )
        .unwrap();
        let (entered, started) = mpsc::channel();
        let (returned, finished) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            entered.send(()).unwrap();
            returned.send(input.read_exact(&mut [0])).unwrap();
        });
        started.recv_timeout(Duration::from_secs(1)).unwrap();
        control.cancel();
        let result = finished.recv_timeout(Duration::from_secs(2));
        if result.is_err() {
            drop(writer);
            reader.join().unwrap();
            panic!("cancel did not stop the protected-pipe reader");
        }
        reader.join().unwrap();
        assert_eq!(
            result.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(fcntl_getfl(&protected).unwrap(), flags);
        rustix::io::write(&writer, b"x").unwrap();
        let mut byte = [0];
        assert_eq!(rustix::io::read(&protected, &mut byte).unwrap(), 1);
        assert_eq!(&byte, b"x", "only the owned duplicate may close");
    }

    #[test]
    fn short_real_stream_eof_is_not_accepted_as_a_complete_frame() {
        let (server, mut peer) = UnixStream::pair().unwrap();
        let mut input =
            ReceiveInput::new(server.into(), IoControl::new(Operation::new(2)).unwrap()).unwrap();
        peer.write_all(b"ab").unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        assert_eq!(
            input.read_exact(&mut [0; 4]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn unsupported_device_and_datagram_inputs_are_rejected_before_read() {
        let control = IoControl::new(Operation::new(2)).unwrap();
        let device = std::fs::File::open("/dev/null").unwrap();
        assert!(
            matches!(ReceiveInput::new(device.into(), control.clone()), Err(error)
                         if error.kind() == io::ErrorKind::InvalidInput)
        );
        let (socket, _peer) = UnixDatagram::pair().unwrap();
        assert!(
            matches!(ReceiveInput::new(socket.into(), control), Err(error)
                         if error.kind() == io::ErrorKind::InvalidInput)
        );
    }

    #[test]
    fn bridge_cancel_stops_a_full_socket_write_and_preserves_original_stdin_description() {
        let (backend, peer) = UnixStream::pair().unwrap();
        let output_flags = fcntl_getfl(&backend).unwrap();
        // No receiver drains this real socket: establish actual backpressure.
        loop {
            match send(
                &backend,
                &[0; 4096],
                SendFlags::DONTWAIT | SendFlags::NOSIGNAL,
            ) {
                Ok(count) => assert!(count > 0),
                Err(Errno::AGAIN) => break,
                Err(error) => panic!("fill send buffer: {error}"),
            }
        }
        let (protected, writer) = rustix::pipe::pipe().unwrap();
        let input_flags = fcntl_getfl(&protected).unwrap();
        rustix::io::write(&writer, b"pending bytes").unwrap();
        let control = IoControl::new(Operation::new(5)).unwrap();
        let input = ReceiveInput::new(
            protected.as_fd().try_clone_to_owned().unwrap(),
            control.clone(),
        )
        .unwrap();
        let forwarding = backend.try_clone().unwrap();
        let (returned, finished) = mpsc::channel();
        let pump = std::thread::spawn(move || {
            returned.send(input.forward_to(&forwarding)).unwrap();
        });
        let limit = Instant::now() + Duration::from_secs(2);
        while ioctl_fionread(&protected).unwrap() != 0 && Instant::now() < limit {
            std::thread::yield_now();
        }
        let consumed = ioctl_fionread(&protected).unwrap() == 0;
        assert!(matches!(
            finished.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        control.cancel();
        let result = finished.recv_timeout(Duration::from_secs(2));
        drop(peer); // release an incorrectly blocked send before failure join
        pump.join().unwrap();
        assert!(consumed, "bridge did not read the staged stdin bytes");
        assert_eq!(
            result
                .expect("cancel did not stop full write")
                .unwrap_err()
                .kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(fcntl_getfl(&protected).unwrap(), input_flags);
        assert_eq!(fcntl_getfl(&backend).unwrap(), output_flags);
        rustix::io::write(&writer, b"s").unwrap();
        let mut byte = [0];
        assert_eq!(rustix::io::read(&protected, &mut byte).unwrap(), 1);
        assert_eq!(&byte, b"s");
    }
}
