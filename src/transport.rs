//! A clock and datagram interface: fake networks advance virtual time without sleeping.
use std::{
    io,
    net::{SocketAddr, UdpSocket},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub struct Datagram {
    pub bytes: Vec<u8>,
    pub from: SocketAddr,
    pub received_at: Duration,
}

pub trait PacketConn {
    fn now(&self) -> Duration;
    fn unix_ns(&self) -> io::Result<i64>;
    fn send_to(&mut self, bytes: &[u8], target: SocketAddr) -> io::Result<()>;
    /// Return one datagram or advance/wait until the monotonic deadline.
    fn receive_until(&mut self, deadline: Duration) -> io::Result<Option<Datagram>>;
}

pub struct RealUdpConn {
    socket: UdpSocket,
    start: Instant,
    buffer: Vec<u8>,
}
impl RealUdpConn {
    pub fn connect(target: SocketAddr) -> io::Result<Self> {
        let bind = if target.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind)?;
        socket.connect(target)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            start: Instant::now(),
            buffer: vec![0; 65_535],
        })
    }
}
impl PacketConn for RealUdpConn {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
    fn unix_ns(&self) -> io::Result<i64> {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        i64::try_from(ns).map_err(io::Error::other)
    }
    fn send_to(&mut self, bytes: &[u8], _target: SocketAddr) -> io::Result<()> {
        let sent = self.socket.send(bytes)?;
        if sent != bytes.len() {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "partial UDP send"));
        }
        Ok(())
    }
    fn receive_until(&mut self, deadline: Duration) -> io::Result<Option<Datagram>> {
        loop {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((len, from)) => {
                    return Ok(Some(Datagram {
                        bytes: self.buffer[..len].to_vec(),
                        from,
                        received_at: self.now(),
                    }));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
            let now = self.now();
            if now >= deadline {
                return Ok(None);
            }
            thread::sleep((deadline - now).min(Duration::from_micros(100)));
        }
    }
}
