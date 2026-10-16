//! Bounded echo policy is independent of sockets and testable with virtual time.
use crate::probe::{BASE_HEADER_SIZE, Kind, MAX_PAYLOAD, Packet};
use serde::Serialize;
use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, UdpSocket},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub max_echo: u16,
    pub rate_limit: u32,
    pub token: Option<u64>,
    pub max_peers: usize,
}
impl ServerConfig {
    pub fn validate(&self) -> Result<(), String> {
        let minimum = BASE_HEADER_SIZE + if self.token.is_some() { 8 } else { 0 };
        if !(minimum..=MAX_PAYLOAD).contains(&self.max_echo) {
            return Err(format!("max-echo must be {minimum}..{MAX_PAYLOAD}"));
        }
        if !(1..=100_000).contains(&self.rate_limit) {
            return Err("rate-limit must be 1..100000".into());
        }
        if !(1..=100_000).contains(&self.max_peers) {
            return Err("max-peers must be 1..100000".into());
        }
        Ok(())
    }
}
#[derive(Default, Debug, Serialize)]
pub struct ServerStats {
    pub received: u64,
    pub malformed: u64,
    pub unauthorized: u64,
    pub oversized: u64,
    pub rate_limited: u64,
    pub peer_limit_dropped: u64,
    pub echoed: u64,
    pub received_bytes: u64,
    pub echoed_bytes: u64,
    pub send_errors: u64,
}

pub struct EchoServer {
    cfg: ServerConfig,
    pub stats: ServerStats,
    peers: HashMap<SocketAddr, Duration>,
    next_global: Duration,
}
impl EchoServer {
    pub fn new(cfg: ServerConfig) -> Result<Self, String> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: ServerStats::default(),
            peers: HashMap::new(),
            next_global: Duration::ZERO,
        })
    }
    /// Counters cover accepted datagrams; caller records the actual send result.
    pub fn handle(&mut self, bytes: &[u8], from: SocketAddr, now: Duration) -> Option<Vec<u8>> {
        self.stats.received += 1;
        self.stats.received_bytes += bytes.len() as u64;
        let packet = match Packet::decode(bytes) {
            Ok(p) if p.kind == Kind::Request => p,
            _ => {
                self.stats.malformed += 1;
                return None;
            }
        };
        if packet.token != self.cfg.token {
            self.stats.unauthorized += 1;
            return None;
        }
        let response = packet.response();
        if response.size > self.cfg.max_echo {
            self.stats.oversized += 1;
            return None;
        }
        if now < self.next_global || self.peers.get(&from).is_some_and(|next| now < *next) {
            self.stats.rate_limited += 1;
            return None;
        }
        if !self.peers.contains_key(&from) && self.peers.len() >= self.cfg.max_peers {
            self.peers
                .retain(|_, next| now.saturating_sub(*next) < Duration::from_secs(60));
            if self.peers.len() >= self.cfg.max_peers {
                self.stats.peer_limit_dropped += 1;
                return None;
            }
        }
        let next = now + Duration::from_secs_f64(1.0 / f64::from(self.cfg.rate_limit));
        self.peers.insert(from, next);
        self.next_global = next;
        response.encode().ok()
    }
    pub fn record_send(&mut self, result: io::Result<usize>, expected: usize) {
        match result {
            Ok(len) if len == expected => {
                self.stats.echoed += 1;
                self.stats.echoed_bytes += len as u64;
            }
            _ => self.stats.send_errors += 1,
        }
    }
}

pub fn serve(
    socket: UdpSocket,
    cfg: ServerConfig,
    stop: &AtomicBool,
    duration: Option<Duration>,
) -> io::Result<ServerStats> {
    let mut server =
        EchoServer::new(cfg).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    socket.set_read_timeout(Some(Duration::from_millis(50)))?;
    socket.set_write_timeout(Some(Duration::from_millis(50)))?;
    let mut buffer = vec![0; 65_535];
    let start = Instant::now();
    let mut next_log = Duration::from_secs(5);
    while !stop.load(Ordering::Relaxed) && duration.is_none_or(|d| start.elapsed() < d) {
        match socket.recv_from(&mut buffer) {
            Ok((len, from)) => {
                if let Some(response) = server.handle(&buffer[..len], from, start.elapsed()) {
                    server.record_send(socket.send_to(&response, from), response.len());
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(e) => return Err(e),
        }
        if start.elapsed() >= next_log {
            eprintln!("server_stats {}", serde_json::to_string(&server.stats)?);
            next_log = start.elapsed() + Duration::from_secs(5);
        }
    }
    Ok(server.stats)
}
