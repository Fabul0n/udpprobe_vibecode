use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use udpprobe::{
    client::{TrialConfig, run_trial},
    probe::{ECHO_SMALL, Kind, MAX_PAYLOAD, NO_PADDING, Packet, TOKEN_PRESENT},
    server::{EchoServer, ServerConfig},
    transport::{Datagram, PacketConn},
};

fn target() -> SocketAddr {
    "127.0.0.1:9999".parse().unwrap()
}
fn peer() -> SocketAddr {
    "127.0.0.1:12345".parse().unwrap()
}
fn config() -> TrialConfig {
    TrialConfig {
        target: target(),
        size: 1200,
        count: 5,
        rate: 100,
        timeout: Duration::from_millis(50),
        token: None,
        echo_small: false,
    }
}
fn server_config() -> ServerConfig {
    ServerConfig {
        max_echo: MAX_PAYLOAD,
        rate_limit: 100_000,
        token: None,
        max_peers: 16,
    }
}

#[derive(Default)]
enum Scenario {
    #[default]
    Echo,
    LossDuplicateReorder,
    Late,
    Invalid,
    SendError,
    Blackhole,
}
struct FakeNetwork {
    now: Duration,
    events: Vec<Datagram>,
    server: EchoServer,
    scenario: Scenario,
    cancel: Option<(Duration, Arc<AtomicBool>)>,
    sent_at: Vec<Duration>,
}
impl FakeNetwork {
    fn new(scenario: Scenario) -> Self {
        Self {
            now: Duration::ZERO,
            events: Vec::new(),
            server: EchoServer::new(server_config()).unwrap(),
            scenario,
            cancel: None,
            sent_at: Vec::new(),
        }
    }
    fn enqueue(&mut self, bytes: Vec<u8>, delay_ms: u64, from: SocketAddr) {
        self.events.push(Datagram {
            bytes,
            from,
            received_at: self.now + Duration::from_millis(delay_ms),
        });
    }
}
impl PacketConn for FakeNetwork {
    fn now(&self) -> Duration {
        self.now
    }
    fn unix_ns(&self) -> io::Result<i64> {
        Ok(1_000_000_000 + self.now.as_nanos() as i64)
    }
    fn send_to(&mut self, bytes: &[u8], _: SocketAddr) -> io::Result<()> {
        self.sent_at.push(self.now);
        let request = Packet::decode(bytes).unwrap();
        if matches!(self.scenario, Scenario::SendError) && request.seq == 0 {
            return Err(io::Error::other("injected send failure"));
        }
        if matches!(self.scenario, Scenario::Blackhole) {
            return Ok(());
        }
        let Some(response) = self.server.handle(bytes, peer(), self.now) else {
            return Ok(());
        };
        self.server.record_send(Ok(response.len()), response.len());
        match self.scenario {
            Scenario::LossDuplicateReorder => match request.seq {
                0 => self.enqueue(response, 30, target()),
                2 => {}
                3 => {
                    self.enqueue(response.clone(), 5, target());
                    self.enqueue(response, 6, target());
                }
                _ => self.enqueue(response, 5, target()),
            },
            Scenario::Late if request.seq == 0 => self.enqueue(response, 60, target()),
            Scenario::Invalid => {
                self.enqueue(vec![1, 2], 1, target());
                self.enqueue(response.clone(), 2, peer());
                let mut wrong = Packet::decode(&response).unwrap();
                wrong.send_ts += 1;
                self.enqueue(wrong.encode().unwrap(), 3, target());
                self.enqueue(response, 5, target());
            }
            _ => self.enqueue(response, 5, target()),
        }
        Ok(())
    }
    fn receive_until(&mut self, deadline: Duration) -> io::Result<Option<Datagram>> {
        if let Some((at, flag)) = &self.cancel
            && deadline >= *at
        {
            self.now = *at;
            flag.store(true, Ordering::Relaxed);
            return Ok(None);
        }
        let earliest = self
            .events
            .iter()
            .enumerate()
            .min_by_key(|(_, d)| d.received_at)
            .map(|(i, d)| (i, d.received_at));
        if let Some((index, at)) = earliest
            && at <= deadline
        {
            self.now = at;
            return Ok(Some(self.events.remove(index)));
        }
        self.now = deadline;
        Ok(None)
    }
}

#[test]
fn loss_duplicates_reorder_have_independent_denominators() {
    let mut network = FakeNetwork::new(Scenario::LossDuplicateReorder);
    let report = run_trial(&mut network, &config(), &AtomicBool::new(false)).unwrap();
    assert_eq!(
        (report.sent, report.received_unique, report.lost),
        (5, 4, 1)
    );
    assert_eq!((report.duplicates, report.reordered), (1, 1));
    assert_eq!(report.loss_ratio, Some(0.2));
    assert_eq!(report.duplicate_ratio, Some(0.2));
    assert_eq!(report.reorder_ratio, Some(0.25));
    assert_eq!(report.rtt.as_ref().unwrap().p95_ms, 30.0);
    assert_eq!(report.elapsed_ms, 90.0);
    assert_eq!(report.achieved_rate_pps, 100.0);
    assert!(
        network
            .sent_at
            .windows(2)
            .all(|w| w[1] - w[0] == Duration::from_millis(10))
    );
}
#[test]
fn late_responses_do_not_resurrect_expired_probes() {
    let r = run_trial(
        &mut FakeNetwork::new(Scenario::Late),
        &config(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!((r.lost, r.late_responses, r.received_unique), (1, 1, 4));
}
#[test]
fn source_timestamp_and_wire_validation_precede_statistics() {
    let r = run_trial(
        &mut FakeNetwork::new(Scenario::Invalid),
        &config(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(r.invalid_responses, 15);
    assert_eq!(r.received_unique, 5);
    assert_eq!(r.duplicates, 0);
}
#[test]
fn send_failures_are_not_network_loss() {
    let r = run_trial(
        &mut FakeNetwork::new(Scenario::SendError),
        &config(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!((r.attempted, r.sent, r.send_errors, r.lost), (5, 4, 1, 0));
}
#[test]
fn no_responses_have_null_rtt_and_finite_json() {
    let r = run_trial(
        &mut FakeNetwork::new(Scenario::Blackhole),
        &config(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(r.loss_ratio, Some(1.0));
    assert!(r.rtt.is_none());
    assert_eq!(r.goodput_bytes_per_second, 0.0);
    assert!(serde_json::to_value(r).unwrap()["rtt"].is_null());
}
#[test]
fn cancellation_does_not_label_pending_requests_as_loss() {
    let flag = Arc::new(AtomicBool::new(false));
    let mut network = FakeNetwork::new(Scenario::Blackhole);
    network.cancel = Some((Duration::from_millis(15), Arc::clone(&flag)));
    let r = run_trial(&mut network, &config(), &flag).unwrap();
    assert!(r.cancelled);
    assert_eq!((r.sent, r.lost, r.unanswered_at_cancel), (2, 0, 2));
}
#[test]
fn fake_server_limits_explain_observed_losses() {
    let mut network = FakeNetwork::new(Scenario::Echo);
    network.server = EchoServer::new(ServerConfig {
        rate_limit: 50,
        ..server_config()
    })
    .unwrap();
    let r = run_trial(&mut network, &config(), &AtomicBool::new(false)).unwrap();
    assert_eq!((r.received_unique, r.lost), (3, 2));
    assert_eq!(network.server.stats.rate_limited, 2);
}
#[test]
fn small_echo_crosses_server_response_size_limit() {
    let mut network = FakeNetwork::new(Scenario::Echo);
    network.server = EchoServer::new(ServerConfig {
        max_echo: 100,
        ..server_config()
    })
    .unwrap();
    let mut cfg = config();
    cfg.echo_small = true;
    let r = run_trial(&mut network, &cfg, &AtomicBool::new(false)).unwrap();
    assert_eq!(r.received_unique, 5);
    assert!((r.goodput_bytes_per_second - 115.0 / 0.09).abs() < 1e-6);
}
#[test]
fn server_rejects_tokens_malformed_and_oversized_packets() {
    let mut server = EchoServer::new(ServerConfig {
        token: Some(42),
        max_echo: 100,
        ..server_config()
    })
    .unwrap();
    let cfg = config();
    assert!(server.handle(&[0; 4], peer(), Duration::ZERO).is_none());
    assert!(
        server
            .handle(&cfg.packet(0, 0).encode().unwrap(), peer(), Duration::ZERO)
            .is_none()
    );
    let mut request = cfg.packet(0, 0);
    request.token = Some(42);
    request.flags = TOKEN_PRESENT;
    assert!(
        server
            .handle(&request.encode().unwrap(), peer(), Duration::ZERO)
            .is_none()
    );
    request.flags |= ECHO_SMALL;
    let response = server
        .handle(&request.encode().unwrap(), peer(), Duration::ZERO)
        .unwrap();
    let decoded = Packet::decode(&response).unwrap();
    assert_eq!(
        (decoded.size, decoded.kind, decoded.token),
        (31, Kind::Response, Some(42))
    );
    assert_eq!(
        (
            server.stats.malformed,
            server.stats.unauthorized,
            server.stats.oversized
        ),
        (1, 1, 1)
    );
}
#[test]
fn bounded_peer_table_expires_old_entries() {
    let mut server = EchoServer::new(ServerConfig {
        max_peers: 1,
        ..server_config()
    })
    .unwrap();
    let request = config().packet(0, 0).encode().unwrap();
    assert!(server.handle(&request, peer(), Duration::ZERO).is_some());
    assert!(
        server
            .handle(&request, target(), Duration::from_secs(1))
            .is_none()
    );
    assert_eq!(server.stats.peer_limit_dropped, 1);
    assert!(
        server
            .handle(&request, target(), Duration::from_secs(61))
            .is_some()
    );
}
#[test]
fn codec_rejects_truncation_unknown_fields_and_length_mismatches() {
    let packet = config().packet(10, -123);
    let wire = packet.encode().unwrap();
    assert_eq!(Packet::decode(&wire).unwrap(), packet);
    for end in 0..wire.len() {
        assert!(Packet::decode(&wire[..end]).is_err());
    }
    for (index, value) in [(0, 0), (2, 99), (3, 99), (4, 128), (22, 0)] {
        let mut bad = wire.clone();
        bad[index] = value;
        assert!(Packet::decode(&bad).is_err());
    }
    let mut with_token = packet;
    with_token.token = Some(42);
    with_token.flags = TOKEN_PRESENT;
    let wire = with_token.encode().unwrap();
    assert_eq!(Packet::decode(&wire).unwrap(), with_token);
    for end in 23..31 {
        assert!(Packet::decode(&wire[..end]).is_err());
    }
}
#[test]
fn padding_flags_and_maximum_sizes_are_validated() {
    let mut packet = config().packet(0, 0);
    packet.flags = NO_PADDING;
    assert!(packet.encode().is_err());
    packet.size = 23;
    assert_eq!(packet.encode().unwrap().len(), 23);
    packet.flags = 0;
    packet.size = MAX_PAYLOAD;
    assert!(Packet::decode(&packet.encode().unwrap()).is_ok());
    packet.size = MAX_PAYLOAD + 1;
    assert!(packet.encode().is_err());
}
#[test]
fn config_rejects_unbounded_or_impossible_trials() {
    let mut cfg = config();
    cfg.count = 0;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.rate = 0;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.size = 23;
    cfg.token = Some(1);
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.timeout = Duration::ZERO;
    assert!(cfg.validate().is_err());
}
