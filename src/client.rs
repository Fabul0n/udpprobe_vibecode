//! Single-owner measurement state. No shared mutable sample array or background receiver.
use crate::{
    probe::{ECHO_SMALL, Kind, Packet, TOKEN_PRESENT},
    report::{Distribution, TrialReport},
    transport::PacketConn,
};
use std::{
    io,
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct TrialConfig {
    pub target: SocketAddr,
    pub size: u16,
    pub count: u32,
    pub rate: u32,
    pub timeout: Duration,
    pub token: Option<u64>,
    pub echo_small: bool,
}
impl TrialConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.target.port() == 0
            || self.target.ip().is_unspecified()
            || self.target.ip().is_multicast()
            || self.target.ip() == std::net::Ipv4Addr::BROADCAST
        {
            return Err("target must be a unicast address with a nonzero port".into());
        }
        if !(1..=1_000_000).contains(&self.count) {
            return Err("count must be 1..1000000".into());
        }
        if !(1..=100_000).contains(&self.rate) {
            return Err("rate must be 1..100000 pps".into());
        }
        if self.timeout < Duration::from_millis(1) || self.timeout > Duration::from_secs(60) {
            return Err("timeout must be 1..60000 ms".into());
        }
        if f64::from(self.count) / f64::from(self.rate) > 600.0 {
            return Err("one trial may schedule at most 600 seconds of traffic".into());
        }
        self.packet(0, 0).validate().map_err(|e| e.to_string())
    }
    pub fn packet(&self, seq: u64, send_ts: i64) -> Packet {
        Packet {
            kind: Kind::Request,
            flags: if self.echo_small { ECHO_SMALL } else { 0 }
                | if self.token.is_some() {
                    TOKEN_PRESENT
                } else {
                    0
                },
            seq,
            send_ts,
            size: self.size,
            token: self.token,
        }
    }
}

struct Sample {
    sent_at: Duration,
    timestamp: i64,
    rtt: Option<Duration>,
}

pub fn run_trial<C: PacketConn>(
    conn: &mut C,
    cfg: &TrialConfig,
    stop: &AtomicBool,
) -> io::Result<TrialReport> {
    cfg.validate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let interval = Duration::from_secs_f64(1.0 / f64::from(cfg.rate));
    let start = conn.now();
    let mut next_send = start;
    let mut finish = start;
    let mut first_sent = None;
    let mut last_sent = None;
    let mut highest_seq = None;
    let mut last_timestamp = i64::MIN;
    let mut samples: Vec<Option<Sample>> = Vec::with_capacity(cfg.count as usize);
    let mut good_bytes = 0_u64;
    let mut report = TrialReport {
        repeat: 1,
        payload_bytes: cfg.size,
        requested_rate_pps: cfg.rate,
        requested_count: cfg.count,
        timeout_ms: cfg.timeout.as_millis() as u64,
        echo_small: cfg.echo_small,
        token_enabled: cfg.token.is_some(),
        attempted: 0,
        sent: 0,
        send_errors: 0,
        first_send_error: None,
        receive_errors: 0,
        first_receive_error: None,
        received_unique: 0,
        lost: 0,
        unanswered_at_cancel: 0,
        loss_ratio: None,
        duplicates: 0,
        duplicate_ratio: None,
        reordered: 0,
        reorder_ratio: None,
        late_responses: 0,
        invalid_responses: 0,
        elapsed_ms: 0.0,
        send_window_ms: 0.0,
        achieved_rate_pps: 0.0,
        goodput_bytes_per_second: 0.0,
        rtt: None,
        cancelled: false,
    };
    loop {
        if stop.load(Ordering::Relaxed) {
            report.cancelled = true;
            break;
        }
        let now = conn.now();
        if samples.len() < cfg.count as usize && now >= next_send {
            let timestamp = conn.unix_ns()?.max(last_timestamp.saturating_add(1));
            last_timestamp = timestamp;
            let packet = cfg.packet(samples.len() as u64, timestamp);
            let bytes = packet.encode().map_err(io::Error::other)?;
            let sent_at = conn.now();
            report.attempted += 1;
            match conn.send_to(&bytes, cfg.target) {
                Ok(()) => {
                    report.sent += 1;
                    first_sent.get_or_insert(sent_at);
                    last_sent = Some(sent_at);
                    finish = sent_at + cfg.timeout;
                    samples.push(Some(Sample {
                        sent_at,
                        timestamp,
                        rtt: None,
                    }));
                }
                Err(e) => {
                    report.send_errors += 1;
                    report.first_send_error.get_or_insert_with(|| e.to_string());
                    samples.push(None);
                }
            }
            // No catch-up bursts after a delayed send.
            next_send = conn.now() + interval;
        }
        let now = conn.now();
        if samples.len() == cfg.count as usize && now >= finish {
            break;
        }
        let deadline = if samples.len() < cfg.count as usize {
            next_send
        } else {
            finish
        };
        let deadline = deadline.min(now + Duration::from_millis(10));
        let datagram = match conn.receive_until(deadline) {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                report.receive_errors += 1;
                report
                    .first_receive_error
                    .get_or_insert_with(|| e.to_string());
                // Connected UDP may report an ICMP port-unreachable as ConnectionRefused.
                // Preserve that evidence and finish the paced experiment.
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::Interrupted
                ) {
                    continue;
                }
                return Err(io::Error::new(e.kind(), format!("UDP receive failed: {e}")));
            }
        };
        let Ok(packet) = Packet::decode(&datagram.bytes) else {
            report.invalid_responses += 1;
            continue;
        };
        let sample = usize::try_from(packet.seq)
            .ok()
            .and_then(|i| samples.get_mut(i))
            .and_then(Option::as_mut);
        let Some(sample) = sample else {
            report.invalid_responses += 1;
            continue;
        };
        let expected = cfg.packet(packet.seq, sample.timestamp).response();
        if datagram.from != cfg.target
            || packet != expected
            || datagram.received_at < sample.sent_at
        {
            report.invalid_responses += 1;
            continue;
        }
        if datagram.received_at > sample.sent_at + cfg.timeout {
            report.late_responses += 1;
            continue;
        }
        if sample.rtt.is_some() {
            report.duplicates += 1;
            continue;
        }
        sample.rtt = Some(datagram.received_at - sample.sent_at);
        if highest_seq.is_some_and(|highest| packet.seq < highest) {
            report.reordered += 1;
        }
        highest_seq = Some(highest_seq.map_or(packet.seq, |highest: u64| highest.max(packet.seq)));
        report.received_unique += 1;
        good_bytes += datagram.bytes.len() as u64;
    }
    report.elapsed_ms = (conn.now() - start).as_secs_f64() * 1000.0;
    report.unanswered_at_cancel = samples
        .iter()
        .flatten()
        .filter(|s| s.rtt.is_none() && conn.now() < s.sent_at + cfg.timeout)
        .count() as u64;
    report.lost = report.sent - report.received_unique - report.unanswered_at_cancel;
    if report.sent > 0 {
        report.loss_ratio = Some(report.lost as f64 / report.sent as f64);
    }
    let total_valid = report.received_unique + report.duplicates;
    if total_valid > 0 {
        report.duplicate_ratio = Some(report.duplicates as f64 / total_valid as f64);
    }
    if report.received_unique > 0 {
        report.reorder_ratio = Some(report.reordered as f64 / report.received_unique as f64);
    }
    if let (Some(first), Some(last)) = (first_sent, last_sent) {
        report.send_window_ms = (last - first + interval).as_secs_f64() * 1000.0;
        report.achieved_rate_pps = report.sent as f64 / (report.send_window_ms / 1000.0);
    }
    if report.elapsed_ms > 0.0 {
        report.goodput_bytes_per_second = good_bytes as f64 / (report.elapsed_ms / 1000.0);
    }
    report.rtt = Distribution::from_ms(
        samples
            .into_iter()
            .flatten()
            .filter_map(|s| s.rtt)
            .map(|r| r.as_secs_f64() * 1000.0)
            .collect(),
    );
    Ok(report)
}
