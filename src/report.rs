//! JSON schema v1. Ratios are fractions (0..1); missing distributions are null.
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Distribution {
    pub min_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub stddev_ms: f64,
}
impl Distribution {
    pub fn from_ms(mut values: Vec<f64>) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let stddev =
            (values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64).sqrt();
        let percentile = |percent: usize| values[(values.len() * percent).div_ceil(100) - 1];
        Some(Self {
            min_ms: values[0],
            mean_ms: mean,
            p50_ms: percentile(50),
            p95_ms: percentile(95),
            p99_ms: percentile(99),
            max_ms: values[values.len() - 1],
            stddev_ms: stddev,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct TrialReport {
    pub repeat: u32,
    pub payload_bytes: u16,
    pub requested_rate_pps: u32,
    pub requested_count: u32,
    pub timeout_ms: u64,
    pub echo_small: bool,
    pub token_enabled: bool,
    pub attempted: u64,
    pub sent: u64,
    pub send_errors: u64,
    pub first_send_error: Option<String>,
    pub receive_errors: u64,
    pub first_receive_error: Option<String>,
    pub received_unique: u64,
    pub lost: u64,
    pub unanswered_at_cancel: u64,
    pub loss_ratio: Option<f64>,
    pub duplicates: u64,
    pub duplicate_ratio: Option<f64>,
    pub reordered: u64,
    pub reorder_ratio: Option<f64>,
    pub late_responses: u64,
    pub invalid_responses: u64,
    pub elapsed_ms: f64,
    pub send_window_ms: f64,
    pub achieved_rate_pps: f64,
    pub goodput_bytes_per_second: f64,
    pub rtt: Option<Distribution>,
    pub cancelled: bool,
}

#[derive(Debug, Serialize)]
pub struct ExperimentReport {
    pub schema_version: u8,
    pub command: String,
    pub target: String,
    pub cancelled: bool,
    pub trials: Vec<TrialReport>,
    pub observations: Vec<String>,
    pub limitations: Vec<String>,
}
impl ExperimentReport {
    pub fn new(command: &str, target: String, trials: Vec<TrialReport>) -> Self {
        let observations = trials.iter().map(|t| format!(
            "{} bytes at requested {} pps, repeat {}: {} of {} successfully sent probes received no response by their deadline",
            t.payload_bytes, t.requested_rate_pps, t.repeat, t.lost, t.sent)).collect();
        Self { schema_version: 1, command: command.into(), target, cancelled: trials.iter().any(|t| t.cancelled), trials, observations,
            limitations: vec![
                "Loss means no valid response by a per-probe deadline, not proven request loss.".into(),
                "A payload threshold does not prove path MTU or fragmentation; DF/PMTUD is not controlled.".into(),
                "Compare server counters to distinguish filtering/rate limits from path loss; server counters are not transmitted to the client.".into(),
                "RTT uses local monotonic receive/read time and includes host scheduling and socket queues. Jitter is population RTT standard deviation.".into(),
                "Goodput counts unique on-time response UDP payload bytes over the full trial observation window, including timeout drain.".into(),
                "The synchronous scheduler skips catch-up bursts; inspect achieved_rate_pps. Polling and OS scheduling limit high-rate precision.".into(),
            ] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nearest_rank_and_empty_samples() {
        assert!(Distribution::from_ms(vec![]).is_none());
        let d = Distribution::from_ms((1..=100).map(f64::from).collect()).unwrap();
        assert_eq!((d.p50_ms, d.p95_ms, d.p99_ms), (50.0, 95.0, 99.0));
        assert_eq!(d.mean_ms, 50.5);
        assert_eq!(Distribution::from_ms(vec![3.0]).unwrap().stddev_ms, 0.0);
    }
}
