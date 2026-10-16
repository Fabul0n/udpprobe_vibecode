use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::{
    error::Error,
    io::{self, Write},
    net::{SocketAddr, UdpSocket},
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use udpprobe::{
    client::{TrialConfig, run_trial},
    report::ExperimentReport,
    server::{self, ServerConfig},
    transport::RealUdpConn,
};

type AppResult<T> = Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(
    version,
    about = "Measure UDP payload and rate boundaries; reports are JSON on stdout"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Bounded UDP echo server. Ctrl-C prints final counters.
    Server(ServerArgs),
    /// Send one paced series and measure responses.
    Train(TrainArgs),
    /// Repeat series for a ladder of UDP payload sizes (not a proof of PMTU).
    Mtu(MtuArgs),
    /// Repeat series at each requested packets-per-second rate.
    Sweep(SweepArgs),
}
#[derive(Args, Clone)]
struct Common {
    /// Numeric IP:port; IPv6 uses [::1]:9999.
    #[arg(long, default_value = "127.0.0.1:9999")]
    to: SocketAddr,
    /// Number of probes per trial.
    #[arg(long, default_value_t = 100)]
    count: u32,
    /// Per-probe response deadline in milliseconds.
    #[arg(long, default_value_t = 1000)]
    timeout_ms: u64,
    /// Optional shared u64 token (decimal or 0x hex); not encryption.
    #[arg(long, value_parser = parse_token)]
    token: Option<u64>,
    /// Request a header-only response while sending the full payload.
    #[arg(long)]
    echo_small: bool,
}
#[derive(Args)]
struct TrainArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long, default_value_t = 1200)]
    size: u16,
    #[arg(long, default_value = "50", value_parser = parse_rate)]
    rate: u32,
}
#[derive(Clone, Copy, ValueEnum)]
enum MtuMode {
    Ladder,
}
#[derive(Args)]
struct MtuArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long, default_value_t = 512)]
    min: u16,
    #[arg(long, default_value_t = 1600)]
    max: u16,
    /// Explicit ladder step; otherwise use the lecture ladder plus endpoints.
    #[arg(long)]
    step: Option<u16>,
    #[arg(long, value_enum, default_value = "ladder")]
    mode: MtuMode,
    #[arg(long, default_value = "50", value_parser = parse_rate)]
    rate: u32,
    #[arg(long, default_value_t = 2)]
    repeats: u32,
}
#[derive(Args)]
struct SweepArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long, default_value_t = 1200)]
    size: u16,
    #[arg(long, value_delimiter = ',', default_value = "50,100,500,1000", value_parser = parse_rate)]
    rates: Vec<u32>,
    #[arg(long, default_value_t = 2)]
    repeats: u32,
}
#[derive(Args)]
struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:9999")]
    addr: SocketAddr,
    #[arg(long, default_value_t = 1200)]
    max_echo: u16,
    /// Global and per-peer maximum response rate, with no catch-up bursts.
    #[arg(long, default_value = "200/s", value_parser = parse_rate)]
    rate_limit: u32,
    #[arg(long, value_parser = parse_token)]
    token: Option<u64>,
    #[arg(long, default_value_t = 4096)]
    max_peers: usize,
    /// Stop after this many seconds; zero means run until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    duration: u32,
}
fn parse_token(text: &str) -> Result<u64, String> {
    if let Some(hex) = text.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
    } else {
        text.parse()
    }
    .map_err(|e| e.to_string())
}
fn parse_rate(text: &str) -> Result<u32, String> {
    let number = text
        .strip_suffix("pps")
        .or_else(|| text.strip_suffix("/s"))
        .unwrap_or(text);
    let value: u32 = number
        .parse()
        .map_err(|_| "rate must be an integer, optionally suffixed with pps or /s".to_string())?;
    if !(1..=100_000).contains(&value) {
        return Err("rate must be 1..100000 pps".into());
    }
    Ok(value)
}
fn config(common: &Common, size: u16, rate: u32) -> TrialConfig {
    TrialConfig {
        target: common.to,
        size,
        count: common.count,
        rate,
        timeout: Duration::from_millis(common.timeout_ms),
        token: common.token,
        echo_small: common.echo_small,
    }
}
fn ladder(min: u16, max: u16, step: Option<u16>) -> Result<Vec<u16>, String> {
    if min > max {
        return Err("min must not exceed max".into());
    }
    let mut sizes = vec![min, max];
    match step {
        Some(0) => return Err("step must be positive".into()),
        Some(step) => sizes.extend((min..=max).step_by(usize::from(step))),
        None => sizes.extend(
            [512, 1024, 1200, 1280, 1400, 1472, 1600, 2048, 4096]
                .into_iter()
                .filter(|s| *s > min && *s < max),
        ),
    }
    sizes.sort_unstable();
    sizes.dedup();
    Ok(sizes)
}
fn write_json(value: &impl Serialize) -> AppResult<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer_pretty(&mut out, value)?;
    writeln!(out)?;
    Ok(())
}
fn experiment(
    name: &str,
    common: Common,
    settings: Vec<(u16, u32)>,
    repeats: u32,
    stop: &AtomicBool,
) -> AppResult<()> {
    if !(1..=10).contains(&repeats) {
        return Err("repeats must be 1..10".into());
    }
    if settings.is_empty() || settings.len() * repeats as usize > 256 {
        return Err("experiment must contain 1..256 trials".into());
    }
    let mut seconds = 0.0;
    // Validate every trial before sending the first datagram.
    for &(size, rate) in &settings {
        config(&common, size, rate).validate()?;
        seconds += (f64::from(common.count) / f64::from(rate) + common.timeout_ms as f64 / 1000.0)
            * f64::from(repeats);
    }
    if seconds > 3600.0 {
        return Err("experiment scheduled duration exceeds one hour".into());
    }
    let mut trials = Vec::new();
    'trials: for (size, rate) in settings {
        for repeat in 1..=repeats {
            if stop.load(Ordering::Relaxed) {
                break 'trials;
            }
            eprintln!("{name}: payload={size}, rate={rate}pps, repeat={repeat}");
            // New socket per trial prevents queued responses leaking across experiments.
            let mut conn = RealUdpConn::connect(common.to)?;
            let mut trial = run_trial(&mut conn, &config(&common, size, rate), stop)?;
            trial.repeat = repeat;
            trials.push(trial);
        }
    }
    let mut report = ExperimentReport::new(name, common.to.to_string(), trials);
    report.cancelled = stop.load(Ordering::Relaxed);
    write_json(&report)
}
fn run() -> AppResult<bool> {
    let cli = Cli::parse();
    let stop = Arc::new(AtomicBool::new(false));
    let signal_stop = Arc::clone(&stop);
    ctrlc::set_handler(move || signal_stop.store(true, Ordering::Relaxed))?;
    match cli.command {
        Command::Server(args) => {
            let cfg = ServerConfig {
                max_echo: args.max_echo,
                rate_limit: args.rate_limit,
                token: args.token,
                max_peers: args.max_peers,
            };
            cfg.validate()?;
            let socket = UdpSocket::bind(args.addr)?;
            let bound = socket.local_addr()?;
            eprintln!("Listening on {bound}");
            let duration = (args.duration > 0).then(|| Duration::from_secs(args.duration.into()));
            let stats = server::serve(socket, cfg, &stop, duration)?;
            write_json(
                &serde_json::json!({ "schema_version": 1, "command": "server", "address": bound.to_string(),
                "max_echo": args.max_echo, "rate_limit_pps": args.rate_limit, "token_enabled": args.token.is_some(), "stats": stats }),
            )?;
        }
        Command::Train(a) => experiment("train", a.common, vec![(a.size, a.rate)], 1, &stop)?,
        Command::Mtu(a) => {
            let sizes = ladder(a.min, a.max, a.step)?;
            experiment(
                "mtu",
                a.common,
                sizes.into_iter().map(|size| (size, a.rate)).collect(),
                a.repeats,
                &stop,
            )?;
        }
        Command::Sweep(a) => experiment(
            "sweep",
            a.common,
            a.rates.into_iter().map(|rate| (a.size, rate)).collect(),
            a.repeats,
            &stop,
        )?,
    }
    Ok(stop.load(Ordering::Relaxed))
}
fn main() -> ExitCode {
    match run() {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(130),
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rates_accept_lecture_syntax() {
        for text in ["500", "500pps", "500/s"] {
            assert_eq!(parse_rate(text).unwrap(), 500);
        }
        for text in ["0", "100001", "-1", "NaN"] {
            assert!(parse_rate(text).is_err());
        }
    }
    #[test]
    fn ladder_includes_endpoints_and_never_overflows() {
        assert_eq!(
            ladder(1200, 1500, None).unwrap(),
            vec![1200, 1280, 1400, 1472, 1500]
        );
        assert_eq!(
            ladder(65500, 65507, Some(4)).unwrap(),
            vec![65500, 65504, 65507]
        );
        assert!(ladder(20, 10, None).is_err());
        assert!(ladder(23, 30, Some(0)).is_err());
    }
    #[test]
    fn cli_parses_examples() {
        assert!(Cli::try_parse_from(["udpprobe", "server", "--rate-limit", "200/s"]).is_ok());
        assert!(Cli::try_parse_from(["udpprobe", "sweep", "--rates", "50,100,1000pps"]).is_ok());
        assert!(Cli::try_parse_from(["udpprobe", "mtu", "--mode", "ladder"]).is_ok());
    }
}
