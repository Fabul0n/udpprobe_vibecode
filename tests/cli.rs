//! Exercise the built binary against actual IPv4/IPv6 loopback sockets.
use serde_json::Value;
use std::{
    net::UdpSocket,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use udpprobe::server::{self, ServerConfig, ServerStats};

struct RunningServer {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<std::io::Result<ServerStats>>>,
    addr: String,
}
impl RunningServer {
    fn new(socket: UdpSocket) -> Self {
        let addr = socket.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            server::serve(
                socket,
                ServerConfig {
                    max_echo: 1600,
                    rate_limit: 100_000,
                    token: Some(42),
                    max_peers: 64,
                },
                &flag,
                Some(Duration::from_secs(10)),
            )
        });
        Self {
            stop,
            handle: Some(handle),
            addr,
        }
    }
}
impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_udpprobe"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn all_experiments_emit_json_against_real_udp() {
    let server = RunningServer::new(UdpSocket::bind("127.0.0.1:0").unwrap());
    for (command, extra, expected) in [
        ("train", vec!["--size", "1200", "--rate", "50pps"], 1),
        (
            "mtu",
            vec![
                "--min",
                "64",
                "--max",
                "128",
                "--step",
                "64",
                "--repeats",
                "2",
            ],
            4,
        ),
        (
            "sweep",
            vec!["--size", "64", "--rates", "20,50pps", "--repeats", "2"],
            4,
        ),
    ] {
        let mut args = vec![
            command,
            "--to",
            &server.addr,
            "--count",
            "3",
            "--timeout-ms",
            "200",
            "--token",
            "0x2a",
        ];
        args.extend(extra);
        let output = run(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["schema_version"], 1);
        assert_eq!(report["command"], command);
        assert_eq!(report["trials"].as_array().unwrap().len(), expected);
        for trial in report["trials"].as_array().unwrap() {
            assert_eq!(trial["sent"], 3);
            assert!(trial["received_unique"].as_u64().unwrap() > 0);
            assert_eq!(trial["invalid_responses"], 0);
        }
    }
}
#[test]
fn ipv6_loopback_when_available() {
    let Ok(socket) = UdpSocket::bind("[::1]:0") else {
        eprintln!("IPv6 loopback is unavailable; skipped");
        return;
    };
    let server = RunningServer::new(socket);
    let output = run(&[
        "train",
        "--to",
        &server.addr,
        "--count",
        "2",
        "--timeout-ms",
        "200",
        "--token",
        "42",
        "--echo-small",
    ]);
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(report["trials"][0]["received_unique"].as_u64().unwrap() > 0);
}
#[test]
fn invalid_parameters_fail_before_producing_a_report() {
    for args in [
        vec!["train", "--count", "0"],
        vec!["train", "--size", "22"],
        vec!["train", "--rate", "0"],
        vec!["train", "--size", "65535"],
        vec!["train", "--size", "23", "--token", "42"],
        vec!["mtu", "--min", "100", "--max", "50"],
        vec!["sweep", "--rates", "50,0"],
        vec!["mtu", "--step", "0"],
        vec!["server", "--max-echo", "22"],
        vec!["train", "--timeout-ms", "0"],
    ] {
        let output = run(&args);
        assert!(!output.status.success(), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(!output.stderr.is_empty());
    }
}
#[test]
fn server_command_emits_final_counters() {
    let output = run(&["server", "--addr", "127.0.0.1:0", "--duration", "1"]);
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["command"], "server");
    assert_eq!(report["stats"]["received"], 0);
}
