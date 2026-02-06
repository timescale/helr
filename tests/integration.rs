//! Integration tests: replay + wiremock + hel run --once; assert stdout NDJSON and behavior.

use base64::Engine;
use insta::assert_snapshot;
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn integration_run_once_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "1", "msg": "first", "published": "2024-01-15T12:00:00Z"},
            {"id": "2", "msg": "second", "published": "2024-01-15T12:00:01Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_test");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  test-log-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    });

    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"1","msg":"first","published":"2024-01-15T12:00:00Z"},"meta":{},"source":"test-log-source","ts":"REDACTED_TS"}
{"endpoint":"/","event":{"id":"2","msg":"second","published":"2024-01-15T12:00:01Z"},"meta":{},"source":"test-log-source","ts":"REDACTED_TS"}
"#);
}

/// Client-side rate limit (max_requests_per_second): hel throttles requests and still emits NDJSON.
#[tokio::test]
async fn integration_rate_limit_rps_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "r1", "msg": "rate-limited", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_rate_limit");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  rps-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
      rate_limit:
        max_requests_per_second: 2
        burst_size: 1
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = hel_bin();
    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once with rate_limit failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"r1","msg":"rate-limited","published":"2024-01-15T12:00:00Z"},"meta":{},"source":"rps-source","ts":"REDACTED_TS"}
"#);
}

/// TLS config (min_version only): hel builds client and emits NDJSON (mock is HTTP; TLS applies to HTTPS sources).
#[tokio::test]
async fn integration_tls_min_version_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "t1", "msg": "tls-config", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_tls");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  tls-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
      tls:
        min_version: "1.2"
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = hel_bin();
    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once with tls min_version failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"t1","msg":"tls-config","published":"2024-01-15T12:00:00Z"},"meta":{},"source":"tls-source","ts":"REDACTED_TS"}
"#);
}

/// Backpressure enabled (strategy block): hel still emits NDJSON to stdout.
#[tokio::test]
async fn integration_backpressure_block_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "bp1", "msg": "backpressure-one", "published": "2024-01-15T12:00:00Z"},
            {"id": "bp2", "msg": "backpressure-two", "published": "2024-01-15T12:00:01Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_backpressure_test");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  backpressure:
    enabled: true
    detection:
      event_queue_size: 100
    strategy: block
sources:
  bp-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    });

    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once with backpressure failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"bp1","msg":"backpressure-one","published":"2024-01-15T12:00:00Z"},"meta":{},"source":"bp-source","ts":"REDACTED_TS"}
{"endpoint":"/","event":{"id":"bp2","msg":"backpressure-two","published":"2024-01-15T12:00:01Z"},"meta":{},"source":"bp-source","ts":"REDACTED_TS"}
"#);
}

/// Backpressure enabled (strategy drop): hel emits NDJSON; under load some events may be dropped (metrics).
#[tokio::test]
async fn integration_backpressure_drop_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "d1", "msg": "drop-one", "published": "2024-01-15T12:00:00Z"},
            {"id": "d2", "msg": "drop-two", "published": "2024-01-15T12:00:01Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_backpressure_drop_test");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  backpressure:
    enabled: true
    detection:
      event_queue_size: 2
    strategy: drop
    drop_policy: newest_first
sources:
  drop-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    });

    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once with backpressure drop failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    // Under drop strategy we only assert at least one line; snapshot the first line's structure.
    let lines: Vec<&str> = snapshot_output
        .lines()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert!(
        !lines.is_empty(),
        "expected at least 1 NDJSON line: {}",
        stdout
    );
    assert_snapshot!(lines[0], @r#"{"endpoint":"/","event":{"id":"d1","msg":"drop-one","published":"2024-01-15T12:00:00Z"},"meta":{},"source":"drop-source","ts":"REDACTED_TS"}"#);
}

/// 429 then 200: retry layer eventually succeeds and we get NDJSON.
#[tokio::test]
async fn integration_429_then_200_retries_and_emits() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "r1", "msg": "after_retry"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_429_test");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  retry-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
      retries:
        max_attempts: 3
        initial_backoff_secs: 0
        multiplier: 1.0
      rate_limit:
        respect_headers: true
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    });

    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once failed after 429: stdout={} stderr={}",
        stdout,
        stderr
    );

    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert!(
        lines.len() >= 1,
        "expected at least 1 NDJSON line after retry, got {}: {:?}",
        lines.len(),
        stdout
    );

    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["source"], "retry-source");
    assert_eq!(first["event"]["id"], "r1");
}

/// --output writes NDJSON to file instead of stdout.
#[tokio::test]
async fn integration_file_output_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "f1", "msg": "file-one", "published": "2024-01-15T12:00:00Z"},
            {"id": "f2", "msg": "file-two", "published": "2024-01-15T12:00:01Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_file_out");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let output_path = config_dir.join("events.ndjson");
    let _ = std::fs::remove_file(&output_path);

    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  file-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out = std::process::Command::new(hel_bin())
        .args([
            "run",
            "--config",
            config_path.to_str().unwrap(),
            "--once",
            "--output",
            output_path.to_str().unwrap(),
        ])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out.status.success(),
        "hel run --once --output failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let content = std::fs::read_to_string(&output_path).expect("read output file");
    let lines: Vec<&str> = content.lines().filter(|s| !s.trim().is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least 2 NDJSON lines, got {:?}",
        content
    );

    for line in &lines {
        let obj: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid NDJSON {:?}: {}", line, e));
        assert_eq!(
            obj.get("source").and_then(|v| v.as_str()),
            Some("file-source")
        );
        assert!(obj.get("event").is_some());
    }
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["event"]["id"], "f1");
    assert_eq!(first["event"]["msg"], "file-one");
}

/// on_parse_error: skip — invalid JSON response does not fail the run; poll stops for that source.
#[tokio::test]
async fn integration_on_parse_error_skip() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not valid json"))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_parse_skip");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  parse-skip-source:
    url: "{}/"
    on_parse_error: skip
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out.status.success(),
        "on_parse_error skip should succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let ndjson_lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert!(
        ndjson_lines.is_empty(),
        "expected no events when parse fails with skip, got {}",
        ndjson_lines.len()
    );
}

/// Session replay --record-dir: run against wiremock, then verify recording files exist.
#[tokio::test]
async fn integration_record_dir_writes_recordings() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "rec1", "msg": "recorded", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_record");
    let _ = std::fs::create_dir_all(&config_dir);
    let record_dir = config_dir.join("recordings");
    let _ = std::fs::remove_dir_all(&record_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  record-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out = std::process::Command::new(hel_bin())
        .args([
            "run",
            "--config",
            config_path.to_str().unwrap(),
            "--once",
            "--record-dir",
            record_dir.to_str().unwrap(),
        ])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out.status.success(),
        "hel run --once --record-dir failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let source_dir = record_dir.join("record-source");
    assert!(
        source_dir.is_dir(),
        "record dir should contain record-source/"
    );
    let file0 = source_dir.join("000.json");
    assert!(file0.is_file(), "record-source/000.json should exist");
    let content = std::fs::read_to_string(&file0).expect("read 000.json");
    let rec: serde_json::Value = serde_json::from_str(&content).expect("parse recording JSON");
    assert_eq!(rec["status"], 200);
    assert!(
        rec.get("body_base64")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .len()
            > 0
    );
    let body_b64 = rec["body_base64"].as_str().unwrap();
    let body = base64::engine::general_purpose::STANDARD
        .decode(body_b64)
        .expect("decode body_base64");
    let body_str = String::from_utf8(body).unwrap();
    let arr: serde_json::Value = serde_json::from_str(&body_str).unwrap();
    assert_eq!(arr[0]["id"], "rec1");
    assert_eq!(arr[0]["msg"], "recorded");
}

/// Session replay --replay-dir: use pre-created recordings; run hel and assert NDJSON matches.
#[tokio::test]
async fn integration_replay_dir_emits_from_recordings() {
    let config_dir = std::env::temp_dir().join("hel_integration_replay");
    let _ = std::fs::create_dir_all(&config_dir);
    let replay_dir = config_dir.join("replay_fixture");
    let _ = std::fs::remove_dir_all(&replay_dir);
    let source_dir = replay_dir.join("replay-source");
    std::fs::create_dir_all(&source_dir).expect("create replay fixture dir");

    let body = json!([{"id": "rp1", "msg": "replayed", "published": "2024-01-20T10:00:00Z"}]);
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let rec = serde_json::json!({
        "url": "http://replay/replay/replay-source",
        "status": 200,
        "headers": {"Content-Type": "application/json"},
        "body_base64": base64::engine::general_purpose::STANDARD.encode(&body_bytes)
    });
    std::fs::write(
        source_dir.join("000.json"),
        serde_json::to_string_pretty(&rec).unwrap(),
    )
    .expect("write 000.json");

    let config_path = config_dir.join("hel.yaml");
    std::fs::write(
        &config_path,
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  replay-source:
    url: "http://placeholder/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
    )
    .expect("write config");

    let out = std::process::Command::new(hel_bin())
        .args([
            "run",
            "--config",
            config_path.to_str().unwrap(),
            "--once",
            "--replay-dir",
            replay_dir.to_str().unwrap(),
        ])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out.status.success(),
        "hel run --once --replay-dir failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert!(
        lines.len() >= 1,
        "expected at least 1 NDJSON line from replay, got {}: stdout={} stderr={}",
        lines.len(),
        stdout,
        stderr
    );
    let obj: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(obj["source"], "replay-source");
    assert_eq!(obj["event"]["id"], "rp1");
    assert_eq!(obj["event"]["msg"], "replayed");
}

fn hel_bin() -> String {
    std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    })
}

fn run_hel(args: &[&str], config_path: &str) -> std::process::Output {
    let mut args_vec = vec![args[0], "--config", config_path];
    args_vec.extend(&args[1..]);
    std::process::Command::new(hel_bin())
        .args(&args_vec)
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel")
}

/// Redact variable parts of hel stdout so inline snapshots are stable across runs.
fn redact_stdout_for_snapshot(stdout: &str, server_uri: Option<&str>) -> String {
    let mut out = if let Some(uri) = server_uri {
        stdout.replace(uri, "http://mock-server/")
    } else {
        stdout.to_string()
    };
    // Redact request_id (e.g. "hel-1770292017945225000").
    let mut i = 0;
    while let Some(pos) = out[i..].find("\"request_id\":\"hel-") {
        let start = i + pos + "\"request_id\":\"hel-".len();
        let end = start
            + out[start..]
                .bytes()
                .take_while(|b| b.is_ascii_digit())
                .count();
        out.replace_range(start..end, "REDACTED");
        i = start + 8; // "REDACTED".len()
    }
    // Redact "ts":"<iso datetime>" so snapshots are stable.
    let mut i = 0;
    while let Some(pos) = out[i..].find("\"ts\":\"") {
        let start = i + pos + "\"ts\":\"".len();
        let end = start + out[start..].bytes().take_while(|b| *b != b'"').count();
        if end > start {
            out.replace_range(start..end, "REDACTED_TS");
            i = start + 11; // "REDACTED_TS".len()
        } else {
            break;
        }
    }
    out
}

/// Dedupe: same event ID twice in one response → only one NDJSON line emitted.
#[tokio::test]
async fn integration_dedupe_skips_duplicate_ids() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "dup-1", "msg": "first"},
            {"id": "dup-1", "msg": "duplicate"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_dedupe");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  dedupe-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    dedupe:
      id_path: id
      capacity: 1000
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        1,
        "expected 1 line (duplicate skipped), got {}: {:?}",
        lines.len(),
        stdout
    );
    let obj: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(obj["event"]["id"], "dup-1");
}

/// Circuit breaker: after N failures, run fails (circuit open).
#[tokio::test]
async fn integration_circuit_breaker_opens_after_failures() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(5)
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_circuit");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  circuit-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
      retries:
        max_attempts: 1
      circuit_breaker:
        enabled: true
        failure_threshold: 2
        success_threshold: 1
        half_open_timeout_secs: 60
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stderr.contains("poll failed"),
        "expected poll failure in stderr; stdout: {} stderr: {}",
        stdout,
        stderr
    );
    let lines: Vec<&str> = stdout.lines().filter(|s| s.contains("\"event\"")).collect();
    assert!(
        lines.is_empty(),
        "expected no events when circuit opens; got {} lines",
        lines.len()
    );
}

/// hel state set: set a single key, then show confirms it.
#[tokio::test]
async fn integration_state_set_then_show() {
    let config_dir = std::env::temp_dir().join("hel_integration_state_set");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let state_path = config_dir.join("hel-state.db");
    let _ = std::fs::remove_file(&state_path);

    std::fs::write(
        &config_path,
        format!(
            r#"
global:
  log_level: error
  state:
    backend: sqlite
    path: "{}"
sources:
  set-test-source:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
"#,
            state_path.display()
        ),
    )
    .expect("write config");

    let out_set = std::process::Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "set",
            "set-test-source",
            "next_url",
            "https://example.com/logs?after=xyz",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel state set");

    assert!(
        out_set.status.success(),
        "hel state set failed: stderr={}",
        String::from_utf8_lossy(&out_set.stderr)
    );

    let out_show = std::process::Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "show",
            "set-test-source",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel state show");

    assert!(
        out_show.status.success(),
        "hel state show failed: stderr={}",
        String::from_utf8_lossy(&out_show.stderr)
    );

    let stdout = String::from_utf8_lossy(&out_show.stdout);
    assert!(
        stdout.contains("next_url") && stdout.contains("https://example.com/logs?after=xyz"),
        "expected next_url in state show output, got: {}",
        stdout
    );
}

/// Per-source state.watermark_field / watermark_param: after one poll tick, watermark is stored and state show displays it.
#[tokio::test]
async fn integration_watermark_state_stored_after_poll() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": {"time": "1000"}, "msg": "a"},
                {"id": {"time": "2000"}, "msg": "b"}
            ],
            "nextPageToken": "p1"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": {"time": "3000"}, "msg": "c"}]
        })))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_watermark");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let state_path = config_dir.join("hel-watermark-state.db");
    let _ = std::fs::remove_file(&state_path);

    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: sqlite
    path: "{}"
sources:
  watermark-source:
    url: "{}/"
    pagination:
      strategy: cursor
      cursor_param: pageToken
      cursor_path: nextPageToken
    state:
      watermark_field: "id.time"
      watermark_param: "startTime"
    resilience:
      timeout_secs: 5
"#,
        state_path.to_str().unwrap().replace('\\', "/"),
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out_run = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out_run.status.success(),
        "hel run --once failed: stderr={}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    let out_show = std::process::Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "show",
            "watermark-source",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel state show");

    assert!(
        out_show.status.success(),
        "hel state show failed: stderr={}",
        String::from_utf8_lossy(&out_show.stderr)
    );

    let stdout = String::from_utf8_lossy(&out_show.stdout);
    assert!(
        stdout.contains("watermark") && stdout.contains("3000"),
        "expected watermark=3000 (max of id.time) in state show output, got: {}",
        stdout
    );
}

/// State backend Redis: run one tick with redis state, then state show. Skips when REDIS_URL is not set.
#[tokio::test]
async fn integration_state_backend_redis() {
    let redis_url = match std::env::var("REDIS_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => return, // skip when REDIS_URL not set (e.g. in CI without Redis)
    };

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "r1", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_redis");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: redis
    url: "{}"
sources:
  redis-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        redis_url,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out_run = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(
        out_run.status.success(),
        "hel run --once failed: stderr={}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    let out_show = std::process::Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "show",
            "redis-source",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel state show");

    assert!(
        out_show.status.success(),
        "hel state show failed: stderr={}",
        String::from_utf8_lossy(&out_show.stderr)
    );

    let stdout = String::from_utf8_lossy(&out_show.stdout);
    assert!(
        stdout.contains("redis-source"),
        "expected source id in state show output, got: {}",
        stdout
    );
}

/// hel validate rejects invalid config.
#[tokio::test]
async fn integration_validate_rejects_invalid_config() {
    let config_dir = std::env::temp_dir().join("hel_integration_validate");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    std::fs::write(
        &config_path,
        r#"
global: {}
sources: {}
"#,
    )
    .expect("write config");

    let output = std::process::Command::new(hel_bin())
        .args(["validate", "--config", config_path.to_str().unwrap()])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    assert!(!output.status.success());
}

/// Cursor pagination: first page returns next_cursor, second page returns no cursor.
#[tokio::test]
async fn integration_cursor_pagination_two_pages() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "c1", "msg": "page1"}],
            "next_cursor": "token2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "c2", "msg": "page2"}],
            "next_cursor": ""
        })))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_cursor");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  cursor-source:
    url: "{}/"
    pagination:
      strategy: cursor
      cursor_param: after
      cursor_path: next_cursor
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"c1","msg":"page1"},"meta":{},"source":"cursor-source","ts":"REDACTED_TS"}
{"endpoint":"/","event":{"id":"c2","msg":"page2"},"meta":{},"source":"cursor-source","ts":"REDACTED_TS"}
"#);
}

/// Page/offset pagination: two pages then empty.
#[tokio::test]
async fn integration_page_offset_pagination_two_pages() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "p1"}, {"id": "p2"}
        ])))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_page_offset");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  page-source:
    url: "{}/"
    pagination:
      strategy: page_offset
      page_param: page
      limit_param: limit
      limit: 2
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"/","event":{"id":"p1"},"meta":{},"source":"page-source","ts":"REDACTED_TS"}
{"endpoint":"/","event":{"id":"p2"},"meta":{},"source":"page-source","ts":"REDACTED_TS"}
"#);
}

/// Link-header max_pages: stop after max_pages even if next link present.
#[tokio::test]
async fn integration_link_header_respects_max_pages() {
    let server = MockServer::start().await;
    let next_link = format!(r#"<{}/>; rel="next""#, server.uri());

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", next_link.as_str())
                .set_body_json(json!([{"id": "m1"}])),
        )
        .up_to_n_times(3)
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_max_pages");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  max-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
      max_pages: 2
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "max_pages=2 so 2 requests, 2 events; got {}: {:?}",
        lines.len(),
        stdout
    );
}

/// Recorded/fixture: wiremock returns Okta-shaped JSON; parser accepts it.
#[tokio::test]
async fn integration_fixture_okta_shaped_events() {
    let server = MockServer::start().await;

    let body = json!([
        {
            "uuid": "dc9fd3c0-598c-11ef-8478-2b7584bf8d5a",
            "published": "2024-08-13T15:58:20.353Z",
            "eventType": "user.session.start",
            "displayMessage": "User login to Okta",
            "actor": {"id": "00u1", "displayName": "Jane"}
        }
    ]);

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_fixture");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  okta-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let output = run_hel(&["run", "--once"], config_path.to_str().unwrap());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1);
    let obj: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(obj["source"], "okta-source");
    assert_eq!(obj["event"]["uuid"], "dc9fd3c0-598c-11ef-8478-2b7584bf8d5a");
    assert_eq!(obj["event"]["eventType"], "user.session.start");
}

/// Health endpoints: /healthz, /readyz, /startupz return 200 and detailed JSON (version, uptime, sources).
#[tokio::test]
async fn integration_health_endpoints_return_200() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": "1"}])))
        .mount(&server)
        .await;

    let health_port = 19283u16;
    let config_dir = std::env::temp_dir().join("hel_integration_health");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  api:
    enabled: true
    address: "127.0.0.1"
    port: {}
sources:
  health-test-source:
    url: "{}/"
    schedule:
      interval_secs: 60
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        health_port,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let mut child = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap()])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hel");

    let base = format!("http://127.0.0.1:{}", health_port);
    let client = reqwest::Client::new();
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        if client
            .get(format!("{}/healthz", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
    }

    let res_health = client
        .get(format!("{}/healthz", base))
        .send()
        .await
        .expect("get healthz");
    let res_ready = client
        .get(format!("{}/readyz", base))
        .send()
        .await
        .expect("get readyz");
    let res_startup = client
        .get(format!("{}/startupz", base))
        .send()
        .await
        .expect("get startupz");

    let _ = child.kill();

    assert!(
        res_health.status().is_success(),
        "GET /healthz: {}",
        res_health.status()
    );
    assert!(
        res_ready.status().is_success(),
        "GET /readyz: {}",
        res_ready.status()
    );
    assert!(
        res_startup.status().is_success(),
        "GET /startupz: {}",
        res_startup.status()
    );

    // All endpoints return JSON with version, uptime_secs, sources
    let ct_health = res_health
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct_health.contains("application/json"),
        "healthz Content-Type: {}",
        ct_health
    );
    let body_health: serde_json::Value = res_health.json().await.expect("healthz JSON");
    assert!(
        !body_health["version"].as_str().unwrap_or("").is_empty(),
        "healthz version"
    );
    assert!(
        body_health["uptime_secs"].as_f64().is_some(),
        "healthz uptime_secs"
    );
    let sources = body_health["sources"]
        .as_object()
        .expect("healthz sources object");
    assert!(
        sources.contains_key("health-test-source"),
        "healthz sources.health-test-source"
    );
    let src = &sources["health-test-source"];
    assert!(src["status"].as_str().is_some(), "healthz source status");
    assert!(
        src["circuit_state"]["state"].as_str().is_some(),
        "healthz source circuit_state.state"
    );

    let body_ready: serde_json::Value = res_ready.json().await.expect("readyz JSON");
    assert!(
        body_ready["ready"].as_bool().unwrap_or(false),
        "readyz ready true when stdout"
    );
    assert!(
        body_ready["state_store_connected"]
            .as_bool()
            .unwrap_or(false),
        "readyz state_store_connected"
    );
    assert!(
        body_ready["at_least_one_source_healthy"]
            .as_bool()
            .unwrap_or(false),
        "readyz at_least_one_source_healthy"
    );
    assert!(body_ready["sources"].is_object(), "readyz sources");

    let body_startup: serde_json::Value = res_startup.json().await.expect("startupz JSON");
    assert!(
        body_startup["started"].as_bool().unwrap_or(false),
        "startupz started true"
    );
    assert!(body_startup["sources"].is_object(), "startupz sources");
}

/// Health /healthz sources: each source has status, circuit_state.state; circuit_state may have failures or open_until_secs.
#[tokio::test]
async fn integration_health_sources_structure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": "1"}])))
        .mount(&server)
        .await;

    let health_port = 19284u16;
    let config_dir = std::env::temp_dir().join("hel_integration_health_sources");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  api:
    enabled: true
    address: "127.0.0.1"
    port: {}
sources:
  source-a:
    url: "{}/"
    schedule:
      interval_secs: 60
    pagination:
      strategy: link_header
      rel: next
  source-b:
    url: "{}/b"
    schedule:
      interval_secs: 60
    pagination:
      strategy: link_header
      rel: next
"#,
        health_port,
        server.uri(),
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let mut child = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap()])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hel");

    let base = format!("http://127.0.0.1:{}", health_port);
    let client = reqwest::Client::new();
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        if client
            .get(format!("{}/healthz", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
    }

    let res = client
        .get(format!("{}/healthz", base))
        .send()
        .await
        .expect("get healthz");
    let _ = child.kill();
    assert!(res.status().is_success());
    let body: serde_json::Value = res.json().await.expect("JSON");
    let sources = body["sources"].as_object().expect("sources");
    assert_eq!(sources.len(), 2, "two sources");
    for (name, src) in sources {
        assert!(src["status"].as_str().is_some(), "{} has status", name);
        let cs = src["circuit_state"]
            .as_object()
            .expect("circuit_state object");
        assert!(
            cs["state"].as_str().is_some(),
            "{} circuit_state.state",
            name
        );
        let state = cs["state"].as_str().unwrap();
        assert!(
            state == "closed" || state == "open" || state == "half_open",
            "{} circuit_state.state one of closed/open/half_open",
            name
        );
    }
}

/// Readyz with file output: returns 200, ready true, output_writable true.
#[tokio::test]
async fn integration_health_readyz_file_output_200() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": "1"}])))
        .mount(&server)
        .await;

    let health_port = 19285u16;
    let config_dir = std::env::temp_dir().join("hel_integration_health_readyz");
    let _ = std::fs::create_dir_all(&config_dir);
    let output_file = config_dir.join("out.ndjson");
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  api:
    enabled: true
    address: "127.0.0.1"
    port: {}
sources:
  readyz-source:
    url: "{}/"
    schedule:
      interval_secs: 60
    pagination:
      strategy: link_header
      rel: next
"#,
        health_port,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let mut child = std::process::Command::new(hel_bin())
        .args([
            "run",
            "--config",
            config_path.to_str().unwrap(),
            "--output",
            output_file.to_str().unwrap(),
        ])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hel");

    let base = format!("http://127.0.0.1:{}", health_port);
    let client = reqwest::Client::new();
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        if client
            .get(format!("{}/readyz", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
    }

    let res = client
        .get(format!("{}/readyz", base))
        .send()
        .await
        .expect("get readyz");
    let _ = child.kill();
    assert!(
        res.status().is_success(),
        "readyz 200 when output file writable"
    );
    let body: serde_json::Value = res.json().await.expect("JSON");
    assert_eq!(body["ready"], true);
    assert_eq!(body["output_writable"], true);
    assert_eq!(body["state_store_connected"], true);
    assert_eq!(body["at_least_one_source_healthy"], true);
}

/// Graceful degradation: when SQLite state store fails to open and state_store_fallback is memory, health reports state_store_fallback_active.
#[tokio::test]
async fn integration_state_store_fallback_health_reports_fallback_active() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": "1"}])))
        .mount(&server)
        .await;

    let health_port = 19286u16;
    let config_dir = std::env::temp_dir().join("hel_integration_fallback");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    // SQLite path that will fail to open (parent dir does not exist), so we fall back to memory.
    let state_path = config_dir.join("nonexistent_subdir").join("hel-state.db");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: sqlite
    path: "{}"
  degradation:
    state_store_fallback: memory
  api:
    enabled: true
    address: "127.0.0.1"
    port: {}
sources:
  fallback-source:
    url: "{}/"
    schedule:
      interval_secs: 60
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 5
"#,
        state_path.to_str().unwrap().replace('\\', "/"),
        health_port,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let mut child = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap()])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hel");

    let base = format!("http://127.0.0.1:{}", health_port);
    let client = reqwest::Client::new();
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        if client
            .get(format!("{}/healthz", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
    }

    let res_health = client
        .get(format!("{}/healthz", base))
        .send()
        .await
        .expect("get healthz");
    assert!(
        res_health.status().is_success(),
        "GET /healthz: {}",
        res_health.status()
    );
    let body: serde_json::Value = res_health.json().await.expect("healthz JSON");
    assert_eq!(
        body["state_store_fallback_active"].as_bool(),
        Some(true),
        "healthz should report state_store_fallback_active true when SQLite failed and fallback to memory is used: {}",
        body
    );

    let res_ready = client
        .get(format!("{}/readyz", base))
        .send()
        .await
        .expect("get readyz");
    assert!(res_ready.status().is_success());
    let body_ready: serde_json::Value = res_ready.json().await.expect("readyz JSON");
    assert_eq!(
        body_ready["state_store_fallback_active"].as_bool(),
        Some(true),
        "readyz should report state_store_fallback_active true"
    );

    let _ = child.kill();
}

/// SIGTERM mid-poll: send SIGTERM while hel is waiting on a slow response; process exits (graceful shutdown).
#[cfg(unix)]
#[tokio::test]
async fn integration_sigterm_mid_poll_graceful_shutdown() {
    use nix::sys::signal::{self, Signal};
    use nix::unistd::Pid;
    use std::process::Stdio;

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(5))
                .set_body_json(json!([{"id": "slow"}])),
        )
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_sigterm");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  sigterm-source:
    url: "{}/"
    schedule:
      interval_secs: 1
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 10
"#,
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let mut child = std::process::Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap()])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn hel");

    std::thread::sleep(Duration::from_millis(1500));

    let pid = child.id();
    signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM).expect("send SIGTERM");

    let wait_timeout = Duration::from_secs(35);
    let start = std::time::Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => {
                if start.elapsed() > wait_timeout {
                    let _ = child.kill();
                    panic!("hel did not exit within {:?}", wait_timeout);
                }
            }
            Err(e) => panic!("try_wait failed: {}", e),
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    let code = status.code();
    let ok = code.map(|c| c == 0 || c == 143 || c == 15).unwrap_or(true);
    assert!(
        ok,
        "expected exit 0 (graceful), 143 or 15 (SIGTERM); got {:?}",
        status
    );
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn integration_hooks_inline_script_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": "h1", "published": "2024-06-01T10:00:00Z", "action": "login"},
                {"id": "h2", "published": "2024-06-01T10:00:01Z", "action": "logout"}
            ]
        })))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_integration_hooks_test");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("hel.yaml");
    // Use script_inline as a single-line string to avoid YAML block-scalar indentation issues.
    let script_inline = r#"function parseResponse(ctx, response) { var body = typeof response.body === 'string' ? JSON.parse(response.body) : response.body; var items = body.items || body.data || []; return items.map(function(e) { return { ts: e.published || '', source: ctx.sourceId, event: e, meta: {} }; }); }"#;
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: memory
  hooks:
    enabled: true
    timeout_secs: 5
sources:
  hooks-inline-source:
    url: "{}/"
    hooks:
      script_inline: "{}"
    resilience:
      timeout_secs: 5
"#,
        server.uri(),
        script_inline.replace('\\', "\\\\").replace('"', "\\\"")
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let hel_bin = std::env::var("CARGO_BIN_EXE_hel").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/hel",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    });

    let output = std::process::Command::new(&hel_bin)
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run hel");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "hel run --once with hooks failed: stdout={} stderr={}",
        stdout,
        stderr
    );

    let snapshot_output = redact_stdout_for_snapshot(&stdout, Some(server.uri().as_str()));
    assert_snapshot!(snapshot_output, @r#"
{"endpoint":"http://mock-server//","event":{"action":"login","id":"h1","published":"2024-06-01T10:00:00Z"},"meta":{"request_id":"hel-REDACTED"},"source":"hooks-inline-source","ts":"REDACTED_TS"}
{"endpoint":"http://mock-server//","event":{"action":"logout","id":"h2","published":"2024-06-01T10:00:01Z"},"meta":{"request_id":"hel-REDACTED"},"source":"hooks-inline-source","ts":"REDACTED_TS"}
"#);
}
