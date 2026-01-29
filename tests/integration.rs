//! Integration test: mock HTTP server + hel run --once; assert stdout NDJSON and behavior.

use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn integration_run_once_emits_ndjson() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([
                {"id": "1", "msg": "first", "published": "2024-01-15T12:00:00Z"},
                {"id": "2", "msg": "second", "published": "2024-01-15T12:00:01Z"}
            ])),
        )
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
        .args(["--config", config_path.to_str().unwrap(), "run", "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(
            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()),
        )
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

    let lines: Vec<&str> = stdout.lines().filter(|s| !s.trim().is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least 2 NDJSON lines, got {}: {:?}",
        lines.len(),
        stdout
    );

    for line in &lines {
        let obj: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid NDJSON line {:?}: {}", line, e));
        assert_eq!(
            obj.get("source").and_then(|v| v.as_str()),
            Some("test-log-source")
        );
        assert!(obj.get("event").is_some());
    }

    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["event"]["id"], "1");
    assert_eq!(first["event"]["msg"], "first");
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
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([
                {"id": "r1", "msg": "after_retry"}
            ])),
        )
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
        .args(["--config", config_path.to_str().unwrap(), "run", "--once"])
        .env("RUST_LOG", "error")
        .env("HEL_LOG_LEVEL", "error")
        .current_dir(
            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()),
        )
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
