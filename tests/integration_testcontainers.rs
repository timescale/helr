//! Integration tests using [testcontainers-rs](https://github.com/testcontainers/testcontainers-rs): Redis and Postgres state backends.
//!
//! Requires Docker and the `testcontainers` feature. Run with:
//! `cargo test --features testcontainers --test integration_testcontainers`

#[cfg(feature = "testcontainers")]
use serde_json::json;
#[cfg(feature = "testcontainers")]
use std::process::Command;
#[cfg(feature = "testcontainers")]
use testcontainers::GenericImage;
#[cfg(feature = "testcontainers")]
use testcontainers::ImageExt;
#[cfg(feature = "testcontainers")]
use testcontainers::core::IntoContainerPort;
#[cfg(feature = "testcontainers")]
use testcontainers::core::WaitFor;
#[cfg(feature = "testcontainers")]
use testcontainers::runners::AsyncRunner;
#[cfg(feature = "testcontainers")]
use wiremock::matchers::method;
#[cfg(feature = "testcontainers")]
use wiremock::{Mock, MockServer, ResponseTemplate};

#[cfg(feature = "testcontainers")]
fn hel_bin() -> String {
    std::env::var("CARGO_BIN_EXE_helr").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/helr",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )
    })
}

/// State backend Redis via testcontainers: start Redis container, run helr run --once, then state show.
#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn testcontainers_redis_state_run_and_show() {
    let redis = GenericImage::new("redis", "7.2")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    let container = redis.start().await.expect("start redis container");
    let host = container.get_host().await.expect("get redis host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("get redis port");
    let redis_url = format!("redis://{host}:{port}/");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "tc1", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_tc_redis");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("helr.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: redis
    url: "{}"
sources:
  tc-redis-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 10
"#,
        redis_url.replace('\\', "\\\\").replace('"', "\\\""),
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out_run = Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run helr");

    assert!(
        out_run.status.success(),
        "helr run --once failed: stderr={}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    let out_show = Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "show",
            "tc-redis-source",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run helr state show");

    assert!(
        out_show.status.success(),
        "helr state show failed: stderr={}",
        String::from_utf8_lossy(&out_show.stderr)
    );

    let stdout = String::from_utf8_lossy(&out_show.stdout);
    assert!(
        stdout.contains("tc-redis-source"),
        "expected source id in state show output, got: {}",
        stdout
    );
}

/// State backend Postgres via testcontainers: start Postgres container, run helr run --once, then state show.
#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn testcontainers_postgres_state_run_and_show() {
    let postgres = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = postgres.start().await.expect("start postgres container");
    let host = container.get_host().await.expect("get postgres host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get postgres port");
    let postgres_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": "tc2", "published": "2024-01-15T12:00:00Z"}
        ])))
        .mount(&server)
        .await;

    let config_dir = std::env::temp_dir().join("hel_tc_postgres");
    let _ = std::fs::create_dir_all(&config_dir);
    let config_path = config_dir.join("helr.yaml");
    let yaml = format!(
        r#"
global:
  log_level: error
  state:
    backend: postgres
    url: "{}"
sources:
  tc-pg-source:
    url: "{}/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 10
"#,
        postgres_url.replace('\\', "\\\\").replace('"', "\\\""),
        server.uri()
    );
    std::fs::write(&config_path, yaml).expect("write config");

    let out_run = Command::new(hel_bin())
        .args(["run", "--config", config_path.to_str().unwrap(), "--once"])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run helr");

    assert!(
        out_run.status.success(),
        "helr run --once failed: stderr={}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    let out_show = Command::new(hel_bin())
        .args([
            "state",
            "--config",
            config_path.to_str().unwrap(),
            "show",
            "tc-pg-source",
        ])
        .env("RUST_LOG", "error")
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
        .output()
        .expect("run helr state show");

    assert!(
        out_show.status.success(),
        "helr state show failed: stderr={}",
        String::from_utf8_lossy(&out_show.stderr)
    );

    let stdout = String::from_utf8_lossy(&out_show.stdout);
    assert!(
        stdout.contains("tc-pg-source"),
        "expected source id in state show output, got: {}",
        stdout
    );
}
