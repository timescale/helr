//! Hel — generic HTTP API log collector.
//!
//! Polls HTTP APIs (Okta, GitHub, etc.), handles pagination and state management,
//! emits NDJSON to stdout for downstream collectors (Alloy, Vector, etc.).

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod circuit;
mod client;
mod config;
mod dedupe;
mod event;
mod metrics;
mod mock_server;
mod oauth2;
mod output;
mod pagination;
mod poll;
mod replay;
mod retry;
mod state;

use axum::http::StatusCode;
use axum::routing::get;
use circuit::new_circuit_store;
use config::Config;
use oauth2::new_oauth2_token_cache;
use output::{parse_rotation, EventSink, FileSink, RotationPolicy, StdoutSink};
use state::{MemoryStateStore, SqliteStateStore, StateStore};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "hel")]
#[command(author, version, about = "Generic HTTP API log collector")]
struct Cli {
    #[arg(short, long, global = true)]
    verbose: bool,

    #[arg(short, long, global = true)]
    quiet: bool,

    #[arg(long, global = true)]
    dry_run: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the collector (default)
    Run {
        /// Config file path (sources, schedule, auth, etc.)
        #[arg(short, long, default_value = "hel.yaml", value_name = "PATH")]
        config: PathBuf,

        /// Run one poll cycle and exit
        #[arg(long)]
        once: bool,

        /// Run only specified source(s)
        #[arg(long, value_name = "NAME")]
        source: Option<String>,

        /// Write NDJSON to file instead of stdout
        #[arg(short, long, value_name = "PATH")]
        output: Option<PathBuf>,

        /// Rotate output file: "daily" or "size:N" (N in MB)
        #[arg(long, value_name = "POLICY", requires = "output")]
        output_rotate: Option<String>,

        /// Record HTTP responses to directory (for later replay)
        #[arg(long, value_name = "PATH")]
        record_dir: Option<PathBuf>,

        /// Replay from recorded responses instead of live API (use with --once for testing)
        #[arg(long, value_name = "PATH")]
        replay_dir: Option<PathBuf>,
    },

    /// Validate configuration file
    Validate {
        /// Config file path
        #[arg(short, long, default_value = "hel.yaml", value_name = "PATH")]
        config: PathBuf,
    },

    /// Test a source configuration (one poll tick for the given source)
    Test {
        /// Config file path
        #[arg(short, long, default_value = "hel.yaml", value_name = "PATH")]
        config: PathBuf,

        #[arg(long, value_name = "NAME")]
        source: String,

        #[arg(long, help = "Run one poll cycle (default for test)")]
        once: bool,
    },

    /// Inspect or manage state store
    State {
        /// Config file path (for global.state backend/path)
        #[arg(short, long, default_value = "hel.yaml", value_name = "PATH")]
        config: PathBuf,

        #[command(subcommand)]
        subcommand: Option<StateSubcommand>,
    },

    /// Run mock HTTP server for development (serves YAML-defined responses).
    MockServer {
        /// Mock server config YAML (endpoint, response rules).
        #[arg(value_name = "CONFIG")]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum StateSubcommand {
    Show { source: String },
    Reset { source: String },
    /// Set a single state key for a source
    Set {
        source: String,
        key: String,
        value: String,
    },
    Export,
    /// Import state from JSON (same format as export). Reads from stdin.
    Import,
}

/// Path to hel config (sources, state, etc.) for commands that use it. Default "hel.yaml" when no subcommand (implicit run).
fn hel_config_path(cli: &Cli) -> PathBuf {
    match &cli.command {
        None => PathBuf::from("hel.yaml"),
        Some(Commands::Run { config, .. }) => config.clone(),
        Some(Commands::Validate { config }) => config.clone(),
        Some(Commands::Test { config, .. }) => config.clone(),
        Some(Commands::State { config, .. }) => config.clone(),
        Some(Commands::MockServer { .. }) => PathBuf::from("hel.yaml"), // MockServer uses its own config
    }
}

/// Ignore SIGPIPE so writes to a broken pipe return EPIPE instead of killing the process.
#[cfg(unix)]
fn ignore_sigpipe() {
    unsafe {
        let _ = nix::sys::signal::signal(
            nix::sys::signal::Signal::SIGPIPE,
            nix::sys::signal::SigHandler::SigIgn,
        );
    }
}
#[cfg(not(unix))]
fn ignore_sigpipe() {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    ignore_sigpipe();

    if cli.dry_run {
        tracing::info!("dry-run: would load config from {:?}", hel_config_path(&cli));
        return Ok(());
    }

    match &cli.command {
        Some(Commands::Validate { config }) => {
            init_logging(None, &cli);
            run_validate(config)
        }
        Some(Commands::MockServer { config }) => {
            init_logging(None, &cli);
            mock_server::run_mock_server(config).await
        }
        other => {
            let config = Config::load(&hel_config_path(&cli))?;
            init_logging(Some(&config), &cli);
            match other {
                Some(Commands::Run {
                    once,
                    source,
                    output,
                    output_rotate,
                    record_dir,
                    replay_dir,
                    ..
                }) => {
                    if record_dir.is_some() && replay_dir.is_some() {
                        anyhow::bail!("cannot use both --record-dir and --replay-dir");
                    }
                    let (event_sink, output_path): (Arc<dyn EventSink>, Option<PathBuf>) = match output {
                        Some(path) => {
                            let rotation = output_rotate
                                .as_deref()
                                .map(parse_rotation)
                                .transpose()?
                                .unwrap_or(RotationPolicy::None);
                            let path_clone = path.clone();
                            (
                                Arc::new(FileSink::new(&path, rotation)?),
                                Some(path_clone),
                            )
                        }
                        None => (Arc::new(StdoutSink), None),
                    };
                    let record_state = if let Some(dir) = record_dir {
                        Some(Arc::new(replay::RecordState::new(dir)?))
                    } else {
                        None
                    };
                    let (config_to_use, record_state) = if let Some(dir) = replay_dir {
                        let recordings = replay::load_recordings(dir)?;
                        if recordings.is_empty() {
                            anyhow::bail!("replay dir has no recordings: {}", dir.display());
                        }
                        let (addr, _join) = replay::start_replay_server(recordings, 0).await?;
                        let base = format!("http://{}", addr);
                        tracing::info!(%base, "replay server started");
                        let rewritten = replay::rewrite_config_for_replay(&config, &base);
                        (rewritten, None)
                    } else {
                        (config.clone(), record_state)
                    };
                    run_collector(&config_to_use, *once, source.as_deref(), event_sink, output_path, record_state).await
                }
                Some(Commands::Test { source, .. }) => run_test(&config, source, Arc::new(StdoutSink)).await,
                Some(Commands::State { subcommand, .. }) => run_state(&config, subcommand.as_ref()).await,
                None => {
                    run_collector(&config, false, None, Arc::new(StdoutSink), None, None).await
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Add "source":"hel" to a JSON log line so collector logs and NDJSON events share a consistent source field.
fn add_source_to_json_log_line(line: &str) -> String {
    let trimmed = line.trim();
    if !trimmed.starts_with('{') {
        return line.to_string();
    }
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("source".to_string(), serde_json::Value::String("hel".to_string()));
            }
            let out = serde_json::to_string(&v).unwrap_or_else(|_| line.to_string());
            if line.ends_with('\n') {
                out + "\n"
            } else {
                out
            }
        }
        Err(_) => line.to_string(),
    }
}

/// Writer that buffers stderr until newline, then adds "source":"hel" to JSON lines before writing.
struct JsonSourceLabelWriter {
    buffer: Vec<u8>,
}

impl JsonSourceLabelWriter {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }
}

impl Write for JsonSourceLabelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        let mut stderr = std::io::stderr().lock();
        while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=pos).collect();
            let line_str = String::from_utf8_lossy(&line);
            let out = add_source_to_json_log_line(&line_str);
            stderr.write_all(out.as_bytes())?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut stderr = std::io::stderr().lock();
        if !self.buffer.is_empty() {
            let line_str = String::from_utf8_lossy(&self.buffer).into_owned();
            self.buffer.clear();
            let out = add_source_to_json_log_line(&line_str);
            stderr.write_all(out.as_bytes())?;
        }
        stderr.flush()
    }
}

/// MakeWriter that returns a writer adding "source":"hel" to JSON log lines (for consistent labeling with NDJSON events).
struct HelJsonStderr;

impl Write for HelJsonStderr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        HEL_JSON_WRITER.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        HEL_JSON_WRITER.lock().unwrap().flush()
    }
}

impl Clone for HelJsonStderr {
    fn clone(&self) -> Self {
        HelJsonStderr
    }
}

static HEL_JSON_WRITER: once_cell::sync::Lazy<Mutex<JsonSourceLabelWriter>> =
    once_cell::sync::Lazy::new(|| Mutex::new(JsonSourceLabelWriter::new()));

impl tracing_subscriber::fmt::MakeWriter<'_> for HelJsonStderr {
    type Writer = HelJsonStderr;
    fn make_writer(&self) -> Self::Writer {
        HelJsonStderr
    }
}

/// Init tracing from config (log_format, log_level) or env. Config takes precedence; env HEL_LOG_FORMAT, HEL_LOG_LEVEL (or RUST_LOG when no config) override.
fn init_logging(config: Option<&Config>, cli: &Cli) {
    let use_json = match config.and_then(|c| c.global.log_format.as_deref()) {
        Some("json") => true,
        _ => std::env::var("HEL_LOG_FORMAT").as_deref() == Ok("json")
            || std::env::var("RUST_LOG_JSON").as_deref() == Ok("1"),
    };
    let filter = if cli.quiet {
        EnvFilter::new("error")
    } else if cli.verbose {
        EnvFilter::new("hel=debug,tower_http=debug")
    } else {
        let level = match config {
            Some(c) => std::env::var("HEL_LOG_LEVEL")
                .ok()
                .and_then(|s| {
                    let s = s.trim();
                    if s.is_empty() {
                        None
                    } else {
                        Some(s.to_string())
                    }
                })
                .unwrap_or_else(|| c.global.log_level.clone()),
            None => std::env::var("RUST_LOG")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "info".to_string()),
        };
        let filter_str = format!("hel={}", level);
        if config.is_some() {
            EnvFilter::new(filter_str)
        } else {
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_str))
        }
    };
    if use_json {
        // Omit current_span and span_list so we don't parse span fields as JSON (they're key=value, not JSON).
        // Use HelJsonStderr so each line gets "source":"hel" for consistent labeling with NDJSON events (stdout).
        let json_fmt = tracing_subscriber::fmt::format()
            .json()
            .with_current_span(false)
            .with_span_list(false);
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(HelJsonStderr)
                    .with_ansi(false)
                    .with_target(false)
                    .event_format(json_fmt),
            )
            .with(filter)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_target(true),
            )
            .with(filter)
            .init();
    }
}

fn run_validate(config_path: &std::path::Path) -> anyhow::Result<()> {
    match Config::load(config_path) {
        Ok(_) => {
            tracing::info!("config valid");
            std::process::exit(0);
        }
        Err(e) => {
            tracing::error!("config invalid: {}", e);
            std::process::exit(1);
        }
    }
}

/// Open state store from config (same logic as run_collector).
fn open_store(config: &Config) -> anyhow::Result<Arc<dyn StateStore>> {
    let store: Arc<dyn StateStore> = match &config.global.state {
        Some(state) if state.backend.eq_ignore_ascii_case("sqlite") => {
            let path = state
                .path
                .as_deref()
                .unwrap_or("./hel-state.db");
            Arc::new(SqliteStateStore::open(Path::new(path))?)
        }
        _ => Arc::new(MemoryStateStore::new()),
    };
    Ok(store)
}

/// Run one poll tick for the given source (test).
async fn run_test(
    config: &Config,
    source_name: &str,
    event_sink: Arc<dyn EventSink>,
) -> anyhow::Result<()> {
    if !config.sources.contains_key(source_name) {
        anyhow::bail!("source {:?} not found in config", source_name);
    }
    tracing::info!("testing source {:?} (one poll tick)", source_name);
    let store = open_store(&config)?;
    let circuit_store = new_circuit_store();
    let token_cache = new_oauth2_token_cache();
    let dedupe_store = dedupe::new_dedupe_store();
    poll::run_one_tick(
        &config,
        store,
        Some(source_name),
        circuit_store,
        token_cache,
        dedupe_store,
        event_sink,
        None,
    )
    .await
}

/// State subcommands: show, reset, export.
async fn run_state(
    config: &Config,
    subcommand: Option<&StateSubcommand>,
) -> anyhow::Result<()> {
    let store = open_store(config)?;
    match subcommand {
        Some(StateSubcommand::Show { source }) => state_show(store.as_ref(), source).await,
        Some(StateSubcommand::Reset { source }) => state_reset(store.as_ref(), source).await,
        Some(StateSubcommand::Set { source, key, value }) => {
            state_set(store.as_ref(), source, key, value).await
        }
        Some(StateSubcommand::Export) => state_export(store.as_ref()).await,
        Some(StateSubcommand::Import) => state_import(store.as_ref()).await,
        None => {
            eprintln!("usage: hel state {{show,reset,set,export,import}}");
            eprintln!("  show <source>   show state keys and values for a source");
            eprintln!("  reset <source>  clear all state for a source");
            eprintln!("  set <source> <key> <value>  set a single state key");
            eprintln!("  export         write all state as JSON to stdout");
            eprintln!("  import         read state from JSON on stdin (same format as export)");
            Ok(())
        }
    }
}

async fn state_show(store: &dyn StateStore, source_id: &str) -> anyhow::Result<()> {
    let keys = store.list_keys(source_id).await?;
    if keys.is_empty() {
        println!("{} (no state)", source_id);
        return Ok(());
    }
    println!("{}", source_id);
    for key in keys {
        let value = store.get(source_id, &key).await?.unwrap_or_default();
        println!("  {}: {}", key, value);
    }
    Ok(())
}

async fn state_reset(store: &dyn StateStore, source_id: &str) -> anyhow::Result<()> {
    store.clear_source(source_id).await?;
    println!("reset state for source {:?}", source_id);
    Ok(())
}

async fn state_set(
    store: &dyn StateStore,
    source_id: &str,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    store.set(source_id, key, value).await?;
    println!("set {} {} = {:?}", source_id, key, value);
    Ok(())
}

async fn state_export(store: &dyn StateStore) -> anyhow::Result<()> {
    let sources = store.list_sources().await?;
    let mut out = serde_json::Map::new();
    for source_id in sources {
        let keys = store.list_keys(&source_id).await?;
        let mut m = serde_json::Map::new();
        for key in keys {
            if let Some(v) = store.get(&source_id, &key).await? {
                m.insert(key, serde_json::Value::String(v));
            }
        }
        out.insert(source_id, serde_json::Value::Object(m));
    }
    println!("{}", serde_json::to_string_pretty(&serde_json::Value::Object(out))?);
    Ok(())
}

/// Import state from JSON on stdin (same shape as export: { "source_id": { "key": "value", ... }, ... }).
async fn state_import(store: &dyn StateStore) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut input = String::new();
    std::io::Read::read_to_string(&mut stdin.lock(), &mut input)?;
    let root: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&input).map_err(|e| anyhow::anyhow!("invalid JSON: {}", e))?;
    for (source_id, val) in root {
        let obj = val
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("source {:?} value must be an object", source_id))?;
        for (key, v) in obj {
            let s = v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("state value for {:?}.{} must be a string", source_id, key))?;
            store.set(&source_id, key, s).await?;
        }
    }
    tracing::info!("state import complete");
    Ok(())
}

async fn run_collector(
    config: &Config,
    once: bool,
    source_filter: Option<&str>,
    event_sink: Arc<dyn EventSink>,
    output_path: Option<PathBuf>,
    record_state: Option<Arc<replay::RecordState>>,
) -> anyhow::Result<()> {
    tracing::info!("loaded config");

    let store: Arc<dyn StateStore> = match &config.global.state {
        Some(state) if state.backend.eq_ignore_ascii_case("sqlite") => {
            let path = state
                .path
                .as_deref()
                .unwrap_or("./hel-state.db");
            Arc::new(SqliteStateStore::open(Path::new(path))?)
        }
        _ => Arc::new(MemoryStateStore::new()),
    };

    let circuit_store = new_circuit_store();
    let token_cache = new_oauth2_token_cache();
    let dedupe_store = dedupe::new_dedupe_store();
    poll::run_one_tick(
        &config,
        store.clone(),
        source_filter,
        circuit_store.clone(),
        token_cache.clone(),
        dedupe_store.clone(),
        event_sink.clone(),
        record_state.clone(),
    )
    .await?;

    if once {
        return Ok(());
    }

    // Metrics: init and serve GET /metrics when enabled
    if config
        .global
        .metrics
        .as_ref()
        .map(|m| m.enabled)
        .unwrap_or(false)
    {
        if let Err(e) = metrics::init() {
            tracing::warn!("metrics init failed: {}", e);
        } else {
            let metrics_cfg = config.global.metrics.as_ref().unwrap();
            let addr: SocketAddr = format!("{}:{}", metrics_cfg.address, metrics_cfg.port)
                .parse()
                .map_err(|e| anyhow::anyhow!("metrics address invalid: {}", e))?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            tracing::info!(%addr, "metrics server listening on GET /metrics");
            tokio::spawn(async move {
                let app = axum::Router::new().route(
                    "/metrics",
                    get(|| async {
                        let body = metrics::encode();
                        ([(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")], body)
                    }),
                );
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("metrics server error: {}", e);
                }
            });
        }
    }

    // Health server: bind only when enabled and running continuously
    if let Some(health) = &config.global.health {
        if health.enabled {
            let addr: SocketAddr = format!("{}:{}", health.address, health.port)
                .parse()
                .map_err(|e| anyhow::anyhow!("health address invalid: {}", e))?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let output_path_ready = Arc::new(output_path);
            tracing::info!(%addr, "health server listening on GET /healthz, /readyz, /startupz");
            tokio::spawn(async move {
                let output_path = output_path_ready;
                let app = axum::Router::new()
                    .route("/healthz", get(|| async { StatusCode::OK }))
                    .route(
                        "/readyz",
                        get({
                            let output_path = output_path.clone();
                            move || {
                                let path = output_path.clone();
                                async move {
                                    if let Some(p) = path.as_ref() {
                                        match std::fs::OpenOptions::new().append(true).open(p) {
                                            Ok(_) => StatusCode::OK,
                                            Err(_) => StatusCode::SERVICE_UNAVAILABLE,
                                        }
                                    } else {
                                        StatusCode::OK
                                    }
                                }
                            }
                        }),
                    )
                    .route("/startupz", get(|| async { StatusCode::OK }));
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("health server error: {}", e);
                }
            });
        }
    }

    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    let mut tick = 0u64;
    'run: loop {
        let delay = next_delay(&config);
        tick += 1;
        tracing::debug!(tick, delay_secs = delay.as_secs(), "scheduling next tick");

        tokio::select! {
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received, stopping scheduler");
                break 'run;
            }
            _ = tokio::time::sleep(delay) => {}
        }

        let config_ref = &config;
        let store_ref = store.clone();
        let source_filter_ref = source_filter;
        let circuit_store_ref = circuit_store.clone();
        let token_cache_ref = token_cache.clone();
        let dedupe_store_ref = dedupe_store.clone();
        let event_sink_ref = event_sink.clone();
        let record_state_ref = record_state.as_ref();
        let mut tick_fut = std::pin::pin!(poll::run_one_tick(
            config_ref,
            store_ref,
            source_filter_ref,
            circuit_store_ref,
            token_cache_ref,
            dedupe_store_ref,
            event_sink_ref,
            record_state_ref.cloned(),
        ));

        tokio::select! {
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received, waiting for in-flight poll (timeout {}s)", SHUTDOWN_TIMEOUT.as_secs());
                match tokio::time::timeout(SHUTDOWN_TIMEOUT, tick_fut).await {
                    Ok(Ok(())) => tracing::debug!("in-flight poll completed"),
                    Ok(Err(e)) => tracing::warn!("in-flight poll failed: {}", e),
                    Err(_) => tracing::warn!("in-flight poll did not finish within shutdown timeout"),
                }
                break 'run;
            }
            result = tick_fut.as_mut() => {
                if let Err(e) = result {
                    tracing::error!("tick failed: {}", e);
                }
            }
        }
    }

    if let Err(e) = event_sink.flush() {
        tracing::warn!("flush output: {}", e);
    }
    tracing::info!("graceful shutdown complete");
    Ok(())
}

/// Future that completes when SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Interval + jitter: min interval across sources, max jitter; delay = interval ± jitter, at least 1s.
fn next_delay(config: &Config) -> Duration {
    let interval_secs = config
        .sources
        .values()
        .map(|s| s.schedule.interval_secs)
        .min()
        .unwrap_or(60);
    let jitter_secs = config
        .sources
        .values()
        .filter_map(|s| s.schedule.jitter_secs)
        .max()
        .unwrap_or(0);
    let delta = if jitter_secs > 0 {
        rand::random_range(-(jitter_secs as i64)..=(jitter_secs as i64))
    } else {
        0
    };
    let secs = (interval_secs as i64 + delta).max(1) as u64;
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_source_to_json_log_line() {
        let line = r#"{"timestamp":"2024-01-15T12:00:00Z","level":"INFO","message":"started"}"#;
        let out = add_source_to_json_log_line(line);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v.get("source").and_then(|s| s.as_str()), Some("hel"));
        assert_eq!(v.get("message").and_then(|s| s.as_str()), Some("started"));
    }

    #[test]
    fn test_add_source_to_json_log_line_non_json_unchanged() {
        let line = "not json\n";
        let out = add_source_to_json_log_line(line);
        assert_eq!(out, "not json\n");
    }
}
