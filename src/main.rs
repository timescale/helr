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
mod event;
mod metrics;
mod oauth2;
mod pagination;
mod poll;
mod retry;
mod state;

use axum::http::StatusCode;
use axum::routing::get;
use circuit::new_circuit_store;
use config::Config;
use oauth2::new_oauth2_token_cache;
use state::{MemoryStateStore, SqliteStateStore, StateStore};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "hel")]
#[command(author, version, about = "Generic HTTP API log collector")]
struct Cli {
    #[arg(short, long, default_value = "hel.yaml", value_name = "PATH")]
    config: PathBuf,

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
        /// Run one poll cycle and exit
        #[arg(long)]
        once: bool,

        /// Run only specified source(s)
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
    },

    /// Validate configuration file
    Validate,

    /// Test a source configuration (future)
    Test {
        #[arg(long, value_name = "NAME")]
        source: String,

        #[arg(long)]
        once: bool,
    },

    /// Inspect or manage state store (future)
    State {
        #[command(subcommand)]
        subcommand: Option<StateSubcommand>,
    },
}

#[derive(Subcommand)]
enum StateSubcommand {
    Show { source: String },
    Reset { source: String },
    Export,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = if cli.quiet {
        EnvFilter::new("error")
    } else if cli.verbose {
        EnvFilter::new("hel=debug,tower_http=debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("hel=info"))
    };

    let use_json = std::env::var("HEL_LOG_FORMAT").as_deref() == Ok("json")
        || std::env::var("RUST_LOG_JSON").as_deref() == Ok("1");
    if use_json {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_ansi(false)
                    .with_target(false)
                    .event_format(tracing_subscriber::fmt::format().json()),
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

    if cli.dry_run {
        tracing::info!("dry-run: would load config from {:?}", cli.config);
        return Ok(());
    }

    match &cli.command {
        Some(Commands::Validate) => run_validate(&cli.config),
        Some(Commands::Run { once, source }) => {
            run_collector(&cli.config, *once, source.as_deref()).await
        }
        Some(Commands::Test { .. }) | Some(Commands::State { .. }) => {
            tracing::warn!("command not yet implemented");
            Ok(())
        }
        None => run_collector(&cli.config, false, None).await,
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

async fn run_collector(
    config_path: &Path,
    once: bool,
    source_filter: Option<&str>,
) -> anyhow::Result<()> {
    let config = Config::load(config_path)?;
    tracing::info!("loaded config from {:?}", config_path);

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
    poll::run_one_tick(
        &config,
        store.clone(),
        source_filter,
        circuit_store.clone(),
        token_cache.clone(),
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
            tracing::info!(%addr, "health server listening on GET /healthz");
            tokio::spawn(async move {
                let app = axum::Router::new().route("/healthz", get(|| async { StatusCode::OK }));
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
        let mut tick_fut = std::pin::pin!(poll::run_one_tick(
            config_ref,
            store_ref,
            source_filter_ref,
            circuit_store_ref,
            token_cache_ref,
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

    if let Err(e) = std::io::Write::flush(&mut std::io::stdout()) {
        tracing::warn!("flush stdout: {}", e);
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
