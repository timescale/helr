//! Hel — generic HTTP API log collector.
//!
//! Polls HTTP APIs (Okta, GitHub, etc.), handles pagination and state management,
//! emits NDJSON to stdout for downstream collectors (Alloy, Vector, etc.).

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod client;
mod config;
mod event;
mod pagination;
mod poll;
mod retry;
mod state;

use config::Config;
use state::{MemoryStateStore, SqliteStateStore, StateStore};
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

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .init();

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

    poll::run_one_tick(&config, store.clone(), source_filter).await?;

    if once {
        return Ok(());
    }

    let mut tick = 0u64;
    loop {
        let delay = next_delay(&config);
        tick += 1;
        tracing::debug!(tick, delay_secs = delay.as_secs(), "scheduling next tick");
        tokio::time::sleep(delay).await;
        if let Err(e) = poll::run_one_tick(&config, store.clone(), source_filter).await {
            tracing::error!("tick failed: {}", e);
            // continue running; next tick may succeed
        }
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
