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
mod state;

use config::Config;

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

fn main() -> anyhow::Result<()> {
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
        Some(Commands::Run { once, source: _ }) => run_collector(&cli.config, *once),
        Some(Commands::Test { .. }) | Some(Commands::State { .. }) => {
            tracing::warn!("command not yet implemented");
            Ok(())
        }
        None => run_collector(&cli.config, false),
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

fn run_collector(config_path: &std::path::Path, once: bool) -> anyhow::Result<()> {
    let _config = Config::load(config_path)?;
    tracing::info!("loaded config from {:?}", config_path);
    if once {
        tracing::info!("--once: one poll cycle (not yet implemented)");
    } else {
        tracing::info!("continuous mode (not yet implemented)");
    }
    Ok(())
}
