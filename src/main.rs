use anyhow::Result;
use clap::Parser;

mod auth;
mod bot;
mod config;
mod events;
mod historical;
mod paper;
mod poller;
mod ui;

#[derive(Parser)]
#[command(
    name = "hoodrat",
    about = "AI-powered trading bot via Robinhood MCP + Claude Code",
    long_about = "Launches a dashboard UI by default. Use --headless for terminal use.\n\n\
                  Trading mode is selected in the UI (or TRADING_MODE env var).\n\n\
                  Authentication is handled automatically by Claude Code."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Run without a UI, logging to stdout
    #[arg(long)]
    headless: bool,

    /// Run one cycle then exit (headless only)
    #[arg(long, requires = "headless")]
    once: bool,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Save a Robinhood access token to .env (legacy)
    Auth { token: String },
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("hoodrat=info")),
        )
        .init();

    let cli = Cli::parse();

    if let Some(Command::Auth { token }) = cli.command {
        auth::write_token(&token, None)?;
        println!("Token saved to .env");
        return Ok(());
    }

    let config = config::Config::from_env()?;

    match &config.mode {
        config::TradingMode::Paper => tracing::info!("Mode: PAPER"),
        config::TradingMode::Live  => tracing::info!("Mode: LIVE"),
    }

    if cli.headless {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let mut bot = bot::Bot::new(config);
                if cli.once { bot.run_once().await } else { bot.run_loop().await }
            })?;
    } else {
        ui::launch(config)?;
    }

    Ok(())
}
