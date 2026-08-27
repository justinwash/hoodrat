use crate::agent::{run_fresh_task, Lane};
use crate::config::Config;
use crate::readiness::{check, ReadinessReport};
use crate::store::Store;
use anyhow::{Context, Result};
use chrono::{Local, NaiveTime, Utc};
use chrono_tz::Tz;
use std::path::Path;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

pub fn run(config: &Config, store: &Store, once: bool) -> Result<()> {
    let report = check(config);
    print_report(&report);
    store.record_audit(
        None,
        "scheduler",
        if report.ready { "started" } else { "blocked" },
        &serde_json::json!({
            "ready": report.ready,
            "blockers": report.blockers,
            "notes": report.notes,
        }),
    )?;

    if !report.ready {
        return Ok(());
    }

    if once {
        run_due_lanes(config, store, true)?;
        return Ok(());
    }

    let mut last_equity =
        Instant::now() - Duration::from_secs(config.schedule.equity_options.interval_secs);
    let mut last_crypto =
        Instant::now() - Duration::from_secs(config.schedule.crypto.interval_secs);
    loop {
        let now = Instant::now();
        if config.schedule.equity_options.enabled
            && now.duration_since(last_equity).as_secs()
                >= config.schedule.equity_options.interval_secs
        {
            if equity_options_open(config) {
                run_lane(config, store, Lane::EquityOptions)?;
            }
            last_equity = now;
        }
        if config.schedule.crypto.enabled
            && now.duration_since(last_crypto).as_secs() >= config.schedule.crypto.interval_secs
        {
            run_lane(config, store, Lane::Crypto)?;
            last_crypto = now;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn run_from_path(config_path: &Path, once: bool) -> Result<()> {
    let mut config = load_config(config_path)?;
    let mut database_path = config.storage.database_path.clone();
    let mut store = Store::open(&database_path)?;

    if once {
        return run(&config, &store, true);
    }

    let mut last_equity =
        Instant::now() - Duration::from_secs(config.schedule.equity_options.interval_secs);
    let mut last_crypto =
        Instant::now() - Duration::from_secs(config.schedule.crypto.interval_secs);
    let mut last_readiness: Option<bool> = None;

    loop {
        config = load_config(config_path)?;
        if config.storage.database_path != database_path {
            database_path = config.storage.database_path.clone();
            store = Store::open(&database_path)?;
        }

        let report = check(&config);
        if last_readiness != Some(report.ready) {
            print_report(&report);
            store.record_audit(
                None,
                "scheduler",
                if report.ready { "started" } else { "blocked" },
                &serde_json::json!({
                    "ready": report.ready,
                    "blockers": report.blockers,
                    "notes": report.notes,
                }),
            )?;
            last_readiness = Some(report.ready);
        }

        if report.ready {
            let now = Instant::now();
            if config.schedule.equity_options.enabled
                && now.duration_since(last_equity).as_secs()
                    >= config.schedule.equity_options.interval_secs
            {
                if equity_options_open(&config) {
                    run_lane(&config, &store, Lane::EquityOptions)?;
                }
                last_equity = now;
            }
            if config.schedule.crypto.enabled
                && now.duration_since(last_crypto).as_secs() >= config.schedule.crypto.interval_secs
            {
                run_lane(&config, &store, Lane::Crypto)?;
                last_crypto = now;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn load_config(config_path: &Path) -> Result<Config> {
    let mut config = Config::load(config_path).with_context(|| {
        format!(
            "could not load {}; run `cargo run -- init` first",
            config_path.display()
        )
    })?;
    config.resolve_paths(config_path);
    config.ensure_parent_directories()?;
    Ok(config)
}

fn run_due_lanes(config: &Config, store: &Store, one_shot: bool) -> Result<()> {
    if config.schedule.equity_options.enabled {
        if equity_options_open(config) {
            run_lane(config, store, Lane::EquityOptions)?;
        } else {
            store.record_audit(
                None,
                "scheduler",
                "equity_lane_skipped",
                &serde_json::json!({"reason": "outside configured local market window", "one_shot": one_shot}),
            )?;
        }
    }
    if config.schedule.crypto.enabled {
        run_lane(config, store, Lane::Crypto)?;
    }
    Ok(())
}

fn run_lane(config: &Config, store: &Store, lane: Lane) -> Result<()> {
    let context = build_context(store)?;
    let policy = serde_json::to_string_pretty(&config.risk)?;
    let result = run_fresh_task(&config.agent, store, lane, &context, &policy)?;
    println!(
        "{} run {} finished with exit={:?}, events={}, tool_events={}",
        lane.as_str(),
        result.run_id,
        result.exit_code,
        result.event_count,
        result.tool_event_count,
    );
    Ok(())
}

fn build_context(store: &Store) -> Result<String> {
    let latest = store.latest_run()?;
    let events = store.recent_events(10)?;
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "generated_at": Utc::now().to_rfc3339(),
        "latest_run": latest,
        "recent_agent_events": events,
        "portfolio_snapshot": "not ingested yet; retrieve it through Robinhood MCP",
    }))?)
}

fn equity_options_open(config: &Config) -> bool {
    let timezone = match Tz::from_str(&config.schedule.equity_options.timezone) {
        Ok(timezone) => timezone,
        Err(_) => return false,
    };
    let start =
        match NaiveTime::parse_from_str(&config.schedule.equity_options.start_local, "%H:%M") {
            Ok(value) => value,
            Err(_) => return false,
        };
    let end = match NaiveTime::parse_from_str(&config.schedule.equity_options.end_local, "%H:%M") {
        Ok(value) => value,
        Err(_) => return false,
    };
    let local_time = Utc::now().with_timezone(&timezone).time();
    local_time >= start && local_time <= end
}

fn print_report(report: &ReadinessReport) {
    println!("{}", report.status());
    for blocker in &report.blockers {
        println!("  blocker: {}", blocker);
    }
    for note in &report.notes {
        println!("  note: {}", note);
    }
    println!("local wall clock: {}", Local::now().to_rfc3339());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn invalid_market_window_is_closed() {
        let mut config = Config::default();
        config.schedule.equity_options.timezone = "not-a-timezone".to_owned();
        assert!(!equity_options_open(&config));
    }
}
