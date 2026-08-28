use crate::agent::{
    run_fresh_task_with_strategy, run_read_only_reconciliation, Lane, ProcessAgentExecutor,
};
use crate::config::{Config, StrategyContract};
use crate::readiness::{check, ReadinessReport};
use crate::store::Store;
use anyhow::{Context, Result};
use chrono::{Local, NaiveTime, Utc};
use chrono_tz::Tz;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

    if !run_startup_reconciliation(config, store)? {
        return Ok(());
    }

    if once {
        run_due_lanes(config, store, true)?;
        return Ok(());
    }

    let mut last_equity =
        Instant::now() - Duration::from_secs(config.schedule.equity_options.interval_secs);
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
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn run_from_path(config_path: &Path, once: bool) -> Result<()> {
    run_loop(config_path, once, Arc::new(AtomicBool::new(false)))
}

/// Run the continuous scheduler until `stop` is set to true. Used by the
/// combined dashboard+scheduler process mode so the GUI process can shut the
/// loop down cleanly when its window closes.
pub fn run_from_path_until(config_path: &Path, stop: Arc<AtomicBool>) -> Result<()> {
    run_loop(config_path, false, stop)
}

fn run_loop(config_path: &Path, once: bool, stop: Arc<AtomicBool>) -> Result<()> {
    let mut config = load_config(config_path)?;
    let mut database_path = config.storage.database_path.clone();
    let mut store = Store::open(&database_path)?;

    if once {
        return run(&config, &store, true);
    }

    let mut last_equity =
        Instant::now() - Duration::from_secs(config.schedule.equity_options.interval_secs);
    let mut last_readiness: Option<bool> = None;
    let mut reconciliation_ready: Option<bool> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
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
            if !report.ready {
                reconciliation_ready = None;
            }
        }

        if report.ready {
            if reconciliation_ready.is_none() {
                reconciliation_ready = Some(run_startup_reconciliation(&config, &store)?);
            }
            if reconciliation_ready != Some(true) {
                thread::sleep(Duration::from_secs(1));
                continue;
            }
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
        }
        thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}

fn run_startup_reconciliation(config: &Config, store: &Store) -> Result<bool> {
    let result = run_read_only_reconciliation(
        &config.agent,
        store,
        &config.robinhood.mcp_server_name,
        &ProcessAgentExecutor,
    );
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            store.record_audit(
                None,
                "reconciliation",
                "blocked",
                &serde_json::json!({"error": error.to_string()}),
            )?;
            eprintln!("startup reconciliation failed: {error:#}");
            return Ok(false);
        }
    };
    let report = result.reconciliation.as_ref();
    let ready = result.exit_code == Some(0)
        && result.mcp_error_count == 0
        && result.unexpected_tool_count == 0
        && report.is_some_and(|report| reconciliation_status_allows_scheduler(&report.status));
    store.record_audit(
        None,
        "reconciliation",
        if ready { "passed" } else { "blocked" },
        &serde_json::json!({
            "run_id": result.run_id,
            "exit_code": result.exit_code,
            "mcp_error_count": result.mcp_error_count,
            "unexpected_tool_count": result.unexpected_tool_count,
            "report": report,
        }),
    )?;
    if !ready {
        eprintln!("scheduler lanes blocked by startup reconciliation");
    }
    Ok(ready)
}

fn reconciliation_status_allows_scheduler(status: &str) -> bool {
    matches!(status, "baseline" | "reconciled")
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
    Ok(())
}

fn run_lane(config: &Config, store: &Store, lane: Lane) -> Result<()> {
    if !strategy_allows_lane(&config.strategy, lane) {
        store.record_audit(
            None,
            "scheduler",
            "lane_skipped",
            &serde_json::json!({
                "lane": lane.as_str(),
                "reason": "lane is not allowed by the strategy contract",
                "strategy_contract_version": config.strategy.contract_version,
                "strategy_contract_fingerprint": config.strategy.fingerprint(),
            }),
        )?;
        println!(
            "{} lane skipped: not allowed by strategy contract {}",
            lane.as_str(),
            config.strategy.contract_version
        );
        return Ok(());
    }
    let context = build_context(store)?;
    let policy = serde_json::to_string_pretty(&config.risk)?;
    let result = run_fresh_task_with_strategy(
        &config.agent,
        store,
        lane,
        &context,
        &policy,
        &config.risk,
        &config.strategy,
    )?;
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

fn strategy_allows_lane(strategy: &StrategyContract, lane: Lane) -> bool {
    strategy
        .allowed_lanes
        .iter()
        .any(|allowed_lane| allowed_lane == lane.as_str())
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

    #[test]
    fn only_successful_reconciliation_statuses_allow_scheduler_lanes() {
        assert!(reconciliation_status_allows_scheduler("baseline"));
        assert!(reconciliation_status_allows_scheduler("reconciled"));
        assert!(!reconciliation_status_allows_scheduler(
            "coverage_incomplete"
        ));
        assert!(!reconciliation_status_allows_scheduler("drift_detected"));
        assert!(!reconciliation_status_allows_scheduler(
            "reconciliation_failed"
        ));
    }

    #[test]
    fn strategy_contract_restricts_scheduler_lanes() {
        let strategy = Config::default().strategy;
        assert!(strategy_allows_lane(&strategy, Lane::EquityOptions));
    }
}
