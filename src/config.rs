use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "hoodrat.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    #[default]
    Disabled,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SessionMode {
    #[default]
    Fresh,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub robinhood: RobinhoodConfig,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default)]
    pub risk: RiskConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub ui: UiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub mode: ExecutionMode,
    #[serde(default = "default_true")]
    pub kill_switch_engaged: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Disabled,
            kill_switch_engaged: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_cline_executable")]
    pub executable: String,
    pub working_directory: Option<PathBuf>,
    #[serde(default = "default_cline_config_dir")]
    pub config_dir: PathBuf,
    #[serde(default = "default_cline_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_agent_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_true")]
    pub auto_approve: bool,
    #[serde(default)]
    pub session_mode: SessionMode,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            executable: default_cline_executable(),
            working_directory: None,
            config_dir: default_cline_config_dir(),
            data_dir: default_cline_data_dir(),
            provider: default_provider(),
            model: default_model(),
            timeout_secs: default_agent_timeout(),
            auto_approve: true,
            session_mode: SessionMode::Fresh,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobinhoodConfig {
    #[serde(default = "default_robinhood_url")]
    pub trading_mcp_url: String,
    #[serde(default = "default_mcp_server_name")]
    pub mcp_server_name: String,
    #[serde(default = "default_true")]
    pub agentic_account_only: bool,
    #[serde(default)]
    pub connection_ready: bool,
}

impl Default for RobinhoodConfig {
    fn default() -> Self {
        Self {
            trading_mcp_url: default_robinhood_url(),
            mcp_server_name: default_mcp_server_name(),
            agentic_account_only: true,
            connection_ready: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub equity_options: EquityOptionsSchedule,
    #[serde(default)]
    pub crypto: CryptoSchedule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityOptionsSchedule {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_equity_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default = "default_start_local")]
    pub start_local: String,
    #[serde(default = "default_end_local")]
    pub end_local: String,
}

impl Default for EquityOptionsSchedule {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_equity_interval(),
            timezone: default_timezone(),
            start_local: default_start_local(),
            end_local: default_end_local(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoSchedule {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_crypto_interval")]
    pub interval_secs: u64,
}

impl Default for CryptoSchedule {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_crypto_interval(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    #[serde(default)]
    pub confirmed: bool,
    #[serde(default = "default_max_order_notional")]
    pub max_order_notional_usd: f64,
    #[serde(default = "default_daily_loss_limit")]
    pub daily_loss_limit_usd: f64,
    #[serde(default = "default_max_total_exposure")]
    pub max_total_exposure_usd: f64,
    #[serde(default = "default_max_positions")]
    pub max_concurrent_positions: u32,
    #[serde(default = "default_max_option_dte")]
    pub max_option_dte: u32,
    #[serde(default = "default_max_crypto_exposure")]
    pub max_crypto_exposure_usd: f64,
    #[serde(default = "default_max_spread_bps")]
    pub max_bid_ask_spread_bps: u32,
    #[serde(default = "default_max_quote_age")]
    pub max_quote_age_secs: u64,
    #[serde(default = "default_duplicate_cooldown")]
    pub duplicate_order_cooldown_secs: u64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            confirmed: false,
            max_order_notional_usd: default_max_order_notional(),
            daily_loss_limit_usd: default_daily_loss_limit(),
            max_total_exposure_usd: default_max_total_exposure(),
            max_concurrent_positions: default_max_positions(),
            max_option_dte: default_max_option_dte(),
            max_crypto_exposure_usd: default_max_crypto_exposure(),
            max_bid_ask_spread_bps: default_max_spread_bps(),
            max_quote_age_secs: default_max_quote_age(),
            duplicate_order_cooldown_secs: default_duplicate_cooldown(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_database_path")]
    pub database_path: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: default_database_path(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    #[serde(default = "default_ui_refresh")]
    pub refresh_secs: u64,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            refresh_secs: default_ui_refresh(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: default_schema_version(),
            execution: ExecutionConfig::default(),
            agent: AgentConfig::default(),
            robinhood: RobinhoodConfig::default(),
            schedule: ScheduleConfig::default(),
            risk: RiskConfig::default(),
            storage: StorageConfig::default(),
            ui: UiConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse config at {}", path.display()))
    }

    pub fn write_default(path: &Path) -> Result<()> {
        if path.exists() {
            anyhow::bail!("config already exists at {}", path.display());
        }
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let contents = serde_json::to_string_pretty(&Self::default())? + "\n";
        fs::write(path, contents)
            .with_context(|| format!("failed to write config at {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let contents = serde_json::to_string_pretty(self)? + "\n";
        fs::write(path, contents)
            .with_context(|| format!("failed to write config at {}", path.display()))
    }

    pub fn resolve_paths(&mut self, config_path: &Path) {
        let base = config_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty());
        let Some(base) = base else {
            return;
        };
        if !self.storage.database_path.is_absolute() {
            self.storage.database_path = base.join(&self.storage.database_path);
        }
        if !self.agent.data_dir.is_absolute() {
            self.agent.data_dir = base.join(&self.agent.data_dir);
        }
        if !self.agent.config_dir.is_absolute() {
            self.agent.config_dir = base.join(&self.agent.config_dir);
        }
        if let Some(directory) = &self.agent.working_directory {
            if !directory.is_absolute() {
                self.agent.working_directory = Some(base.join(directory));
            }
        }
    }

    pub fn ensure_parent_directories(&self) -> Result<()> {
        if let Some(parent) = self.storage.database_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::create_dir_all(&self.agent.data_dir)?;
        Ok(())
    }
}

pub fn default_config_path() -> PathBuf {
    PathBuf::from(DEFAULT_CONFIG_PATH)
}

fn default_schema_version() -> u32 {
    1
}
fn default_true() -> bool {
    true
}
fn default_cline_executable() -> String {
    "cline".to_owned()
}
fn default_cline_data_dir() -> PathBuf {
    PathBuf::from("data/cline/data")
}
fn default_cline_config_dir() -> PathBuf {
    PathBuf::from("data/cline")
}
fn default_provider() -> String {
    "openrouter".to_owned()
}
fn default_model() -> String {
    "gpt-5.6-luna".to_owned()
}
fn default_agent_timeout() -> u64 {
    300
}
fn default_robinhood_url() -> String {
    "https://agent.robinhood.com/mcp/trading".to_owned()
}
fn default_mcp_server_name() -> String {
    "robinhood-trading".to_owned()
}
fn default_equity_interval() -> u64 {
    300
}
fn default_crypto_interval() -> u64 {
    900
}
fn default_timezone() -> String {
    "America/New_York".to_owned()
}
fn default_start_local() -> String {
    "09:35".to_owned()
}
fn default_end_local() -> String {
    "15:55".to_owned()
}
fn default_max_order_notional() -> f64 {
    1_000.0
}
fn default_daily_loss_limit() -> f64 {
    250.0
}
fn default_max_total_exposure() -> f64 {
    5_000.0
}
fn default_max_positions() -> u32 {
    10
}
fn default_max_option_dte() -> u32 {
    1
}
fn default_max_crypto_exposure() -> f64 {
    1_000.0
}
fn default_max_spread_bps() -> u32 {
    150
}
fn default_max_quote_age() -> u64 {
    30
}
fn default_duplicate_cooldown() -> u64 {
    60
}
fn default_database_path() -> PathBuf {
    PathBuf::from("data/hoodrat.db")
}
fn default_ui_refresh() -> u64 {
    2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fail_closed() {
        let config = Config::default();
        assert_eq!(config.execution.mode, ExecutionMode::Disabled);
        assert!(config.execution.kill_switch_engaged);
        assert!(!config.risk.confirmed);
        assert!(!config.robinhood.connection_ready);
        assert_eq!(config.agent.session_mode, SessionMode::Fresh);
    }

    #[test]
    fn round_trip_json_preserves_defaults() {
        let json = serde_json::to_string(&Config::default()).unwrap();
        let decoded: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.agent.model, "gpt-5.6-luna");
        assert_eq!(decoded.agent.provider, "openrouter");
    }

    #[test]
    fn relative_runtime_paths_follow_config_directory() {
        let mut config = Config::default();
        config.resolve_paths(Path::new("profiles/dev/hoodrat.json"));
        assert_eq!(
            config.storage.database_path,
            PathBuf::from("profiles/dev/data/hoodrat.db")
        );
        assert_eq!(
            config.agent.data_dir,
            PathBuf::from("profiles/dev/data/cline/data")
        );
        assert_eq!(
            config.agent.config_dir,
            PathBuf::from("profiles/dev/data/cline")
        );
    }
}
