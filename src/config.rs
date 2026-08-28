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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    #[default]
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SizingMode {
    #[default]
    FixedNotional,
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
    pub strategy: StrategyContract,
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
pub struct StrategyContract {
    #[serde(default = "default_strategy_contract_version")]
    pub contract_version: String,
    #[serde(default)]
    pub canary_enabled: bool,
    #[serde(default = "default_strategy_lanes")]
    pub allowed_lanes: Vec<String>,
    #[serde(default = "default_strategy_asset_classes")]
    pub allowed_asset_classes: Vec<String>,
    #[serde(default)]
    pub approved_symbols: Vec<String>,
    #[serde(default)]
    pub order_type: OrderType,
    #[serde(default)]
    pub sizing_mode: SizingMode,
    #[serde(default = "default_fixed_order_notional")]
    pub fixed_order_notional_usd: f64,
    #[serde(default = "default_strategy_max_order_notional")]
    pub max_order_notional_usd: f64,
    #[serde(default = "default_strategy_daily_loss_limit")]
    pub daily_loss_limit_usd: f64,
    #[serde(default = "default_strategy_max_total_exposure")]
    pub max_total_exposure_usd: f64,
    #[serde(default = "default_strategy_cooldown")]
    pub duplicate_order_cooldown_secs: u64,
    #[serde(default = "default_strategy_interval")]
    pub minimum_interval_secs: u64,
    #[serde(default)]
    pub allow_options: bool,
    #[serde(default)]
    pub allow_leverage: bool,
    #[serde(default = "default_no_op_conditions")]
    pub no_op_conditions: Vec<String>,
}

impl Default for StrategyContract {
    fn default() -> Self {
        Self {
            contract_version: default_strategy_contract_version(),
            canary_enabled: false,
            allowed_lanes: default_strategy_lanes(),
            allowed_asset_classes: default_strategy_asset_classes(),
            approved_symbols: Vec::new(),
            order_type: OrderType::default(),
            sizing_mode: SizingMode::default(),
            fixed_order_notional_usd: default_fixed_order_notional(),
            max_order_notional_usd: default_strategy_max_order_notional(),
            daily_loss_limit_usd: default_strategy_daily_loss_limit(),
            max_total_exposure_usd: default_strategy_max_total_exposure(),
            duplicate_order_cooldown_secs: default_strategy_cooldown(),
            minimum_interval_secs: default_strategy_interval(),
            allow_options: false,
            allow_leverage: false,
            no_op_conditions: default_no_op_conditions(),
        }
    }
}

impl StrategyContract {
    pub fn validate_against_risk(&self, risk: &RiskConfig) -> Result<()> {
        if self.contract_version.trim().is_empty() {
            anyhow::bail!("strategy contract version must not be empty");
        }
        if self.allowed_lanes.is_empty() {
            anyhow::bail!("strategy contract must allow at least one scheduler lane");
        }
        if self
            .allowed_lanes
            .iter()
            .any(|lane| !matches!(lane.as_str(), "crypto" | "equity_options"))
        {
            anyhow::bail!("strategy contract contains an unsupported scheduler lane");
        }
        if self.allowed_asset_classes.is_empty() {
            anyhow::bail!("strategy contract must allow at least one asset class");
        }
        if self
            .allowed_asset_classes
            .iter()
            .any(|asset_class| !matches!(asset_class.as_str(), "crypto" | "equity"))
        {
            anyhow::bail!("strategy contract contains an unsupported asset class");
        }
        if self.canary_enabled && self.approved_symbols.is_empty() {
            anyhow::bail!("enabled strategy canary requires at least one approved symbol");
        }
        if self.allow_options {
            anyhow::bail!("strategy contract must forbid options in the initial canary");
        }
        if self.allow_leverage {
            anyhow::bail!("strategy contract must forbid leverage in the initial canary");
        }
        if self.fixed_order_notional_usd <= 0.0
            || self.max_order_notional_usd <= 0.0
            || self.daily_loss_limit_usd <= 0.0
            || self.max_total_exposure_usd <= 0.0
        {
            anyhow::bail!("strategy contract monetary limits must be greater than zero");
        }
        if self.fixed_order_notional_usd > self.max_order_notional_usd {
            anyhow::bail!("fixed strategy order notional exceeds the strategy order cap");
        }
        if self.minimum_interval_secs == 0 || self.duplicate_order_cooldown_secs == 0 {
            anyhow::bail!("strategy timing limits must be greater than zero");
        }
        if self.max_order_notional_usd > risk.max_order_notional_usd {
            anyhow::bail!("strategy order cap exceeds the configured risk order cap");
        }
        if self.daily_loss_limit_usd > risk.daily_loss_limit_usd {
            anyhow::bail!("strategy daily loss limit exceeds the configured risk limit");
        }
        if self.max_total_exposure_usd > risk.max_total_exposure_usd {
            anyhow::bail!("strategy exposure cap exceeds the configured risk limit");
        }
        if self.duplicate_order_cooldown_secs < risk.duplicate_order_cooldown_secs {
            anyhow::bail!("strategy duplicate cooldown is less conservative than risk policy");
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> String {
        serde_json::to_string(self).expect("strategy contract serialization cannot fail")
    }

    pub fn summary(&self) -> String {
        serde_json::to_string_pretty(self).expect("strategy contract serialization cannot fail")
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
            strategy: StrategyContract::default(),
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
fn default_strategy_contract_version() -> String {
    "canary-v1".to_owned()
}
fn default_strategy_lanes() -> Vec<String> {
    vec!["crypto".to_owned()]
}
fn default_strategy_asset_classes() -> Vec<String> {
    vec!["crypto".to_owned()]
}
fn default_fixed_order_notional() -> f64 {
    25.0
}
fn default_strategy_max_order_notional() -> f64 {
    25.0
}
fn default_strategy_daily_loss_limit() -> f64 {
    25.0
}
fn default_strategy_max_total_exposure() -> f64 {
    100.0
}
fn default_strategy_cooldown() -> u64 {
    3_600
}
fn default_strategy_interval() -> u64 {
    900
}
fn default_no_op_conditions() -> Vec<String> {
    vec![
        "no approved symbol or asset class match".to_owned(),
        "quote is missing, stale, or outside the spread policy".to_owned(),
        "daily loss, exposure, position, or notional limit would be exceeded".to_owned(),
        "duplicate-order cooldown or ambiguous prior run is active".to_owned(),
        "any unexpected tool, MCP error, reconciliation drift, or failed audit".to_owned(),
    ]
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
        assert!(!config.strategy.canary_enabled);
        assert!(!config.strategy.allow_options);
        assert!(!config.strategy.allow_leverage);
    }

    #[test]
    fn round_trip_json_preserves_defaults() {
        let json = serde_json::to_string(&Config::default()).unwrap();
        let decoded: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.agent.model, "gpt-5.6-luna");
        assert_eq!(decoded.agent.provider, "openrouter");
        assert_eq!(decoded.strategy.contract_version, "canary-v1");
        assert_eq!(decoded.strategy.order_type, OrderType::Limit);
    }

    #[test]
    fn default_strategy_contract_is_conservative_and_valid() {
        let config = Config::default();
        config.strategy.validate_against_risk(&config.risk).unwrap();
        assert_eq!(config.strategy.allowed_lanes, vec!["crypto"]);
        assert!(config.strategy.approved_symbols.is_empty());
        assert_eq!(config.strategy.fixed_order_notional_usd, 25.0);
        assert!(config.strategy.fingerprint().contains("canary-v1"));
    }

    #[test]
    fn strategy_contract_rejects_unsafe_or_overbroad_changes() {
        let config = Config::default();
        let mut strategy = config.strategy.clone();
        strategy.allow_options = true;
        assert!(strategy.validate_against_risk(&config.risk).is_err());

        strategy = config.strategy.clone();
        strategy.max_order_notional_usd = config.risk.max_order_notional_usd + 1.0;
        assert!(strategy.validate_against_risk(&config.risk).is_err());

        strategy = config.strategy;
        strategy.canary_enabled = true;
        assert!(strategy
            .validate_against_risk(&RiskConfig::default())
            .is_err());
    }

    #[test]
    fn strategy_fingerprint_changes_when_contract_changes() {
        let first = StrategyContract::default();
        let mut second = first.clone();
        second.fixed_order_notional_usd += 1.0;
        assert_ne!(first.fingerprint(), second.fingerprint());
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
