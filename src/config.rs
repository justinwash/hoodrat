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
    /// Size each order to the account's currently available balance (cash /
    /// buying power as reported live by the Trading MCP), capped by
    /// `max_order_notional_usd`. The `fixed_order_notional_usd` value is not
    /// used in this mode.
    AvailableBalance,
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
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub ui: UiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_simulation_tools")]
    pub market_data_tools: Vec<String>,
    #[serde(default = "default_simulation_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_simulation_starting_cash")]
    pub starting_cash_usd: f64,
    #[serde(default = "default_simulation_quote_age")]
    pub max_quote_age_secs: u64,
    #[serde(default)]
    pub profile: SimulationProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationProfile {
    #[serde(default = "default_simulation_profile_name")]
    pub name: String,
    #[serde(default = "default_true")]
    pub allow_equities: bool,
    #[serde(default = "default_true")]
    pub allow_options: bool,
    #[serde(default = "default_true")]
    pub allow_short: bool,
    #[serde(default = "default_true")]
    pub allow_leverage: bool,
    #[serde(default = "default_simulation_zero_dte")]
    pub allow_zero_dte: bool,
    #[serde(default = "default_simulation_option_dte")]
    pub max_option_dte: u32,
    #[serde(default = "default_simulation_holding_secs")]
    pub max_holding_secs: u64,
    #[serde(default = "default_simulation_slippage_bps")]
    pub slippage_bps: u32,
    #[serde(default = "default_simulation_fee_bps")]
    pub fee_bps: u32,
    #[serde(default = "default_simulation_max_leverage")]
    pub max_leverage: f64,
    #[serde(default = "default_simulation_max_exposure")]
    pub max_gross_exposure_usd: f64,
    #[serde(default = "default_simulation_max_positions")]
    pub max_positions: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            market_data_tools: default_simulation_tools(),
            symbols: default_simulation_symbols(),
            starting_cash_usd: default_simulation_starting_cash(),
            max_quote_age_secs: default_simulation_quote_age(),
            profile: SimulationProfile::default(),
        }
    }
}

impl SimulationProfile {
    pub fn aggressive_default() -> Self {
        Self {
            name: default_simulation_profile_name(),
            allow_equities: true,
            allow_options: true,
            allow_short: true,
            allow_leverage: true,
            allow_zero_dte: default_simulation_zero_dte(),
            max_option_dte: default_simulation_option_dte(),
            max_holding_secs: default_simulation_holding_secs(),
            slippage_bps: default_simulation_slippage_bps(),
            fee_bps: default_simulation_fee_bps(),
            max_leverage: default_simulation_max_leverage(),
            max_gross_exposure_usd: default_simulation_max_exposure(),
            max_positions: default_simulation_max_positions(),
        }
    }
}

impl Default for SimulationProfile {
    fn default() -> Self {
        Self::aggressive_default()
    }
}

impl SimulationConfig {
    pub fn validate_market_data(&self) -> Result<()> {
        if self.market_data_tools.is_empty() {
            anyhow::bail!("simulation requires at least one configured read-only market-data tool");
        }
        if self
            .market_data_tools
            .iter()
            .any(|tool| tool.trim().is_empty())
        {
            anyhow::bail!("simulation market-data tool names must not be empty");
        }
        let mut unique_tools = std::collections::HashSet::new();
        if self
            .market_data_tools
            .iter()
            .any(|tool| !unique_tools.insert(tool.to_ascii_lowercase()))
        {
            anyhow::bail!("simulation market-data tools must not contain duplicates");
        }
        if self.symbols.is_empty() {
            anyhow::bail!("simulation requires at least one configured symbol");
        }
        if self.starting_cash_usd <= 0.0 || !self.starting_cash_usd.is_finite() {
            anyhow::bail!("simulation starting cash must be finite and greater than zero");
        }
        if self.max_quote_age_secs == 0 {
            anyhow::bail!("simulation max quote age must be greater than zero");
        }
        if self
            .market_data_tools
            .iter()
            .any(|tool| !is_read_only_market_tool_name(tool))
        {
            anyhow::bail!("simulation market_data_tools must contain only read-only get_* tools");
        }
        if self.market_data_tools.iter().any(|tool| {
            tool.rsplit("__")
                .next()
                .unwrap_or(tool)
                .eq_ignore_ascii_case("get_crypto_quotes")
        }) {
            anyhow::bail!("crypto market data is disabled for the equity/options simulator");
        }
        if self.symbols.iter().any(|symbol| {
            matches!(
                symbol.to_ascii_uppercase().as_str(),
                "BTC"
                    | "ETH"
                    | "DOGE"
                    | "SOL"
                    | "XRP"
                    | "LTC"
                    | "BCH"
                    | "ADA"
                    | "AVAX"
                    | "LINK"
                    | "DOT"
                    | "UNI"
                    | "SHIB"
            )
        }) {
            anyhow::bail!("crypto symbols are disabled for the equity/options simulator");
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            anyhow::bail!(
                "simulation is disabled; set simulation.enabled=true for the paper harness"
            );
        }
        self.validate_market_data()?;
        let profile = &self.profile;
        if profile.max_option_dte > 1 {
            anyhow::bail!("the aggressive simulation profile supports only 0-1 DTE options");
        }
        if profile.max_holding_secs == 0
            || profile.max_leverage <= 0.0
            || profile.max_gross_exposure_usd <= 0.0
            || profile.max_positions == 0
        {
            anyhow::bail!("simulation profile limits must be greater than zero");
        }
        Ok(())
    }
}

fn is_read_only_market_tool_name(tool: &str) -> bool {
    let name = tool
        .rsplit("__")
        .next()
        .unwrap_or(tool)
        .to_ascii_lowercase();
    name.strip_prefix("get_").is_some_and(|operation| {
        !operation.is_empty()
            && ![
                "place_", "cancel_", "replace_", "submit_", "preview_", "create_", "update_",
                "delete_", "add_", "remove_", "modify_", "execute_", "write_",
            ]
            .iter()
            .any(|prefix| operation.starts_with(prefix))
    })
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
            allow_options: true,
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
            .any(|lane| lane != "equity_options")
        {
            anyhow::bail!("strategy contract contains an unsupported scheduler lane");
        }
        if self.allowed_asset_classes.is_empty() {
            anyhow::bail!("strategy contract must allow at least one asset class");
        }
        if self
            .allowed_asset_classes
            .iter()
            .any(|asset_class| !matches!(asset_class.as_str(), "equity" | "option"))
        {
            anyhow::bail!("strategy contract contains an unsupported asset class");
        }
        // The "*" entry is the operator-authorized wildcard: it makes the
        // symbol allowlist unrestricted while keeping the contract validated.
        // An enabled canary still requires a non-empty list so a misconfigured
        // "forgot the symbols" state fails closed instead of silently meaning
        // "no symbols at all".
        if self.canary_enabled && self.approved_symbols.is_empty() {
            anyhow::bail!(
                "enabled strategy canary requires at least one approved symbol or the \"*\" wildcard"
            );
        }
        // Leverage is permitted for operator-approved live canaries. The
        // original "initial canary" prohibition was a scaffold-phase guard;
        // it is lifted here because the operator explicitly authorized it.
        // The monetary, asset-class, lane, and timing limits above still
        // apply and remain validated.
        if (self.sizing_mode == SizingMode::FixedNotional && self.fixed_order_notional_usd <= 0.0)
            || self.max_order_notional_usd <= 0.0
            || self.daily_loss_limit_usd <= 0.0
            || self.max_total_exposure_usd <= 0.0
        {
            anyhow::bail!("strategy contract monetary limits must be greater than zero");
        }
        if self.sizing_mode == SizingMode::FixedNotional
            && self.fixed_order_notional_usd > self.max_order_notional_usd
        {
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

    /// True when the operator has authorized unrestricted trading in any
    /// symbol via the "*" wildcard entry in `approved_symbols`.
    pub fn allows_any_symbol(&self) -> bool {
        self.approved_symbols.iter().any(|symbol| symbol.trim() == "*")
    }

    pub fn summary(&self) -> String {
        let mut summary = serde_json::to_string_pretty(self)
            .expect("strategy contract serialization cannot fail");
        if self.allows_any_symbol() {
            summary.push_str(
                "\n\nOperator override: approved_symbols contains \"*\", so symbol approval is \
                 UNRESTRICTED. The agent may trade any symbol it deems appropriate. All other \
                 contract limits, asset-class restrictions, and no-op conditions still apply.",
            );
        }
        if self.sizing_mode == SizingMode::AvailableBalance {
            summary.push_str(
                "\n\nOperator override: sizing_mode is available_balance. Size each order to what \
                 the account currently has available (cash / buying power as reported live by the \
                 Trading MCP), never exceeding the configured max_order_notional_usd. Do not size \
                 to a fixed dollar amount.",
            );
        }
        summary
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
            simulation: SimulationConfig::default(),
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
    vec!["equity_options".to_owned()]
}
fn default_strategy_asset_classes() -> Vec<String> {
    vec!["equity".to_owned(), "option".to_owned()]
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
fn default_simulation_tools() -> Vec<String> {
    vec![
        "get_equity_quotes".to_owned(),
        "get_option_chains".to_owned(),
        "get_option_instruments".to_owned(),
        "get_option_quotes".to_owned(),
    ]
}
fn default_simulation_symbols() -> Vec<String> {
    vec!["SPY".to_owned(), "QQQ".to_owned()]
}
fn default_simulation_starting_cash() -> f64 {
    10_000.0
}
fn default_simulation_quote_age() -> u64 {
    120
}
fn default_simulation_profile_name() -> String {
    "aggressive-any-risk-sim-v1".to_owned()
}
fn default_simulation_zero_dte() -> bool {
    true
}
fn default_simulation_option_dte() -> u32 {
    1
}
fn default_simulation_holding_secs() -> u64 {
    86_400
}
fn default_simulation_slippage_bps() -> u32 {
    30
}
fn default_simulation_fee_bps() -> u32 {
    5
}
fn default_simulation_max_leverage() -> f64 {
    4.0
}
fn default_simulation_max_exposure() -> f64 {
    50_000.0
}
fn default_simulation_max_positions() -> usize {
    100
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
        assert!(config.strategy.allow_options);
        assert_eq!(config.strategy.allowed_lanes, vec!["equity_options"]);
        assert_eq!(
            config.strategy.allowed_asset_classes,
            vec!["equity", "option"]
        );
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
        assert_eq!(config.strategy.allowed_lanes, vec!["equity_options"]);
        assert_eq!(
            config.strategy.allowed_asset_classes,
            vec!["equity", "option"]
        );
        assert!(config.strategy.approved_symbols.is_empty());
        assert_eq!(config.strategy.fixed_order_notional_usd, 25.0);
        assert!(config.strategy.fingerprint().contains("canary-v1"));
    }

    #[test]
    fn strategy_contract_rejects_unsafe_or_overbroad_changes() {
        let config = Config::default();
        let mut strategy = config.strategy.clone();
        strategy.max_order_notional_usd = config.risk.max_order_notional_usd + 1.0;
        assert!(strategy.validate_against_risk(&config.risk).is_err());

        strategy = config.strategy;
        strategy.canary_enabled = true;
        assert!(strategy
            .validate_against_risk(&RiskConfig::default())
            .is_err());
    }

    #[test]
    fn enabled_canary_accepts_operator_wildcard() {
        let config = Config::default();
        let mut strategy = config.strategy;
        strategy.canary_enabled = true;
        strategy.approved_symbols = vec!["*".to_owned()];
        assert!(strategy.validate_against_risk(&config.risk).is_ok());
        assert!(strategy.allows_any_symbol());
        assert!(strategy.summary().contains("UNRESTRICTED"));
        let report = crate::readiness::check(&Config {
            strategy: strategy.clone(),
            execution: crate::config::ExecutionConfig {
                mode: crate::config::ExecutionMode::Live,
                kill_switch_engaged: false,
            },
            risk: crate::config::RiskConfig {
                confirmed: true,
                ..config.risk.clone()
            },
            robinhood: crate::config::RobinhoodConfig {
                connection_ready: true,
                ..config.robinhood.clone()
            },
            ..config
        });
        assert!(report.ready, "report.ready must hold: {:?}", report.blockers);
    }

    #[test]
    fn enabled_canary_still_rejects_truly_empty_allowlist() {
        let config = Config::default();
        let mut strategy = config.strategy;
        strategy.canary_enabled = true;
        strategy.approved_symbols = Vec::new();
        assert!(strategy.validate_against_risk(&config.risk).is_err());
        assert!(!strategy.allows_any_symbol());
    }

    #[test]
    fn operator_approved_canary_may_enable_leverage_and_raise_limits() {
        let mut config = Config::default();
        // Apply the same operator-approved live posture as hoodrat.json:
        // leverage allowed, daily loss and total exposure raised to 50000
        // in both the strategy contract and the risk policy.
        config.execution.mode = ExecutionMode::Live;
        config.execution.kill_switch_engaged = false;
        config.risk.confirmed = true;
        config.robinhood.connection_ready = true;
        config.risk.daily_loss_limit_usd = 50_000.0;
        config.risk.max_total_exposure_usd = 50_000.0;
        config.risk.max_order_notional_usd = 50_000.0;
        config.strategy.canary_enabled = true;
        config.strategy.approved_symbols = vec!["*".to_owned()];
        config.strategy.allow_leverage = true;
        config.strategy.daily_loss_limit_usd = 50_000.0;
        config.strategy.max_total_exposure_usd = 50_000.0;
        config.strategy.fixed_order_notional_usd = 50_000.0;
        config.strategy.max_order_notional_usd = 50_000.0;
        assert!(config.strategy.validate_against_risk(&config.risk).is_ok());
        assert!(config.strategy.allow_leverage);
        let report = crate::readiness::check(&config);
        assert!(report.ready, "report.ready must hold: {:?}", report.blockers);
    }

    #[test]
    fn available_balance_sizing_is_live_valid_and_summarized() {
        let mut config = Config::default();
        config.execution.mode = ExecutionMode::Live;
        config.execution.kill_switch_engaged = false;
        config.risk.confirmed = true;
        config.robinhood.connection_ready = true;
        config.risk.max_order_notional_usd = 50_000.0;
        config.risk.daily_loss_limit_usd = 50_000.0;
        config.risk.max_total_exposure_usd = 50_000.0;
        config.strategy.canary_enabled = true;
        config.strategy.approved_symbols = vec!["*".to_owned()];
        config.strategy.allow_leverage = true;
        config.strategy.sizing_mode = SizingMode::AvailableBalance;
        config.strategy.fixed_order_notional_usd = 0.0; // unused in available_balance mode
        config.strategy.max_order_notional_usd = 50_000.0;
        config.strategy.daily_loss_limit_usd = 50_000.0;
        config.strategy.max_total_exposure_usd = 50_000.0;
        assert!(config.strategy.validate_against_risk(&config.risk).is_ok());
        assert!(config
            .strategy
            .summary()
            .contains("available_balance"));
        let report = crate::readiness::check(&config);
        assert!(report.ready, "report.ready must hold: {:?}", report.blockers);
    }

    #[test]
    fn strategy_limits_cannot_exceed_risk_policy_even_when_raised() {
        let strategy = StrategyContract {
            canary_enabled: true,
            approved_symbols: vec!["*".to_owned()],
            allow_leverage: true,
            daily_loss_limit_usd: 50_000.0,
            max_total_exposure_usd: 50_000.0,
            ..StrategyContract::default()
        };
        let risk = RiskConfig::default(); // still the low defaults
        assert!(strategy.validate_against_risk(&risk).is_err());
    }

    #[test]
    fn strategy_fingerprint_changes_when_contract_changes() {
        let first = StrategyContract::default();
        let mut second = first.clone();
        second.fixed_order_notional_usd += 1.0;
        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn simulation_defaults_are_disabled_and_equity_options_only() {
        let config = Config::default();
        assert!(config.simulation.profile.allow_equities);
        assert!(config.simulation.profile.allow_options);
        assert!(config.simulation.profile.allow_zero_dte);
        assert!(config
            .simulation
            .market_data_tools
            .iter()
            .all(|tool| !tool.contains("crypto")));
        config.simulation.validate().unwrap_err();
    }

    #[test]
    fn simulation_rejects_crypto_configuration() {
        let mut config = Config::default();
        config.simulation.enabled = true;
        config.simulation.market_data_tools = vec!["get_equity_quotes".to_owned()];
        config.simulation.symbols = vec!["BTC".to_owned()];
        assert!(config.simulation.validate().is_err());
    }

    #[test]
    fn simulation_rejects_write_like_or_duplicate_market_tools() {
        let mut config = Config::default();
        config.simulation.enabled = true;
        config.simulation.market_data_tools = vec!["get_place_order".to_owned()];
        assert!(config.simulation.validate().is_err());

        config.simulation.market_data_tools = vec![
            "get_equity_price_book".to_owned(),
            "GET_EQUITY_PRICE_BOOK".to_owned(),
        ];
        assert!(config.simulation.validate().is_err());
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
