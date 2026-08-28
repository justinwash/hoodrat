# Hoodrat

Hoodrat is a Windows-first Rust supervisor for an agentic Robinhood trading
workflow. It launches a fresh, headless Cline task for each scheduled
evaluation. Cline connects directly to Robinhood's Trading MCP, while Hoodrat
stores the run history, audit events, portfolio snapshots, and execution
records in SQLite.

> **Live-trading warning:** this repository is an execution scaffold, not a
> trading recommendation or a guarantee of safe execution. With the current
> direct Cline-to-Robinhood design, Rust can audit and monitor after the fact,
> but it cannot guarantee a pre-trade block on an MCP write that Cline submits
> directly. Robinhood's agent controls and confirmations are therefore the
> primary execution boundary. Do not enable live mode until you have reviewed
> the configuration, account permissions, and risk policy.

## Current architecture

```text
Slint dashboard / CLI
          |
          v
Rust scheduler + SQLite state/audit store
          |
          v
Fresh Cline CLI task (--json)
          |
          v
Robinhood Trading MCP (direct, HTTPS)
```

Every scheduled task is isolated. Continuity comes from a context packet built
by Rust from persisted state, not from a long-lived Cline conversation.

The active scaffold has one equity/options schedule lane. Its capability model,
risk policy, strategy contract, and simulator are all limited to equities and
options.

## Prerequisites

- Rust stable with Cargo.
- A Cline CLI installation (`npm install -g cline`) for the initial prototype.
- An OpenRouter account/API key with access to the configured model ID.
- A Robinhood Agentic account.
- Robinhood Trading MCP authorization completed in Cline.

On Windows, the app resolves npm-installed `cline.cmd` shims automatically.

The planned Windows distribution will package the Cline CLI and a pinned Node
runtime as sidecars. The current scaffold deliberately uses a configurable
`agent.executable` so packaging can be added without changing the trading
engine. Hoodrat passes both `--config` and `--data-dir` to keep the bot's Cline
profile separate from the user's global Cline profile. The generated defaults
use `data/cline` as the Cline profile root and `data/cline/data` as its data
directory.

## First run

Create a default config and database:

```text
cargo run -- init
```

The default config is intentionally locked:

- execution mode: `disabled`
- kill switch: engaged
- risk policy: unconfirmed
- Robinhood connection: not marked ready
- strategy canary: disabled
- strategy scope: equity/options lane, limit orders, no leverage

Configure Cline and Robinhood MCP. Robinhood's documented MCP endpoint is:

```text
https://agent.robinhood.com/mcp/trading
```

The documented Cline setup command is:

```text
cline --config data/cline --data-dir data/cline/data mcp add --yes --json --transport http robinhood-trading https://agent.robinhood.com/mcp/trading
```

Then configure OpenRouter in that same Cline profile. The documented provider
settings are OpenRouter, your OpenRouter API key, and model ID
`gpt-5.6-luna`. The runner passes `--provider openrouter` on every fresh task.
Use this Cline CLI auth command with the same `--config` and `--data-dir` (do
not add `--baseurl` for standard OpenRouter), or enter the values in Cline's
settings UI:

```text
cline --config data/cline --data-dir data/cline/data auth --provider openrouter --apikey "<OPENROUTER_KEY>" --modelid "gpt-5.6-luna"
```

Then complete Robinhood MCP OAuth in the same Cline profile. Cline's MCP
settings may show an authorization-required state until that interactive step
is complete. The runner passes both profile flags on every fresh task, so
configuring a different Cline profile will not configure the profile Hoodrat
uses. Use:

```text
cargo run -- doctor
```

to inspect local readiness and print the setup instructions.

Run the read-only connectivity smoke test after authenticating the MCP server:

```text
cargo run -- smoke-test
```

This command is separate from scheduled execution. It requires the application
to remain disabled, the kill switch to remain engaged, and the risk policy to
remain unconfirmed. It launches Cline with `--plan`, `--json`, `--auto-approve true`,
and `--retries 1`, launches Cline with its isolated data directory and a
deny-all `CLINE_COMMAND_PERMISSIONS` defense-in-depth policy, records the result
in SQLite, and does not change the configuration. Auto-approval is required for
Cline's headless MCP call to run;
the smoke-test system prompt permits only the single read-only `get_accounts`
probe, and the supervisor requires that successful read to be the only tool
call observed. A successful Cline exit alone is not sufficient. Plan mode and
the prompt reduce write risk, but direct MCP access still means this is not an
application-owned pre-trade firewall.

Run the broader typed read-only account reconciliation:

```text
cargo run -- reconcile
```

This command remains fail-closed and requires `execution.mode=disabled`, an
engaged kill switch, an unconfirmed risk policy, and `agentic_account_only=true`.
It permits exactly four Robinhood reads—`get_accounts`, `get_portfolio`,
`get_realized_pnl`, and `get_pnl_trade_history`—and persists both the raw MCP
envelopes and recognized typed records. The first successful run establishes a
baseline. Later successful runs compare account, balance, position, PnL, and
realized-trade fingerprints; detected drift is categorized and blocks scheduler
lanes until it is reviewed. Required response fields are classified as
`present`, `empty`, or `missing`; missing required data produces
`coverage_incomplete`, while an empty collection is accepted as a valid zero
state. The reconciliation also requires exactly one successful call per
permitted tool, exactly one agent-accessible account, and unchanged account
identifiers on dependent reads. Any missing read, duplicate read, MCP error,
typed parsing error, wrong-account read, or non-Robinhood tool use blocks
reconciliation.

If a reconciliation reports `drift_detected`, do not silently overwrite the
baseline. An operator must review the balance or account change and explicitly
accept that exact latest run:

```text
cargo run -- accept-baseline --confirm --operator "<operator-id>" --reason "<reviewed reason>"
```

Acceptance is available only while execution remains disabled, the kill switch
is engaged, risk is unconfirmed, and the Agentic-account-only restriction is
enabled. Each acceptance is append-only and records the operator, reason,
reconciliation run, prior fingerprint, and accepted fingerprint. The command
does not change configuration, enable live execution, or place an order. Do not
run it until the operator has reviewed and approved the detected change.

Robinhood's current Trading MCP documentation describes transaction and order
history as readable account data, but does not document a dedicated
order-history MCP tool. Hoodrat therefore records full order-history coverage
as `not_documented` and does not invent or call an unsupported tool. The
documented `get_pnl_trade_history` read is persisted separately as realized-PnL
trade history.

Run the dashboard (monitoring only):

```text
cargo run -- dashboard
```

The dashboard is a tabbed, dark terminal-style monitor (KPIs, equity/PnL
charts, and data tables for positions, balances, returns, trades, accounts,
runs, MCP tool events, audit trail, reconciliations, baseline acceptances,
the strategy contract, and paper simulations). Its charts are drawn from stored
reconciliation/ingestion history, so no extra market-data API is required.
Note: the reconciliation `get_portfolio` read returns aggregate totals only
(cash/buying power/equity), so the Positions tab is populated when per-symbol
holdings are captured, rather than from the aggregate portfolio read.

Run the scheduler and dashboard together in a single process (recommended for
live operation — one command, one window, the scheduler runs in a background
thread and stops cleanly when the window closes):

```text
cargo run -- app
```

Run the scheduler headlessly in its current safe mode:

```text
cargo run -- run
```

The long-running scheduler reloads the config once per loop. Disengaging the
persistent kill switch or otherwise making the readiness gate fail prevents
future evaluations without requiring a process restart.

The scheduler will not invoke Cline while execution is disabled or the kill
switch is engaged. A one-shot run is useful for development:

```text
cargo run -- run --once
```

## Configuration

`config.example.json` documents every current setting. Copy it to
`hoodrat.json` or edit the file generated by `init`.

The live readiness gate requires all of the following:

1. `execution.mode` is `live`.
2. `execution.kill_switch_engaged` is `false`.
3. `risk.confirmed` is `true`.
4. `robinhood.connection_ready` is `true`.
5. `strategy.canary_enabled` is `true`.

The initial strategy contract is intentionally narrower than the general
capability model: it permits only the `equity_options` lane and `equity`/`option`
asset classes, uses fixed small notional limit orders, permits options, forbids
leverage, and requires an explicit approved-symbol list when enabled. Its limits
must be no looser than the configured risk policy. Any contract violation, stale
or ambiguous data, reconciliation drift, unexpected tool, or other no-op
condition must result in no action. The strategy contract is guidance bound to
the prompt and audit record; under the direct Cline-to-Robinhood architecture it
is not a deterministic pre-trade firewall.

The initial moderate risk values are placeholders for development. They must
be reviewed and explicitly confirmed before live mode is considered ready.

## Read-only live-data paper simulation

The repository also includes a separate simulation-only harness. It uses a
fresh Cline task to call configured read-only `get_*` MCP tools. Hoodrat
persists each raw successful MCP response in the local SQLite tool-event store
and normalizes quote fields in Rust; the agent is allowed to return only a
paper-proposals envelope and is not trusted to supply prices or timestamps.
The simulator never sends an order, watchlist update, account change, or other
write to Robinhood.

Enable it in a non-live configuration by setting `simulation.enabled` to
`true`, reviewing the configured `market_data_tools` and `symbols`, and then
run:

```text
cargo run -- --config simulation.json market-probe
cargo run -- --config simulation.json simulate
```

Run `market-probe` first. It calls every configured market-data tool exactly
once, persists the raw responses, and prints only redacted response shapes and
field paths. It is the schema-discovery step; do not run `simulate` until the
probe confirms the actual tool names and quote fields.

The command remains fail-closed unless all of the following are true:

- `execution.mode` is `disabled`;
- `execution.kill_switch_engaged` is `true`;
- `risk.confirmed` is `false`;
- `robinhood.agentic_account_only` is `true`;
- every configured market-data tool is a read-only `get_*` tool;
- the Cline task calls each configured market-data tool exactly once, without
  MCP errors or unexpected tools; and
- successful raw MCP output contains current quote fields with fresh RFC3339
  timestamps and valid quote shape.

The default `aggressive-any-risk-sim-v1` profile supports equities, short
positions, leverage, and options limited to 0–1 DTE. Its slippage, fees,
gross exposure, leverage, position-count, and holding-period limits still
apply. “Aggressive” changes only the local paper engine; it cannot enable live
execution or override the live strategy contract.

The isolated simulation is currently equity/options-only. It uses Robinhood's
documented read-only endpoints: `get_equity_quotes`, `get_option_chains`,
`get_option_instruments`, and `get_option_quotes`. The option flow is
dependency-ordered: a returned chain ID is passed to instrument discovery, and
only returned instrument IDs are passed to quote retrieval. Historical bars from
`get_equity_historicals` and `get_option_historicals` are never treated as
current executable quotes.

The `market-probe` command still verifies that the connected MCP profile exposes
each configured endpoint and that its raw fields are usable before simulation.
Missing, stale, ambiguous, inactive, unsupported, or dependency-incomplete raw
data prevents simulation rather than producing paper fills. No live simulation
has been run as part of repository validation.

## SQLite state

The database defaults to `data/hoodrat.db`. It contains:

- scheduled agent runs and their status;
- parsed Cline JSON events plus raw output;
- recognized Cline/MCP tool events, including inputs, outputs, and errors;
- append-only audit events;
- portfolio snapshots;
- execution records;
- typed broker accounts, balances, positions, realized PnL snapshots, and PnL trade history;
- reconciliation runs, raw read payloads, stable fingerprints, categorized drift,
  response coverage, and order-history coverage status;
- append-only operator baseline-acceptance records with prior and accepted
  fingerprints;
- paper simulations, their timestamped market snapshots, simulated fills and
  option-expiry settlements, and final simulated positions;
- schema version metadata.

No API keys or Robinhood credentials belong in `hoodrat.json` or SQLite. Keep
authentication in Cline's supported credential flow and the OS credential
store where applicable.

## Important limitations of this scaffold

- It does not implement a Robinhood client or bypass the Trading MCP.
- It does not infer or intercept every order tool call inside Cline output.
- Rust risk checks are readiness and monitoring checks only under the direct
  MCP design; they are not a deterministic pre-trade firewall.
- The typed reconciliation covers the documented account, portfolio,
  realized-PnL, and realized-PnL trade-history reads. It does not infer a full
  order ledger from those reads; order/execution records remain a separate
  opportunistic ingestion path because the current Robinhood documentation
  does not name a dedicated order-history MCP read.
- Market hours are schedule guards, not an exchange calendar or an order
  execution guarantee.
- No strategy recommends a symbol or trade. The strategy prompt is deliberately
  conservative and asks Cline to retrieve current data before acting.
- `simulation.json` is configured with Robinhood's documented
  equity/options read endpoints. The configuration remains unusable until the
  configured reads pass the exact-once, no-error probe and produce fresh
  normalized quotes. Downstream option reads require returned chain and
  instrument IDs; unjoined or stale option quotes are rejected.
- `simulate` does not trust model-transformed prices: its Rust normalizer
  requires symbol, price, and timestamp fields in the raw MCP response.
- A single snapshot validates plumbing, not strategy performance. Use repeated
  snapshots or historical data for evaluation, and do not treat historical
  candles as current executable quotes.

## Tests

Run formatting, tests, and the compiler checks with:

```text
cargo fmt --all -- --check
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
```