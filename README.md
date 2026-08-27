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

The scaffold has separate equity/options and crypto schedule lanes. The
capability model includes equities, options, and crypto because those are the
categories Robinhood currently documents for Agentic Trading. The config keeps
per-lane switches so they can be disabled independently.

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
and `--retries 1`, records the result in SQLite, and does not change the
configuration. Auto-approval is required for Cline's headless MCP call to run;
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

Robinhood's current Trading MCP documentation describes transaction and order
history as readable account data, but does not document a dedicated
order-history MCP tool. Hoodrat therefore records full order-history coverage
as `not_documented` and does not invent or call an unsupported tool. The
documented `get_pnl_trade_history` read is persisted separately as realized-PnL
trade history.

Run the dashboard:

```text
cargo run -- dashboard
```

Run the scheduler in its current safe mode:

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

The initial moderate risk values are placeholders for development. They must
be reviewed and explicitly confirmed before live mode is considered ready.

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

## Tests

Run formatting, tests, and the compiler checks with:

```text
cargo fmt --all -- --check
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
```