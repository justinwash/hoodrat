#!/usr/bin/env python3
"""
Local MCP server — Robinhood Machine account (unofficial API via robin_stocks).

This gives Claude access to options trading on your "Machine" brokerage account,
which is not available through the official Robinhood agentic MCP during beta.

First run: credentials are prompted automatically and stored in the OS keychain
(Windows Credential Manager / macOS Keychain / libsecret).
No env vars, no plaintext files, no separate setup step needed.

Registration (run once):
  pip install mcp robin-stocks keyring
  claude mcp add robinhood-machine -- python scripts/robinhood_machine_mcp.py

IMPORTANT: This uses Robinhood's unofficial API. Only use it on accounts you own
and in compliance with Robinhood's terms of service.
"""

import asyncio
import getpass
import json
import sys
import traceback

import keyring
import robin_stocks.robinhood as r
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Credential management — OS keychain, auto-prompted on first use
# ---------------------------------------------------------------------------

_KEYCHAIN_SERVICE = "hoodrat-robinhood-machine"
_logged_in = False


def _read_terminal_line(prompt: str) -> str:
    """
    Read a line from the actual terminal, bypassing piped stdin/stdout.

    When running as an MCP server, stdin is a pipe owned by Claude Code.
    We open the terminal device directly so the prompt appears in the user's
    terminal without corrupting the MCP protocol stream.
    """
    sys.stderr.write(prompt)
    sys.stderr.flush()
    try:
        if sys.platform == "win32":
            with open("CONIN$", "r") as con:
                return con.readline().strip()
        else:
            with open("/dev/tty") as tty:
                return tty.readline().strip()
    except OSError:
        raise RuntimeError(
            "No terminal available for credential entry. "
            "Set ROBINHOOD_MACHINE_USERNAME / ROBINHOOD_MACHINE_PASSWORD env vars "
            "as a fallback, or run in a terminal where /dev/tty is accessible."
        )


def _prompt_and_store_credentials() -> tuple[str, str]:
    """Interactively collect credentials and store them in the OS keychain."""
    sys.stderr.write(
        "\nhoodrat: Robinhood Machine credentials not found in keychain.\n"
        "Enter them now — they will be stored securely and not asked again.\n\n"
    )
    username = _read_terminal_line("Email: ")
    # getpass opens the terminal device directly on all platforms
    password = getpass.getpass("Password: ")
    keyring.set_password(_KEYCHAIN_SERVICE, "username", username)
    keyring.set_password(_KEYCHAIN_SERVICE, "password", password)
    sys.stderr.write("Credentials saved to OS keychain.\n\n")
    return username, password


def _get_credentials() -> tuple[str, str]:
    """Return credentials from keychain, prompting and storing them if missing."""
    username = keyring.get_password(_KEYCHAIN_SERVICE, "username")
    password = keyring.get_password(_KEYCHAIN_SERVICE, "password") if username else None
    if username and password:
        return username, password
    return _prompt_and_store_credentials()


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in ("401", "403", "unauthorized", "forbidden", "expired", "token"))


def ensure_logged_in() -> None:
    global _logged_in
    if _logged_in:
        return
    username, password = _get_credentials()
    r.login(username, password, store_session=True, pickle_name="machine")
    _logged_in = True


def force_reauth() -> None:
    """Re-authenticate using stored keychain credentials (handles session expiry)."""
    global _logged_in
    _logged_in = False
    username, password = _get_credentials()
    r.login(username, password, store_session=True, pickle_name="machine")
    _logged_in = True


def ok(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]


def err(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps({"error": msg}))]


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    Tool(
        name="get_account_info",
        description=(
            "Get the Machine account's portfolio: cash balance, equity value, "
            "options value, and open positions."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_options_chain",
        description=(
            "List available options contracts for a stock. Returns strikes and "
            "expirations. Optionally filter by expiration date and/or option type."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol":          {"type": "string",  "description": "Ticker symbol, e.g. SPY"},
                "expiration_date": {"type": "string",  "description": "Filter by expiry YYYY-MM-DD (optional)"},
                "option_type":     {"type": "string",  "enum": ["call", "put"], "description": "Optional filter"},
            },
            "required": ["symbol"],
        },
    ),
    Tool(
        name="get_option_market_data",
        description=(
            "Get live bid/ask, IV, delta, gamma, theta, and vega for a specific "
            "options contract."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol":          {"type": "string"},
                "expiration_date": {"type": "string", "description": "YYYY-MM-DD"},
                "strike":          {"type": "number"},
                "option_type":     {"type": "string", "enum": ["call", "put"]},
            },
            "required": ["symbol", "expiration_date", "strike", "option_type"],
        },
    ),
    Tool(
        name="get_stock_quote",
        description="Get the current price and basic fundamentals for a stock.",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
            },
            "required": ["symbol"],
        },
    ),
    Tool(
        name="get_equity_positions",
        description="List all open stock positions in the Machine account.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_options_positions",
        description="List all open options positions in the Machine account.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="place_options_order",
        description=(
            "Place an options order (buy or sell to open or close) on the Machine account. "
            "limit_price is per share (1 contract = 100 shares). "
            "Examples: buy 1 SPY 530C 2026-07-18 at $2.50/share → costs $250."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol":          {"type": "string"},
                "expiration_date": {"type": "string", "description": "YYYY-MM-DD"},
                "strike":          {"type": "number"},
                "option_type":     {"type": "string", "enum": ["call", "put"]},
                "quantity":        {"type": "integer", "description": "Number of contracts"},
                "side":            {"type": "string", "enum": ["buy", "sell"]},
                "position_effect": {"type": "string", "enum": ["open", "close"]},
                "limit_price":     {"type": "number", "description": "Per-share limit price in USD"},
                "time_in_force":   {"type": "string", "enum": ["gfd", "gtc"], "default": "gfd"},
            },
            "required": [
                "symbol", "expiration_date", "strike", "option_type",
                "quantity", "side", "position_effect", "limit_price",
            ],
        },
    ),
    Tool(
        name="place_equity_order",
        description="Place a stock buy or sell order on the Machine account.",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol":       {"type": "string"},
                "quantity":     {"type": "number"},
                "side":         {"type": "string", "enum": ["buy", "sell"]},
                "order_type":   {"type": "string", "enum": ["market", "limit"], "default": "market"},
                "limit_price":  {"type": "number", "description": "Required for limit orders"},
                "time_in_force":{"type": "string", "enum": ["gfd", "gtc"], "default": "gfd"},
            },
            "required": ["symbol", "quantity", "side"],
        },
    ),
    Tool(
        name="get_options_orders",
        description="Get the 20 most recent options orders from the Machine account.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    for attempt in range(2):
        try:
            ensure_logged_in()
            return _dispatch(name, arguments)
        except Exception as exc:
            if attempt == 0 and _is_auth_error(exc):
                sys.stderr.write(f"[robinhood-machine] Auth error: {exc} — re-authenticating…\n")
                try:
                    force_reauth()
                except Exception:
                    pass
                continue
            traceback.print_exc(file=sys.stderr)
            return err(str(exc))
    return err("Authentication failed after retry.")


def _dispatch(name: str, arguments: dict) -> list[TextContent]:
    try:

        if name == "get_account_info":
            profile   = r.load_portfolio_profile()
            positions = r.get_open_stock_positions()
            enriched  = []
            for p in (positions or []):
                sym = r.get_symbol_by_url(p.get("instrument", ""))
                enriched.append({
                    "symbol":            sym,
                    "quantity":          p.get("quantity"),
                    "average_buy_price": p.get("average_buy_price"),
                })
            return ok({
                "equity":        profile.get("equity"),
                "cash":          profile.get("withdrawable_amount"),
                "total_return":  profile.get("total_return"),
                "positions":     enriched,
            })

        elif name == "get_options_chain":
            sym  = arguments["symbol"].upper()
            exp  = arguments.get("expiration_date")
            kind = arguments.get("option_type", "both")
            data = r.find_options_for_stock_by_expiration_and_strike(sym, exp, None, kind)
            return ok((data or [])[:30])

        elif name == "get_option_market_data":
            sym    = arguments["symbol"].upper()
            exp    = arguments["expiration_date"]
            strike = str(float(arguments["strike"]))
            kind   = arguments["option_type"]
            contracts = r.find_options_by_expiration_and_strike(sym, exp, strike, kind)
            if not contracts:
                return err(f"No contract found for {sym} {strike}{kind[0].upper()} {exp}")
            contract_id = contracts[0]["id"]
            data = r.get_option_market_data_by_id(contract_id)
            return ok(data or {})

        elif name == "get_stock_quote":
            sym    = arguments["symbol"].upper()
            prices = r.get_latest_price(sym)
            fundam = r.get_fundamentals(sym)
            return ok({
                "symbol":       sym,
                "price":        prices[0] if prices else None,
                "fundamentals": fundam[0] if fundam else None,
            })

        elif name == "get_equity_positions":
            positions = r.get_open_stock_positions() or []
            result = []
            for p in positions:
                sym = r.get_symbol_by_url(p.get("instrument", ""))
                result.append({
                    "symbol":            sym,
                    "quantity":          p.get("quantity"),
                    "average_buy_price": p.get("average_buy_price"),
                    "current_price":     p.get("current_price"),
                })
            return ok(result)

        elif name == "get_options_positions":
            return ok(r.get_open_option_positions() or [])

        elif name == "place_options_order":
            sym    = arguments["symbol"].upper()
            exp    = arguments["expiration_date"]
            strike = str(float(arguments["strike"]))
            kind   = arguments["option_type"]
            qty    = int(arguments["quantity"])
            side   = arguments["side"]          # "buy" or "sell"
            effect = arguments["position_effect"]  # "open" or "close"
            price  = float(arguments["limit_price"])
            tif    = arguments.get("time_in_force", "gfd")

            # robin_stocks uses "debit" for buying, "credit" for selling
            credit_debit = "debit" if side == "buy" else "credit"

            if side == "buy":
                result = r.order_buy_option_limit(
                    effect, credit_debit, price, sym, qty, exp, strike, kind, tif
                )
            else:
                result = r.order_sell_option_limit(
                    effect, credit_debit, price, sym, qty, exp, strike, kind, tif
                )
            return ok(result or {})

        elif name == "place_equity_order":
            sym  = arguments["symbol"].upper()
            qty  = float(arguments["quantity"])
            side = arguments["side"]
            otype = arguments.get("order_type", "market")
            tif   = arguments.get("time_in_force", "gfd")

            if otype == "limit":
                price = float(arguments["limit_price"])
                if side == "buy":
                    result = r.order_buy_limit(sym, qty, price, timeInForce=tif)
                else:
                    result = r.order_sell_limit(sym, qty, price, timeInForce=tif)
            else:
                if side == "buy":
                    result = r.order_buy_market(sym, qty, timeInForce=tif)
                else:
                    result = r.order_sell_market(sym, qty, timeInForce=tif)
            return ok(result or {})

        elif name == "get_options_orders":
            orders = r.get_all_option_orders() or []
            return ok(orders[:20])

        else:
            return err(f"Unknown tool: {name}")

    except Exception as exc:
        raise  # re-raise so the retry wrapper in call_tool can inspect it


# ---------------------------------------------------------------------------
# Entry point — credential check happens here, before MCP stdio starts
# ---------------------------------------------------------------------------

async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    # Prompt for credentials (if missing) before asyncio.run() hands stdin/stdout
    # over to the MCP protocol.  _get_credentials() reads from /dev/tty (Unix) or
    # CONIN$ (Windows), so the prompt appears in the user's terminal without
    # corrupting the MCP pipe.
    _get_credentials()
    asyncio.run(main())
