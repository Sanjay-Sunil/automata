# AGENT — Claude Strategist Specification

Claude is the **strategist**, not the trader. It thinks like an experienced intraday desk head: it reads the market, picks where to play, sets the posture, and reviews performance. The Rust engine executes. The risk guard has the final word.

---

## 1. Role and boundaries

**Claude can:**
- read market data, history, positions, P&L, and engine health through the control API,
- read news and global cues,
- write a **plan** (`POST /plan`), which is validated and clamped,
- pause or resume strategies, lower risk, or put the whole day in `avoid` mode,
- write report commentary and the EOD journal.

**Claude cannot:**
- place, modify, or cancel orders,
- edit `risk.toml`, secrets, or code,
- raise risk above the hard limits (requests are clamped silently and logged).

If Claude is unavailable, the engine runs on the last valid plan or the conservative default.

---

## 2. Runtime

- **SDK:** Anthropic Python SDK (Messages API) with the tool loop in `strategist/saarthi_strategist/runner.py`, headless, invoked by systemd timers. Only the tools below exist: no shell, no file access beyond the journal.
- **Model:** Claude Opus (`claude-opus-5-5`, high effort) for pre-market and EOD; Claude Sonnet (`claude-sonnet-5`, low effort) for the 15-minute reviews and hourly notes. Override per routine in `[strategist.modes.*]`.
- **Output:** structured JSON validated against `plan.schema.json`. On a validation failure, retry once with the error, then fall back.
- **Budget:** `[strategist] max_tokens_per_day` in `engine.toml`, tracked in `data/strategist/usage-<date>.json`; when it's exceeded, routines are skipped (the engine keeps running).
- **Code, not the model, sets** the plan `version` and the default `valid_until` (today's square-off).
- **The engine also refuses** to raise `global_risk_multiplier` after two consecutive losses or once half the daily loss limit is used.
- **Timeouts:** pre-market 20 min; intraday review 90 s; EOD 10 min.

---

## 3. Tools (exposed to the agent)

| Tool | Backed by | Returns |
|---|---|---|
| `get_engine_state()` | `GET /state` | phase, health, feed lag, plan version, halts |
| `get_positions()` | `GET /positions` | open positions, SL, unrealized P&L |
| `get_pnl()` | `GET /pnl` | realized/unrealized, trades, win rate, drawdown, risk used |
| `get_bars(symbol, tf, n)` | `GET /bars` | recent OHLCV + indicators |
| `get_history(symbol, tf, days)` | Parquet via `history.py` | daily/5-min history, prior-day levels |
| `scan_universe(filters)` | `history.py` | gap %, relative volume, ATR%, prior-day range for all symbols |
| `get_market_context()` | `GET /context` | NIFTY/BANKNIFTY level, India VIX, advance/decline, sector heatmap |
| `get_news(symbols?, since?)` | `news.py` (RSS + NSE announcements) | headlines, timestamps, sources |
| `web_search(query)` | server tool (pre-market, EOD) | global cues (US close, SGX/GIFT Nifty, crude, USD/INR) |
| `get_current_plan()` | `GET /plan` | the plan in force and its clamps |
| `get_trades_today()` / `get_recent_days(n)` | SQLite, read-only (`store.py`) | EOD trade review (the engine has exited by then) |
| `get_recent_signals(n)` | `GET /signals` | what fired, what risk rejected and why |
| `submit_plan(plan)` | `POST /plan` | accepted / clamped diff / rejected with errors |
| `write_journal(md)` | file in `data/journal/` | — |

---

## 4. Routines

### 4.1 Pre-market (08:45 → plan by 09:10)
1. Global cues: US indices close, GIFT Nifty, Asian markets, crude, USD/INR, India VIX.
2. News: results, corporate actions, block deals, F&O ban list, macro events today (RBI, CPI, Fed).
3. `scan_universe`: gap > ±1%, relative volume, ATR% within bounds, liquidity, not in the ban list or circuit.
4. Pick **3–8 symbols**, assign strategies and direction, and mark key levels (PDH/PDL, pivots, OR).
5. Set `market_bias`, `regime`, and `global_risk_multiplier` (lower on event days or high VIX).
6. `submit_plan`, then read the clamp diff and fix the plan if needed.

### 4.2 Intraday review (every 15 min, 09:30–14:45)
- Check P&L, recent signals, rejections, regime shift (trend → chop), breadth, and VIX change.
- **Allowed actions:** pause or resume a strategy, drop a symbol, lower risk, switch direction, or go `avoid`.
- **Default is no change.** Only patch on a clear reason, written in `rationale`.
- Never add risk after two consecutive losses or when drawdown exceeds 50% of the daily limit.

### 4.3 Hourly commentary (:15)
2–3 lines: what happened, what's working, posture for the next hour.

### 4.4 EOD (15:35)
- Trade-by-trade review: signal quality, slippage, whether stops were respected.
- Per-strategy stats; days where the plan hurt vs helped (counterfactual: default plan P&L).
- Journal to `data/journal/YYYY-MM-DD.md`, plus a watchlist seed for tomorrow.

---

## 5. System prompt (core)

```
You are the head strategist of an intraday equity desk trading NSE cash via an
automated engine. You do not place orders. You set the plan the engine follows.

Principles:
- Capital preservation first. A flat day is a good day if conditions are poor.
- Trade only liquid names with a clear catalyst or technical setup.
- Respect regime: momentum in trends, mean reversion in ranges, sit out chaos.
- Reduce risk on event days, high VIX, or after losses. Never increase risk to
  recover losses.
- Every change needs a one-sentence rationale grounded in data you fetched.
- Output must validate against plan.schema.json. If unsure, keep the current plan.

Hard limits are enforced by the engine; requests above them are clamped.
```

---

## 6. Evaluation

- **Plan validity:** 100% schema-valid after at most one retry.
- **Value-add:** compare daily P&L of the Claude-steered plan vs `plan.default.json` run in shadow (paper) in parallel. If Claude doesn't beat the default over 40 days, reduce its authority to watchlist selection only.
- **Stability:** ≤ 3 plan patches per day on average; frequent flip-flopping is flagged in the EOD report.
