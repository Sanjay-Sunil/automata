# Saarthi

Autonomous intraday equity trading on NSE through Angel One SmartAPI. A Rust
engine executes; Claude acts as an off-path strategist whose plans are clamped
by hard risk limits in code.

Specs live in [`docs/`](docs): [PLAN](docs/PLAN.md) (milestones),
[ARCHITECTURE](docs/ARCHITECTURE.md), [AGENT](docs/AGENT.md) (strategist) and
[BUILD](docs/BUILD.md).

> **Status: milestone M5 (reporting + autonomy), code complete.** The workspace
> logs in, downloads instruments and candles, streams live ticks, builds bars
> and indicators, backtests three strategies walk-forward, and **paper-trades
> a full session unattended**: every signal passes the hard risk limits,
> every entry gets a broker-side stop, everything is squared off by 15:10 and
> a crash mid-trade is recovered from SQLite and the broker's book. A Claude
> strategist writes the day's plan before the open, reviews it every 15
> minutes and journals the day; the engine validates and clamps every plan
> and can only be steered toward less risk. Hourly and end-of-day reports go
> to Telegram, `/halt` works from the phone, and systemd timers run the whole
> day on a VPS with boot checks and daily backups. It
> **cannot place real orders**: the SmartAPI order methods return
> `Unsupported` and `SAARTHI_MODE=live` is refused until go-live.

## Layout

| Path | What |
|---|---|
| `crates/core` (`saarthi-core`) | Money as integer paise, instruments, bars, timeframes, `MarketClock` (IST phases + NSE holidays) |
| `crates/broker` (`saarthi-broker`) | `Broker` trait; SmartAPI adapter: TOTP login, re-login on expiry, throttling, retries, instrument master, candles |
| `crates/store` (`saarthi-store`) | Atomic Parquet writers/readers for bars and instruments; resume manifest |
| `crates/feed` (`saarthi-feed`) | SmartAPI WebSocket 2.0: binary decode, subscribe, heartbeat, reconnect with backoff, staleness watchdog, raw-frame recorder |
| `crates/bars` (`saarthi-bars`) | Tick → 1m → 5m bars, session VWAP, opening range, per-symbol indicator `Snapshot` |
| `crates/ta` (`saarthi-ta`) | Incremental EMA, ATR, RSI, VWAP bands, volume z-score (checked against TA-Lib) |
| `crates/strategy` (`saarthi-strategy`) | `Strategy` trait; ORB, VWAP reversion, trend momentum; parameter specs and optimiser grids |
| `crates/risk` (`saarthi-risk`) | `risk.toml` hard limits, the pre-trade risk guard, daily loss stop / profit lock, kill switch, order-rate throttle |
| `crates/oms` (`saarthi-oms`) | Order state machine, broker-side SL pairing, engine-managed targets, exits until flat, reconciliation, crash recovery |
| `crates/broker::paper` | `Paper` broker: simulated fills on live ticks with charges and slippage; its account survives restarts |
| `crates/store::sqlite` | SQLite (WAL) hot store: trades, orders (written before sending), fills, events, day summaries |
| `crates/plan` (`saarthi-plan`) | Strategist plan: strict parsing, validation and clamping against `risk.toml`; per-entry verdicts |
| `crates/control` (`saarthi-control`) | Loopback HTTP control API with a bearer token (state, positions, P&L, bars, signals, context, plan, halt, resume); no order endpoints |
| `strategist/` | Python: the Claude strategist (pre-market plan, 15-minute review, hourly notes, EOD journal); see [AGENT](docs/AGENT.md) |
| `crates/notify` (`saarthi-notify`) | Telegram Bot API: text, PNG, documents, owner-only command polling |
| `bins/report` (`report`) | Hourly update and EOD report (text + equity PNG + HTML archive in `data/reports/`) |
| `deploy/` | `provision.sh`, systemd units and timers, `backup.sh`, `notify.sh` |
| `bins/engine` (`engine`) | The daemon: feed → bars → strategies → risk → OMS; replay, status, kill switch, M3 exit check |
| `crates/broker::backtest` | `Backtest` broker: simulated exchange with Indian intraday charges (`config/costs.toml`) and slippage |
| `bins/backtest` (`backtest`) | Walk-forward backtester, per-strategy metrics, equity curve, M2 go-live gate |
| `bins/fetch` (`fetch`) | Market-data downloader CLI |
| `bins/feedcheck` (`feedcheck`) | Records a live session and checks its bars against Angel's candles (M1 exit) |
| `config/` | `engine.toml`, `risk.toml`, `plan.default.json`, `plan.schema.json`, `universe.toml` (NIFTY 100), `holidays.toml`, `costs.toml`, `backtest.toml`, `secrets.env.example` |

## Quick start

```bash
cp config/secrets.env.example config/secrets.env && chmod 600 config/secrets.env
# fill in ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_MPIN, ANGEL_TOTP_SECRET

cargo run -p fetch -- login-check          # verifies credentials; read-only
cargo run --release -p fetch               # instruments + 1 year of 5-min candles for NIFTY 100
cargo run --release -p fetch -- candles --tf 1d --days 730
```

Output: `data/instruments.parquet` and `data/candles/<tf>/<SYMBOL>.parquet`
(prices as `Int64` paise, `ts` as UTC ms tagged `Asia/Kolkata`). The fetcher
spaces historical requests at 2/s (SmartAPI documents 3/s), retries rate-limit
and server errors with backoff, and writes after every request. Interrupt it
any time and rerun: it resumes from `data/candles/<tf>/_manifest.json` and only
refetches the last (possibly partial) day.

Refresh the universe after NSE's semi-annual rebalance:
download `ind_nifty100list.csv` from niftyindices.com, then
`cargo run -p fetch -- universe --csv ind_nifty100list.csv`.

Refresh `config/holidays.toml` every December. The clock refuses to answer for
a year the file does not cover, so a stale file means "don't trade", never
"trade on a holiday".

## Live feed check (M1 exit)

The M1 exit criterion is "live 1-min bars match Angel's candles for a full
session (±1 tick)". On a trading day:

```bash
cargo run --release -p feedcheck -- record            # start before 09:15; stops at 15:31
# after the close:
cargo run --release -p fetch -- candles --tf 1m --from 2026-10-28 --to 2026-10-28
cargo run --release -p feedcheck -- compare --date 2026-10-28
```

`record` subscribes the universe in Quote mode, writes every raw frame to
`data/live/<date>/frames.bin` and the bars it builds to
`data/live/<date>/1m/`. `compare` prints PASS/FAIL per symbol with the
minutes that differ. Bars after a feed drop are marked partial and skipped.
If the 09:15 bar is the only mismatch, flip `[bars] pre_open` in
`engine.toml` and run `feedcheck replay --date …` to rebuild the bars from the
recording without waiting for another session.

## Backtest (M2)

```bash
cargo run --release -p fetch -- candles --tf 5m --days 365     # real data first
cargo run --release -p backtest -- --strategy all --walk-forward 60:20 --out data/reports/bt
```

Each strategy is optimised on 60 trading days over its parameter grid,
traded on the next 20 with the winner, and rolled forward; only those test
segments count. Per strategy, `data/reports/bt/<strategy>/` gets
`report.md`, `summary.json` (metrics + gate), `trades.csv`, `equity.csv`,
`equity.svg` and `windows.csv`; `overview.md` compares them. Add
`--fail-on-gate` to exit with status 2 unless every strategy passes.

The **gate** (PLAN M2), judged on out-of-sample results only: profit factor
\> 1.3, max drawdown < 8 %, ≥ 200 trades, and parameter stability (≥ 60 % of
the chosen parameters' grid neighbours also profitable in-sample).
Thresholds live in `config/backtest.toml`.

How a backtest trades:

- Signals form at a bar's close and fill at the **next** bar's open; a test
  proves past trades don't change when future bars do.
- Entries are bracket orders: market entry, SL-M stop, limit target. If a bar
  could have hit both, the stop wins. Gaps through a stop fill at the open.
  Targets need price to trade one tick through.
- Sizing and limits come from `risk.toml`: risk per trade, max positions,
  gross exposure, daily loss stop, trades per day, consecutive-loss
  cooldown, no new entries after 14:45, square-off at 15:10.
- Charges per executed order from `config/costs.toml` (Angel One brokerage,
  STT, NSE transaction charge, SEBI fee, IPFT, stamp duty, GST), plus 2 bps
  + 1 tick slippage on market and stop fills. Rates change; check
  `as_of` against a recent contract note.

No market data? `backtest synth` writes synthetic candles to exercise the
pipeline. Every report built on them is stamped **SYNTHETIC DATA**:
`--mode regimes` plants trend and range days for the strategies to find
(passing the gate there proves nothing), `--mode random-walk` has no
structure, and nothing should pass on it after costs.

```bash
cargo run --release -p backtest -- synth --data-dir /tmp/syn --symbols 30 --days 250
cargo run --release -p backtest -- --data-dir /tmp/syn --universe /tmp/syn/synthetic_universe.toml --out /tmp/syn/reports
```

## Paper trading (M3)

```bash
SAARTHI_MODE=paper cargo run --release -p engine -- run    # start before 09:15; exits ~15:35 once flat
cargo run -p engine -- status                              # today's summary and trades
cargo run -p engine -- halt "reason"                       # kill switch: flatten + no new entries
cargo run -p engine -- resume                              # lift it (refused after 14:45 / after the loss stop)
cargo run -p engine -- exit-check --days 5                 # M3 exit criterion
cargo run -p engine -- replay --date 2026-10-28            # paper-trade a `feedcheck record` session offline
```

Per tick: the paper exchange plays it, the OMS picks up fills, targets are
checked, bars and indicators update, and each completed 5m bar runs the
strategies. Every entry signal passes the risk guard (ARCHITECTURE §5.7, in
order: kill switch, phase, feed health, daily P&L, trade count, positions,
exposure, risk sizing, price band, duplicate) before the OMS sees it.
Once a second: broker sync, kill-switch file, feed staleness (120 s silent
with positions → flatten), the daily P&L monitor (2 % loss → flatten and
halt for the day), reconciliation every 30 s (unknown broker positions →
exit them and halt), and from 15:10 the square-off, retried until the broker
confirms flat (critical alert if not flat after three checks).

State lives in `data/accounts/<client>/paper/`: `saarthi.db` (SQLite WAL;
orders are written before they are sent), `paper.json` (the simulated
broker account) and the `HALT` kill-switch file. Kill the process at any
point (`kill -9` included) and start it again: unfinished trades come back
from SQLite, the broker's book says what happened meanwhile, and missing
stops are re-armed. In paper mode the simulated exchange lives inside the
engine, so stops do not trigger while it is down; live, the broker holds
them.

Alerts go to the log and, when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`
are set in `secrets.env`, to Telegram.

**M3 exit:** 5 consecutive paper days with no crash and everything flat by
15:10, checked by `engine exit-check --days 5` (a day with a restart does not
count).

## Simulation and dashboard

No credentials or market hours needed:

```bash
cargo run --release -p engine -- sim                 # a generated day at 20× (~20 min); prints the dashboard link
cargo run --release -p engine -- sim --speed 60 --symbols 30 --seed 7
cargo run --release -p engine -- sim --ai off        # without the strategist
```

`engine sim` generates 30 days of 5m history and one session of ticks for
`SIMnnn-EQ` symbols (trend, range and volatile days planted at random),
then runs the real engine, risk guard, OMS and paper broker over it on a
simulated clock. With `ANTHROPIC_API_KEY` set (and `uv` installed) it runs
the strategist too: the clock waits for the pre-market plan, reviews run
every 15 simulated minutes and the EOD journal is written after the close.
The strategist is told it is a simulation. Everything lives in `data/sim/`,
which is wiped on every start; the strategist's output goes to
`data/sim/strategist.log`. **The prices are synthetic: a simulated day shows
how the system behaves, never whether it makes money.**

The dashboard (`http://127.0.0.1:7070/`, served by the control API) shows
the day's P&L curve, open positions with stops and targets, every trade with
its charges and exit reason, every signal with the risk guard's decision,
the strategist's plan with anything the engine clamped, and Halt/Resume
buttons. It works the same during `engine run` (open
`http://127.0.0.1:7070/#token=<CONTROL_API_TOKEN>`; on a VPS, reach it
through `ssh -L 7070:127.0.0.1:7070`, never by exposing the port).

## Claude strategist (M4)

```bash
cd strategist && uv sync && cd ..
# secrets.env: ANTHROPIC_API_KEY, CONTROL_API_TOKEN (≥ 16 chars, e.g. `openssl rand -hex 32`)
uv run --project strategist strategist/agent.py premarket   # 08:45: plan for the day (deadline 09:10)
uv run --project strategist strategist/agent.py review      # every 15 min 09:30–14:45: patch only on a clear reason
uv run --project strategist strategist/agent.py hourly      # 2–3 lines for the hourly report
uv run --project strategist strategist/agent.py eod         # 15:35: trade review → data/journal/<date>.md
uv run --project strategist strategist/agent.py validity --runs 20   # M4 exit: 20/20 schema-valid plans
```

The engine serves a control API on `127.0.0.1:7070` (`[engine] control_addr`)
when `CONTROL_API_TOKEN` is set. The strategist reads the engine through it,
reads history from Parquet, headlines from RSS and global cues via web search,
and writes one thing: a plan (`config/plan.schema.json`). The engine parses
it strictly, rejects it if malformed, stale or not newer, and clamps
everything else (risk multiplier to 0–1, per-symbol risk to the hard limit,
unknown symbols and strategies dropped), then reports every clamp back. A
plan can only narrow and scale down trading, never raise risk above
`risk.toml`; after two straight losses or half the daily loss budget the
engine also refuses to raise the risk multiplier. With no valid plan the
engine trades `config/plan.default.json` (all enabled strategies, half risk).
If Claude is down, over budget or declines, nothing changes on the engine.

## Unattended operation (M5)

```bash
sudo ./deploy/provision.sh                         # on a fresh Ubuntu VPS (Mumbai region)
cargo run -p engine -- preflight                   # boot checks: trading day, disk, clock drift, secrets
cargo run -p report -- hourly --print              # what the hourly Telegram update says right now
cargo run -p report -- eod --print                 # build data/reports/<today>.html and print the summary
cargo run -p engine -- exit-check --days 10 --reports   # M5 exit criterion
```

Every weekday the timers start the engine at 08:30 IST, run the strategist at
08:45 and every 15 minutes, send the hourly update at :15, the EOD report
(text, equity chart, HTML) at ~15:40 and back up at 15:50. Any failed unit
pages Telegram. While the engine runs, the owner's Telegram chat (and only
that chat) can send `/status`, `/pnl`, `/positions`, `/plan`, `/halt
[reason]` and `/resume`. See BUILD §8 for the unit table.

## Multiple accounts

Nothing is tied to a single account. Credentials are a value passed to the
broker, not a global. Each account gets its own secrets file (and, if needed,
its own config dir):

```bash
fetch --secrets config/profiles/alice/secrets.env --config config/profiles/alice/engine.toml login-check
SAARTHI_SECRETS=/etc/saarthi/bob.env fetch candles
```

Market data under `data/` is shareable across accounts on one host;
per-account state will live under `data/accounts/<client_code>/`.

## Test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace        # or: cargo nextest run --workspace
cargo test -p saarthi-risk -- breach            # every hard limit breached
cargo test -p saarthi-oms -- crash_recovery     # real kill -9 mid-trade, then recovery
cd strategist && uv run pytest && uv run ruff check .   # strategist: schema, retry/fallback, tools
```

Broker tests run against a mocked HTTP server (`wiremock`) and feed tests
against a local WebSocket server sending synthetic binary frames, so no Angel
One credentials are needed. Indicator reference values come from TA-Lib; see
`crates/ta/tests/data/gen_reference.py` to regenerate them.

## Selling this to other people: regulatory note

Running this on your own account is personal algo trading under SEBI's retail
algo framework (static IP registration, order tagging, order-rate thresholds).
**Offering it to other people is different.** SEBI's framework (circular of
4 Feb 2025, phased in through 2025–26) requires algos offered to retail
investors to be provided through a broker, registered with the exchange, and
the algo provider to be empanelled with the exchange through that broker.
Selling signals or strategy recommendations can additionally require
Research Analyst (or Investment Adviser) registration. Confirm the current
rules with Angel One and a SEBI-registered compliance professional before
charging anyone or onboarding a single external user.
