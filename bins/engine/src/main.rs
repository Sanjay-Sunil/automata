//! `engine`: the Saarthi trading daemon.
//!
//! ```text
//! SAARTHI_MODE=paper engine run          # 08:30 → 15:35 under systemd
//! engine replay --date 2026-10-28        # paper-trade a recorded session offline
//! engine sim --speed 20                  # paper-trade a generated day, dashboard on :7070
//! engine status                          # today's trades and P&L
//! engine halt "reason" / engine resume   # kill switch
//! engine exit-check --days 5             # M3 exit criterion
//! ```
//!
//! Live mode is refused until the go-live milestones: the SmartAPI order
//! methods are still disabled in this build.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, TimeDelta, Utc};
use clap::{Parser, Subcommand};
use engine::alerts::{Alerts, LogAlerts, MemoryAlerts, SharedAlerts, TelegramAlerts};
use engine::config::{Config, Mode};
use engine::{AccountFiles, DaySummary, Engine, Loaded, Parts, exit_check, load, paper, past, secs, warmup};
use saarthi_broker::smartapi::{Credentials, SmartApi};
use saarthi_broker::{Broker, Paper};
use saarthi_core::MarketClock;
use saarthi_feed::record::read_frames;
use saarthi_feed::{ExchangeType, FeedAuth, FeedEvent, Mode as FeedMode, decode, market_hours};
use saarthi_oms::Severity;
use saarthi_store::Db;

mod simulate;

#[derive(Parser)]
#[command(
    name = "engine",
    version,
    about = "Saarthi trading engine (paper mode until go-live)"
)]
struct Cli {
    #[arg(long, env = "SAARTHI_CONFIG", default_value = "config/engine.toml", global = true)]
    config: PathBuf,
    #[arg(long, env = "SAARTHI_SECRETS", default_value = "config/secrets.env", global = true)]
    secrets: PathBuf,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    /// Account id for the state directory (default: ANGEL_CLIENT_CODE).
    #[arg(long, global = true)]
    account: Option<String>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Trade today's session (the systemd service runs this).
    Run,
    /// Paper-trade a session recorded with `feedcheck record`, offline.
    Replay {
        #[arg(long)]
        date: NaiveDate,
        /// Frames file (default: data/live/<date>/frames.bin).
        #[arg(long)]
        frames: Option<PathBuf>,
        /// Where the replay's database and paper account go.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Paper-trade a generated market day at N× speed, with the dashboard
    /// and (when ANTHROPIC_API_KEY is set) the Claude strategist. Needs no
    /// broker login; the prices are synthetic.
    Sim {
        /// Simulated seconds per real second (20 runs a day in about 20 minutes).
        #[arg(long, default_value_t = 20.0)]
        speed: f64,
        #[arg(long, default_value_t = 20)]
        symbols: usize,
        /// Random seed (default: from the clock, so every run differs).
        #[arg(long)]
        seed: Option<u64>,
        /// Day to simulate (default: today, or the last trading day before it).
        #[arg(long)]
        date: Option<NaiveDate>,
        /// The simulation's data and paper account; wiped at every start.
        #[arg(long, default_value = "data/sim")]
        dir: PathBuf,
        /// Claude strategist: auto (on when ANTHROPIC_API_KEY is set), on, off.
        #[arg(long, value_enum, default_value_t = AiMode::Auto)]
        ai: AiMode,
        /// Runs the strategist: `<cmd> premarket|review|eod`.
        #[arg(long, default_value = "uv run --project strategist strategist/agent.py")]
        strategist_cmd: String,
        /// Exit when the session is over instead of keeping the dashboard up.
        #[arg(long)]
        exit: bool,
    },
    /// Today's (or a day's) summary and live trades.
    Status {
        #[arg(long)]
        date: Option<NaiveDate>,
    },
    /// Kill switch: no new entries, flatten everything.
    Halt {
        #[arg(default_value = "manual halt")]
        reason: String,
    },
    /// Lift the kill switch (refused after 14:45 or after the daily loss stop).
    Resume,
    /// M3 exit: N consecutive paper days without a crash, all flat after square-off.
    ExitCheck {
        #[arg(long, default_value_t = 5)]
        days: usize,
        /// M5: also require every hourly and EOD report to have been delivered.
        #[arg(long)]
        reports: bool,
    },
    /// Boot checks before a session: trading day, disk, clock, secrets.
    /// Exit 0 = go (or not a trading day), 78 = do not trade.
    Preflight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum AiMode {
    Auto,
    On,
    Off,
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json = std::env::var("SAARTHI_LOG_JSON").is_ok_and(|v| v == "1");
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(false);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(e) => {
            tracing::error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

fn secrets_path(p: &Path) -> Option<&Path> {
    p.exists().then_some(p)
}

/// The account id without logging in.
fn account_id(cli: &Cli) -> String {
    engine::account_id(cli.account.as_deref(), &cli.secrets)
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let cfg = Config::load(&cli.config)?;
    if let Some(Cmd::Sim { .. }) = &cli.cmd {
        return simulate::run(&cli, cfg).await;
    }
    let mode = cfg.mode()?;
    let l = load(cfg, cli.data_dir.clone())?;
    let acct = AccountFiles::new(&l.layout, &account_id(&cli), "paper");
    match cli.cmd.as_ref().unwrap_or(&Cmd::Run) {
        Cmd::Run => match mode {
            Mode::Paper => run_paper(&cli, &l, &acct).await,
            Mode::Live => bail!(
                "live trading is locked until the go-live milestones (PLAN M5/M6): \
                 SmartAPI order placement is disabled in this build. Use SAARTHI_MODE=paper."
            ),
            Mode::Backtest => bail!("backtests run with the `backtest` binary"),
        },
        Cmd::Preflight => preflight(&cli, &l, &acct).await,
        Cmd::Sim { .. } => unreachable!("handled above"),
        Cmd::Replay { date, frames, out } => replay(&l, *date, frames.clone(), out.clone()).await,
        Cmd::Status { date } => status(&l, &acct, date.unwrap_or_else(|| MarketClock::ist_date(Utc::now()))),
        Cmd::Halt { reason } => {
            engine::ensure_dir(&acct.dir)?;
            let p = acct.halt_file(&l.cfg.engine.halt_file);
            std::fs::write(&p, reason)?;
            println!("kill switch set ({}); the engine flattens within a second", p.display());
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Resume => {
            let p = acct.halt_file(&l.cfg.engine.halt_file);
            match std::fs::remove_file(&p) {
                Ok(()) => {
                    println!("kill switch removed; the engine resumes if before 14:45 and the loss limit is not hit")
                }
                Err(_) => println!("no kill switch was set"),
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::ExitCheck { days, reports } => {
            let db = Db::open(&acct.db())?;
            let all: Vec<(NaiveDate, DaySummary)> = db.days()?;
            let sent: Option<std::collections::HashMap<NaiveDate, engine::Delivered>> = if *reports {
                let mut m = std::collections::HashMap::new();
                for (d, _) in &all {
                    if let Some(v) = db.kv_get(&engine::delivered_key(*d))? {
                        m.insert(*d, v);
                    }
                }
                Some(m)
            } else {
                None
            };
            let r = exit_check(&all, &l.clock, *days, sent.as_ref());
            for (d, ok, why) in &r.days {
                println!("{d}  {}  {why}", if *ok { "PASS" } else { "FAIL" });
            }
            println!(
                "{} consecutive clean paper day(s) at the end; {} required: {}",
                r.streak,
                r.required,
                match (r.passed, *reports) {
                    (true, true) => "M5 EXIT PASSED",
                    (true, false) => "M3 EXIT PASSED",
                    _ => "not yet",
                }
            );
            Ok(if r.passed { ExitCode::SUCCESS } else { ExitCode::from(2) })
        }
    }
}

async fn preflight(cli: &Cli, l: &Loaded, acct: &AccountFiles) -> Result<ExitCode> {
    use engine::preflight as pf;
    let now = Utc::now();
    let date = MarketClock::ist_date(now);
    let trading = match l.clock.is_trading_day(date) {
        Ok(false) => {
            println!("{date} is not a trading day: nothing to do");
            return Ok(ExitCode::SUCCESS);
        }
        Ok(true) => (true, format!("{date} is a trading day")),
        Err(e) => (false, format!("{e:#} (refresh config/holidays.toml)")),
    };
    engine::ensure_dir(&acct.dir)?;
    let mode = l.cfg.mode()?;
    let checks: Vec<pf::Check> = vec![
        ("calendar", trading.0, trading.1),
        (
            "mode",
            mode == Mode::Paper,
            format!(
                "{mode:?}{}",
                if mode == Mode::Live {
                    " is locked in this build"
                } else {
                    ""
                }
            ),
        ),
        pf::secrets(&cli.secrets),
        pf::disk(l.layout.root()),
        pf::clock(),
    ];
    let mut failed = Vec::new();
    for (name, ok, detail) in &checks {
        println!("{:<9} {}  {detail}", name, if *ok { "ok  " } else { "FAIL" });
        if !ok {
            failed.push(format!("{name}: {detail}"));
        }
    }
    if failed.is_empty() {
        return Ok(ExitCode::SUCCESS);
    }
    let msg = format!("preflight failed, not trading today: {}", failed.join("; "));
    match TelegramAlerts::from_secrets(secrets_path(&cli.secrets), "Saarthi") {
        Some(t) => {
            t.send(Severity::Critical, &msg);
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        None => LogAlerts.send(Severity::Critical, &msg),
    }
    // EX_CONFIG: systemd does not retry it (RestartPreventExitStatus=78).
    Ok(ExitCode::from(78))
}

fn status(l: &Loaded, acct: &AccountFiles, date: NaiveDate) -> Result<ExitCode> {
    let db = Db::open(&acct.db())?;
    match db.day::<DaySummary>(date)? {
        Some(s) => println!("{}", serde_json::to_string_pretty(&s)?),
        None => println!("{date}: no session recorded"),
    }
    let trades: Vec<saarthi_oms::Trade> = db.trades_on(date)?;
    for t in trades {
        println!(
            "{:<12} {:<14} {:<10} {:>5} {:?} entry {:>9} exit {:>9} net {:>9} {}",
            t.id,
            t.symbol.as_str(),
            t.strategy,
            t.entry_qty,
            t.state,
            t.avg_entry().map(|p| p.to_string()).unwrap_or_default(),
            t.avg_exit().map(|p| p.to_string()).unwrap_or_default(),
            t.net(None),
            t.exit_reason.unwrap_or_default()
        );
    }
    let _ = l;
    Ok(ExitCode::SUCCESS)
}

fn parts(
    l: &Loaded,
    paper: Paper,
    db: Db,
    insts: Vec<saarthi_core::Instrument>,
    date: NaiveDate,
    alerts: SharedAlerts,
    halt_file: Option<PathBuf>,
) -> Result<Parts<Paper>> {
    let e = &l.cfg.engine;
    let default_plan = saarthi_plan::load(&e.default_plan).with_context(|| "[engine] default_plan")?;
    let warm = warmup(l, &insts, e.indicators.tf, date, e.warmup_days);
    Ok(Parts {
        broker: Arc::new(paper.clone()),
        paper: Some(paper),
        db,
        limits: l.limits.clone(),
        costs: l.costs.charges.clone(),
        clock: l.clock.clone(),
        instruments: insts,
        strategies: e.strategies.clone(),
        direction: e.direction,
        bars: saarthi_bars::BarConfig {
            session: l.clock.times,
            ..l.cfg.bars.clone()
        },
        indicators: e.indicators.clone(),
        warmup: warm,
        alerts,
        halt_file,
        reconcile_every: secs(e.reconcile_secs),
        sync_every: secs(e.sync_secs),
        square_off_retry: secs(e.square_off_retry_secs),
        square_off_alert_after: e.square_off_alert_after,
        default_plan,
        index_tokens: e.index_tokens.clone(),
    })
}

async fn run_paper(cli: &Cli, l: &Loaded, acct: &AccountFiles) -> Result<ExitCode> {
    let now = Utc::now();
    let date = MarketClock::ist_date(now);
    if !l.clock.is_trading_day(date)? {
        tracing::info!(%date, "not a trading day; nothing to do");
        return Ok(ExitCode::SUCCESS);
    }
    if past(now, date, &l.cfg.engine.stop_at)? {
        tracing::info!("past today's stop time; nothing to do");
        return Ok(ExitCode::SUCCESS);
    }
    let creds = Credentials::load(secrets_path(&cli.secrets)).context("loading credentials")?;
    let api_key = creds.api_key.clone();
    let label = format!("Saarthi paper {}", creds.client_code);
    let api = SmartApi::new(l.cfg.smartapi.clone(), creds)?;
    let session = api.login().await.context("SmartAPI login (needed for the live feed)")?;
    let all = fetch::load_or_fetch_instruments(&l.cfg.smartapi, &l.layout, false).await?;
    let insts = engine::instruments_from(all, l)?;

    engine::ensure_dir(&acct.dir)?;
    let alerts: SharedAlerts = match TelegramAlerts::from_secrets(secrets_path(&cli.secrets), &label) {
        Some(t) => Arc::new(t),
        None => {
            tracing::warn!("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; alerts go to the log only");
            Arc::new(LogAlerts)
        }
    };
    let paper = paper(l, Some(acct.paper_state()))?;
    let db = Db::open(&acct.db())?;
    let mut tokens: Vec<_> = insts
        .iter()
        .map(|i| (ExchangeType::from_exchange(i.exchange), i.token.clone()))
        .collect();
    let traded = tokens.len();
    let p = parts(
        l,
        paper,
        db,
        insts,
        date,
        alerts.clone(),
        Some(acct.halt_file(&l.cfg.engine.halt_file)),
    )?;
    let mut eng = Engine::new(p, now).await?;
    alerts.send(
        Severity::Info,
        &format!(
            "engine up for {date} (paper): {} symbols, capital base ₹{}",
            traded,
            eng.risk().capital()
        ),
    );

    let (feed, mut events) = saarthi_feed::spawn(
        l.cfg.feed.clone(),
        FeedAuth::from_session(&session, &api_key),
        market_hours(l.clock.clone()),
        None,
    );
    tokens.extend(
        l.cfg
            .engine
            .index_tokens
            .iter()
            .map(|(_, t)| (ExchangeType::NseCm, t.clone())),
    );
    feed.subscribe(FeedMode::Quote, tokens).await?;
    let (control, mut requests) = saarthi_control::Handle::channel(16);
    if l.cfg.engine.control_addr.is_empty() {
        tracing::info!("control API disabled");
    } else {
        match engine::secret(secrets_path(&cli.secrets), "CONTROL_API_TOKEN") {
            Some(token) => {
                let addr = l.cfg.engine.control_addr.parse()?;
                saarthi_control::serve(addr, token, control.clone()).await?;
            }
            None => alerts.send(
                Severity::Warning,
                "CONTROL_API_TOKEN not set: control API off, the strategist cannot submit plans",
            ),
        }
    }
    match saarthi_notify::Telegram::from_secrets(secrets_path(&cli.secrets)) {
        Some(tg) => {
            tokio::spawn(engine::telegram::serve(tg, control.clone(), Utc::now().timestamp()));
        }
        None => tracing::info!("Telegram commands off (no bot token / chat id)"),
    }
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stop_at = MarketClock::at(date, saarthi_core::clock::parse_hhmm(&l.cfg.engine.stop_at)?);
    #[cfg(unix)]
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        #[cfg(unix)]
        let terminated = term.recv();
        #[cfg(not(unix))]
        let terminated = std::future::pending::<Option<()>>();
        tokio::select! {
            ev = events.recv() => match ev {
                Some(FeedEvent::Packet(p)) => {
                    if let Some(t) = p.tick() {
                        eng.on_tick(&t, Utc::now()).await?;
                    }
                }
                Some(FeedEvent::Disconnected { reason }) => {
                    tracing::warn!(%reason, "feed down; entries blocked until it recovers");
                    eng.mark_gap();
                }
                Some(FeedEvent::AuthRejected { .. }) => {
                    tracing::warn!("feed refused the session; logging in again");
                    match api.login_now().await {
                        Ok(s) => feed.set_auth(FeedAuth::from_session(&s, &api_key)).await?,
                        Err(e) => alerts.send(Severity::Critical, &format!("re-login failed: {e}")),
                    }
                }
                Some(other) => tracing::debug!(?other, "feed event"),
                None => bail!("feed task stopped"),
            },
            Some((req, reply)) = requests.recv() => {
                let _ = reply.send(eng.handle(req, Utc::now()).await);
            }
            _ = timer.tick() => {
                let now = Utc::now();
                let st = feed.status();
                eng.set_feed(st.entries_allowed(), st.should_flatten());
                eng.on_timer(now).await?;
                if now >= stop_at && (eng.summary().flat_after_square_off || eng.oms().live_trades().next().is_none()) {
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("interrupted; open positions keep their stops and are recovered on restart");
                break;
            }
            _ = terminated => {
                tracing::warn!("SIGTERM; open positions keep their stops and are recovered on restart");
                break;
            }
        }
    }
    feed.shutdown().await;
    let s = eng.summary().clone();
    alerts.send(
        if s.flat_after_square_off {
            Severity::Info
        } else {
            Severity::Critical
        },
        &format!(
            "session over: {} trades ({}W/{}L), net ₹{}, charges ₹{}, flat: {}",
            s.trades, s.wins, s.losses, s.net, s.charges, s.flat_after_square_off
        ),
    );
    // Give the last alert a moment to leave.
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(ExitCode::SUCCESS)
}

/// Replays a recorded session through the full engine against a fresh paper
/// account, using each frame's receive time as the clock.
async fn replay(l: &Loaded, date: NaiveDate, frames: Option<PathBuf>, out: Option<PathBuf>) -> Result<ExitCode> {
    let frames_path = frames.unwrap_or_else(|| l.layout.live_frames(date));
    let frames = read_frames(&frames_path).with_context(|| format!("reading {}", frames_path.display()))?;
    if frames.is_empty() {
        bail!("{} has no frames", frames_path.display());
    }
    let out = out.unwrap_or_else(|| l.layout.root().join("replay").join(date.to_string()));
    if out.exists() {
        std::fs::remove_dir_all(&out).with_context(|| format!("clearing {}", out.display()))?;
    }
    engine::ensure_dir(&out)?;
    let insts = engine::saved_instruments(l)?;
    let paper = paper(l, None)?;
    let alerts = Arc::new(MemoryAlerts::default());
    let p = parts(
        l,
        paper,
        Db::open(&out.join("saarthi.db"))?,
        insts,
        date,
        alerts.clone(),
        None,
    )?;
    let start = frames[0].0;
    let mut eng = Engine::new(p, start).await?;
    let mut next_timer = start;
    let mut undecodable = 0u64;
    for (received, frame) in &frames {
        while next_timer <= *received {
            eng.on_timer(next_timer).await?;
            next_timer += TimeDelta::seconds(1);
        }
        match decode(frame) {
            Ok(p) => {
                if let Some(t) = p.tick() {
                    eng.on_tick(&t, *received).await?;
                }
            }
            Err(_) => undecodable += 1,
        }
    }
    // Run the clock on through the square-off.
    let end = MarketClock::at(date, saarthi_core::clock::parse_hhmm(&l.cfg.engine.stop_at)?);
    while next_timer <= end {
        eng.on_timer(next_timer).await?;
        next_timer += TimeDelta::seconds(1);
    }
    let s = eng.summary().clone();
    println!("{}", serde_json::to_string_pretty(&s)?);
    println!(
        "{} frames ({undecodable} undecodable); day P&L ₹{}; state in {}",
        frames.len(),
        eng.day_pnl(),
        out.display()
    );
    let crit = alerts
        .0
        .lock()
        .map(|a| a.iter().filter(|(s, _)| *s == Severity::Critical).count());
    println!("critical alerts: {}", crit.unwrap_or(0));
    Ok(if s.flat_after_square_off {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
