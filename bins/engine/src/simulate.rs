//! `engine sim`: a whole paper-trading day on a generated market, at N×
//! speed, with the control API and dashboard up and the strategist run on
//! the simulated clock.
//!
//! The engine, risk guard, OMS and paper broker are the ones `engine run`
//! uses; only the feed (generated ticks) and the clock (simulated) differ.
//! The strategist is told it is a simulation and gets the simulated time in
//! `SAARTHI_NOW`.

use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{NaiveTime, TimeDelta, Utc};
use engine::alerts::{LogAlerts, SharedAlerts};
use engine::config::Config;
use engine::{AccountFiles, Engine, load, paper};
use saarthi_control::Request;
use saarthi_core::{MarketClock, Ts};
use saarthi_store::Db;
use tokio::process::{Child, Command};
use tokio::time::Instant;

use super::{AiMode, Cli, Cmd, parts, secrets_path};

/// Left in the simulation directory so a restart only ever wipes a
/// directory this command created.
const MARKER: &str = ".saarthi-sim";
/// Simulated minutes between strategist reviews (AGENT.md: every 15 minutes).
const REVIEW_EVERY_MIN: i64 = 15;
/// The pre-market plan may hold the clock this long before trading starts anyway.
const PREMARKET_TIMEOUT: Duration = Duration::from_secs(20 * 60);

struct Strategist {
    cmd: Vec<String>,
    env: Vec<(String, String)>,
    log: PathBuf,
    running: Option<(&'static str, Child, Instant)>,
}

impl Strategist {
    fn spawn(&mut self, mode: &'static str, now: Ts) -> Result<()> {
        let log = std::fs::OpenOptions::new().create(true).append(true).open(&self.log)?;
        let child = Command::new(&self.cmd[0])
            .args(&self.cmd[1..])
            .arg(mode)
            .envs(self.env.iter().cloned())
            .env("SAARTHI_NOW", now.with_timezone(&saarthi_core::IST).to_rfc3339())
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting the strategist ({})", self.cmd.join(" ")))?;
        tracing::info!(mode, at = %now.with_timezone(&saarthi_core::IST).format("%H:%M"), "strategist started");
        self.running = Some((mode, child, Instant::now()));
        Ok(())
    }

    /// The routine that just finished, if one did.
    fn poll(&mut self) -> Option<&'static str> {
        let (mode, child, _) = self.running.as_mut()?;
        match child.try_wait() {
            Ok(Some(st)) => {
                let mode = *mode;
                tracing::info!(mode, status = %st, "strategist finished");
                self.running = None;
                Some(mode)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("strategist: {e}");
                let mode = *mode;
                self.running = None;
                Some(mode)
            }
        }
    }
}

fn random_token() -> String {
    let mut b = [0u8; 16];
    let read = std::fs::File::open("/dev/urandom").and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b));
    if read.is_err() {
        let n = Utc::now().timestamp_nanos_opt().unwrap_or_default() as u128 ^ (std::process::id() as u128) << 64;
        b = n.to_le_bytes();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Empties `dir` if this command made it; refuses to touch anything else.
fn fresh_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        let empty = std::fs::read_dir(dir)?.next().is_none();
        if !empty && !dir.join(MARKER).exists() {
            bail!(
                "{} exists and was not made by `engine sim`; pass another --dir",
                dir.display()
            );
        }
        std::fs::remove_dir_all(dir).with_context(|| format!("clearing {}", dir.display()))?;
    }
    engine::ensure_dir(dir)?;
    std::fs::write(dir.join(MARKER), "simulated market data; wiped by every `engine sim`\n")?;
    Ok(())
}

pub async fn run(cli: &Cli, mut cfg: Config) -> Result<ExitCode> {
    let Some(Cmd::Sim {
        speed,
        symbols,
        seed,
        date,
        dir,
        ai,
        strategist_cmd,
        exit,
    }) = &cli.cmd
    else {
        unreachable!("called for `engine sim` only")
    };
    if !(1.0..=3600.0).contains(speed) {
        bail!("--speed must be between 1 and 3600");
    }
    if *symbols == 0 || *symbols > 200 {
        bail!("--symbols must be between 1 and 200");
    }
    let secrets = secrets_path(&cli.secrets);
    let ai_key = engine::secret(secrets, "ANTHROPIC_API_KEY");
    let use_ai = match (ai, &ai_key) {
        (AiMode::Off, _) => false,
        (AiMode::On, None) => bail!(
            "--ai on needs ANTHROPIC_API_KEY (environment or {})",
            cli.secrets.display()
        ),
        (_, k) => k.is_some(),
    };

    fresh_dir(dir)?;
    let dir = std::path::absolute(dir)?;
    // The real calendar picks the day; the simulated universe replaces the real one.
    let clock = load(cfg.clone(), Some(dir.clone()))?.clock;
    let date = match date {
        Some(d) => *d,
        None => {
            let mut d = MarketClock::ist_date(Utc::now());
            while !clock.is_trading_day(d)? {
                d = d.pred_opt().context("date out of range")?;
            }
            d
        }
    };
    if !clock.is_trading_day(date)? {
        bail!("{date} is not a trading day");
    }
    let seed = seed.unwrap_or_else(|| Utc::now().timestamp_millis() as u64);
    let market = engine::sim::build(
        &dir,
        &clock,
        date,
        *symbols,
        cfg.engine.warmup_days.max(30),
        seed,
        &cfg.engine.index_tokens,
    )?;
    cfg.paths.universe = market.universe.clone();
    cfg.engine.symbols.clear();
    let l = load(cfg, Some(dir.clone()))?;
    let acct = AccountFiles::new(&l.layout, "sim", "paper");
    engine::ensure_dir(&acct.dir)?;
    let alerts: SharedAlerts = Arc::new(LogAlerts);
    let p = parts(
        &l,
        paper(&l, Some(acct.paper_state()))?,
        Db::open(&acct.db())?,
        market.instruments.clone(),
        date,
        alerts,
        Some(acct.halt_file(&l.cfg.engine.halt_file)),
    )?;

    let at = |h, m| MarketClock::at(date, NaiveTime::from_hms_opt(h, m, 0).expect("valid time"));
    let start = MarketClock::at(date, l.clock.times.pre_open) - TimeDelta::minutes(1);
    let stop = MarketClock::at(date, saarthi_core::clock::parse_hhmm(&l.cfg.engine.stop_at)?);
    let mut eng = Engine::new(p, start).await?;

    let token = engine::secret(secrets, "CONTROL_API_TOKEN").unwrap_or_else(random_token);
    let addr_s = if l.cfg.engine.control_addr.is_empty() {
        "127.0.0.1:7070"
    } else {
        l.cfg.engine.control_addr.as_str()
    };
    let (control, mut requests) = saarthi_control::Handle::channel(16);
    let addr = saarthi_control::serve(addr_s.parse()?, token.clone(), control)
        .await
        .with_context(|| format!("control API on {addr_s} (is `engine run` or another sim using it?)"))?;

    let mut strategist = Strategist {
        cmd: strategist_cmd.split_whitespace().map(String::from).collect(),
        env: vec![
            ("SAARTHI_SIMULATION".into(), "1".into()),
            ("SAARTHI_DATA_DIR".into(), dir.display().to_string()),
            ("SAARTHI_UNIVERSE".into(), market.universe.display().to_string()),
            ("CONTROL_API_TOKEN".into(), token.clone()),
        ],
        log: dir.join("strategist.log"),
        running: None,
    };
    if strategist.cmd.is_empty() {
        bail!("--strategist-cmd is empty");
    }

    let wall_minutes = (stop - start).num_seconds() as f64 / speed / 60.0;
    println!(
        "\nSIMULATED MARKET (seed {seed}): {} generated symbols on {date}, at {speed}× (~{wall_minutes:.0} min).\n\
         Prices are synthetic: this shows how the engine and the strategist behave, not whether they make money.\n\
         AI strategist: {}\n\n  Dashboard: http://{addr}/#token={token}\n",
        market.instruments.len(),
        if use_ai {
            format!("on (log: {})", strategist.log.display())
        } else if *ai == AiMode::Off {
            "off (--ai off); the default plan trades at half risk".into()
        } else {
            "off (no ANTHROPIC_API_KEY); the default plan trades at half risk".into()
        },
    );

    let ticks = market.ticks;
    let mut next = 0usize;
    let mut now = start;
    let (mut sim_anchor, mut wall_anchor) = (now, Instant::now());
    let mut next_review = at(9, 30);
    let last_review = at(14, 45);
    let mut premarket = if use_ai { Phase::Pending } else { Phase::Done };
    let mut eod = if use_ai { Phase::Pending } else { Phase::Done };
    let mut finished = false;
    loop {
        if let Some(mode) = strategist.poll() {
            if mode == "premarket" {
                premarket = Phase::Done;
                (sim_anchor, wall_anchor) = (now, Instant::now());
            }
            if mode == "eod" {
                eod = Phase::Done;
            }
        }
        if premarket == Phase::Pending {
            strategist.spawn("premarket", now)?;
            premarket = Phase::Running;
        }
        if premarket == Phase::Running
            && strategist
                .running
                .as_ref()
                .is_some_and(|(_, _, t)| t.elapsed() > PREMARKET_TIMEOUT)
        {
            tracing::warn!("pre-market plan is late; trading starts on the plan in force");
            strategist.running = None;
            premarket = Phase::Done;
            (sim_anchor, wall_anchor) = (now, Instant::now());
        }
        let paused = premarket == Phase::Running;

        if !paused && !finished {
            now += TimeDelta::seconds(1);
            while next < ticks.len() && ticks[next].ts <= now {
                eng.on_tick(&ticks[next], now).await?;
                next += 1;
            }
            eng.on_timer(now).await?;
            if use_ai && now >= next_review && now <= last_review {
                if strategist.running.is_none() {
                    strategist.spawn("review", now)?;
                }
                while next_review <= now {
                    next_review += TimeDelta::minutes(REVIEW_EVERY_MIN);
                }
            }
            if now >= stop && (eng.summary().flat_after_square_off || eng.oms().live_trades().next().is_none()) {
                finished = true;
                let s = eng.summary();
                println!(
                    "\nsession over: {} trades ({}W/{}L), net ₹{}, charges ₹{}, flat: {}",
                    s.trades, s.wins, s.losses, s.net, s.charges, s.flat_after_square_off
                );
            }
        }
        if finished {
            if eod == Phase::Pending && strategist.running.is_none() {
                strategist.spawn("eod", now)?;
                eod = Phase::Running;
            }
            if *exit && eod == Phase::Done {
                break;
            }
        }

        let target = if paused || finished {
            Instant::now() + Duration::from_millis(200)
        } else {
            wall_anchor + Duration::from_secs_f64((now - sim_anchor).num_milliseconds() as f64 / 1000.0 / speed)
        };
        let ai_status = match (&strategist.running, use_ai) {
            (_, false) => "off",
            (Some(("premarket", ..)), _) => "writing the day's plan",
            (Some(("review", ..)), _) => "reviewing",
            (Some(("eod", ..)), _) => "writing the journal",
            (Some(_), _) => "running",
            (None, _) if finished && eod == Phase::Done => "done",
            (None, _) => "idle",
        };
        loop {
            tokio::select! {
                Some((req, reply)) = requests.recv() => {
                    let is_state = matches!(req, Request::State);
                    let mut r = eng.handle(req, now).await;
                    if is_state && let Ok(serde_json::Value::Object(m)) = &mut r {
                        m.insert("simulated".into(), true.into());
                        m.insert("speed".into(), (*speed).into());
                        m.insert("strategist".into(), ai_status.into());
                    }
                    let _ = reply.send(r);
                }
                _ = tokio::time::sleep_until(target) => break,
                _ = tokio::signal::ctrl_c() => {
                    println!("interrupted");
                    return Ok(ExitCode::SUCCESS);
                }
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Running,
    Done,
}
