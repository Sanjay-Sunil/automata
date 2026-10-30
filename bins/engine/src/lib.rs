//! Library half of the `engine` daemon, so the whole trading loop can be
//! driven by tests and by `engine replay`.

pub mod alerts;
pub mod config;
pub mod engine;
pub mod sim;
pub mod telegram;

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, TimeDelta};
use saarthi_broker::backtest::ExecutionCosts;
use saarthi_broker::{Paper, PaperConfig};
use saarthi_core::{Bar, HolidayCalendar, Instrument, MarketClock, Symbol, Tf, Ts};
use saarthi_risk::RiskLimits;
use saarthi_store::DataLayout;
use saarthi_store::parquet::{read_bars, read_instruments};
use serde::Serialize;

pub use engine::{DaySummary, Engine, Parts};

use crate::config::Config;

/// Files of one account's paper-trading state.
#[derive(Debug, Clone)]
pub struct AccountFiles {
    pub dir: PathBuf,
}

impl AccountFiles {
    /// `data/accounts/<client>/<mode>/`.
    pub fn new(layout: &DataLayout, client_code: &str, mode: &str) -> Self {
        AccountFiles {
            dir: layout.account_dir(client_code).join(mode),
        }
    }

    pub fn db(&self) -> PathBuf {
        self.dir.join("saarthi.db")
    }

    pub fn paper_state(&self) -> PathBuf {
        self.dir.join("paper.json")
    }

    pub fn halt_file(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

/// Everything loaded from config files (no network).
pub struct Loaded {
    pub cfg: Config,
    pub layout: DataLayout,
    pub limits: RiskLimits,
    pub costs: ExecutionCosts,
    pub clock: MarketClock,
    pub symbols: Vec<Symbol>,
    pub exchange: saarthi_core::Exchange,
}

pub fn load(cfg: Config, data_dir: Option<PathBuf>) -> Result<Loaded> {
    let layout = DataLayout::new(data_dir.unwrap_or_else(|| cfg.paths.data_dir.clone()));
    let limits = RiskLimits::load(&cfg.engine.risk)?;
    let costs = ExecutionCosts::load(&cfg.engine.costs)?;
    let calendar = HolidayCalendar::load(&cfg.paths.holidays)?;
    let times = limits.session()?;
    let clock = MarketClock::new(times, calendar);
    let universe = fetch::universe::Universe::load(&cfg.paths.universe)?;
    let symbols = if cfg.engine.symbols.is_empty() {
        universe.symbols.clone()
    } else {
        let allowed: BTreeSet<&Symbol> = universe.symbols.iter().collect();
        let mut v = Vec::new();
        for s in &cfg.engine.symbols {
            let s = Symbol::new(s.trim());
            if !allowed.contains(&s) {
                bail!("[engine] symbols: {s} is not in the universe");
            }
            v.push(s);
        }
        v
    };
    Ok(Loaded {
        cfg,
        layout,
        limits,
        costs,
        clock,
        symbols,
        exchange: universe.exchange,
    })
}

/// The instruments to trade, from the saved instrument master.
pub fn instruments_from(all: Vec<Instrument>, l: &Loaded) -> Result<Vec<Instrument>> {
    let missing = fetch::missing_symbols(&all, l.exchange, &l.symbols);
    if !missing.is_empty() {
        tracing::warn!(?missing, "symbols not in the instrument master are skipped");
    }
    let wanted: BTreeSet<&Symbol> = l.symbols.iter().collect();
    let v: Vec<Instrument> = all
        .into_iter()
        .filter(|i| i.exchange == l.exchange && wanted.contains(&i.symbol))
        .collect();
    if v.is_empty() {
        bail!("no tradable instruments (is data/instruments.parquet current?)");
    }
    Ok(v)
}

pub fn saved_instruments(l: &Loaded) -> Result<Vec<Instrument>> {
    let p = l.layout.instruments();
    let all = read_instruments(&p).with_context(|| format!("reading {} (run `fetch instruments`)", p.display()))?;
    instruments_from(all, l)
}

/// The last `days` trading days of candles on `tf` before `date`, per token.
pub fn warmup(l: &Loaded, insts: &[Instrument], tf: Tf, date: NaiveDate, days: usize) -> HashMap<String, Vec<Bar>> {
    let mut out = HashMap::new();
    if days == 0 {
        return out;
    }
    let start = MarketClock::at(date, l.clock.times.pre_open);
    for i in insts {
        let path = l.layout.candles(tf, &i.symbol);
        let Ok(bars) = read_bars(&path) else { continue };
        let before: Vec<Bar> = bars.into_iter().filter(|b| b.ts < start).collect();
        let dates: BTreeSet<NaiveDate> = before.iter().map(|b| MarketClock::ist_date(b.ts)).collect();
        let Some(&first) = dates.iter().rev().nth(days.saturating_sub(1)).or(dates.iter().next()) else {
            continue;
        };
        out.insert(
            i.token.clone(),
            before
                .into_iter()
                .filter(|b| MarketClock::ist_date(b.ts) >= first)
                .collect(),
        );
    }
    out
}

pub fn paper(l: &Loaded, state: Option<PathBuf>) -> Result<Paper> {
    Ok(Paper::open(PaperConfig {
        capital: saarthi_risk::inr(l.cfg.engine.paper_capital_inr),
        costs: l.costs.charges.clone(),
        slippage: l.costs.slippage.clone(),
        max_fill_qty_per_tick: None,
        state_path: state,
    })?)
}

/// The account id without logging in: `explicit`, else `ANGEL_CLIENT_CODE`
/// (environment, then the secrets file), else `default`.
pub fn account_id(explicit: Option<&str>, secrets: &Path) -> String {
    if let Some(a) = explicit {
        return a.to_string();
    }
    secret(secrets.exists().then_some(secrets), "ANGEL_CLIENT_CODE").unwrap_or_else(|| "default".into())
}

/// A secret from the environment, else from the secrets file.
pub fn secret(path: Option<&Path>, key: &str) -> Option<String> {
    let from_env = std::env::var(key).ok().filter(|v| !v.trim().is_empty());
    from_env
        .or_else(|| {
            dotenvy::from_path_iter(path?)
                .ok()?
                .flatten()
                .find(|(k, v)| k == key && !v.trim().is_empty())
                .map(|(_, v)| v)
        })
        .map(|v| v.trim().to_string())
}

pub fn secs(s: u64) -> TimeDelta {
    TimeDelta::seconds(s as i64)
}

/// The M3 exit check: `n` consecutive trading days with no crash or
/// restart and everything flat after the square-off.
#[derive(Debug, Clone, Serialize)]
pub struct ExitCheck {
    pub required: usize,
    pub streak: usize,
    pub passed: bool,
    pub days: Vec<(NaiveDate, bool, String)>,
}

/// Reports delivered for one day (written by the `report` binary).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Delivered {
    pub hourly: u32,
    pub eod: bool,
}

/// Hourly reports a full day should have (10:15 … 14:15).
pub const HOURLY_PER_DAY: u32 = 5;

pub fn delivered_key(date: NaiveDate) -> String {
    format!("reports:{date}")
}

/// The M3 exit check (`reports: None`) or the M5 one (every report
/// delivered as well).
pub fn exit_check(
    days: &[(NaiveDate, DaySummary)],
    clock: &MarketClock,
    n: usize,
    reports: Option<&HashMap<NaiveDate, Delivered>>,
) -> ExitCheck {
    let mut rows = Vec::new();
    let mut streak = 0usize;
    let mut best = 0usize;
    let mut prev: Option<NaiveDate> = None;
    for (d, s) in days {
        let mut why = Vec::new();
        if s.restarts > 0 {
            why.push(format!("{} restart(s)", s.restarts));
        }
        if !s.flat_after_square_off {
            why.push("not confirmed flat".to_string());
        }
        if let Some(r) = reports {
            let got = r.get(d).copied().unwrap_or_default();
            if !got.eod {
                why.push("EOD report not delivered".to_string());
            }
            if got.hourly < HOURLY_PER_DAY {
                why.push(format!("{}/{HOURLY_PER_DAY} hourly reports", got.hourly));
            }
        }
        let ok = why.is_empty();
        let consecutive = prev.is_none_or(|p| clock.next_trading_day(p).ok() == Some(*d));
        streak = if ok {
            if consecutive { streak + 1 } else { 1 }
        } else {
            0
        };
        best = best.max(streak);
        rows.push((*d, ok, if ok { "ok".into() } else { why.join(", ") }));
        prev = Some(*d);
    }
    ExitCheck {
        required: n,
        streak,
        passed: best >= n,
        days: rows,
    }
}

/// Is `now` past the daemon's stop time on its day?
pub fn past(now: Ts, date: NaiveDate, hhmm: &str) -> Result<bool> {
    let t = saarthi_core::clock::parse_hhmm(hhmm)?;
    Ok(now >= MarketClock::at(date, t))
}

/// Boot checks (ARCHITECTURE §6, 08:30). Each line is (check, ok, detail).
pub mod preflight {
    use std::path::Path;
    use std::process::Command;

    /// Refuse to trade when the clock is off by more than this.
    pub const MAX_DRIFT_SECS: f64 = 0.5;
    /// Keep at least this much free under `data/`.
    pub const MIN_FREE_KB: u64 = 2 * 1024 * 1024;

    pub type Check = (&'static str, bool, String);

    /// Offset from `chronyc -c tracking` (CSV: field 5 is the system time
    /// offset in seconds; the last field the leap status).
    pub fn parse_chrony(csv: &str) -> Result<f64, String> {
        let f: Vec<&str> = csv.trim().split(',').collect();
        if f.len() < 6 {
            return Err(format!("unexpected chronyc output: {csv:?}"));
        }
        if f.last().is_some_and(|s| s.contains("Not synchronised")) {
            return Err("chrony is not synchronised".into());
        }
        f[4].trim()
            .parse::<f64>()
            .map_err(|_| format!("unexpected chronyc offset: {:?}", f[4]))
    }

    /// Available KiB from `df -Pk <dir>`.
    pub fn parse_df(out: &str) -> Option<u64> {
        out.lines().nth(1)?.split_whitespace().nth(3)?.parse().ok()
    }

    pub fn clock() -> Check {
        match Command::new("chronyc").args(["-c", "tracking"]).output() {
            Ok(o) if o.status.success() => match parse_chrony(&String::from_utf8_lossy(&o.stdout)) {
                Ok(off) => (
                    "clock",
                    off.abs() <= MAX_DRIFT_SECS,
                    format!(
                        "offset {:.0} ms (limit {:.0} ms)",
                        off * 1000.0,
                        MAX_DRIFT_SECS * 1000.0
                    ),
                ),
                Err(e) => ("clock", false, e),
            },
            _ => (
                "clock",
                true,
                "chronyc not available: drift unchecked (install chrony; provision.sh does)".into(),
            ),
        }
    }

    pub fn disk(dir: &Path) -> Check {
        match Command::new("df").arg("-Pk").arg(dir).output() {
            Ok(o) if o.status.success() => match parse_df(&String::from_utf8_lossy(&o.stdout)) {
                Some(kb) => (
                    "disk",
                    kb >= MIN_FREE_KB,
                    format!("{} MiB free under {}", kb / 1024, dir.display()),
                ),
                None => ("disk", false, "could not read df output".into()),
            },
            _ => ("disk", true, "df not available: free space unchecked".into()),
        }
    }

    /// The secrets file must exist, hold the broker keys, and not be readable
    /// by other users.
    pub fn secrets(path: &Path) -> Check {
        let Ok(meta) = std::fs::metadata(path) else {
            let env_ok = ["ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_MPIN", "ANGEL_TOTP_SECRET"]
                .iter()
                .all(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()));
            return (
                "secrets",
                env_ok,
                if env_ok {
                    "from the environment".into()
                } else {
                    format!("{} missing and ANGEL_* not in the environment", path.display())
                },
            );
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode();
            if mode & 0o077 != 0 {
                return (
                    "secrets",
                    false,
                    format!(
                        "{} is readable by other users (mode {:o}); chmod 600 it",
                        path.display(),
                        mode & 0o777
                    ),
                );
            }
        }
        #[cfg(not(unix))]
        let _ = meta;
        let missing: Vec<&str> = ["ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_MPIN", "ANGEL_TOTP_SECRET"]
            .into_iter()
            .filter(|k| crate::secret(Some(path), k).is_none())
            .collect();
        (
            "secrets",
            missing.is_empty(),
            if missing.is_empty() {
                "broker keys present".into()
            } else {
                format!("missing {}", missing.join(", "))
            },
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_chrony_and_df() {
            let ok = "A9FEA9FE,169.254.169.254,3,1761650000.1,-0.000123456,0.0001,0.0002,-12.3,0.01,0.05,0.001,0.002,64.5,Normal";
            assert!((parse_chrony(ok).unwrap() + 0.000123456).abs() < 1e-12);
            assert!(parse_chrony("x,y,3,1,0.1,0,0,0,0,0,0,0,0,Not synchronised").is_err());
            assert!(parse_chrony("garbage").is_err());
            let df =
                "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/root 50000000 1000 4194304 1% /\n";
            assert_eq!(parse_df(df), Some(4_194_304));
        }

        #[cfg(unix)]
        #[test]
        fn world_readable_secrets_fail() {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("secrets.env");
            std::fs::write(
                &p,
                "ANGEL_API_KEY=a\nANGEL_CLIENT_CODE=b\nANGEL_MPIN=c\nANGEL_TOTP_SECRET=d\n",
            )
            .unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(!secrets(&p).1);
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(secrets(&p).1, "{:?}", secrets(&p));
        }
    }
}

pub fn ensure_dir(p: &Path) -> Result<()> {
    std::fs::create_dir_all(p).with_context(|| format!("creating {}", p.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock() -> MarketClock {
        let p = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/holidays.toml"));
        MarketClock::new(Default::default(), HolidayCalendar::load(p).unwrap())
    }

    fn d(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    fn ok(date: &str) -> (NaiveDate, DaySummary) {
        (
            d(date),
            DaySummary {
                date: d(date),
                flat_after_square_off: true,
                ..DaySummary::default()
            },
        )
    }

    #[test]
    fn exit_check_needs_consecutive_clean_trading_days() {
        // Mon 26 Oct .. Fri 30 Oct 2026 are trading days.
        let mut days: Vec<_> = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29"]
            .iter()
            .map(|s| ok(s))
            .collect();
        assert!(!exit_check(&days, &clock(), 5, None).passed);
        days.push(ok("2026-10-30"));
        assert!(exit_check(&days, &clock(), 5, None).passed);
        // A restart breaks the streak.
        days[2].1.restarts = 1;
        let r = exit_check(&days, &clock(), 5, None);
        assert!(!r.passed);
        assert_eq!(r.streak, 2);
        // A missing trading day breaks it too.
        let gap: Vec<_> = ["2026-10-26", "2026-10-27", "2026-10-29", "2026-10-30", "2026-11-02"]
            .iter()
            .map(|s| ok(s))
            .collect();
        assert!(!exit_check(&gap, &clock(), 5, None).passed);
        // M5: reports must have gone out too.
        let mut sent: HashMap<NaiveDate, Delivered> = days
            .iter()
            .map(|(d, _)| (*d, Delivered { hourly: 5, eod: true }))
            .collect();
        days[2].1.restarts = 0;
        assert!(exit_check(&days, &clock(), 5, Some(&sent)).passed);
        sent.insert(d("2026-10-28"), Delivered { hourly: 4, eod: true });
        let r = exit_check(&days, &clock(), 5, Some(&sent));
        assert!(!r.passed && r.days[2].2.contains("4/5 hourly"), "{:?}", r.days);
    }
}
