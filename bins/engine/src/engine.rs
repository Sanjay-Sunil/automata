//! The engine's heart, independent of where ticks and time come from: the
//! daemon feeds it the live feed and the wall clock, `replay` feeds it a
//! recorded session, tests feed it synthetic ticks.
//!
//! Per tick: the paper exchange (if any) plays it, the OMS picks up fills,
//! targets are checked, bars and indicators update, and at each completed
//! indicator bar every strategy on the symbol runs. Entry signals go through
//! the risk guard before the OMS sees them.
//!
//! Per timer (about once a second): broker sync, kill switch, feed health,
//! the daily P&L monitor, reconciliation, and from 15:10 the square-off,
//! repeated until the broker confirms everything is flat.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeDelta};
use saarthi_bars::{BarConfig, BarHub, IndicatorParams, SymbolState};
use saarthi_broker::backtest::CostModel;
use saarthi_broker::{Broker, Paper};
use saarthi_control::{Reply, Request};
use saarthi_core::{Bar, IST, Instrument, MarketClock, Phase, Price, Side, Tick, Ts};
use saarthi_oms::{Intent, Oms, OmsConfig, OmsEvent, Severity, Trade, TradeState};
use saarthi_plan::{Context as PlanCtx, EffectivePlan, Plan, Verdict};
use saarthi_risk::{Book, Breach, EntryProposal, HaltReason, OrderThrottle, RiskGuard, RiskLimits, RiskState};
use saarthi_store::Db;
use saarthi_strategy::{BarCtx, Direction, Exit, Signal, Strategy, registry};
use serde::{Deserialize, Serialize};

use crate::alerts::SharedAlerts;

const RISK_KEY: &str = "risk";
const PLAN_KEY: &str = "plan";

/// Everything needed to build an [`Engine`].
pub struct Parts<B: Broker + ?Sized> {
    pub broker: Arc<B>,
    /// The paper exchange, when paper trading: ticks are played against it.
    pub paper: Option<Paper>,
    pub db: Db,
    pub limits: RiskLimits,
    pub costs: CostModel,
    pub clock: MarketClock,
    pub instruments: Vec<Instrument>,
    pub strategies: Vec<String>,
    pub direction: Direction,
    pub bars: BarConfig,
    pub indicators: IndicatorParams,
    /// Warm-up history per token on the indicator timeframe, oldest first.
    pub warmup: HashMap<String, Vec<Bar>>,
    pub alerts: SharedAlerts,
    pub halt_file: Option<PathBuf>,
    pub reconcile_every: TimeDelta,
    pub sync_every: TimeDelta,
    pub square_off_retry: TimeDelta,
    pub square_off_alert_after: u32,
    /// The conservative fallback plan (`config/plan.default.json`).
    pub default_plan: Plan,
    /// Index tokens (name, NSE token) whose LTP the context endpoint shows.
    pub index_tokens: Vec<(String, String)>,
}

/// One signal and what became of it, for `/signals`.
#[derive(Debug, Clone, Serialize)]
pub struct SignalRecord {
    pub ts: Ts,
    pub symbol: String,
    pub strategy: String,
    pub side: &'static str,
    pub entry: Price,
    pub stop: Price,
    pub target: Option<Price>,
    pub reason: String,
    /// `accepted`, or the risk/plan rejection code.
    pub decision: String,
    pub qty: Option<u32>,
}

const MAX_SIGNALS: usize = 200;
const MAX_BARS: usize = 120;

/// One day's outcome, stored in the `days` table (the M3 exit check reads
/// it).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaySummary {
    pub date: NaiveDate,
    pub trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub gross: Price,
    pub charges: Price,
    pub net: Price,
    pub worst_pnl: Price,
    /// Every position flat (confirmed with the broker) after the square-off.
    pub flat_after_square_off: bool,
    pub flat_confirmed_at: Option<Ts>,
    /// Engine starts on this day beyond the first (crashes or restarts).
    pub restarts: u32,
    pub halted: Option<String>,
    pub critical_alerts: u32,
    pub rejected: BTreeMap<String, u32>,
}

#[derive(Debug, Default)]
struct SquareOff {
    started: bool,
    checks: u32,
    last_check: Option<Ts>,
    alerted: bool,
}

pub struct Engine<B: Broker + ?Sized> {
    broker: Arc<B>,
    paper: Option<Paper>,
    clock: MarketClock,
    risk: RiskGuard,
    oms: Oms<B>,
    hub: BarHub,
    tf: saarthi_core::Tf,
    insts: HashMap<String, Instrument>,
    strategies: HashMap<String, Vec<Box<dyn Strategy>>>,
    direction: Direction,
    ltp: HashMap<String, Price>,
    feed_ok: bool,
    feed_critical: bool,
    feed_flattened: bool,
    alerts: SharedAlerts,
    halt_file: Option<PathBuf>,
    reconcile_every: TimeDelta,
    sync_every: TimeDelta,
    square_off_retry: TimeDelta,
    square_off_alert_after: u32,
    last_reconcile: Option<Ts>,
    last_sync: Option<Ts>,
    square: SquareOff,
    summary: DaySummary,
    plan: EffectivePlan,
    default_plan: Plan,
    limits: RiskLimits,
    enabled: BTreeSet<String>,
    signals: VecDeque<SignalRecord>,
    recent_bars: HashMap<String, VecDeque<Bar>>,
    prev_close: HashMap<String, Price>,
    index_tokens: Vec<(String, String)>,
}

impl<B: Broker + ?Sized> Engine<B> {
    /// Builds the engine for the trading day of `now`, restoring today's
    /// risk counters and unfinished trades, and catching up with the broker.
    pub async fn new(p: Parts<B>, now: Ts) -> Result<Self> {
        let day = MarketClock::ist_date(now);
        let saved: Option<RiskState> = p.db.kv_get(RISK_KEY)?;
        let mut risk = match saved {
            Some(s) => RiskGuard::restore(p.limits.clone(), s),
            None => RiskGuard::new(p.limits.clone()),
        };
        let mut summary: DaySummary = p.db.day(day)?.unwrap_or(DaySummary {
            date: day,
            ..DaySummary::default()
        });
        let restarted = risk.state().day == Some(day);
        if restarted {
            summary.restarts += 1;
        }
        let oms = Oms::new(
            p.broker.clone(),
            p.db,
            OrderThrottle::new(p.limits.max_orders_per_second),
            OmsConfig::new(p.costs),
            day,
        )?;
        let mut hub = BarHub::new();
        let mut strategies = HashMap::new();
        let mut insts = HashMap::new();
        let mut prev_close = HashMap::new();
        for inst in p.instruments {
            let mut st = SymbolState::new(inst.token.clone(), p.bars.clone(), p.indicators.clone());
            if let Some(w) = p.warmup.get(&inst.token) {
                st.warm_up(w);
                if let Some(b) = w.iter().rev().find(|b| MarketClock::ist_date(b.ts) < day) {
                    prev_close.insert(inst.token.clone(), b.close);
                }
            }
            hub.insert(st);
            let mut v: Vec<Box<dyn Strategy>> = Vec::new();
            for id in &p.strategies {
                v.push(registry::build(id, &registry::defaults(id)?)?);
            }
            strategies.insert(inst.token.clone(), v);
            insts.insert(inst.token.clone(), inst);
        }
        if !restarted {
            let funds = p.broker.funds().await.context("reading funds for the capital base")?;
            risk.begin_day(day, funds.net);
        }
        let enabled: BTreeSet<String> = p.strategies.iter().cloned().collect();
        let symbols: BTreeSet<_> = insts.values().map(|i: &Instrument| i.symbol.clone()).collect();
        let ctx = PlanCtx {
            now,
            limits: &p.limits,
            clock: &p.clock,
            symbols: &symbols,
            strategies: &enabled,
            current_version: 0,
        };
        let default = saarthi_plan::validate(p.default_plan.clone(), &ctx, true).context("plan.default.json")?;
        let saved: Option<EffectivePlan> = oms.db().kv_get(PLAN_KEY)?;
        let plan = match saved {
            Some(s) if !s.is_default && s.day() == day && !s.expired(now) => s,
            _ => default,
        };
        let mut e = Engine {
            broker: p.broker,
            paper: p.paper,
            clock: p.clock,
            risk,
            oms,
            hub,
            tf: p.indicators.tf,
            insts,
            strategies,
            direction: p.direction,
            ltp: HashMap::new(),
            feed_ok: true,
            feed_critical: false,
            feed_flattened: false,
            alerts: p.alerts,
            halt_file: p.halt_file,
            reconcile_every: p.reconcile_every,
            sync_every: p.sync_every,
            square_off_retry: p.square_off_retry,
            square_off_alert_after: p.square_off_alert_after,
            last_reconcile: None,
            last_sync: None,
            square: SquareOff::default(),
            summary,
            plan,
            default_plan: p.default_plan,
            limits: p.limits,
            enabled,
            signals: VecDeque::new(),
            recent_bars: HashMap::new(),
            prev_close,
            index_tokens: p.index_tokens,
        };
        let recovered = e.oms.recover(now).await?;
        if restarted {
            e.alert(
                Severity::Warning,
                &format!(
                    "engine restarted mid-day ({recovered} unfinished trade(s) recovered, capital base ₹{})",
                    e.risk.capital()
                ),
            );
        }
        e.after(now).await?;
        Ok(e)
    }

    pub fn oms(&self) -> &Oms<B> {
        &self.oms
    }

    pub fn risk(&self) -> &RiskGuard {
        &self.risk
    }

    pub fn summary(&self) -> &DaySummary {
        &self.summary
    }

    pub fn day_pnl(&self) -> Price {
        self.oms.day_pnl(|t| self.ltp.get(t).copied())
    }

    /// Feed health from the feed task (connected and fresh; silent long
    /// enough to flatten).
    pub fn set_feed(&mut self, ok: bool, critical: bool) {
        self.feed_ok = ok;
        self.feed_critical = critical;
        if ok {
            self.feed_flattened = false;
        }
    }

    /// The feed reconnected: bars in flight may be missing trades.
    pub fn mark_gap(&mut self) {
        self.hub.mark_gap();
    }

    fn alert(&mut self, sev: Severity, text: &str) {
        if sev == Severity::Critical {
            self.summary.critical_alerts += 1;
        }
        self.alerts.send(sev, text);
        if sev != Severity::Info {
            // The EOD report's incident log.
            let detail = serde_json::json!({ "severity": format!("{sev:?}").to_lowercase(), "message": text });
            let ts = chrono::Utc::now().to_rfc3339();
            if let Err(e) = self.oms.db_mut().write(|w| w.event(&ts, "engine_alert", &detail)) {
                tracing::warn!(error = %e, "could not record the alert");
            }
        }
    }

    fn reject(&mut self, code: &str) {
        *self.summary.rejected.entry(code.to_string()).or_default() += 1;
    }

    fn phase(&self, now: Ts) -> Phase {
        self.clock.phase(now).unwrap_or(Phase::Closed)
    }

    /// A new trading day begins (the daemon normally starts fresh each day;
    /// this covers one left running).
    async fn roll_day(&mut self, now: Ts) -> Result<()> {
        let day = MarketClock::ist_date(now);
        if self.oms.day() == day {
            return Ok(());
        }
        self.oms.roll_day(day)?;
        let funds = self.broker.funds().await?;
        self.risk.begin_day(day, funds.net);
        self.summary = DaySummary {
            date: day,
            ..DaySummary::default()
        };
        self.square = SquareOff::default();
        Ok(())
    }

    pub async fn on_tick(&mut self, t: &Tick, now: Ts) -> Result<()> {
        if self.index_tokens.iter().any(|(_, tok)| tok == &t.token) {
            // Context only: indices are never traded.
            self.ltp.insert(t.token.clone(), t.ltp);
            return Ok(());
        }
        if let Some(paper) = &self.paper {
            let fills = paper.on_tick(t)?;
            if !fills.is_empty() {
                self.oms.sync(now).await?;
            }
        }
        self.ltp.insert(t.token.clone(), t.ltp);
        if self.oms.symbol_busy(&t.token) {
            self.oms.on_ltp(&t.token, t.ltp, now).await?;
        }
        if let Some(u) = self.hub.on_tick(t) {
            for ev in u.bars {
                if ev.tf == self.tf && !ev.partial {
                    self.on_bar(&t.token, ev.bar, now).await?;
                }
            }
        }
        self.after(now).await
    }

    async fn on_bar(&mut self, token: &str, bar: Bar, now: Ts) -> Result<()> {
        let bar_end = bar.ts + TimeDelta::minutes(i64::from(self.tf.minutes()));
        let date = MarketClock::ist_date(bar_end);
        let open_at = MarketClock::at(date, self.clock.times.open);
        let phase = self.phase(now);
        let q = self.recent_bars.entry(token.to_string()).or_default();
        q.push_back(bar);
        if q.len() > MAX_BARS {
            q.pop_front();
        }
        let entries_allowed =
            phase == Phase::Open && self.risk.entries_open(now) && self.feed_ok && !self.plan.sits_out();
        let Some(snap) = self.hub.snapshot(token).cloned() else {
            return Ok(());
        };
        let Some(mut strats) = self.strategies.remove(token) else {
            return Ok(());
        };
        let mut wanted: Vec<(String, Signal)> = Vec::new();
        let mut exits: Vec<(String, String)> = Vec::new();
        for s in strats.iter_mut() {
            let live = self.oms.trade_for(token, s.id());
            let ctx = BarCtx {
                bar,
                now: bar_end,
                since_open: bar_end - open_at,
                entries_allowed,
                position: live.filter(|t| t.state != TradeState::Pending).map(|t| t.side),
                direction: self.direction,
            };
            let sig = s.on_bar(&snap, &ctx);
            match live {
                Some(t) if t.state == TradeState::Open => {
                    if let Exit::Close(why) = s.manage(&snap, &ctx) {
                        exits.push((t.id.clone(), why));
                    }
                }
                Some(_) => {}
                None => {
                    if let Some(sig) = sig.filter(|g| entries_allowed && self.direction.allows(g.side)) {
                        wanted.push((s.id().to_string(), sig));
                    }
                }
            }
        }
        self.strategies.insert(token.to_string(), strats);
        for (id, why) in exits {
            self.oms.request_exit(&id, &why, now).await?;
        }
        for (strategy, sig) in wanted {
            self.try_enter(&strategy, sig, now).await?;
        }
        Ok(())
    }

    async fn try_enter(&mut self, strategy: &str, sig: Signal, now: Ts) -> Result<()> {
        let Some(inst) = self.insts.get(&sig.token).cloned() else {
            return Ok(());
        };
        let mut record = SignalRecord {
            ts: now,
            symbol: inst.symbol.to_string(),
            strategy: strategy.to_string(),
            side: if sig.side == Side::Buy { "long" } else { "short" },
            entry: sig.entry,
            stop: sig.stop,
            target: sig.target,
            reason: sig.reason.clone(),
            decision: String::new(),
            qty: None,
        };
        let risk_scale = match self.plan.verdict(&inst.symbol, strategy, sig.side, &self.limits) {
            Verdict::Allow { risk_scale } => risk_scale,
            Verdict::Deny(code) => {
                self.reject(code);
                record.decision = code.to_string();
                self.push_signal(record);
                return Ok(());
            }
        };
        let ltp = |t: &str| self.ltp.get(t).copied();
        let book = Book {
            phase: self.phase(now),
            feed_ok: self.feed_ok,
            open_positions: self.oms.open_count(),
            gross_exposure: self.oms.gross_exposure(ltp),
            symbol_busy: self.oms.symbol_busy(&sig.token),
        };
        let proposal = EntryProposal {
            now,
            token: &sig.token,
            strategy,
            side: sig.side,
            entry: sig.entry,
            stop: sig.stop,
            ltp: self.ltp.get(&sig.token).copied(),
            band: None,
            risk_scale,
        };
        match self.risk.check(&proposal, &book) {
            Ok(a) => {
                record.decision = "accepted".into();
                record.qty = Some(a.qty);
                self.push_signal(record);
                let intent = Intent {
                    strategy: strategy.to_string(),
                    token: inst.token.clone(),
                    symbol: inst.symbol.clone(),
                    exchange: inst.exchange,
                    side: sig.side,
                    qty: a.qty,
                    signal_px: sig.entry,
                    stop: sig.stop,
                    target: sig.target,
                    planned_risk: a.risk,
                    reason: sig.reason,
                };
                self.save_risk()?;
                if let Err(e) = self.oms.open(intent, now).await {
                    self.alert(Severity::Warning, &format!("entry for {} failed: {e}", inst.symbol));
                }
            }
            Err(r) => {
                tracing::debug!(symbol = %inst.symbol, strategy, reason = %r, "entry refused");
                self.reject(r.code());
                record.decision = r.code().to_string();
                self.push_signal(record);
            }
        }
        Ok(())
    }

    /// The periodic checks. Call about once a second.
    pub async fn on_timer(&mut self, now: Ts) -> Result<()> {
        let trading_day = self.clock.is_trading_day(MarketClock::ist_date(now)).unwrap_or(false);
        if trading_day {
            self.roll_day(now).await?;
        }
        let phase = self.phase(now);
        if !self.plan.is_default && self.plan.expired(now) {
            let v = self.plan.version();
            self.alert(
                Severity::Warning,
                &format!("plan v{v} expired; falling back to the default plan"),
            );
            self.use_default_plan(now)?;
        }
        for (tok, ev) in self.hub.on_time(now) {
            if ev.tf == self.tf && !ev.partial {
                self.on_bar(&tok, ev.bar, now).await?;
            }
        }
        if self.paper.is_some() || self.last_sync.is_none_or(|t| now - t >= self.sync_every) {
            self.oms.sync(now).await?;
            self.last_sync = Some(now);
        }
        self.kill_switch(now, phase).await?;
        if self.feed_critical && !self.feed_flattened && self.oms.live_trades().next().is_some() {
            self.feed_flattened = true;
            self.alert(
                Severity::Critical,
                "feed silent too long with open positions: flattening",
            );
            self.oms.flatten_all("feed_stale", now).await?;
        }
        if self.last_reconcile.is_none_or(|t| now - t >= self.reconcile_every) {
            self.last_reconcile = Some(now);
            let rec = self.oms.reconcile(now).await?;
            if rec.severe {
                self.risk.halt(HaltReason::Fault(format!(
                    "reconciliation: {}",
                    rec.mismatches.join("; ")
                )));
                self.oms.flatten_all("reconcile_halt", now).await?;
            }
        }
        if matches!(phase, Phase::SquareOff) || (trading_day && phase == Phase::Closed && self.past_square_off(now)) {
            self.square_off(now).await?;
        }
        self.after(now).await
    }

    fn past_square_off(&self, now: Ts) -> bool {
        now.with_timezone(&IST).time() >= self.clock.times.square_off
    }

    async fn kill_switch(&mut self, now: Ts, phase: Phase) -> Result<()> {
        let Some(path) = self.halt_file.clone() else {
            return Ok(());
        };
        let present = path.exists();
        let manual = matches!(self.risk.halted(), Some(HaltReason::Manual(_)));
        if present && !manual {
            let why = std::fs::read_to_string(&path).unwrap_or_default();
            let why = why.trim();
            self.risk.halt(HaltReason::Manual(if why.is_empty() {
                "HALT file".into()
            } else {
                why.to_string()
            }));
            self.alert(Severity::Critical, "kill switch: halted, flattening everything");
            self.oms.flatten_all("halt", now).await?;
        } else if !present && manual {
            match self.risk.resume(phase) {
                Ok(()) => self.alert(Severity::Warning, "kill switch removed: trading resumed"),
                Err(why) => tracing::debug!(why, "kill switch removed but resume refused"),
            }
        }
        Ok(())
    }

    async fn square_off(&mut self, now: Ts) -> Result<()> {
        if !self.square.started {
            self.square.started = true;
            self.alert(Severity::Info, "square-off: closing every position");
            self.oms.flatten_all("square_off", now).await?;
        }
        if self.summary.flat_after_square_off {
            return Ok(());
        }
        if self.square.last_check.is_some_and(|t| now - t < self.square_off_retry) {
            return Ok(());
        }
        self.square.last_check = Some(now);
        self.square.checks += 1;
        self.oms.sync(now).await?;
        let open = self.oms.broker_open_positions().await?;
        if open.is_empty() && self.oms.live_trades().next().is_none() {
            self.summary.flat_after_square_off = true;
            self.summary.flat_confirmed_at = Some(now);
            self.alert(Severity::Info, "square-off complete: flat");
            return Ok(());
        }
        // Anything the broker still holds that we no longer track is exited
        // by reconciliation; live trades keep retrying their exits.
        self.oms.flatten_all("square_off", now).await?;
        if self.square.checks >= self.square_off_alert_after && !self.square.alerted {
            self.square.alerted = true;
            let what: Vec<String> = open.iter().map(|p| format!("{} {}", p.symbol, p.net_qty)).collect();
            self.alert(
                Severity::Critical,
                &format!("NOT FLAT after square-off, still retrying: {}", what.join(", ")),
            );
        }
        Ok(())
    }

    /// Bookkeeping after every step: OMS events into risk and alerts, the
    /// P&L monitor, persistence.
    async fn after(&mut self, now: Ts) -> Result<()> {
        for (ts, ev) in self.oms.take_events() {
            match ev {
                OmsEvent::Closed {
                    net, symbol, reason, ..
                } => {
                    self.risk.on_trade_closed(ts, net);
                    tracing::info!(%symbol, %net, reason, "trade closed");
                }
                OmsEvent::EntryRejected { reason, trade_id } => {
                    self.reject("broker_rejected");
                    tracing::warn!(trade_id, reason, "entry rejected by broker");
                }
                OmsEvent::Alert { severity, message } if severity != Severity::Info => {
                    self.alert(severity, &message);
                }
                _ => {}
            }
        }
        let pnl = self.day_pnl();
        match self.risk.on_pnl(pnl) {
            Some(Breach::FlattenAndHalt) => {
                self.alert(
                    Severity::Critical,
                    &format!(
                        "daily loss limit hit (₹{pnl} vs limit ₹{}): flattening and halting for the day",
                        self.risk.daily_loss_limit()
                    ),
                );
                self.oms.flatten_all("daily_loss", now).await?;
            }
            Some(Breach::ProfitLocked) => {
                self.alert(
                    Severity::Info,
                    &format!("profit lock reached (₹{pnl}): no new entries today"),
                );
            }
            None => {}
        }
        self.refresh_summary();
        self.save_risk()?;
        Ok(())
    }

    fn refresh_summary(&mut self) {
        let day = self.oms.day();
        let mut s = DaySummary {
            date: day,
            flat_after_square_off: self.summary.flat_after_square_off,
            flat_confirmed_at: self.summary.flat_confirmed_at,
            restarts: self.summary.restarts,
            critical_alerts: self.summary.critical_alerts,
            rejected: std::mem::take(&mut self.summary.rejected),
            halted: self.risk.halted().map(|h| h.to_string()),
            worst_pnl: self.risk.state().worst_pnl,
            ..DaySummary::default()
        };
        for t in self.oms.trades().filter(|t| t.day == day && t.entry_qty > 0) {
            s.trades += 1;
            let net = t.net(self.ltp.get(&t.token).copied());
            if t.state == TradeState::Closed {
                if net.0 > 0 {
                    s.wins += 1;
                } else {
                    s.losses += 1;
                }
            }
            s.gross = s.gross + t.realized() + t.unrealized(self.ltp.get(&t.token).copied());
            s.charges = s.charges + t.charges;
            s.net = s.net + net;
        }
        self.summary = s;
    }

    fn push_signal(&mut self, r: SignalRecord) {
        self.signals.push_back(r);
        if self.signals.len() > MAX_SIGNALS {
            self.signals.pop_front();
        }
    }

    pub fn plan(&self) -> &EffectivePlan {
        &self.plan
    }

    fn plan_ctx<'a>(&'a self, now: Ts, symbols: &'a BTreeSet<saarthi_core::Symbol>) -> PlanCtx<'a> {
        PlanCtx {
            now,
            limits: &self.limits,
            clock: &self.clock,
            symbols,
            strategies: &self.enabled,
            current_version: self.plan.version(),
        }
    }

    fn use_default_plan(&mut self, now: Ts) -> Result<()> {
        let symbols: BTreeSet<_> = self.insts.values().map(|i| i.symbol.clone()).collect();
        let mut ctx = self.plan_ctx(now, &symbols);
        ctx.current_version = 0;
        let d = saarthi_plan::validate(self.default_plan.clone(), &ctx, true)?;
        // Keep counting versions up from the expired plan.
        self.plan = EffectivePlan {
            plan: Plan {
                version: self.plan.version(),
                ..d.plan
            },
            ..d
        };
        self.save_plan()
    }

    fn save_plan(&mut self) -> Result<()> {
        let plan = self.plan.clone();
        self.oms.db_mut().write(|w| {
            w.kv_put(PLAN_KEY, &plan)?;
            w.event(
                &chrono::Utc::now().to_rfc3339(),
                "plan",
                &serde_json::json!({ "version": plan.version(), "default": plan.is_default, "changes": plan.changes }),
            )
        })?;
        Ok(())
    }

    /// Validates, clamps and applies a submitted plan.
    pub fn submit_plan(&mut self, json: &str, now: Ts) -> Reply {
        let plan = saarthi_plan::parse(json).map_err(|e| (422, e.to_string()))?;
        let symbols: BTreeSet<_> = self.insts.values().map(|i| i.symbol.clone()).collect();
        let ctx = self.plan_ctx(now, &symbols);
        let mut eff = saarthi_plan::validate(plan, &ctx, false).map_err(|e| (422, e.to_string()))?;
        // AGENT.md §4.2: never add risk after two straight losses or once half
        // the daily loss budget is gone.
        let st = self.risk.state();
        let limit = self.risk.daily_loss_limit().0;
        let used_half = limit > 0 && (-st.worst_pnl.0).max(0) * 2 >= limit;
        let current = self.plan.plan.global_risk_multiplier;
        if (st.consecutive_losses >= 2 || used_half) && eff.plan.global_risk_multiplier > current {
            eff.changes.push(format!(
                "global_risk_multiplier {} held at {current}: no adding risk after losses",
                eff.plan.global_risk_multiplier
            ));
            eff.plan.global_risk_multiplier = current;
        }
        let reply = serde_json::json!({
            "accepted": true,
            "version": eff.version(),
            "valid_until": eff.valid_until,
            "clamped": eff.changes,
            "watchlist": eff.plan.watchlist.iter().map(|w| w.symbol.as_str()).collect::<Vec<_>>(),
        });
        if !eff.changes.is_empty() {
            tracing::warn!(changes = ?eff.changes, "plan clamped");
        }
        self.plan = eff;
        self.save_plan().map_err(|e| (500, e.to_string()))?;
        tracing::info!(version = self.plan.version(), "plan applied");
        Ok(reply)
    }

    /// Answers a control-API request.
    pub async fn handle(&mut self, req: Request, now: Ts) -> Reply {
        let ltp = |t: &str| self.ltp.get(t).copied();
        let err = |e: anyhow::Error| (500, format!("{e:#}"));
        Ok(match req {
            Request::State => serde_json::json!({
                "now": now,
                "day": self.oms.day(),
                "phase": format!("{:?}", self.phase(now)),
                "feed_ok": self.feed_ok,
                "feed_critical": self.feed_critical,
                "halted": self.risk.halted().map(|h| h.to_string()),
                "entries_open": self.risk.entries_open(now) && !self.plan.sits_out(),
                "plan_version": self.plan.version(),
                "plan_is_default": self.plan.is_default,
                "plan_valid_until": self.plan.valid_until,
                "capital": self.risk.capital(),
                "symbols": self.insts.len(),
                "strategies": self.enabled,
                "square_off_done": self.summary.flat_after_square_off,
                "restarts_today": self.summary.restarts,
            }),
            Request::Positions => {
                let v: Vec<_> = self
                    .oms
                    .live_trades()
                    .map(|t| {
                        let l = ltp(&t.token);
                        serde_json::json!({
                            "trade_id": t.id, "symbol": t.symbol, "strategy": t.strategy,
                            "side": if t.side == Side::Buy { "long" } else { "short" },
                            "state": t.state.as_str(), "qty": t.open_qty(), "avg_entry": t.avg_entry(),
                            "stop": t.stop, "target": t.target, "ltp": l,
                            "unrealized": t.unrealized(l), "net": t.net(l),
                        })
                    })
                    .collect();
                serde_json::json!({ "positions": v })
            }
            Request::Pnl => {
                let s = &self.summary;
                let closed = s.wins + s.losses;
                serde_json::json!({
                    "day_pnl": self.day_pnl(),
                    "gross": s.gross, "charges": s.charges, "net": s.net,
                    "trades": s.trades, "wins": s.wins, "losses": s.losses,
                    "win_rate": if closed > 0 { f64::from(s.wins) / f64::from(closed) } else { 0.0 },
                    "worst_pnl": s.worst_pnl,
                    "daily_loss_limit": self.risk.daily_loss_limit(),
                    "loss_limit_used": if self.risk.daily_loss_limit().0 > 0 {
                        (-s.worst_pnl.0).max(0) as f64 / self.risk.daily_loss_limit().0 as f64
                    } else { 0.0 },
                    "trades_today": self.risk.state().trades_today,
                    "consecutive_losses": self.risk.state().consecutive_losses,
                    "max_trades_per_day": self.limits.max_trades_per_day,
                    "rejections": s.rejected,
                })
            }
            Request::Bars { symbol, n } => {
                let Some(inst) = self.insts.values().find(|i| i.symbol.as_str() == symbol) else {
                    return Err((404, format!("{symbol} is not traded by this engine")));
                };
                let bars: Vec<_> = self
                    .recent_bars
                    .get(&inst.token)
                    .map(|q| q.iter().rev().take(n).rev().copied().collect())
                    .unwrap_or_default();
                let snap = self.hub.snapshot(&inst.token).cloned().unwrap_or_default();
                serde_json::json!({
                    "symbol": symbol, "tf": self.tf.as_str(), "bars": bars,
                    "ltp": snap.ltp, "vwap": snap.vwap, "ema_fast": snap.ema_fast, "ema_slow": snap.ema_slow,
                    "atr": snap.atr, "rsi": snap.rsi, "vol_z": snap.vol_z,
                    "or_high": snap.or_high, "or_low": snap.or_low,
                    "vwap_upper": snap.vwap_upper, "vwap_lower": snap.vwap_lower,
                    "prev_close": self.prev_close.get(&inst.token),
                    "note": "prices in paise; indicators are f64 paise",
                })
            }
            Request::Signals { n } => {
                let v: Vec<_> = self.signals.iter().rev().take(n).collect();
                serde_json::json!({ "signals": v })
            }
            Request::Trades => {
                let all: Vec<Trade> = self
                    .oms
                    .db()
                    .trades_on(self.oms.day())
                    .map_err(|e| (500, e.to_string()))?;
                let v: Vec<_> = all
                    .iter()
                    .map(|t| {
                        let l = if t.open_qty() > 0 { ltp(&t.token) } else { None };
                        serde_json::json!({
                            "trade_id": t.id, "symbol": t.symbol, "strategy": t.strategy,
                            "side": if t.side == Side::Buy { "long" } else { "short" },
                            "state": t.state.as_str(), "qty": t.entry_qty, "open_qty": t.open_qty(),
                            "avg_entry": t.avg_entry(), "avg_exit": t.avg_exit(),
                            "stop": t.stop, "target": t.target, "reason": t.reason,
                            "exit_reason": t.exit_reason, "charges": t.charges, "net": t.net(l),
                            "opened_at": t.opened_at, "closed_at": t.closed_at,
                        })
                    })
                    .collect();
                serde_json::json!({ "trades": v })
            }
            Request::Context => {
                let indices: Vec<_> = self
                    .index_tokens
                    .iter()
                    .map(|(name, tok)| serde_json::json!({ "name": name, "ltp": ltp(tok) }))
                    .collect();
                let (mut adv, mut dec, mut unch) = (0, 0, 0);
                for (tok, pc) in &self.prev_close {
                    match ltp(tok).map(|l| l.cmp(pc)) {
                        Some(std::cmp::Ordering::Greater) => adv += 1,
                        Some(std::cmp::Ordering::Less) => dec += 1,
                        Some(std::cmp::Ordering::Equal) => unch += 1,
                        None => {}
                    }
                }
                serde_json::json!({
                    "indices": indices,
                    "breadth": { "advancing": adv, "declining": dec, "unchanged": unch },
                })
            }
            Request::Plan => serde_json::to_value(&self.plan).map_err(|e| (500, e.to_string()))?,
            Request::SubmitPlan(json) => return self.submit_plan(&json, now),
            Request::Halt(reason) => {
                self.risk.halt(HaltReason::Manual(reason.clone()));
                self.alert(
                    Severity::Critical,
                    &format!("halt via control API ({reason}): flattening"),
                );
                self.oms.flatten_all("halt", now).await.map_err(|e| err(e.into()))?;
                self.after(now).await.map_err(err)?;
                serde_json::json!({ "halted": true })
            }
            Request::Resume => {
                if self.halt_file.as_ref().is_some_and(|p| p.exists()) {
                    return Err((409, "the HALT file is present; remove it (engine resume)".into()));
                }
                self.risk.resume(self.phase(now)).map_err(|e| (409, e.to_string()))?;
                self.alert(Severity::Warning, "trading resumed via control API");
                self.save_risk().map_err(err)?;
                serde_json::json!({ "halted": false })
            }
        })
    }

    fn save_risk(&mut self) -> Result<()> {
        let state = self.risk.state();
        let summary = self.summary.clone();
        self.oms.db_mut().write(|w| {
            w.kv_put(RISK_KEY, &state)?;
            w.put_day(summary.date, &summary)
        })?;
        Ok(())
    }
}
