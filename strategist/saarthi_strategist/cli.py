"""`agent.py premarket|review|hourly|eod|validity` (systemd timers run these).

Exit status is 0 whenever the engine is left in a safe state, which includes
"no plan submitted" (the engine falls back on its own). It is non-zero only
for configuration errors, so systemd flags what a human must fix.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from zoneinfo import ZoneInfo

from . import config, planio, runner
from .budget import Budget
from .engine_api import EngineApi
from .tools import Session, account_store

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger("strategist")


def _client(api_key: str):
    import anthropic

    c = anthropic.Anthropic(api_key=api_key, max_retries=2)
    return c.beta.messages.create


SIM_NOTE = (
    "Note: this is a SIMULATION. The engine is paper-trading a generated market (symbols SIMnnn-EQ, "
    "synthetic prices with planted trend, range and volatile days) on a simulated clock. Real news, "
    "global cues and web results do not describe these symbols; base the plan on the engine's state, "
    "bars, signals and the parquet history instead, and say so in the rationale."
)


def _state_note(api: EngineApi | None) -> str:
    notes = []
    if config.simulated():
        notes.append(SIM_NOTE)
    if api is None:
        notes.append("Note: the engine's control API is not reachable; engine tools will fail.")
    return "\n".join(notes)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="agent.py", description=__doc__)
    ap.add_argument("mode", choices=["premarket", "review", "hourly", "eod", "validity"])
    ap.add_argument("--dry-run", action="store_true", help="validate plans locally; submit nothing")
    ap.add_argument("--runs", type=int, default=20, help="validity: number of premarket dry runs")
    ap.add_argument("--json", action="store_true", help="print the outcome as JSON")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    s = config.load()
    key = config.secret("ANTHROPIC_API_KEY")
    if not key:
        log.error("ANTHROPIC_API_KEY is not set (environment or config/secrets.env)")
        return 2
    token = config.secret("CONTROL_API_TOKEN")
    api = EngineApi(s.control_url, token) if token else None
    if api is not None and not api.health():
        api = None
    client_code = config.secret("ANGEL_CLIENT_CODE") or "default"
    store = account_store(s, client_code)
    schema = planio.load_schema(s.schema_path)
    now = config.now(IST)
    create = _client(key)

    if a.mode == "validity":
        # M4 exit check: N premarket runs, every one must produce a schema-valid plan.
        valid = 0
        for i in range(a.runs):
            sess = Session(s, "premarket", now.date(), api, store, schema, dry_run=True)
            budget = Budget.for_day(s.state_dir, now.date(), s.max_tokens_per_day)
            o = runner.run(sess, budget, create, now, _state_note(api))
            good = bool(o.plans) and not planio.errors(planio.validator(schema), o.plans[-1])
            valid += good
            log.info("run %d/%d: %s (%s, %d tokens)", i + 1, a.runs, "valid" if good else "INVALID", o.reason, o.tokens)
        print(json.dumps({"runs": a.runs, "valid": valid, "passed": valid == a.runs}))
        return 0 if valid == a.runs else 1

    if a.mode in ("premarket", "review") and api is None and not a.dry_run:
        log.warning("engine control API unreachable; %s skipped (the engine keeps its plan)", a.mode)
        return 0
    sess = Session(s, a.mode, now.date(), api, store, schema, dry_run=a.dry_run)
    budget = Budget.for_day(s.state_dir, now.date(), s.max_tokens_per_day)
    o = runner.run(sess, budget, create, now, _state_note(api))
    if a.mode == "hourly" and o.ok and o.text and not a.dry_run:
        sess.mode = "hourly"
        sess.write_journal(f"**{now:%H:%M}** {o.text}")
    if a.json:
        print(json.dumps(o.__dict__, default=str, indent=1))
    else:
        log.info("%s: %s (%d tokens, %d turns)", a.mode, o.reason, o.tokens, o.turns)
        if o.text:
            print(o.text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
