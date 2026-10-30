"""The agent loop against a scripted model and a fake engine: no network."""

import copy
import json
from datetime import date, datetime
from types import SimpleNamespace as NS
from zoneinfo import ZoneInfo

import httpx
import pytest

from saarthi_strategist import runner, tools
from saarthi_strategist.budget import Budget
from saarthi_strategist.config import Settings
from saarthi_strategist.engine_api import EngineApi

NOW = datetime(2026, 10, 28, 8, 50, tzinfo=ZoneInfo("Asia/Kolkata"))


def plan(**over):
    p = {
        "market_bias": "neutral",
        "regime": "range",
        "global_risk_multiplier": 0.5,
        "watchlist": [],
        "paused_strategies": [],
        "rationale": "Quiet cues.",
    }
    return p | over


class FakeEngine:
    """The control API: GET /plan and POST /plan, with the engine's clamping."""

    def __init__(self, reject=False):
        self.version = 0
        self.posted = []
        self.reject = reject

    def handler(self, req: httpx.Request) -> httpx.Response:
        assert req.headers["authorization"] == "Bearer t" * 1
        if req.url.path == "/health":
            return httpx.Response(200, json={"ok": True})
        if req.url.path == "/plan" and req.method == "GET":
            return httpx.Response(200, json={"plan": {"version": self.version}})
        if req.url.path == "/plan" and req.method == "POST":
            body = json.loads(req.content)
            self.posted.append(body)
            if self.reject:
                return httpx.Response(422, json={"error": "valid_until has already passed"})
            self.version = body["version"]
            return httpx.Response(200, json={"accepted": True, "version": self.version, "clamped": []})
        if req.url.path == "/pnl":
            return httpx.Response(200, json={"net": 0, "trades": 0})
        return httpx.Response(404, json={"error": "nope"})

    def api(self):
        return EngineApi("http://engine", "t", transport=httpx.MockTransport(self.handler))


def text(t):
    return NS(type="text", text=t, model_dump=lambda **_: {"type": "text", "text": t})


def use(i, name, inp):
    return NS(
        type="tool_use",
        id=f"tu{i}",
        name=name,
        input=inp,
        model_dump=lambda **_: {"type": "tool_use", "id": f"tu{i}", "name": name, "input": inp},
    )


def msg(stop, *blocks, tokens=1000):
    return NS(stop_reason=stop, content=list(blocks), usage=NS(input_tokens=tokens, output_tokens=0))


class Script:
    """Replays model turns in order and records every request."""

    def __init__(self, *turns):
        self.turns = list(turns)
        self.requests = []

    def __call__(self, **params):
        self.requests.append(copy.deepcopy(params))
        return self.turns.pop(0)


def session(tmp_path, mode="premarket", engine=None, dry_run=False):
    s = Settings(data_dir=tmp_path)
    schema = json.loads((s.schema_path).read_text())
    return tools.Session(s, mode, NOW.date(), engine.api() if engine else None, None, schema, dry_run=dry_run)


def budget(tmp_path, limit=1_000_000):
    return Budget.for_day(tmp_path, date(2026, 10, 28), limit)


def test_premarket_submits_a_plan_with_code_filled_version(tmp_path):
    eng = FakeEngine()
    eng.version = 4
    m = Script(
        msg("tool_use", text("Checking."), use(1, "submit_plan", plan())),
        msg("end_turn", text("Plan v5 submitted.")),
    )
    o = runner.run(session(tmp_path, engine=eng), budget(tmp_path), m, NOW)
    assert o.ok, o.reason
    assert eng.posted[0]["version"] == 5
    assert eng.posted[0]["valid_until"] == "2026-10-28T15:10:00+05:30"
    req = m.requests[0]
    assert req["model"] == "claude-opus-5-5"
    assert req["thinking"] == {"type": "adaptive"}
    assert req["output_config"] == {"effort": "high"}
    names = {t["name"] for t in req["tools"]}
    assert "web_search" in names and "submit_plan" in names
    assert not names & {"place_order", "cancel_order", "modify_order"}
    # The tool result went back to the model with the same id.
    assert m.requests[1]["messages"][-1]["content"][0]["tool_use_id"] == "tu1"


def test_invalid_plan_gets_one_retry_then_falls_back(tmp_path):
    eng = FakeEngine()
    bad = plan(global_risk_multiplier=7, market_bias="yolo")
    m = Script(
        msg("tool_use", use(1, "submit_plan", bad)),
        msg("tool_use", use(2, "submit_plan", bad)),
        msg("tool_use", use(3, "submit_plan", plan())),
        msg("end_turn", text("Gave up.")),
    )
    o = runner.run(session(tmp_path, engine=eng), budget(tmp_path), m, NOW)
    assert not o.ok
    assert "no plan was accepted" in o.reason
    assert eng.posted == []  # nothing invalid ever reached the engine
    r1 = m.requests[1]["messages"][-1]["content"][0]
    assert r1["is_error"] and "1 attempt(s) left" in r1["content"]
    r3 = m.requests[3]["messages"][-1]["content"][0]
    assert r3["is_error"] and "no attempts left" in r3["content"]


def test_retry_after_schema_error_succeeds(tmp_path):
    eng = FakeEngine()
    m = Script(
        msg("tool_use", use(1, "submit_plan", plan(regime="sideways"))),
        msg("tool_use", use(2, "submit_plan", plan())),
        msg("end_turn", text("ok")),
    )
    o = runner.run(session(tmp_path, engine=eng), budget(tmp_path), m, NOW)
    assert o.ok and len(eng.posted) == 1


def test_engine_rejection_is_reported_to_the_model(tmp_path):
    eng = FakeEngine(reject=True)
    m = Script(msg("tool_use", use(1, "submit_plan", plan())), msg("end_turn", text("rejected")))
    o = runner.run(session(tmp_path, engine=eng), budget(tmp_path), m, NOW)
    assert not o.ok
    assert "already passed" in m.requests[1]["messages"][-1]["content"][0]["content"]


def test_refusal_and_budget_end_the_run_safely(tmp_path):
    eng = FakeEngine()
    o = runner.run(session(tmp_path, engine=eng), budget(tmp_path), Script(msg("refusal")), NOW)
    assert not o.ok and "declined" in o.reason
    b = budget(tmp_path, limit=1500)
    b.add("premarket", 2000, "x")
    called = Script()
    o = runner.run(session(tmp_path, engine=eng), b, called, NOW)
    assert "budget" in o.reason and called.requests == []


def test_review_without_change_is_ok_and_uses_the_fast_model(tmp_path):
    eng = FakeEngine()
    m = Script(msg("tool_use", use(1, "get_pnl", {})), msg("end_turn", text("No change: on plan.")))
    o = runner.run(session(tmp_path, mode="review", engine=eng), budget(tmp_path), m, NOW)
    assert o.ok and o.text.startswith("No change")
    assert m.requests[0]["model"] == "claude-sonnet-5"
    assert "web_search" not in {t["name"] for t in m.requests[0]["tools"]}
    assert eng.posted == []


def test_tools_outside_the_routine_are_refused(tmp_path):
    s = session(tmp_path, mode="hourly", engine=FakeEngine())
    body, err = tools.call(s, "submit_plan", plan())
    assert err and "not available" in body


def test_dry_run_never_posts(tmp_path):
    eng = FakeEngine()
    m = Script(msg("tool_use", use(1, "submit_plan", plan())), msg("end_turn", text("ok")))
    o = runner.run(session(tmp_path, engine=eng, dry_run=True), budget(tmp_path), m, NOW)
    assert o.ok and eng.posted == [] and o.plans[0]["version"] == 1


@pytest.mark.parametrize("mode", ["premarket", "review", "hourly", "eod"])
def test_prompts_render(mode):
    p = runner.prompt(mode, NOW)
    assert "2026-10-28" in p and "{" not in p
