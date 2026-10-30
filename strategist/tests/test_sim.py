"""`engine sim` hands the strategist its generated market through the environment."""

from datetime import datetime
from zoneinfo import ZoneInfo

from saarthi_strategist import cli, config

IST = ZoneInfo("Asia/Kolkata")


def test_sim_environment_overrides_paths_and_clock(monkeypatch, tmp_path):
    monkeypatch.setenv("SAARTHI_DATA_DIR", str(tmp_path / "sim"))
    monkeypatch.setenv("SAARTHI_UNIVERSE", str(tmp_path / "sim" / "sim_universe.toml"))
    monkeypatch.setenv("SAARTHI_NOW", "2026-10-28T10:45:00+05:30")
    monkeypatch.setenv("SAARTHI_SIMULATION", "1")
    s = config.load(tmp_path / "missing.toml")
    assert s.data_dir == tmp_path / "sim"
    assert s.universe_path == tmp_path / "sim" / "sim_universe.toml"
    assert config.now(IST) == datetime(2026, 10, 28, 10, 45, tzinfo=IST)
    assert "SIMULATION" in cli._state_note(object())


def test_without_sim_environment_nothing_changes(monkeypatch):
    for k in ("SAARTHI_DATA_DIR", "SAARTHI_UNIVERSE", "SAARTHI_NOW", "SAARTHI_SIMULATION"):
        monkeypatch.delenv(k, raising=False)
    s = config.load()
    assert s.data_dir == config.REPO / "data"
    assert abs((config.now(IST) - datetime.now(IST)).total_seconds()) < 5
    assert cli._state_note(object()) == ""
