"""Settings: the `[strategist]` table of `config/engine.toml` plus secrets.

Secrets come from the environment first, then `config/secrets.env`. They are
never logged.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

DEFAULT_FEEDS = [
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.livemint.com/rss/markets",
    "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
]


@dataclass
class ModeSettings:
    model: str
    effort: str
    timeout_secs: int
    max_turns: int


@dataclass
class Settings:
    repo: Path = REPO
    data_dir: Path = REPO / "data"
    control_url: str = "http://127.0.0.1:7070"
    schema_path: Path = REPO / "config" / "plan.schema.json"
    universe_path: Path = REPO / "config" / "universe.toml"
    max_tokens_per_day: int = 1_500_000
    web_search_max_uses: int = 6
    server_fallbacks: bool = True
    news_feeds: list[str] = field(default_factory=lambda: list(DEFAULT_FEEDS))
    modes: dict[str, ModeSettings] = field(
        default_factory=lambda: {
            # AGENT.md §2: Opus for the deep routines, a faster model for reviews.
            "premarket": ModeSettings("claude-opus-5-5", "high", 20 * 60, 40),
            "review": ModeSettings("claude-sonnet-5", "low", 90, 8),
            "hourly": ModeSettings("claude-sonnet-5", "low", 90, 6),
            "eod": ModeSettings("claude-opus-5-5", "high", 10 * 60, 30),
        }
    )

    @property
    def journal_dir(self) -> Path:
        return self.data_dir / "journal"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "strategist"


def load(path: Path | None = None) -> Settings:
    """Reads `[strategist]` from engine.toml (missing file or table = defaults)."""
    path = path or Path(os.environ.get("SAARTHI_CONFIG", REPO / "config" / "engine.toml"))
    s = Settings()
    if not path.exists():
        return s
    doc = tomllib.loads(path.read_text())
    data_dir = doc.get("paths", {}).get("data_dir")
    if data_dir:
        s.data_dir = (REPO / data_dir) if not Path(data_dir).is_absolute() else Path(data_dir)
    t = doc.get("strategist", {})
    for key in ("control_url", "max_tokens_per_day", "web_search_max_uses", "server_fallbacks", "news_feeds"):
        if key in t:
            setattr(s, key, t[key])
    addr = doc.get("engine", {}).get("control_addr")
    if addr and "control_url" not in t:
        s.control_url = f"http://{addr}"
    for mode, m in t.get("modes", {}).items():
        if mode not in s.modes:
            raise ValueError(f"[strategist.modes] unknown mode {mode!r}")
        cur = s.modes[mode]
        s.modes[mode] = ModeSettings(
            model=m.get("model", cur.model),
            effort=m.get("effort", cur.effort),
            timeout_secs=int(m.get("timeout_secs", cur.timeout_secs)),
            max_turns=int(m.get("max_turns", cur.max_turns)),
        )
    return s


def secret(key: str, secrets_file: Path | None = None) -> str | None:
    v = os.environ.get(key, "").strip()
    if v:
        return v
    p = secrets_file or Path(os.environ.get("SAARTHI_SECRETS", REPO / "config" / "secrets.env"))
    if not p.exists():
        return None
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip().removeprefix("export ").strip() == key:
            v = v.strip().strip('"').strip("'")
            return v or None
    return None
