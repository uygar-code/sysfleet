"""Hub settings, loaded from the environment via pydantic-settings."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# hub/app/config.py -> hub/
HUB_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- storage ---------------------------------------------------------
    # aiosqlite driver. Swapping this for postgresql+asyncpg://... is the only
    # change the storage layer needs to move to Postgres; nothing below imports
    # a SQLite-specific type, and the PRAGMA tuning in db.py is guarded by a
    # dialect check.
    database_url: str = f"sqlite+aiosqlite:///{HUB_DIR / 'sysfleet.db'}"

    # --- ingest batching -------------------------------------------------
    # The Hub buffers incoming samples in memory and flushes them in one
    # transaction. At 1 sample/s/agent, per-row commits would mean thousands of
    # fsyncs an hour for data nobody reads in real time (the live view is
    # served straight off the WebSocket, not the DB).
    flush_interval_s: float = 5.0
    flush_max_rows: int = 500
    # Backpressure bound. If the writer stalls, we drop the oldest samples
    # rather than let the queue consume all memory: a monitoring tool must
    # never be the thing that takes down the host it runs on.
    ingest_queue_max: int = 10_000

    # --- liveness --------------------------------------------------------
    # A machine is "offline" once it misses this many expected samples. Three
    # gives one lost packet and one slow tick of headroom before we alarm.
    offline_after_missed_intervals: int = 3
    reaper_interval_s: float = 5.0
    # How often last_seen is refreshed for a streaming agent. Writing it on
    # every sample would reintroduce the per-row transaction cost the batch
    # writer exists to remove, so it is throttled to this cadence.
    #
    # The reaper MUST account for this: last_seen legitimately lags real
    # liveness by up to touch_interval_s, so the offline grace period is
    # touch_interval_s + (interval_ms * misses). Without that term a healthy
    # 1s agent -- grace 3s, touched every 10s -- would be declared offline
    # every single sweep while happily streaming.
    touch_interval_s: float = 10.0


    # --- alerting (phase 4) ----------------------------------------------
    alert_eval_enabled: bool = True

    api_title: str = "SysFleet Hub"


@lru_cache
def get_settings() -> Settings:
    return Settings()
