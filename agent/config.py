"""Agent configuration, read from environment variables.

The agent is deliberately dependency-light: no pydantic, no settings framework.
It runs on machines we may not control, so every extra import is a liability.
Everything here is plain stdlib.
"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass

# Matches the Hub's own clamp. Sending faster than this buys nothing (psutil's
# own rate calculations get noisy below ~250ms) and just burns CPU on both ends.
MIN_INTERVAL_MS = 250
MAX_INTERVAL_MS = 60_000

DEFAULT_HUB_URL = "ws://127.0.0.1:8000/ws/agent"


def _env_int(name: str, default: int) -> int:
    """Read an int from the environment, falling back on anything unparseable."""
    try:
        return int(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default


def _default_agent_key() -> str:
    """Derive a stable identity for this machine.

    The Hub keys machines on ``agent_key``, not hostname: in the Docker demo two
    containers can easily share a hostname, and a renamed host should still map
    to the same row. ``uuid.getnode()`` returns the MAC address (or a random but
    process-stable fallback), which combined with the hostname is stable across
    agent restarts without needing to persist any state to disk.
    """
    return f"{socket.gethostname()}-{uuid.getnode():012x}"


@dataclass(frozen=True)
class AgentConfig:
    hub_url: str
    agent_key: str
    agent_name: str
    interval_ms: int
    # Reconnect backoff bounds. The agent is expected to outlive the Hub (the
    # Hub may restart for a deploy), so giving up is never the right move.
    backoff_initial_s: float
    backoff_max_s: float

    @property
    def interval_s(self) -> float:
        return self.interval_ms / 1000

    @classmethod
    def from_env(cls) -> "AgentConfig":
        interval = _env_int("INTERVAL_MS", 1000)
        interval = max(MIN_INTERVAL_MS, min(MAX_INTERVAL_MS, interval))

        hostname = socket.gethostname()
        return cls(
            hub_url=os.environ.get("HUB_URL", DEFAULT_HUB_URL),
            agent_key=os.environ.get("AGENT_KEY") or _default_agent_key(),
            # AGENT_NAME is the human-facing label; hostname is a sane default
            # but the Docker demo overrides it so the fleet list is readable.
            agent_name=os.environ.get("AGENT_NAME") or hostname,
            interval_ms=interval,
            backoff_initial_s=1.0,
            backoff_max_s=30.0,
        )
