"""Rule-based threshold alerting.

Evaluation happens **in memory, per sample**, on the ingest path. The database
is touched only on a state transition -- an alert opening or closing -- which is
rare compared to the sample rate. Polling the metrics table on a timer instead
would mean re-reading rows we already had in hand and detecting problems a
window late.

Sustained breaches are tracked with a per-(rule, machine) timer rather than a
counter of samples, because agents report at different intervals: "CPU > 85%
for 30 seconds" must mean the same thing for a 1s agent and a 10s one.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from .models import Alert, AlertRule, AlertState

logger = logging.getLogger("sysfleet.hub.alerting")

# How often the cached rule set is refreshed from the database. Rules change at
# human speed (a POST to /alert-rules), so re-reading them per sample would be
# thousands of pointless queries a minute.
RULE_REFRESH_S = 10.0


@dataclass
class _BreachState:
    """Live tracking for one (rule, machine) pair."""

    # Monotonic clock: this measures a real elapsed duration, so it must not be
    # affected by NTP corrections or DST, which wall-clock time would be.
    since: float
    peak: float
    alert_id: int | None = None


@dataclass
class AlertEvent:
    """A transition worth telling the outside world about."""

    kind: str  # "triggered" | "resolved"
    alert_id: int
    machine_id: int
    machine_name: str
    metric: str
    value: float
    threshold: float
    message: str

    def as_frame(self) -> dict:
        return {
            "type": "alert",
            "event": self.kind,
            "alert_id": self.alert_id,
            "machine_id": self.machine_id,
            "machine_name": self.machine_name,
            "metric": self.metric,
            "value": self.value,
            "threshold": self.threshold,
            "message": self.message,
        }


def _breaches(value: float, operator: str, threshold: float) -> bool:
    return value > threshold if operator == ">" else value < threshold


class AlertEngine:
    """Evaluates cached rules against each incoming sample."""

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory
        self._rules: list[AlertRule] = []
        self._rules_loaded_at = 0.0
        self._state: dict[tuple[int, int], _BreachState] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ rules

    async def refresh_rules(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._rules_loaded_at < RULE_REFRESH_S:
            return

        async with self._session_factory() as session:
            result = await session.execute(
                select(AlertRule).where(AlertRule.enabled.is_(True))
            )
            # Detach plain snapshots: these objects outlive the session, and
            # touching a lazy attribute on an expired instance later would
            # raise. Only the scalar fields are ever read.
            self._rules = list(result.scalars().all())

        self._rules_loaded_at = now

    # ------------------------------------------------------------- evaluation

    async def evaluate(
        self, machine_id: int, machine_name: str, sample: dict
    ) -> list[AlertEvent]:
        """Check one sample against every applicable rule."""
        await self.refresh_rules()
        if not self._rules:
            return []

        events: list[AlertEvent] = []
        now_mono = time.monotonic()

        for rule in self._rules:
            # NULL machine_id means the rule is fleet-wide.
            if rule.machine_id is not None and rule.machine_id != machine_id:
                continue

            value = sample.get(rule.metric)
            if value is None:
                # The metric wasn't collectable on this platform. Skipping is
                # right: a missing disk counter is not a threshold breach.
                continue

            key = (rule.id, machine_id)
            state = self._state.get(key)

            if _breaches(value, rule.operator, rule.threshold):
                if state is None:
                    state = _BreachState(since=now_mono, peak=value)
                    self._state[key] = state
                else:
                    state.peak = (
                        max(state.peak, value)
                        if rule.operator == ">"
                        else min(state.peak, value)
                    )

                elapsed = now_mono - state.since
                if state.alert_id is None and elapsed >= rule.duration_seconds:
                    event = await self._open_alert(
                        rule, machine_id, machine_name, value, state
                    )
                    if event:
                        events.append(event)

            elif state is not None:
                # Recovered. Drop the timer so a later breach starts a fresh
                # duration window rather than inheriting the old one.
                del self._state[key]
                if state.alert_id is not None:
                    event = await self._close_alert(
                        rule, machine_id, machine_name, value, state
                    )
                    if event:
                        events.append(event)

        return events

    # -------------------------------------------------------------- mutations

    async def _open_alert(
        self,
        rule: AlertRule,
        machine_id: int,
        machine_name: str,
        value: float,
        state: _BreachState,
    ) -> AlertEvent | None:
        sustained = (
            f" for {rule.duration_seconds}s" if rule.duration_seconds else ""
        )
        message = (
            f"{machine_name}: {rule.metric} {value:g} "
            f"{rule.operator} {rule.threshold:g}{sustained}"
        )

        try:
            async with self._session_factory() as session:
                async with session.begin():
                    # Guard against duplicates: the Hub may have restarted while
                    # this breach was already open, in which case the in-memory
                    # state is gone but the database row is not.
                    existing = await session.execute(
                        select(Alert.id).where(
                            Alert.rule_id == rule.id,
                            Alert.machine_id == machine_id,
                            Alert.state == AlertState.ACTIVE.value,
                        )
                    )
                    found = existing.scalar_one_or_none()
                    if found is not None:
                        state.alert_id = found
                        return None

                    alert = Alert(
                        rule_id=rule.id,
                        machine_id=machine_id,
                        metric=rule.metric,
                        threshold=rule.threshold,
                        triggered_value=value,
                        peak_value=state.peak,
                        message=message,
                        state=AlertState.ACTIVE.value,
                        started_at=datetime.now(timezone.utc),
                    )
                    session.add(alert)
                    await session.flush()
                    state.alert_id = alert.id
                    alert_id = alert.id

            logger.warning("ALERT %s", message)
            return AlertEvent(
                kind="triggered",
                alert_id=alert_id,
                machine_id=machine_id,
                machine_name=machine_name,
                metric=rule.metric,
                value=value,
                threshold=rule.threshold,
                message=message,
            )
        except Exception:
            logger.exception("Failed to open alert for rule %s", rule.id)
            return None

    async def _close_alert(
        self,
        rule: AlertRule,
        machine_id: int,
        machine_name: str,
        value: float,
        state: _BreachState,
    ) -> AlertEvent | None:
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    alert = await session.get(Alert, state.alert_id)
                    if alert is None or alert.state != AlertState.ACTIVE.value:
                        return None
                    alert.state = AlertState.RESOLVED.value
                    alert.resolved_at = datetime.now(timezone.utc)
                    alert.peak_value = state.peak

            message = f"{machine_name}: {rule.metric} recovered ({value:g})"
            logger.info("RESOLVED %s", message)
            return AlertEvent(
                kind="resolved",
                alert_id=state.alert_id or 0,
                machine_id=machine_id,
                machine_name=machine_name,
                metric=rule.metric,
                value=value,
                threshold=rule.threshold,
                message=message,
            )
        except Exception:
            logger.exception("Failed to resolve alert %s", state.alert_id)
            return None

    def forget_machine(self, machine_id: int) -> None:
        """Drop in-memory timers for a disconnected machine.

        Its alerts stay active in the database -- a host that vanished at 99%
        CPU did not recover, and silently closing the alert would erase the
        evidence. Only the duration timers are discarded, so a reconnect starts
        a clean window.
        """
        for key in [k for k in self._state if k[1] == machine_id]:
            del self._state[key]
