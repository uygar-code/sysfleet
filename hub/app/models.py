"""SQLAlchemy 2.0 ORM models.

Timestamps are stored as timezone-aware UTC. Agents send epoch seconds and the
Hub converts on ingest, so a fleet spanning timezones stays comparable.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MachineStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class AlertState(str, enum.Enum):
    ACTIVE = "active"
    RESOLVED = "resolved"


class Machine(Base):
    """One monitored host.

    Identity is ``agent_key``, not hostname: container hostnames collide and
    real hosts get renamed, but the agent key is stable across both.
    """

    __tablename__ = "machines"

    id: Mapped[int] = mapped_column(primary_key=True)
    agent_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    hostname: Mapped[str] = mapped_column(String(255))

    os: Mapped[str | None] = mapped_column(String(255), default=None)
    architecture: Mapped[str | None] = mapped_column(String(64), default=None)
    cpu_model: Mapped[str | None] = mapped_column(String(255), default=None)
    cpu_cores: Mapped[int | None] = mapped_column(Integer, default=None)
    total_ram_gb: Mapped[float | None] = mapped_column(Float, default=None)
    agent_version: Mapped[str | None] = mapped_column(String(32), default=None)

    # Cadence this agent reports at. Stored so the offline reaper can scale its
    # threshold per machine instead of assuming everyone ticks at 1s.
    interval_ms: Mapped[int] = mapped_column(Integer, default=1000)

    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )
    # Denormalised from last_seen so list queries don't recompute it per row.
    # last_seen remains the source of truth; the reaper keeps this in sync.
    status: Mapped[str] = mapped_column(String(16), default=MachineStatus.OFFLINE.value)

    metrics: Mapped[list["Metric"]] = relationship(
        back_populates="machine", cascade="all, delete-orphan", passive_deletes=True
    )
    alerts: Mapped[list["Alert"]] = relationship(
        back_populates="machine", cascade="all, delete-orphan", passive_deletes=True
    )


class Metric(Base):
    """One sample from one machine."""

    __tablename__ = "metrics"
    __table_args__ = (
        # Every historical read is "this machine, this time range, in order".
        # A composite index in exactly that shape lets SQLite satisfy the range
        # scan and the ORDER BY from the index alone.
        Index("ix_metrics_machine_ts", "machine_id", "ts"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    machine_id: Mapped[int] = mapped_column(
        ForeignKey("machines.id", ondelete="CASCADE"), index=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    cpu_percent: Mapped[float | None] = mapped_column(Float, default=None)
    mem_percent: Mapped[float | None] = mapped_column(Float, default=None)
    mem_used_gb: Mapped[float | None] = mapped_column(Float, default=None)
    disk_percent: Mapped[float | None] = mapped_column(Float, default=None)
    disk_read_mbs: Mapped[float | None] = mapped_column(Float, default=None)
    disk_write_mbs: Mapped[float | None] = mapped_column(Float, default=None)
    net_up_kbs: Mapped[float | None] = mapped_column(Float, default=None)
    net_down_kbs: Mapped[float | None] = mapped_column(Float, default=None)
    uptime_seconds: Mapped[int | None] = mapped_column(Integer, default=None)

    machine: Mapped["Machine"] = relationship(back_populates="metrics")


class AlertRule(Base):
    """A user-defined threshold.

    ``machine_id`` is nullable on purpose: NULL means the rule applies to every
    machine in the fleet, so "CPU > 85% anywhere" is one row rather than one
    row per host that must be maintained as the fleet changes.
    """

    __tablename__ = "alert_rules"
    __table_args__ = (
        # NOTE: this only constrains *machine-specific* rules. SQL treats NULL
        # as distinct from every other NULL, so two fleet-wide rules (both with
        # machine_id IS NULL) do not violate it. The duplicate check for global
        # rules therefore lives in the create endpoint, which can express
        # "machine_id IS NULL" explicitly.
        UniqueConstraint(
            "machine_id", "metric", "operator", "threshold", name="uq_alert_rule_shape"
        ),
    )


    id: Mapped[int] = mapped_column(primary_key=True)
    machine_id: Mapped[int | None] = mapped_column(
        ForeignKey("machines.id", ondelete="CASCADE"), default=None, index=True
    )

    name: Mapped[str] = mapped_column(String(255))
    metric: Mapped[str] = mapped_column(String(64))  # e.g. "cpu_percent"
    operator: Mapped[str] = mapped_column(String(2))  # ">" or "<"
    threshold: Mapped[float] = mapped_column(Float)

    # Sustained-breach window. A rule with duration 0 fires on the first
    # breaching sample; anything higher requires the breach to persist, which
    # is what keeps a one-second CPU spike from paging anyone.
    duration_seconds: Mapped[int] = mapped_column(Integer, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    alerts: Mapped[list["Alert"]] = relationship(
        back_populates="rule", cascade="all, delete-orphan", passive_deletes=True
    )


class Alert(Base):
    """A firing (or since-resolved) breach of a rule."""

    __tablename__ = "alerts"
    __table_args__ = (
        # /alerts defaults to active-only, newest first.
        Index("ix_alerts_state_started", "state", "started_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("alert_rules.id", ondelete="CASCADE"), index=True
    )
    machine_id: Mapped[int] = mapped_column(
        ForeignKey("machines.id", ondelete="CASCADE"), index=True
    )

    metric: Mapped[str] = mapped_column(String(64))
    threshold: Mapped[float] = mapped_column(Float)
    triggered_value: Mapped[float] = mapped_column(Float)
    # Worst value seen while this alert was active -- the useful number when
    # reading back an incident after the fact.
    peak_value: Mapped[float] = mapped_column(Float)
    message: Mapped[str] = mapped_column(String(512), default="")

    state: Mapped[str] = mapped_column(String(16), default=AlertState.ACTIVE.value)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )

    rule: Mapped["AlertRule"] = relationship(back_populates="alerts")
    machine: Mapped["Machine"] = relationship(back_populates="alerts")
