"""Pydantic v2 schemas: the documented contract for every REST endpoint.

Inbound agent frames are validated here too. Agents are semi-trusted at best --
they run on machines the Hub operator may not control -- so nothing goes into the
database without passing through a model in this file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Only these columns may be targeted by an alert rule. Without a whitelist, a
# rule's `metric` string would flow into a getattr()/SQL column reference and
# become an injection surface.
ALERTABLE_METRICS = (
    "cpu_percent",
    "mem_percent",
    "disk_percent",
    "disk_read_mbs",
    "disk_write_mbs",
    "net_up_kbs",
    "net_down_kbs",
)

MetricName = Literal[
    "cpu_percent",
    "mem_percent",
    "disk_percent",
    "disk_read_mbs",
    "disk_write_mbs",
    "net_up_kbs",
    "net_down_kbs",
]

Percent = Annotated[float, Field(ge=0, le=100)]


# --------------------------------------------------------------- agent inbound


class AgentHello(BaseModel):
    """The handshake frame. Everything needed to register the machine."""

    model_config = ConfigDict(extra="ignore")

    type: Literal["hello"]
    agent_key: str = Field(min_length=1, max_length=255)
    agent_name: str = Field(min_length=1, max_length=255)
    hostname: str = Field(default="unknown", max_length=255)
    os: str | None = Field(default=None, max_length=255)
    architecture: str | None = Field(default=None, max_length=64)
    cpu_model: str | None = Field(default=None, max_length=255)
    cpu_cores: int | None = Field(default=None, ge=0, le=4096)
    total_ram_gb: float | None = Field(default=None, ge=0)
    agent_version: str | None = Field(default=None, max_length=32)
    interval_ms: int = Field(default=1000, ge=250, le=60_000)


class AgentMetrics(BaseModel):
    """One sample. Bounds here are the Hub's guard against a broken agent."""

    model_config = ConfigDict(extra="ignore")

    type: Literal["metrics"]
    ts: datetime
    cpu_percent: Percent | None = None
    mem_percent: Percent | None = None
    mem_used_gb: float | None = Field(default=None, ge=0)
    disk_percent: Percent | None = None
    disk_read_mbs: float | None = Field(default=None, ge=0)
    disk_write_mbs: float | None = Field(default=None, ge=0)
    net_up_kbs: float | None = Field(default=None, ge=0)
    net_down_kbs: float | None = Field(default=None, ge=0)
    uptime_seconds: int | None = Field(default=None, ge=0)

    @field_validator("ts", mode="after")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        """Normalise to aware UTC.

        Agents send epoch seconds, which pydantic decodes as aware UTC already.
        A naive datetime would mean a hand-rolled client; assume UTC rather than
        the Hub's local timezone, so a fleet spanning zones stays comparable.
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


# ------------------------------------------------------------------- machines


class MachineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    agent_key: str
    name: str
    hostname: str
    os: str | None
    architecture: str | None
    cpu_model: str | None
    cpu_cores: int | None
    total_ram_gb: float | None
    agent_version: str | None
    interval_ms: int
    status: Literal["online", "offline"]
    first_seen: datetime
    last_seen: datetime


class MachineListOut(BaseModel):
    total: int
    online: int
    offline: int
    machines: list[MachineOut]


# -------------------------------------------------------------------- metrics


class MetricOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ts: datetime
    cpu_percent: float | None
    mem_percent: float | None
    mem_used_gb: float | None
    disk_percent: float | None
    disk_read_mbs: float | None
    disk_write_mbs: float | None
    net_up_kbs: float | None
    net_down_kbs: float | None


class MetricPageOut(BaseModel):
    """A page of history.

    ``returned`` vs ``limit`` tells a client whether to keep paging without
    forcing a second COUNT(*) over what may be millions of rows.
    """

    machine_id: int
    from_: datetime | None = Field(default=None, alias="from")
    to: datetime | None = None
    returned: int
    limit: int
    offset: int
    metrics: list[MetricOut]

    model_config = ConfigDict(populate_by_name=True)


# --------------------------------------------------------------- alert rules


class AlertRuleBase(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    metric: MetricName
    operator: Literal[">", "<"]
    threshold: float = Field(ge=0)
    # 0 fires on the first breaching sample; higher values require the breach to
    # persist, which is what stops a momentary spike from paging anyone.
    duration_seconds: int = Field(default=0, ge=0, le=86_400)
    enabled: bool = True
    # NULL = applies to the whole fleet.
    machine_id: int | None = None


class AlertRuleCreate(AlertRuleBase):
    pass


class AlertRuleUpdate(BaseModel):
    """PATCH body. Every field optional; unset fields are left untouched."""

    name: str | None = Field(default=None, min_length=1, max_length=255)
    metric: MetricName | None = None
    operator: Literal[">", "<"] | None = None
    threshold: float | None = Field(default=None, ge=0)
    duration_seconds: int | None = Field(default=None, ge=0, le=86_400)
    enabled: bool | None = None


class AlertRuleOut(AlertRuleBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime


# --------------------------------------------------------------------- alerts


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    rule_id: int
    machine_id: int
    metric: str
    threshold: float
    triggered_value: float
    peak_value: float
    message: str
    state: Literal["active", "resolved"]
    started_at: datetime
    resolved_at: datetime | None


class AlertListOut(BaseModel):
    total: int
    alerts: list[AlertOut]


# ---------------------------------------------------------------------- misc


class HealthOut(BaseModel):
    status: Literal["ok"]
    version: str
    connected_agents: int
    dashboard_clients: int
    ingest: dict[str, Any]
