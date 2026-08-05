"""Machine registration and liveness tracking.

``status`` on the machines table is a *cache*, not the source of truth --
``last_seen`` is. A hub that is killed mid-flight would otherwise leave every
machine frozen as "online" forever, since nothing would ever run the code path
that flips it back. The reaper below reconciles the cached column against
last_seen on a timer, so status is eventually correct no matter how the Hub
died.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .models import Machine, MachineStatus
from .schemas import AgentHello

logger = logging.getLogger("sysfleet.hub.registry")


async def register_machine(session: AsyncSession, hello: AgentHello) -> Machine:
    """Find-or-create the machine for this handshake, and mark it online.

    Static facts are refreshed on every connect: an agent upgrade, a RAM
    upgrade, or an OS update should be reflected without operator action.
    """
    now = datetime.now(timezone.utc)

    result = await session.execute(
        select(Machine).where(Machine.agent_key == hello.agent_key)
    )
    machine = result.scalar_one_or_none()

    if machine is None:
        machine = Machine(
            agent_key=hello.agent_key,
            name=hello.agent_name,
            hostname=hello.hostname,
            first_seen=now,
        )
        session.add(machine)
        logger.info("Registered new machine: %s (%s)", hello.agent_name, hello.agent_key)
    else:
        logger.info("Machine reconnected: %s (id=%s)", machine.name, machine.id)

    machine.name = hello.agent_name
    machine.hostname = hello.hostname
    machine.os = hello.os
    machine.architecture = hello.architecture
    machine.cpu_model = hello.cpu_model
    machine.cpu_cores = hello.cpu_cores
    machine.total_ram_gb = hello.total_ram_gb
    machine.agent_version = hello.agent_version
    machine.interval_ms = hello.interval_ms
    machine.last_seen = now
    machine.status = MachineStatus.ONLINE.value

    await session.commit()
    await session.refresh(machine)
    return machine


async def touch_machine(
    session_factory: async_sessionmaker, machine_id: int, ts: datetime
) -> None:
    """Update last_seen for a machine.

    Called on a timer rather than per-sample: at 1 sample/s/agent, writing this
    on every frame would add a whole second's worth of UPDATE transactions per
    agent, which is exactly the per-row write pattern the batch writer exists
    to avoid.
    """
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                update(Machine)
                .where(Machine.id == machine_id)
                .values(last_seen=ts, status=MachineStatus.ONLINE.value)
            )


async def mark_offline(session_factory: async_sessionmaker, machine_id: int) -> None:
    """Flip a machine offline on clean disconnect, without waiting for the reaper."""
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                update(Machine)
                .where(Machine.id == machine_id)
                .values(status=MachineStatus.OFFLINE.value)
            )


async def reap_stale_machines(
    session_factory: async_sessionmaker,
    settings: Settings,
    connected_ids: set[int] | None = None,
) -> int:
    """Mark machines offline once they miss too many expected samples.

    Two things keep this from firing on healthy agents:

    * The grace period scales with each machine's own reporting interval, so a
      30s-interval agent isn't declared dead for being slower than a 1s one.
    * It adds ``touch_interval_s``, because last_seen is only refreshed on that
      cadence. Comparing against the raw sample interval alone would flag every
      streaming agent as offline in the window between touches.

    ``connected_ids`` is the authoritative live set: a machine holding an open
    socket right now is online by definition, whatever last_seen says.
    """
    now = datetime.now(timezone.utc)
    misses = settings.offline_after_missed_intervals
    connected_ids = connected_ids or set()

    async with session_factory() as session:
        async with session.begin():
            result = await session.execute(
                select(Machine.id, Machine.interval_ms, Machine.last_seen).where(
                    Machine.status == MachineStatus.ONLINE.value
                )
            )
            stale: list[int] = []
            for machine_id, interval_ms, last_seen in result.all():
                if machine_id in connected_ids:
                    continue
                # SQLite hands back naive datetimes; treat them as the UTC they
                # were written as, or the comparison below silently misbehaves.
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                grace = timedelta(
                    milliseconds=(interval_ms or 1000) * misses
                ) + timedelta(seconds=settings.touch_interval_s)
                if now - last_seen > grace:
                    stale.append(machine_id)

            if stale:
                await session.execute(
                    update(Machine)
                    .where(Machine.id.in_(stale))
                    .values(status=MachineStatus.OFFLINE.value)
                )
                logger.info("Marked %d machine(s) offline", len(stale))

    return len(stale)
