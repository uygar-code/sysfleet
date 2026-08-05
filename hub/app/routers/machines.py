"""Machine listing and historical metric reads."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..models import Machine, MachineStatus, Metric
from ..schemas import MachineListOut, MachineOut, MetricOut, MetricPageOut

router = APIRouter(tags=["machines"])

# Hard ceiling on a single page. At one sample/second a day of history is ~86k
# rows per machine; without a cap, one unbounded request would serialise all of
# it into memory and stall the event loop for every other client.
MAX_LIMIT = 5_000
DEFAULT_LIMIT = 1_000

# Used when the caller supplies no time range at all.
DEFAULT_WINDOW = timedelta(hours=1)


def _aware(value: datetime) -> datetime:
    """SQLite returns naive datetimes; re-attach the UTC they were stored in."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


@router.get(
    "/machines",
    response_model=MachineListOut,
    summary="List all registered machines",
)
async def list_machines(
    session: Annotated[AsyncSession, Depends(get_session)],
    status: Annotated[
        str | None,
        Query(description="Filter by status: `online` or `offline`."),
    ] = None,
) -> MachineListOut:
    """Every machine that has ever connected, with its current status."""
    stmt = select(Machine).order_by(Machine.name)

    if status is not None:
        if status not in (MachineStatus.ONLINE.value, MachineStatus.OFFLINE.value):
            raise HTTPException(
                status_code=422, detail="status must be 'online' or 'offline'"
            )
        stmt = stmt.where(Machine.status == status)

    machines = (await session.execute(stmt)).scalars().all()

    online = sum(1 for m in machines if m.status == MachineStatus.ONLINE.value)
    return MachineListOut(
        total=len(machines),
        online=online,
        offline=len(machines) - online,
        machines=[MachineOut.model_validate(m) for m in machines],
    )


@router.get(
    "/machines/{machine_id}",
    response_model=MachineOut,
    summary="Get one machine",
)
async def get_machine(
    machine_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> MachineOut:
    machine = await session.get(Machine, machine_id)
    if machine is None:
        raise HTTPException(status_code=404, detail=f"machine {machine_id} not found")
    return MachineOut.model_validate(machine)


@router.get(
    "/machines/{machine_id}/metrics",
    response_model=MetricPageOut,
    response_model_by_alias=True,
    summary="Historical metrics for a machine",
)
async def get_machine_metrics(
    machine_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
    # `from` is a Python keyword, so the parameter is named from_ and aliased
    # back to the documented query-string name.
    from_: Annotated[
        datetime | None,
        Query(alias="from", description="Start of range (ISO 8601). Inclusive."),
    ] = None,
    to: Annotated[
        datetime | None,
        Query(description="End of range (ISO 8601). Inclusive."),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
    order: Annotated[str, Query(pattern="^(asc|desc)$")] = "asc",
) -> MetricPageOut:
    """Read a time range of samples.

    Defaults to the last hour rather than "everything": an unqualified request
    against a machine with weeks of history should not be the expensive case.
    """
    machine = await session.get(Machine, machine_id)
    if machine is None:
        raise HTTPException(status_code=404, detail=f"machine {machine_id} not found")

    now = datetime.now(timezone.utc)
    end = _aware(to) if to else now
    start = _aware(from_) if from_ else end - DEFAULT_WINDOW

    if start > end:
        raise HTTPException(status_code=422, detail="'from' must be earlier than 'to'")

    stmt = (
        select(Metric)
        .where(
            Metric.machine_id == machine_id,
            Metric.ts >= start,
            Metric.ts <= end,
        )
        # Matches the ix_metrics_machine_ts index, so this is an index range
        # scan rather than a scan-and-sort.
        .order_by(Metric.ts.desc() if order == "desc" else Metric.ts.asc())
        .limit(limit)
        .offset(offset)
    )

    rows = (await session.execute(stmt)).scalars().all()

    return MetricPageOut(
        machine_id=machine_id,
        **{"from": start},
        to=end,
        returned=len(rows),
        limit=limit,
        offset=offset,
        metrics=[MetricOut.model_validate(r) for r in rows],
    )
