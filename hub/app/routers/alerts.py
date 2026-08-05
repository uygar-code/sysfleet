"""Alert listing and alert-rule management."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..models import Alert, AlertRule, AlertState, Machine
from ..schemas import (
    AlertListOut,
    AlertOut,
    AlertRuleCreate,
    AlertRuleOut,
    AlertRuleUpdate,
)

router = APIRouter(tags=["alerts"])


@router.get("/alerts", response_model=AlertListOut, summary="List alerts")
async def list_alerts(
    session: Annotated[AsyncSession, Depends(get_session)],
    state: Annotated[
        str,
        Query(
            pattern="^(active|resolved|all)$",
            description="Defaults to `active` -- what is wrong *right now*.",
        ),
    ] = "active",
    machine_id: Annotated[int | None, Query(ge=1)] = None,
    since: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> AlertListOut:
    """Active alerts by default.

    An alert list that defaulted to "everything ever" would bury the handful of
    currently-firing problems under weeks of resolved history.
    """
    stmt = select(Alert)

    if state != "all":
        stmt = stmt.where(Alert.state == state)
    if machine_id is not None:
        stmt = stmt.where(Alert.machine_id == machine_id)
    if since is not None:
        stmt = stmt.where(Alert.started_at >= since)

    # Newest first: the most recent breach is the one being investigated.
    stmt = stmt.order_by(Alert.started_at.desc()).limit(limit)

    rows = (await session.execute(stmt)).scalars().all()
    return AlertListOut(
        total=len(rows), alerts=[AlertOut.model_validate(r) for r in rows]
    )


# ------------------------------------------------------------------ rules CRUD


@router.get("/alert-rules", response_model=list[AlertRuleOut], summary="List rules")
async def list_rules(
    session: Annotated[AsyncSession, Depends(get_session)],
    machine_id: Annotated[int | None, Query(ge=1)] = None,
) -> list[AlertRuleOut]:
    stmt = select(AlertRule).order_by(AlertRule.id)
    if machine_id is not None:
        # Global rules (machine_id IS NULL) apply to this machine too, so they
        # belong in a per-machine view of "what is being watched here".
        stmt = stmt.where(
            (AlertRule.machine_id == machine_id) | (AlertRule.machine_id.is_(None))
        )
    rows = (await session.execute(stmt)).scalars().all()
    return [AlertRuleOut.model_validate(r) for r in rows]


@router.post(
    "/alert-rules",
    response_model=AlertRuleOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a rule",
)
async def create_rule(
    payload: AlertRuleCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertRuleOut:
    """Create a threshold rule.

    ``machine_id: null`` means the rule applies fleet-wide, including machines
    that register in the future.
    """
    if payload.machine_id is not None:
        machine = await session.get(Machine, payload.machine_id)
        if machine is None:
            raise HTTPException(
                status_code=404, detail=f"machine {payload.machine_id} not found"
            )

    # Explicit duplicate check. The uq_alert_rule_shape constraint cannot cover
    # fleet-wide rules: their machine_id is NULL, and SQL considers every NULL
    # distinct from every other, so the database happily accepts an unlimited
    # number of identical global rules. Each duplicate would then fire its own
    # alert for the same breach.
    target = (
        AlertRule.machine_id.is_(None)
        if payload.machine_id is None
        else AlertRule.machine_id == payload.machine_id
    )
    duplicate = await session.execute(
        select(AlertRule.id).where(
            target,
            AlertRule.metric == payload.metric,
            AlertRule.operator == payload.operator,
            AlertRule.threshold == payload.threshold,
        )
    )
    if duplicate.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=409,
            detail="an identical rule already exists for this target",
        )

    rule = AlertRule(**payload.model_dump())
    session.add(rule)

    try:
        await session.commit()
    except IntegrityError:
        # Backstop for machine-specific rules, and for the race where two
        # concurrent requests both pass the check above.
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="an identical rule already exists for this target",
        ) from None

    await session.refresh(rule)
    return AlertRuleOut.model_validate(rule)


@router.patch(
    "/alert-rules/{rule_id}", response_model=AlertRuleOut, summary="Update a rule"
)
async def update_rule(
    rule_id: Annotated[int, Path(ge=1)],
    payload: AlertRuleUpdate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertRuleOut:
    rule = await session.get(AlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"rule {rule_id} not found")

    # exclude_unset distinguishes "field omitted" from "field set to null",
    # which is the whole point of PATCH over PUT.
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(rule, field, value)

    await session.commit()
    await session.refresh(rule)
    return AlertRuleOut.model_validate(rule)


@router.delete(
    "/alert-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a rule",
)
async def delete_rule(
    rule_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    rule = await session.get(AlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"rule {rule_id} not found")

    # Alerts cascade with the rule: an alert whose rule is gone has no
    # threshold to explain it and would render as an orphan in the UI.
    await session.delete(rule)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/alerts/{alert_id}/resolve",
    response_model=AlertOut,
    summary="Manually resolve an alert",
)
async def resolve_alert(
    alert_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertOut:
    """Acknowledge an alert by hand.

    Useful when a machine went offline mid-breach, so no further samples will
    ever arrive to clear the alert automatically.
    """
    alert = await session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail=f"alert {alert_id} not found")

    # Idempotent: resolving an already-resolved alert must not overwrite the
    # original resolved_at, or the incident's true duration is lost.
    if alert.state == AlertState.ACTIVE.value:
        alert.state = AlertState.RESOLVED.value
        alert.resolved_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(alert)

    return AlertOut.model_validate(alert)
