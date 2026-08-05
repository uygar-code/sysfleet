"""SysFleet Hub: FastAPI application.

Two WebSocket endpoints with opposite directions of flow:

    WS /ws/agent      agents push metrics *in*
    WS /ws/dashboard  the Hub pushes live metrics *out*

and a REST surface for everything historical. The split matters: live data is
served from memory via the broadcaster, historical data from SQL. Neither path
blocks the other.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from . import __version__
from .alerting import AlertEngine
from .broadcast import DashboardBroadcaster
from .config import get_settings
from .db import SessionLocal, dispose_engine
from .ingest import MetricWriter
from .registry import (
    mark_offline,
    reap_stale_machines,
    register_machine,
    touch_machine,
)
from .routers import alerts as alerts_router
from .routers import machines as machines_router
from .schemas import AgentHello, AgentMetrics, HealthOut

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sysfleet.hub")

settings = get_settings()

writer = MetricWriter(SessionLocal, settings)
broadcaster = DashboardBroadcaster()
alert_engine = AlertEngine(SessionLocal)

# Agents currently holding an open socket, by machine id. Used for /health and
# to reject a second connection from the same agent key.
connected_agents: dict[int, str] = {}


async def _reaper_loop() -> None:
    """Reconcile cached machine.status against last_seen, forever."""
    while True:
        try:
            await asyncio.sleep(settings.reaper_interval_s)
            # Pass the live socket set: an agent connected right now is online
            # regardless of how stale its throttled last_seen looks.
            await reap_stale_machines(
                SessionLocal, settings, set(connected_agents.keys())
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed sweep must not kill the loop; the next one will retry.
            logger.exception("Reaper sweep failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    writer.start()
    reaper = asyncio.create_task(_reaper_loop(), name="offline-reaper")
    logger.info("SysFleet Hub %s ready", __version__)
    logger.warning(
        "Agent connections are unauthenticated - bind to a trusted network only."
    )

    yield

    reaper.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reaper
    # Drain the buffer before the process exits, or the last few seconds of
    # every agent's data dies with it.
    await writer.stop()
    await dispose_engine()
    logger.info("Hub shut down cleanly")


app = FastAPI(
    title=settings.api_title,
    version=__version__,
    description=(
        "Centralised monitoring hub. Agents stream metrics over WebSocket; "
        "clients read history and alerts over REST."
    ),
    lifespan=lifespan,
)

app.include_router(machines_router.router)
app.include_router(alerts_router.router)


@app.get("/health", response_model=HealthOut, tags=["system"])
async def health() -> HealthOut:
    return HealthOut(
        status="ok",
        version=__version__,
        connected_agents=len(connected_agents),
        dashboard_clients=broadcaster.client_count,
        ingest=writer.stats_dict(),
    )


# --------------------------------------------------------------- agent ingest


@app.websocket("/ws/agent")
async def agent_socket(websocket: WebSocket) -> None:
    """Receive one agent's metric stream."""
    await websocket.accept()

    machine_id: int | None = None
    machine_name = "<unregistered>"
    last_touch = 0.0

    try:
        while True:
            raw = await websocket.receive_json()

            if not isinstance(raw, dict):
                continue

            kind = raw.get("type")

            # ---------------------------------------------------- handshake
            if kind == "hello":
                try:
                    hello = AgentHello.model_validate(raw)
                except ValidationError as exc:
                    logger.warning("Rejecting malformed hello: %s", exc.errors())
                    await websocket.close(code=1008, reason="invalid hello")
                    return

                async with SessionLocal() as session:
                    machine = await register_machine(session, hello)

                machine_id = machine.id
                machine_name = machine.name
                connected_agents[machine_id] = machine_name
                last_touch = time.monotonic()

                await websocket.send_json(
                    {
                        "type": "welcome",
                        "machine_id": machine.id,
                        "hub_version": __version__,
                    }
                )
                continue

            # ------------------------------------------------------ metrics
            if kind == "metrics":
                if machine_id is None:
                    # Nothing to attribute the sample to. Close rather than
                    # silently discard, so a broken client is obvious.
                    await websocket.close(code=1008, reason="hello required first")
                    return

                try:
                    sample = AgentMetrics.model_validate(raw)
                except ValidationError as exc:
                    # One bad frame shouldn't kill an otherwise healthy stream.
                    logger.warning(
                        "Discarding invalid sample from %s: %s",
                        machine_name,
                        exc.errors()[:2],
                    )
                    continue

                payload = sample.model_dump(exclude={"type"})

                # Buffer for durable storage...
                writer.submit(machine_id, sample.ts, payload)

                # ...and publish live, independent of the flush cycle, so a
                # dashboard sees the sample now rather than up to 5s later.
                broadcaster.publish(
                    {
                        "type": "metrics",
                        "machine_id": machine_id,
                        "machine_name": machine_name,
                        **{
                            k: (v.isoformat() if hasattr(v, "isoformat") else v)
                            for k, v in payload.items()
                        },
                    }
                )

                # Evaluate thresholds against the sample we already hold, so a
                # breach is detected now rather than on some later poll of the
                # metrics table.
                if settings.alert_eval_enabled:
                    try:
                        for event in await alert_engine.evaluate(
                            machine_id, machine_name, payload
                        ):
                            broadcaster.publish(event.as_frame())
                    except Exception:
                        # Alerting is a secondary concern; never let it break
                        # the ingest of the metrics themselves.
                        logger.exception("Alert evaluation failed for %s", machine_name)

                now = time.monotonic()
                if now - last_touch >= settings.touch_interval_s:
                    last_touch = now
                    await touch_machine(SessionLocal, machine_id, sample.ts)
                continue

            logger.debug("Ignoring unknown agent frame: %r", kind)

    except WebSocketDisconnect:
        logger.info("Agent disconnected: %s", machine_name)
    except Exception:
        logger.exception("Agent socket error (%s)", machine_name)
    finally:
        if machine_id is not None:
            connected_agents.pop(machine_id, None)
            # Clear the breach timers but leave any open alerts alone: a host
            # that disappeared while breaching did not recover.
            alert_engine.forget_machine(machine_id)
            with contextlib.suppress(Exception):
                await mark_offline(SessionLocal, machine_id)


# ----------------------------------------------------------- dashboard stream


@app.websocket("/ws/dashboard")
async def dashboard_socket(websocket: WebSocket) -> None:
    """Live fan-out for a future frontend.

    No UI ships with this project, but the seam exists so a dashboard can be
    added without touching the ingest path.
    """
    await websocket.accept()
    queue = await broadcaster.subscribe(websocket)

    await websocket.send_json(
        {
            "type": "hello",
            "hub_version": __version__,
            "machines": [
                {"machine_id": mid, "name": name}
                for mid, name in connected_agents.items()
            ],
        }
    )

    sender = asyncio.create_task(broadcaster.sender_loop(websocket, queue))
    try:
        # Nothing meaningful is expected from a dashboard, but the receive call
        # is what surfaces the disconnect.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.debug("Dashboard socket closed unexpectedly", exc_info=True)
    finally:
        sender.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sender
        await broadcaster.unsubscribe(websocket)


def run() -> None:
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    run()
