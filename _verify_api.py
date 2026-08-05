"""THROWAWAY Phase 3+4 verification. Deleted before the project is finished.

Exercises the REST surface and the alert engine against a live Hub, using a
synthetic agent whose CPU values are scripted so the breach/recovery sequence
is deterministic instead of depending on what this machine happens to be doing.

Proves:
  * GET /machines, /machines/{id}, /machines/{id}/metrics?from=&to=
  * the `from`/`to` window actually filters (not just accepted and ignored)
  * 404s and 422s where they belong
  * alert-rule CRUD, including the 409 on a duplicate rule
  * duration_seconds suppresses a short spike but a sustained breach fires
  * an alert auto-resolves when the metric recovers
  * /ws/dashboard receives both live metrics and the alert event
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HUB_DIR = Path(__file__).resolve().parent / "hub"
sys.path.insert(0, str(HUB_DIR))

os.environ["FLUSH_INTERVAL_S"] = "1"
os.environ["REAPER_INTERVAL_S"] = "30"  # keep the reaper out of the way

import httpx  # noqa: E402
import uvicorn  # noqa: E402
from websockets.asyncio.client import connect  # noqa: E402

from app.main import app  # noqa: E402

PORT = 8802
BASE = f"http://127.0.0.1:{PORT}"
WS_AGENT = f"ws://127.0.0.1:{PORT}/ws/agent"
WS_DASH = f"ws://127.0.0.1:{PORT}/ws/dashboard"

# Scripted CPU: calm, then a 1-sample spike (must NOT fire a 2s-sustained
# rule), then a long breach (must fire), then recovery (must resolve).
CPU_SCRIPT = (
    [10.0] * 3
    + [99.0]            # lone spike
    + [10.0] * 2
    + [95.0] * 12       # sustained breach
    + [5.0] * 4         # recovery
)

failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label} {detail}")
        failures.append(label)


async def fake_agent(frames: list[float], interval: float = 0.25) -> None:
    """An agent with scripted CPU values."""
    async with connect(WS_AGENT) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "hello",
                    "agent_key": "api-test-node",
                    "agent_name": "api-test",
                    "hostname": "api-test-host",
                    "os": "TestOS 1.0",
                    "cpu_cores": 8,
                    "total_ram_gb": 32.0,
                    "interval_ms": 250,
                }
            )
        )
        await ws.recv()  # welcome

        for cpu in frames:
            await ws.send(
                json.dumps(
                    {
                        "type": "metrics",
                        "ts": time.time(),
                        "cpu_percent": cpu,
                        "mem_percent": 40.0,
                        "mem_used_gb": 12.8,
                        "disk_percent": 55.0,
                        "disk_read_mbs": 1.0,
                        "disk_write_mbs": 2.0,
                        "net_up_kbs": 10.0,
                        "net_down_kbs": 20.0,
                        "uptime_seconds": 1000,
                    }
                )
            )
            await asyncio.sleep(interval)


async def dashboard_collector(frames: list[dict], stop: asyncio.Event) -> None:
    async with connect(WS_DASH) as ws:
        await ws.recv()  # hello
        while not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except (asyncio.TimeoutError, TimeoutError):
                continue
            frames.append(json.loads(raw))


async def main() -> None:
    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    dash_frames: list[dict] = []
    stop = asyncio.Event()
    dash_task = asyncio.create_task(dashboard_collector(dash_frames, stop))
    await asyncio.sleep(0.3)

    async with httpx.AsyncClient(base_url=BASE, timeout=10) as client:
        print("\n--- alert rules -------------------------------------------")

        # duration_seconds=2 must swallow the single-sample spike below.
        r = await client.post(
            "/alert-rules",
            json={
                "name": "CPU sustained high",
                "metric": "cpu_percent",
                "operator": ">",
                "threshold": 85,
                "duration_seconds": 2,
                "machine_id": None,
            },
        )
        check("POST /alert-rules -> 201", r.status_code == 201, str(r.status_code))
        rule = r.json()
        rule_id = rule["id"]
        check("rule applies fleet-wide", rule["machine_id"] is None)

        r = await client.post(
            "/alert-rules",
            json={
                "name": "duplicate",
                "metric": "cpu_percent",
                "operator": ">",
                "threshold": 85,
                "duration_seconds": 2,
                "machine_id": None,
            },
        )
        check("duplicate rule -> 409", r.status_code == 409, str(r.status_code))

        r = await client.post(
            "/alert-rules",
            json={"name": "bad", "metric": "not_a_column",
                  "operator": ">", "threshold": 1},
        )
        check("unknown metric rejected -> 422", r.status_code == 422, str(r.status_code))

        r = await client.get("/alert-rules")
        check("GET /alert-rules lists it", len(r.json()) == 1)

        # ------------------------------------------------------ stream metrics
        print("\n--- streaming scripted metrics ----------------------------")
        started = datetime.now(timezone.utc)
        agent = asyncio.create_task(fake_agent(CPU_SCRIPT))

        # Let the spike pass and confirm it did NOT open an alert.
        await asyncio.sleep(1.8)
        r = await client.get("/alerts")
        check(
            "1-sample spike suppressed by duration_seconds",
            r.json()["total"] == 0,
            f"got {r.json()['total']} alert(s)",
        )

        await agent
        await asyncio.sleep(1.5)

        print("\n--- machines ----------------------------------------------")
        r = await client.get("/machines")
        body = r.json()
        check("GET /machines -> 200", r.status_code == 200)
        check("machine registered", body["total"] == 1, json.dumps(body)[:200])
        machine = body["machines"][0]
        machine_id = machine["id"]
        check("static facts stored", machine["cpu_cores"] == 8 and machine["os"] == "TestOS 1.0")

        r = await client.get(f"/machines/{machine_id}")
        check("GET /machines/{id} -> 200", r.status_code == 200)

        r = await client.get("/machines/99999")
        check("unknown machine -> 404", r.status_code == 404, str(r.status_code))

        r = await client.get("/machines?status=bogus")
        check("bad status filter -> 422", r.status_code == 422, str(r.status_code))

        print("\n--- metrics history ---------------------------------------")
        r = await client.get(f"/machines/{machine_id}/metrics")
        body = r.json()
        check("GET metrics -> 200", r.status_code == 200)
        check(
            f"rows returned ({body['returned']})",
            body["returned"] >= len(CPU_SCRIPT) - 2,
            f"expected ~{len(CPU_SCRIPT)}",
        )
        check("response echoes 'from' alias", "from" in body)

        # A window that ends before streaming began must be empty. This is what
        # proves the filter is applied rather than silently ignored.
        past = (started - timedelta(hours=2)).isoformat()
        past_end = (started - timedelta(hours=1)).isoformat()
        r = await client.get(
            f"/machines/{machine_id}/metrics", params={"from": past, "to": past_end}
        )
        check("empty window returns 0 rows", r.json()["returned"] == 0)

        # A window covering only the run must return rows.
        r = await client.get(
            f"/machines/{machine_id}/metrics",
            params={"from": started.isoformat(), "to": datetime.now(timezone.utc).isoformat()},
        )
        check("covering window returns rows", r.json()["returned"] > 0)

        r = await client.get(
            f"/machines/{machine_id}/metrics", params={"limit": 5, "order": "desc"}
        )
        rows = r.json()["metrics"]
        check("limit respected", len(rows) == 5, str(len(rows)))
        check(
            "order=desc newest first",
            rows[0]["ts"] >= rows[-1]["ts"],
        )

        r = await client.get(
            f"/machines/{machine_id}/metrics",
            params={"from": datetime.now(timezone.utc).isoformat(), "to": past},
        )
        check("from > to -> 422", r.status_code == 422, str(r.status_code))

        print("\n--- alerts -------------------------------------------------")
        r = await client.get("/alerts?state=all")
        alerts = r.json()["alerts"]
        check("sustained breach opened an alert", len(alerts) == 1, f"got {len(alerts)}")

        if alerts:
            alert = alerts[0]
            print(f"    message : {alert['message']}")
            print(f"    state   : {alert['state']}  peak={alert['peak_value']}")
            check("alert auto-resolved on recovery", alert["state"] == "resolved")
            check("peak recorded", alert["peak_value"] >= 95.0, str(alert["peak_value"]))
            check("resolved_at set", alert["resolved_at"] is not None)
            check("rule linked", alert["rule_id"] == rule_id)

        r = await client.get("/alerts")  # default = active only
        check("default filter hides resolved", r.json()["total"] == 0)

        print("\n--- dashboard stream ---------------------------------------")
        stop.set()
        await asyncio.sleep(0.6)
        dash_task.cancel()
        try:
            await dash_task
        except (asyncio.CancelledError, Exception):
            pass

        metric_frames = [f for f in dash_frames if f.get("type") == "metrics"]
        alert_frames = [f for f in dash_frames if f.get("type") == "alert"]
        check(f"live metrics broadcast ({len(metric_frames)})", len(metric_frames) > 10)
        check(f"alert events broadcast ({len(alert_frames)})", len(alert_frames) >= 2)
        if alert_frames:
            kinds = [f["event"] for f in alert_frames]
            check("triggered + resolved both pushed",
                  "triggered" in kinds and "resolved" in kinds, str(kinds))

        print("\n--- cleanup -------------------------------------------------")
        r = await client.delete(f"/alert-rules/{rule_id}")
        check("DELETE rule -> 204", r.status_code == 204, str(r.status_code))
        r = await client.get("/alerts?state=all")
        check("alerts cascade with rule", r.json()["total"] == 0)

    server.should_exit = True
    await server_task

    print("\n" + "=" * 60)
    if failures:
        print(f"{len(failures)} CHECK(S) FAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("ALL PHASE 3+4 CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
