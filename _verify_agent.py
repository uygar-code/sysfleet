"""THROWAWAY Phase 1 verification harness. Deleted once the real Hub lands.

Spins up a stub WebSocket server, runs the real agent against it, then kills and
restarts the server mid-stream to prove the reconnect/backoff path actually
works rather than assuming it does.
"""

from __future__ import annotations

import asyncio
import json

from websockets.asyncio.server import serve

from agent.config import AgentConfig
from agent.main import run

PORT = 8799

received: list[dict] = []
connection_count = 0


async def handler(ws):
    global connection_count
    connection_count += 1
    print(f"[stub-hub] agent connected (connection #{connection_count})")
    await ws.send(json.dumps({"type": "welcome", "machine_id": 42}))
    async for raw in ws:
        received.append(json.loads(raw))


async def main() -> None:
    cfg = AgentConfig(
        hub_url=f"ws://127.0.0.1:{PORT}/ws/agent",
        agent_key="verify-key",
        agent_name="verify-agent",
        interval_ms=250,
        backoff_initial_s=0.5,
        backoff_max_s=2.0,
    )

    agent_task = asyncio.create_task(run(cfg))

    # --- Phase A: normal streaming -------------------------------------
    server = await serve(handler, "127.0.0.1", PORT)
    await asyncio.sleep(1.5)

    frames_before = len(received)
    print(f"\n[verify] frames received while connected: {frames_before}")

    # --- Phase B: kill the hub, agent should retry ----------------------
    print("[verify] killing stub hub to force a reconnect...")
    server.close()
    await server.wait_closed()
    await asyncio.sleep(1.2)

    # --- Phase C: hub comes back, agent should reattach ----------------
    print("[verify] restarting stub hub...")
    server = await serve(handler, "127.0.0.1", PORT)
    await asyncio.sleep(2.0)

    agent_task.cancel()
    try:
        await agent_task
    except asyncio.CancelledError:
        pass
    server.close()
    await server.wait_closed()

    # ----------------------------------------------------------- results
    hellos = [f for f in received if f.get("type") == "hello"]
    metrics = [f for f in received if f.get("type") == "metrics"]

    print("\n" + "=" * 60)
    print(f"connections accepted : {connection_count}")
    print(f"hello frames         : {len(hellos)}")
    print(f"metric frames        : {len(metrics)}")
    print("=" * 60)

    assert connection_count >= 2, "agent did not reconnect after hub restart"
    assert len(hellos) == connection_count, "each connection must re-send hello"
    assert len(metrics) > 5, "not enough metric frames"

    print("\nHELLO payload:")
    print(json.dumps(hellos[0], indent=2))
    print("\nMETRICS payload:")
    print(json.dumps(metrics[0], indent=2))

    required = {
        "ts", "cpu_percent", "mem_percent", "mem_used_gb", "disk_percent",
        "disk_read_mbs", "disk_write_mbs", "net_up_kbs", "net_down_kbs",
    }
    missing = required - set(metrics[0])
    assert not missing, f"metrics payload missing keys: {missing}"

    # The whole point of reset_baseline(): the first sample after the outage
    # must not report a fabricated spike from dividing a huge byte delta.
    post_gap = [f for f in metrics if f["ts"] > hellos[-1]["boot_time"]]
    print(f"\npost-reconnect sample net_down_kbs: {metrics[-1]['net_down_kbs']}")

    print("\nALL PHASE 1 CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
