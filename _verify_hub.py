"""THROWAWAY Phase 2 verification. Deleted before the project is finished.

Runs the real Hub in-process (via uvicorn) and points two real agents at it,
then inspects the database directly to prove:

  * both machines auto-registered from their hello frames
  * metric rows are actually being written by the batch writer
  * rows are written in *batches*, not one transaction per sample
  * WAL mode is active
  * the composite (machine_id, ts) index is the one SQLite picks for a range read
  * the offline reaper flips status after agents stop
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from pathlib import Path

HUB_DIR = Path(__file__).resolve().parent / "hub"
DB_PATH = HUB_DIR / "sysfleet.db"

sys.path.insert(0, str(HUB_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Fast flush + fast reaper so the test finishes in seconds instead of minutes.
os.environ["FLUSH_INTERVAL_S"] = "2"
os.environ["REAPER_INTERVAL_S"] = "1"
os.environ["OFFLINE_AFTER_MISSED_INTERVALS"] = "3"

import uvicorn  # noqa: E402

from agent.config import AgentConfig  # noqa: E402
from agent.main import run as run_agent  # noqa: E402
from app.main import app, writer  # noqa: E402

PORT = 8801


def q(sql: str, params: tuple = ()) -> list:
    conn = sqlite3.connect(DB_PATH)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


async def main() -> None:
    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    while not server.started:
        await asyncio.sleep(0.05)
    print("[verify] hub is up")

    agents = [
        asyncio.create_task(
            run_agent(
                AgentConfig(
                    hub_url=f"ws://127.0.0.1:{PORT}/ws/agent",
                    agent_key=f"verify-node-{n}",
                    agent_name=f"node-{n}",
                    interval_ms=250,
                    backoff_initial_s=0.5,
                    backoff_max_s=2.0,
                )
            )
        )
        for n in (1, 2)
    ]

    # Stream long enough to span several flush windows AND several reaper
    # sweeps. The reaper previously fired on healthy agents here, which the
    # end-state-only assertions happily missed -- so check mid-stream.
    await asyncio.sleep(9)

    live = q(
        "SELECT name, status FROM machines WHERE agent_key LIKE 'verify-node-%'"
    )
    print("\n[verify] status WHILE streaming:")
    for name, status in live:
        print(f"    {name}: {status}")
    assert all(s == "online" for _, s in live), (
        "reaper marked a healthy streaming agent offline -- grace period is "
        "shorter than the last_seen touch interval"
    )

    print("[verify] stopping agents to exercise the offline reaper")
    for task in agents:
        task.cancel()
    await asyncio.gather(*agents, return_exceptions=True)

    # Give the reaper time to pass the full grace period, which now includes
    # touch_interval_s on top of interval*misses.
    await asyncio.sleep(4)

    stats = writer.stats_dict()

    machines = q(
        "SELECT id, agent_key, name, status, interval_ms, cpu_cores FROM machines"
        " WHERE agent_key LIKE 'verify-node-%' ORDER BY id"
    )
    print("\n" + "=" * 64)
    print("MACHINES")
    for row in machines:
        print(f"  id={row[0]} key={row[1]} name={row[2]} status={row[3]} "
              f"interval={row[4]}ms cores={row[5]}")

    ids = [m[0] for m in machines]
    placeholders = ",".join("?" * len(ids))
    counts = q(
        f"SELECT machine_id, COUNT(*) FROM metrics WHERE machine_id IN ({placeholders})"
        " GROUP BY machine_id",
        tuple(ids),
    )
    print("\nMETRIC ROWS PER MACHINE")
    for machine_id, count in counts:
        print(f"  machine {machine_id}: {count} rows")

    sample = q(
        f"SELECT ts, cpu_percent, mem_percent, disk_percent, net_down_kbs"
        f" FROM metrics WHERE machine_id IN ({placeholders})"
        " ORDER BY ts DESC LIMIT 3",
        tuple(ids),
    )
    print("\nNEWEST ROWS")
    for row in sample:
        print(f"  ts={row[0]} cpu={row[1]} mem={row[2]} disk={row[3]} net_down={row[4]}")

    print("\nINGEST STATS")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    journal = q("PRAGMA journal_mode")[0][0]
    print(f"\njournal_mode: {journal}")

    plan = q(
        "EXPLAIN QUERY PLAN SELECT ts, cpu_percent FROM metrics"
        " WHERE machine_id = ? AND ts BETWEEN ? AND ? ORDER BY ts",
        (ids[0], "2000-01-01", "2100-01-01"),
    )
    print("QUERY PLAN for the /metrics range read:")
    for row in plan:
        print(f"  {row[-1]}")

    server.should_exit = True
    await server_task

    # ------------------------------------------------------------ assertions
    total_rows = sum(c for _, c in counts)

    assert len(machines) == 2, f"expected 2 machines, got {len(machines)}"
    assert all(m[3] == "offline" for m in machines), (
        "reaper should have marked stopped agents offline"
    )
    assert all(m[5] and m[5] > 0 for m in machines), "cpu_cores not captured from hello"
    assert len(counts) == 2, "both machines should have metric rows"
    assert total_rows > 20, f"expected plenty of rows, got {total_rows}"
    assert stats["failed_flushes"] == 0, f"flush failures: {stats['last_error']}"
    assert stats["dropped"] == 0, "samples were dropped unexpectedly"

    # The batching claim, tested rather than asserted in prose: if each sample
    # were its own transaction, flushes would equal rows written.
    assert stats["flushes"] < stats["written"], "writes were not batched"
    rows_per_flush = stats["written"] / max(stats["flushes"], 1)
    print(f"\naverage rows per flush: {rows_per_flush:.1f}")
    assert rows_per_flush > 3, "batching is not amortising writes as intended"

    assert journal.lower() == "wal", f"expected WAL, got {journal}"

    plan_text = " ".join(r[-1] for r in plan)
    assert "ix_metrics_machine_ts" in plan_text, (
        f"composite index not used; planner chose: {plan_text}"
    )

    print("\nALL PHASE 2 CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
