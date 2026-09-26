# SysFleet

**Multi-node system monitoring hub** — a centralized FastAPI server that collects real-time metrics from a fleet of lightweight agents and stores them for historical analysis and alerting.

🚀 **Live demo**: [https://sysfleet.onrender.com](https://sysfleet.onrender.com/)

Born as the natural evolution of [realtime-sysmonitor](https://github.com/...), transitioning from a single-localhost view to a distributed Agent-Hub architecture.

---

## Features

- **Real-time ingest**: Agents stream metrics (CPU, RAM, disk, network) over WebSocket at configurable intervals (250ms–60s).
- **Persistent history**: Batched writes to SQLite (easily swappable for PostgreSQL) with WAL mode and a composite index for efficient time-range queries.
- **REST + WebSocket hybrid**: Historical reads via REST (`GET /machines`, `GET /machines/{id}/metrics?from=&to=`), live streams via `/ws/dashboard`.
- **Threshold alerting**: Rule-based engine evaluates every sample in memory. Sustained breaches (e.g., "CPU > 85% for 30s") trigger alerts that auto-resolve on recovery.
- **Fleet-wide or machine-specific rules**: A single rule can watch every node, or target one machine by ID.
- **Auto-registration**: Agents identify themselves on connect; the Hub registers new machines automatically.
- **Liveness tracking**: The Hub marks agents offline after missing N expected samples, accounting for both the reporting interval and the last-seen touch cadence.
- **Schema migrations**: Alembic manages the database, so the schema evolves cleanly as the project grows.
- **Docker Compose demo**: `docker-compose up` spins up the Hub and two agents to demonstrate the fleet in one command.

---

## Architecture

```
┌─────────────┐
│   Agent 1   │───┐
└─────────────┘   │
                  ├──→ WebSocket ──→ ┌─────────────────┐
┌─────────────┐   │                  │   Hub (FastAPI) │
│   Agent 2   │───┤                  ├─────────────────┤
└─────────────┘   │                  │  • SQLAlchemy   │
                  │                  │  • Alembic      │
┌─────────────┐   │                  │  • Alert engine │
│   Agent N   │───┘                  └─────────────────┘
└─────────────┘                               │
                                              ▼
                                      SQLite (or Postgres)
```

**Agent**: Lightweight Python script using `psutil`. Streams metrics every `INTERVAL_MS` (default 1000ms). Reconnects with exponential backoff if the Hub restarts.

**Hub**: FastAPI application. Accepts agent connections on `/ws/agent`, writes metrics in batches, runs threshold rules per sample, and serves historical data via REST. A separate `/ws/dashboard` endpoint fans out live metrics for future frontends.

---

## Quick Start (Docker)

```bash
git clone https://github.com/.../SysFleet.git
cd SysFleet
docker-compose up --build
```

The Hub will be available at **http://localhost:8000**. Two agents (`agent-1`, `agent-2`) register automatically and start streaming.

### Explore the API

```bash
# List machines
curl http://localhost:8000/machines

# Get the last hour of metrics for machine 1
curl "http://localhost:8000/machines/1/metrics"

# Get a specific time window (ISO 8601)
curl "http://localhost:8000/machines/1/metrics?from=2026-08-04T10:00:00Z&to=2026-08-04T11:00:00Z&limit=100&order=desc"

# Create a fleet-wide alert rule
curl -X POST http://localhost:8000/alert-rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "CPU sustained high",
    "metric": "cpu_percent",
    "operator": ">",
    "threshold": 85,
    "duration_seconds": 30,
    "machine_id": null
  }'

# List active alerts
curl http://localhost:8000/alerts

# List all rules
curl http://localhost:8000/alert-rules

# Health check (includes connected agents and ingest stats)
curl http://localhost:8000/health

# Interactive API docs
open http://localhost:8000/docs
```

---

## Local Development

### Prerequisites

- **Python 3.12+**
- **pip**

### Setup

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r agent/requirements.txt
pip install -r hub/requirements.txt

# Run migrations (creates hub/sysfleet.db)
cd hub
alembic upgrade head
cd ..
```

### Run the Hub

```bash
# From the repository root
python -m hub.app.main
# or
cd hub && uvicorn app.main:app --reload
```

The Hub listens on **http://127.0.0.1:8000** by default.

### Run an Agent

```bash
# From the repository root, in a separate terminal
python -m agent.main
```

The agent connects to `ws://127.0.0.1:8000/ws/agent` by default.

**Environment variables** (agent):

- `HUB_URL`: WebSocket endpoint (default: `ws://127.0.0.1:8000/ws/agent`)
- `AGENT_NAME`: Human-readable label (default: hostname)
- `AGENT_KEY`: Stable identity key (default: derived from hostname + MAC address)
- `INTERVAL_MS`: Sampling interval in milliseconds (default: 1000, clamped to 250–60000)

**Environment variables** (hub):

- `DATABASE_URL`: SQLAlchemy connection string (default: `sqlite+aiosqlite:///hub/sysfleet.db`)
- `FLUSH_INTERVAL_S`: How often to batch-write metrics (default: 5.0)
- `FLUSH_MAX_ROWS`: Max rows per flush (default: 500)
- `REAPER_INTERVAL_S`: How often to check for stale machines (default: 5.0)
- `OFFLINE_AFTER_MISSED_INTERVALS`: Grace period in multiples of the agent's interval (default: 3)
- `TOUCH_INTERVAL_S`: How often `last_seen` is updated per streaming agent (default: 10.0)
- `ALERT_EVAL_ENABLED`: Whether to run the alert engine (default: true)

---

## Project Structure

```
SysFleet/
├── agent/
│   ├── __init__.py
│   ├── main.py          # Entry point, WebSocket client, retry loop
│   ├── config.py        # Environment-based configuration
│   ├── collector.py     # psutil sampling logic
│   ├── requirements.txt
│   └── Dockerfile
│
├── hub/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py           # FastAPI app, WebSocket endpoints, lifespan
│   │   ├── config.py         # Settings (pydantic-settings)
│   │   ├── db.py             # SQLAlchemy async engine + session factory
│   │   ├── models.py         # ORM models (Machine, Metric, AlertRule, Alert)
│   │   ├── schemas.py        # Pydantic schemas for REST + WebSocket frames
│   │   ├── ingest.py         # Batched metric writer
│   │   ├── broadcast.py      # Fan-out for /ws/dashboard
│   │   ├── registry.py       # Machine registration, liveness, offline reaper
│   │   ├── alerting.py       # Threshold rule engine
│   │   └── routers/
│   │       ├── machines.py   # GET /machines, /machines/{id}/metrics
│   │       └── alerts.py     # Alert and alert-rule CRUD
│   ├── alembic/
│   │   ├── env.py
│   │   ├── script.py.mako
│   │   └── versions/
│   │       └── 6f719856ccb1_initial_schema.py
│   ├── alembic.ini
│   ├── requirements.txt
│   └── Dockerfile
│
├── docker-compose.yml
├── .dockerignore
└── README.md
```

---

## REST API Reference

All timestamps are ISO 8601 UTC. All endpoints return JSON.

### `GET /machines`

List all registered machines.

**Query params**:
- `status` (optional): Filter by `online` or `offline`.

**Response**:
```json
{
  "total": 2,
  "online": 1,
  "offline": 1,
  "machines": [
    {
      "id": 1,
      "agent_key": "demo-agent-1",
      "name": "agent-1",
      "hostname": "a1b2c3d4e5f6",
      "status": "online",
      "cpu_cores": 8,
      "total_ram_gb": 32.0,
      "os": "Linux 5.15.0",
      "first_seen": "2026-08-04T19:00:00Z",
      "last_seen": "2026-08-04T19:15:00Z"
    }
  ]
}
```

### `GET /machines/{id}`

Get one machine by ID. Returns `404` if not found.

### `GET /machines/{id}/metrics`

Historical metrics for a machine.

**Query params**:
- `from` (optional): Start of range (ISO 8601). Defaults to 1 hour ago.
- `to` (optional): End of range (ISO 8601). Defaults to now.
- `limit` (optional): Max rows to return (1–5000, default 1000).
- `offset` (optional): Pagination offset (default 0).
- `order` (optional): `asc` or `desc` (default `asc`).

**Response**:
```json
{
  "machine_id": 1,
  "from": "2026-08-04T19:00:00Z",
  "to": "2026-08-04T20:00:00Z",
  "returned": 100,
  "limit": 1000,
  "offset": 0,
  "metrics": [
    {
      "ts": "2026-08-04T19:00:01.123Z",
      "cpu_percent": 12.3,
      "mem_percent": 45.6,
      "disk_percent": 67.8,
      ...
    }
  ]
}
```

### `GET /alerts`

List alerts.

**Query params**:
- `state` (optional): `active`, `resolved`, or `all` (default `active`).
- `machine_id` (optional): Filter by machine.
- `since` (optional): Only alerts started after this timestamp.
- `limit` (optional): Max rows (1–1000, default 100).

### `GET /alert-rules`

List all alert rules.

**Query params**:
- `machine_id` (optional): Show rules targeting this machine (includes global rules).

### `POST /alert-rules`

Create a threshold rule.

**Body**:
```json
{
  "name": "CPU sustained high",
  "metric": "cpu_percent",
  "operator": ">",
  "threshold": 85,
  "duration_seconds": 30,
  "machine_id": null
}
```

`machine_id: null` means the rule applies fleet-wide. Returns `201` on success, `409` if an identical rule already exists, `422` if the metric name is invalid.

### `PATCH /alert-rules/{id}`

Update a rule (partial update).

### `DELETE /alert-rules/{id}`

Delete a rule. Alerts cascade: deleting the rule also deletes all alerts it generated.

### `POST /alerts/{id}/resolve`

Manually resolve an active alert (idempotent).

### `GET /health`

Hub health check. Returns connected agent count, dashboard client count, and ingest statistics (queued, written, dropped, pending, flushes, last error).

---

## WebSocket Endpoints

### `WS /ws/agent`

**Agent → Hub** connection. The agent sends:

1. **hello** (once, immediately after connect):
   ```json
   {
     "type": "hello",
     "agent_key": "unique-key",
     "agent_name": "my-server",
     "hostname": "ip-10-0-1-42",
     "os": "Linux 5.15.0-1045-aws",
     "cpu_cores": 8,
     "total_ram_gb": 32.0,
     "interval_ms": 1000,
     "agent_version": "1.0.0"
   }
   ```

2. **metrics** (every `interval_ms`):
   ```json
   {
     "type": "metrics",
     "ts": 1722796800.123,
     "cpu_percent": 12.3,
     "mem_percent": 45.6,
     "mem_used_gb": 14.6,
     "disk_percent": 67.8,
     "disk_read_mbs": 1.2,
     "disk_write_mbs": 3.4,
     "net_up_kbs": 56.7,
     "net_down_kbs": 123.4,
     "uptime_seconds": 86400
   }
   ```

**Hub → Agent** responses:
- **welcome**: Sent once after hello, echoes `machine_id`.
- **config** (optional): The Hub can retune the agent's `interval_ms` mid-stream.

### `WS /ws/dashboard`

**Hub → Client** fan-out for live metrics and alert events. The Hub pushes:

- **hello** (once, on connect): Lists currently connected agents.
- **metrics** (every time an agent sends one): Live broadcast of the sample.
- **alert** (on state transitions): `{"type": "alert", "event": "triggered" | "resolved", ...}`

No UI ships with this project, but the seam exists for a future frontend.

---

## Alert Rules

Rules define thresholds that trigger alerts when breached. Each rule specifies:

- **metric**: One of `cpu_percent`, `mem_percent`, `disk_percent`, `mem_used_gb`, `disk_read_mbs`, `disk_write_mbs`, `net_up_kbs`, `net_down_kbs`, `uptime_seconds`.
- **operator**: `>` or `<`.
- **threshold**: Numeric value.
- **duration_seconds**: How long the breach must persist before an alert fires. `0` means instant.
- **machine_id**: Target one machine (`1`, `2`, ...) or `null` for fleet-wide.

### Example: Fleet-wide CPU rule

```bash
curl -X POST http://localhost:8000/alert-rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "High CPU anywhere",
    "metric": "cpu_percent",
    "operator": ">",
    "threshold": 85,
    "duration_seconds": 30,
    "machine_id": null
  }'
```

This rule watches **every machine** (including ones that register in the future). If any agent reports CPU > 85% for 30 continuous seconds, the Hub opens an alert. When CPU drops back below the threshold, the alert auto-resolves.

### Example: Machine-specific disk rule

```bash
curl -X POST http://localhost:8000/alert-rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Disk full on prod-db-1",
    "metric": "disk_percent",
    "operator": ">",
    "threshold": 90,
    "duration_seconds": 0,
    "machine_id": 3
  }'
```

This rule targets only machine ID 3. `duration_seconds: 0` means it fires on the first sample that breaches.

---

## Design Notes

### Why batched writes?

At 1 sample/second/agent, per-row commits would mean thousands of fsyncs an hour for data that nobody reads in real time (the live view is served from memory, not SQL). Buffering samples and flushing every 5 seconds reduces write amplification by ~5× and keeps the database responsive.

### Why SQLite?

It's zero-config, ships as a single file, and [handles this workload gracefully](https://www.sqlite.org/whentouse.html) with WAL mode. The composite `(machine_id, ts)` index means range queries are index-only scans. When you outgrow it, change `DATABASE_URL` to PostgreSQL and re-run the migrations — nothing else in the code needs to know.

### Why evaluate alerts per-sample?

Polling the metrics table on a timer would mean re-reading rows we already had in hand, and detecting problems a window late. The alert engine sees every sample as it arrives, checks it against cached rules, and only touches the database on a state transition (opening or closing an alert) — which is rare compared to the sample rate.

### Why throttle `last_seen` updates?

Writing `last_seen` on every sample would reintroduce exactly the per-row transaction cost the batch writer exists to eliminate. Touching it every 10 seconds is plenty: the reaper accounts for both the touch interval and the agent's reporting interval when computing the offline grace period, so a healthy agent never gets flagged as offline.

### Why not Prometheus / Grafana / InfluxDB / ...?

This project is a **teaching implementation** of the Agent-Hub pattern, built from scratch to show how WebSocket ingest, batched persistence, and threshold alerting fit together. If you need a production monitoring stack, use the battle-tested tools. If you want to understand how they work under the hood, build this.

---

## Roadmap

- [ ] Aggregate queries (min/max/avg per hour, per day)
- [ ] Retention policies (auto-delete metrics older than N days)
- [ ] Dashboard UI (websockets + charting library)
- [ ] Multi-condition rules (CPU > 85% AND mem > 90%)
- [ ] Notification channels (email, Slack, webhook)
- [ ] Agent authentication (HMAC signatures, TLS)
- [ ] Horizontal Hub scaling (share alert state via Redis)

---

## License

MIT

---

## Contributing

Pull requests are welcome. For major changes, open an issue first to discuss what you'd like to change.

---

**Built with**: FastAPI • SQLAlchemy • Alembic • psutil • websockets • Pydantic • httpx • uvicorn
