"""System metric collection built on psutil.

Ported from the single-host realtime-sysmonitor project, with the payload
flattened to exactly the columns the Hub persists. The process table was
dropped: at fleet scale it is the most expensive part of a sample and the Hub
has nowhere to put it.

The collector is deliberately **stateful**. Disk and network counters exposed by
the OS are cumulative totals since boot, so a *rate* (MB/s, KB/s) can only be
derived by diffing two consecutive readings against the elapsed wall time
between them.
"""

from __future__ import annotations

import os
import platform
import socket
import time
from typing import Any, Callable, TypeVar

import psutil

KB = 1024
MB = 1024**2
GB = 1024**3

T = TypeVar("T")


def _safe(fn: Callable[[], T], default: T | None = None) -> T | None:
    """Run a psutil call, swallowing platform-specific failures.

    psutil raises on hardware/permission edge cases that differ per OS (empty
    CD-ROM drives, containers without full /proc access, missing sensors). For a
    monitoring agent, a missing value is far better than a crashed sampler --
    especially since nobody is watching the agent's own stdout.
    """
    try:
        return fn()
    except Exception:
        return default


def _root_mount() -> str:
    """The filesystem root: ``/`` on POSIX, ``C:\\`` (or equivalent) on Windows."""
    return os.path.abspath(os.sep)


def system_info(agent_key: str, agent_name: str) -> dict[str, Any]:
    """Static machine facts, sent once in the handshake.

    None of this changes while the agent runs, so streaming it every tick would
    waste bandwidth and duplicate data the Hub already has on the machine row.
    """
    vmem = psutil.virtual_memory()
    return {
        "agent_key": agent_key,
        "agent_name": agent_name,
        "hostname": _safe(socket.gethostname, "unknown"),
        "os": f"{platform.system()} {platform.release()}".strip(),
        "platform_version": platform.version(),
        "architecture": platform.machine(),
        "cpu_model": platform.processor() or "Unknown CPU",
        "cpu_cores": psutil.cpu_count(logical=True) or 0,
        "total_ram_gb": round(vmem.total / GB, 2),
        "boot_time": psutil.boot_time(),
    }


class MetricsCollector:
    """Samples CPU, memory, disk and network into one flat metric row."""

    def __init__(self) -> None:
        # Prime the CPU counter. The first psutil.cpu_percent() call always
        # returns 0.0 because it has no previous reading to compare against, so
        # we get that throwaway call out of the way at construction time
        # instead of shipping a bogus 0% as the machine's first sample.
        psutil.cpu_percent(interval=None)

        self._root = _root_mount()
        self._first_sample = True
        self._last_time = time.monotonic()
        self._last_disk = _safe(psutil.disk_io_counters)
        self._last_net = _safe(psutil.net_io_counters)

    # ------------------------------------------------------------------ public

    def sample(self) -> dict[str, Any]:
        """Take one snapshot. Blocking; the caller runs it off the event loop."""
        now = time.monotonic()
        elapsed = max(now - self._last_time, 1e-6)
        self._last_time = now

        vmem = psutil.virtual_memory()
        disk = self._disk_rates(elapsed)
        net = self._net_rates(elapsed)
        usage = _safe(lambda: psutil.disk_usage(self._root))
        cpu = self._cpu_percent()

        return {
            "type": "metrics",
            # Epoch seconds (UTC). The Hub converts to a timezone-aware
            # datetime; we deliberately do not send a local-time string, since
            # agents and Hub may sit in different timezones.
            "ts": time.time(),
            "cpu_percent": cpu,
            "mem_percent": round(vmem.percent, 1),
            "mem_used_gb": round(vmem.used / GB, 2),
            "disk_percent": round(usage.percent, 1) if usage else None,
            "disk_read_mbs": disk[0],
            "disk_write_mbs": disk[1],
            "net_up_kbs": net[0],
            "net_down_kbs": net[1],
            "uptime_seconds": int(time.time() - psutil.boot_time()),
        }

    def reset_baseline(self) -> None:
        """Re-prime the rate counters after a gap in sampling.

        Called on reconnect. Without it, the first sample after a two-minute
        outage would divide two minutes' worth of accumulated bytes by the
        elapsed time and report a meaningless average as if it were a live
        instantaneous rate.
        """
        self._first_sample = True
        self._last_time = time.monotonic()
        self._last_disk = _safe(psutil.disk_io_counters)
        self._last_net = _safe(psutil.net_io_counters)
        psutil.cpu_percent(interval=None)

    # ----------------------------------------------------------------- private

    def _cpu_percent(self) -> float:
        """Read CPU load, blocking briefly on the very first sample.

        Priming in __init__ is not enough on its own: the agent connects and
        samples within microseconds of construction, and cpu_percent(None)
        measures load *since the previous call*, so that first reading covers a
        near-zero window and always comes back 0.0. Rather than ship a fake
        idle sample as a machine's first-ever data point, we block for one
        short window exactly once. sample() already runs in a worker thread, so
        this does not stall the event loop or the socket's keepalive pings.
        """
        if self._first_sample:
            self._first_sample = False
            return round(psutil.cpu_percent(interval=0.1), 1)
        return round(psutil.cpu_percent(interval=None), 1)

    def _disk_rates(self, elapsed: float) -> tuple[float, float]:
        counters = _safe(psutil.disk_io_counters)
        read_mbs = write_mbs = 0.0

        if counters:
            if self._last_disk:
                # max(..., 0) guards against counter resets, which happen on
                # some platforms and would otherwise yield negative rates.
                read_delta = max(counters.read_bytes - self._last_disk.read_bytes, 0)
                write_delta = max(counters.write_bytes - self._last_disk.write_bytes, 0)
                read_mbs = round(read_delta / MB / elapsed, 2)
                write_mbs = round(write_delta / MB / elapsed, 2)
            self._last_disk = counters

        return read_mbs, write_mbs

    def _net_rates(self, elapsed: float) -> tuple[float, float]:
        counters = _safe(psutil.net_io_counters)
        up_kbs = down_kbs = 0.0

        if counters:
            if self._last_net:
                sent_delta = max(counters.bytes_sent - self._last_net.bytes_sent, 0)
                recv_delta = max(counters.bytes_recv - self._last_net.bytes_recv, 0)
                up_kbs = round(sent_delta / KB / elapsed, 1)
                down_kbs = round(recv_delta / KB / elapsed, 1)
            self._last_net = counters

        return up_kbs, down_kbs
