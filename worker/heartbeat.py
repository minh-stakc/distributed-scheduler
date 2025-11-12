"""
Worker heartbeat and health reporting.

Each worker periodically reports its health, resource utilization, and
active task count to Redis so the dispatcher can make informed scheduling
decisions and detect dead workers.
"""

import json
import logging
import threading
import time
from typing import Optional

import psutil
import redis

from config import get_config

logger = logging.getLogger(__name__)


class HeartbeatReporter:
    """
    Sends periodic heartbeats to Redis with worker health information.

    Heartbeat payload includes:
      - status (active / draining / shutting_down)
      - active_tasks / max_concurrent
      - cpu_percent / memory_percent
      - completed_tasks / failed_tasks
      - last_heartbeat timestamp
    """

    def __init__(
        self,
        worker_id: str,
        redis_client: Optional[redis.Redis] = None,
        max_concurrent: Optional[int] = None,
    ):
        self._cfg = get_config()
        self.worker_id = worker_id
        self.max_concurrent = max_concurrent or self._cfg.WORKER_MAX_CONCURRENT_TASKS

        if redis_client is not None:
            self._redis = redis_client
        else:
            self._redis = redis.Redis(
                host=self._cfg.REDIS_HOST,
                port=self._cfg.REDIS_PORT,
                db=self._cfg.REDIS_DB,
                password=self._cfg.REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )

        self._active_tasks = 0
        self._completed_tasks = 0
        self._failed_tasks = 0
        self._status = "active"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Task counters (thread-safe)
    # ------------------------------------------------------------------

    def task_started(self) -> None:
        with self._lock:
            self._active_tasks += 1

    def task_completed(self) -> None:
        with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            self._completed_tasks += 1

    def task_failed(self) -> None:
        with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            self._failed_tasks += 1

    @property
    def active_tasks(self) -> int:
        with self._lock:
            return self._active_tasks

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _build_heartbeat(self) -> dict:
        """Collect current system metrics and build heartbeat payload."""
        with self._lock:
            active = self._active_tasks
            completed = self._completed_tasks
            failed = self._failed_tasks
            status = self._status

        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            memory_percent = mem.percent
        except Exception:
            cpu_percent = 0.0
            memory_percent = 0.0

        return {
            "worker_id": self.worker_id,
            "status": status,
            "active_tasks": active,
            "max_concurrent": self.max_concurrent,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "last_heartbeat": time.time(),
        }

    def send_heartbeat(self) -> bool:
        """Send a single heartbeat to Redis."""
        try:
            heartbeat = self._build_heartbeat()
            key = f"{self._cfg.WORKER_PREFIX}{self.worker_id}"

            pipe = self._redis.pipeline()
            pipe.set(key, json.dumps(heartbeat), ex=self._cfg.WORKER_HEARTBEAT_TIMEOUT * 2)
            pipe.sadd(self._cfg.WORKER_SET_KEY, self.worker_id)
            pipe.execute()

            logger.debug(
                "Heartbeat sent: worker=%s active=%d cpu=%.1f%% mem=%.1f%%",
                self.worker_id,
                heartbeat["active_tasks"],
                heartbeat["cpu_percent"],
                heartbeat["memory_percent"],
            )
            return True
        except redis.ConnectionError:
            logger.error("Failed to send heartbeat: Redis connection error")
            return False
        except Exception:
            logger.exception("Failed to send heartbeat")
            return False

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background heartbeat loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name=f"heartbeat-{self.worker_id}"
        )
        self._thread.start()
        # Send initial heartbeat immediately
        self.send_heartbeat()
        logger.info("Heartbeat started for worker %s (interval=%ds)", self.worker_id, self._cfg.WORKER_HEARTBEAT_INTERVAL)

    def stop(self) -> None:
        """Stop the heartbeat loop and deregister."""
        self._running = False
        with self._lock:
            self._status = "shutting_down"
        self.send_heartbeat()  # Final heartbeat with shutdown status

        if self._thread:
            self._thread.join(timeout=5)

        # Clean up Redis registration
        try:
            self._redis.srem(self._cfg.WORKER_SET_KEY, self.worker_id)
            self._redis.delete(f"{self._cfg.WORKER_PREFIX}{self.worker_id}")
        except Exception:
            pass

        logger.info("Heartbeat stopped for worker %s", self.worker_id)

    def set_draining(self) -> None:
        """Mark the worker as draining (will finish current tasks but accept no new ones)."""
        with self._lock:
            self._status = "draining"
        self.send_heartbeat()

    def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats."""
        while self._running:
            self.send_heartbeat()
            time.sleep(self._cfg.WORKER_HEARTBEAT_INTERVAL)
