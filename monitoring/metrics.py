"""
Prometheus metrics collection.

Exposes scheduler, worker, and task metrics for Prometheus scraping.
"""

import logging
import threading
import time
from typing import Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    start_http_server,
)

from config import get_config

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Global metrics registry
# ------------------------------------------------------------------

REGISTRY = CollectorRegistry()

# Task metrics
TASKS_SUBMITTED = Counter(
    "scheduler_tasks_submitted_total",
    "Total tasks submitted",
    ["task_type"],
    registry=REGISTRY,
)

TASKS_COMPLETED = Counter(
    "scheduler_tasks_completed_total",
    "Total tasks completed successfully",
    ["task_type", "worker_id"],
    registry=REGISTRY,
)

TASKS_FAILED = Counter(
    "scheduler_tasks_failed_total",
    "Total tasks that failed permanently",
    ["task_type", "worker_id"],
    registry=REGISTRY,
)

TASKS_RETRIED = Counter(
    "scheduler_tasks_retried_total",
    "Total task retry attempts",
    ["task_type"],
    registry=REGISTRY,
)

TASK_EXECUTION_TIME = Histogram(
    "scheduler_task_execution_seconds",
    "Task execution time in seconds",
    ["task_type"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    registry=REGISTRY,
)

TASK_QUEUE_TIME = Histogram(
    "scheduler_task_queue_seconds",
    "Time tasks spend waiting in queue",
    ["task_type"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0),
    registry=REGISTRY,
)

# Queue metrics
QUEUE_DEPTH = Gauge(
    "scheduler_queue_depth",
    "Current number of tasks in the queue",
    registry=REGISTRY,
)

# Worker metrics
WORKERS_ACTIVE = Gauge(
    "scheduler_workers_active",
    "Number of active (healthy) workers",
    registry=REGISTRY,
)

WORKERS_TOTAL = Gauge(
    "scheduler_workers_total",
    "Total number of registered workers",
    registry=REGISTRY,
)

WORKER_CPU_PERCENT = Gauge(
    "scheduler_worker_cpu_percent",
    "Worker CPU utilization percentage",
    ["worker_id"],
    registry=REGISTRY,
)

WORKER_MEMORY_PERCENT = Gauge(
    "scheduler_worker_memory_percent",
    "Worker memory utilization percentage",
    ["worker_id"],
    registry=REGISTRY,
)

WORKER_ACTIVE_TASKS = Gauge(
    "scheduler_worker_active_tasks",
    "Number of active tasks on a worker",
    ["worker_id"],
    registry=REGISTRY,
)

WORKER_LOAD_FACTOR = Gauge(
    "scheduler_worker_load_factor",
    "Worker load factor (0.0 to 1.0)",
    ["worker_id"],
    registry=REGISTRY,
)

# Throughput
THROUGHPUT = Summary(
    "scheduler_throughput",
    "Tasks processed per collection interval",
    registry=REGISTRY,
)


class MetricsCollector:
    """
    Periodically collects metrics from Redis and updates Prometheus gauges.

    This runs as a background thread in the scheduler process and polls
    the dispatcher for current state.
    """

    def __init__(self, dispatcher=None):
        self._cfg = get_config()
        self._dispatcher = dispatcher
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_completed = 0

    def start(self, expose_port: Optional[int] = None) -> None:
        """Start metrics collection and optionally the Prometheus HTTP server."""
        if self._running:
            return
        self._running = True

        if expose_port:
            start_http_server(expose_port, registry=REGISTRY)
            logger.info("Prometheus metrics exposed on port %d", expose_port)

        self._thread = threading.Thread(
            target=self._collection_loop, daemon=True, name="metrics-collector"
        )
        self._thread.start()
        logger.info("Metrics collector started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _collection_loop(self) -> None:
        """Periodically gather metrics from the dispatcher."""
        while self._running:
            try:
                self._collect()
            except Exception:
                logger.exception("Error collecting metrics")
            time.sleep(self._cfg.METRICS_COLLECTION_INTERVAL)

    def _collect(self) -> None:
        """Single metrics collection pass."""
        if self._dispatcher is None:
            return

        queue = self._dispatcher.task_queue

        # Queue depth
        depth = queue.queue_depth()
        QUEUE_DEPTH.set(depth)

        # Worker metrics
        workers = self._dispatcher.get_workers()
        healthy_count = 0
        for w in workers:
            WORKER_CPU_PERCENT.labels(worker_id=w.worker_id).set(w.cpu_percent)
            WORKER_MEMORY_PERCENT.labels(worker_id=w.worker_id).set(w.memory_percent)
            WORKER_ACTIVE_TASKS.labels(worker_id=w.worker_id).set(w.active_tasks)
            WORKER_LOAD_FACTOR.labels(worker_id=w.worker_id).set(w.load_factor)
            if w.is_healthy:
                healthy_count += 1

        WORKERS_TOTAL.set(len(workers))
        WORKERS_ACTIVE.set(healthy_count)

        # Throughput (completed tasks since last collection)
        stats = queue.get_stats()
        current_completed = stats.get("completed_count", 0)
        delta = current_completed - self._last_completed
        if delta > 0:
            THROUGHPUT.observe(delta)
        self._last_completed = current_completed


def get_metrics() -> bytes:
    """Return current metrics in Prometheus exposition format."""
    return generate_latest(REGISTRY)
