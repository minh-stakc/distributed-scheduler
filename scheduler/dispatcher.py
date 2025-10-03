"""
Task dispatcher with pluggable scheduling algorithms.

Supports round-robin, least-loaded, and priority-based scheduling.
Monitors worker health and avoids dispatching to unhealthy nodes.
"""

import json
import logging
import threading
import time
from typing import Dict, List, Optional

import redis

from config import get_config
from scheduler.queue import TaskQueue, TaskStatus

logger = logging.getLogger(__name__)


class WorkerInfo:
    """Snapshot of a worker's state for scheduling decisions."""

    def __init__(self, worker_id: str, data: dict):
        self.worker_id = worker_id
        self.active_tasks = data.get("active_tasks", 0)
        self.max_concurrent = data.get("max_concurrent", 4)
        self.cpu_percent = data.get("cpu_percent", 0.0)
        self.memory_percent = data.get("memory_percent", 0.0)
        self.last_heartbeat = data.get("last_heartbeat", 0)
        self.status = data.get("status", "unknown")
        self.completed_tasks = data.get("completed_tasks", 0)
        self.failed_tasks = data.get("failed_tasks", 0)

    @property
    def is_healthy(self) -> bool:
        cfg = get_config()
        age = time.time() - self.last_heartbeat
        return self.status == "active" and age < cfg.WORKER_HEARTBEAT_TIMEOUT

    @property
    def has_capacity(self) -> bool:
        return self.active_tasks < self.max_concurrent

    @property
    def load_factor(self) -> float:
        """0.0 (idle) to 1.0 (fully loaded)."""
        if self.max_concurrent == 0:
            return 1.0
        return self.active_tasks / self.max_concurrent


class Dispatcher:
    """
    Central dispatcher that assigns tasks to workers using a chosen algorithm.

    Scheduling algorithms:
      - round_robin:  Cycle through healthy workers sequentially.
      - least_loaded: Pick the worker with the fewest active tasks.
      - priority:     Dequeue highest-priority tasks first, assign to least-loaded worker.
    """

    def __init__(
        self,
        task_queue: Optional[TaskQueue] = None,
        redis_client: Optional[redis.Redis] = None,
    ):
        self._cfg = get_config()

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

        self._queue = task_queue or TaskQueue(self._redis)
        self._algorithm = self._cfg.SCHEDULER_ALGORITHM
        self._rr_index = 0
        self._running = False
        self._dispatch_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def task_queue(self) -> TaskQueue:
        return self._queue

    # ------------------------------------------------------------------
    # Worker registry helpers
    # ------------------------------------------------------------------

    def get_workers(self) -> List[WorkerInfo]:
        """Return all registered workers."""
        worker_ids = self._redis.smembers(self._cfg.WORKER_SET_KEY)
        workers = []
        for wid in worker_ids:
            data = self._redis.get(f"{self._cfg.WORKER_PREFIX}{wid}")
            if data:
                workers.append(WorkerInfo(wid, json.loads(data)))
        return workers

    def get_healthy_workers(self) -> List[WorkerInfo]:
        """Return workers that are healthy and have capacity."""
        return [w for w in self.get_workers() if w.is_healthy and w.has_capacity]

    def get_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        data = self._redis.get(f"{self._cfg.WORKER_PREFIX}{worker_id}")
        if data:
            return WorkerInfo(worker_id, json.loads(data))
        return None

    # ------------------------------------------------------------------
    # Scheduling algorithms
    # ------------------------------------------------------------------

    def _select_round_robin(self, workers: List[WorkerInfo]) -> Optional[WorkerInfo]:
        """Select the next worker in round-robin order."""
        if not workers:
            return None
        with self._lock:
            self._rr_index = self._rr_index % len(workers)
            selected = workers[self._rr_index]
            self._rr_index += 1
        return selected

    def _select_least_loaded(self, workers: List[WorkerInfo]) -> Optional[WorkerInfo]:
        """Select the worker with the lowest load factor."""
        if not workers:
            return None
        return min(workers, key=lambda w: w.load_factor)

    def _select_worker(self, workers: List[WorkerInfo]) -> Optional[WorkerInfo]:
        """Dispatch to a worker using the configured algorithm."""
        selectors = {
            "round_robin": self._select_round_robin,
            "least_loaded": self._select_least_loaded,
            "priority": self._select_least_loaded,  # priority affects dequeue order, not worker selection
        }
        selector = selectors.get(self._algorithm, self._select_least_loaded)
        return selector(workers)

    # ------------------------------------------------------------------
    # Dispatch logic
    # ------------------------------------------------------------------

    def dispatch_one(self) -> Optional[str]:
        """
        Dequeue one task and assign it to a worker.

        Returns the task_id if dispatched, None otherwise.
        """
        workers = self.get_healthy_workers()
        if not workers:
            return None

        task_ids = self._queue.dequeue(count=1)
        if not task_ids:
            return None

        task_id = task_ids[0]
        worker = self._select_worker(workers)
        if worker is None:
            # Re-enqueue if no worker available
            self._queue.enqueue(task_id)
            return None

        # Assign task to worker via their personal queue
        assignment = json.dumps({"task_id": task_id, "assigned_at": time.time()})
        worker_queue_key = f"{self._cfg.WORKER_PREFIX}{worker.worker_id}:tasks"
        self._redis.rpush(worker_queue_key, assignment)

        self._queue.update_task(
            task_id,
            {
                "status": TaskStatus.DISPATCHED.value,
                "worker_id": worker.worker_id,
            },
        )

        logger.info(
            "Dispatched task %s to worker %s (algo=%s, load=%.2f)",
            task_id,
            worker.worker_id,
            self._algorithm,
            worker.load_factor,
        )
        return task_id

    def dispatch_batch(self, max_tasks: int = 10) -> List[str]:
        """Dispatch up to max_tasks in a single pass."""
        dispatched = []
        for _ in range(max_tasks):
            task_id = self.dispatch_one()
            if task_id is None:
                break
            dispatched.append(task_id)
        return dispatched

    # ------------------------------------------------------------------
    # Continuous dispatch loop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background dispatch loop."""
        if self._running:
            return
        self._running = True
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, daemon=True, name="dispatcher"
        )
        self._dispatch_thread.start()
        logger.info("Dispatcher started (algorithm=%s)", self._algorithm)

    def stop(self) -> None:
        """Stop the dispatch loop."""
        self._running = False
        if self._dispatch_thread:
            self._dispatch_thread.join(timeout=5)
        logger.info("Dispatcher stopped")

    def _dispatch_loop(self) -> None:
        """Continuously dispatch tasks from the queue."""
        while self._running:
            try:
                dispatched = self.dispatch_batch(max_tasks=10)
                if not dispatched:
                    time.sleep(0.25)
            except redis.ConnectionError:
                logger.error("Redis connection lost in dispatch loop, retrying...")
                time.sleep(2)
            except Exception:
                logger.exception("Unexpected error in dispatch loop")
                time.sleep(1)

    # ------------------------------------------------------------------
    # Dead worker reaping
    # ------------------------------------------------------------------

    def reap_dead_workers(self) -> List[str]:
        """
        Detect workers that have missed heartbeats and reassign their tasks.

        Returns list of reaped worker IDs.
        """
        reaped = []
        for worker in self.get_workers():
            if not worker.is_healthy and worker.status != "draining":
                logger.warning("Reaping dead worker: %s", worker.worker_id)
                self._reassign_worker_tasks(worker.worker_id)
                self._redis.srem(self._cfg.WORKER_SET_KEY, worker.worker_id)
                self._redis.delete(f"{self._cfg.WORKER_PREFIX}{worker.worker_id}")
                reaped.append(worker.worker_id)
        return reaped

    def _reassign_worker_tasks(self, worker_id: str) -> int:
        """Re-enqueue all tasks assigned to a dead worker."""
        worker_queue_key = f"{self._cfg.WORKER_PREFIX}{worker_id}:tasks"
        count = 0
        while True:
            assignment = self._redis.lpop(worker_queue_key)
            if assignment is None:
                break
            data = json.loads(assignment)
            task_id = data["task_id"]
            task = self._queue.get_task(task_id)
            if task and task["status"] in (
                TaskStatus.DISPATCHED.value,
                TaskStatus.RUNNING.value,
            ):
                self._queue.enqueue(task_id)
                count += 1
                logger.info("Re-enqueued task %s from dead worker %s", task_id, worker_id)

        return count
