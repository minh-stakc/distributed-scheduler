"""
Task queue backed by Redis.

Provides reliable task queuing with priority support, atomic operations,
and dead-letter handling for failed tasks.
"""

import json
import logging
import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

import redis

from config import get_config

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class TaskQueue:
    """Redis-backed task queue with priority support."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        if redis_client is not None:
            self._redis = redis_client
        else:
            cfg = get_config()
            self._redis = redis.Redis(
                host=cfg.REDIS_HOST,
                port=cfg.REDIS_PORT,
                db=cfg.REDIS_DB,
                password=cfg.REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
        self._cfg = get_config()

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self._redis.ping()
        except redis.ConnectionError:
            return False

    def create_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        priority: int = 0,
        max_retries: int = 3,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """
        Create a new task and store it in Redis.

        Args:
            task_type: Registered task type name.
            payload: Task-specific parameters.
            priority: Priority level (higher = more urgent, 0-10).
            max_retries: Maximum retry attempts on failure.
            timeout: Task execution timeout in seconds.

        Returns:
            Full task record dict.
        """
        task_id = str(uuid.uuid4())
        now = time.time()

        task = {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload,
            "priority": min(max(priority, 0), 10),
            "status": TaskStatus.PENDING.value,
            "max_retries": max_retries,
            "retry_count": 0,
            "timeout": timeout,
            "created_at": now,
            "updated_at": now,
            "queued_at": None,
            "started_at": None,
            "completed_at": None,
            "worker_id": None,
            "result": None,
            "error": None,
        }

        # Store task data
        task_key = f"{self._cfg.TASK_PREFIX}{task_id}"
        self._redis.set(task_key, json.dumps(task), ex=86400)  # 24h TTL

        logger.info("Created task %s (type=%s, priority=%d)", task_id, task_type, priority)
        return task

    def enqueue(self, task_id: str) -> bool:
        """
        Add a task to the queue. Uses a sorted set for priority ordering.

        Tasks are scored by: (10 - priority) * 1e12 + timestamp
        This ensures higher-priority tasks are dequeued first, with FIFO
        ordering within the same priority level.
        """
        task = self.get_task(task_id)
        if task is None:
            logger.error("Cannot enqueue: task %s not found", task_id)
            return False

        priority = task.get("priority", 0)
        score = (10 - priority) * 1e12 + time.time()

        pipe = self._redis.pipeline()
        pipe.zadd(self._cfg.PRIORITY_QUEUE_KEY, {task_id: score})
        self._update_task_fields(
            task_id,
            {"status": TaskStatus.QUEUED.value, "queued_at": time.time(), "updated_at": time.time()},
            pipe=pipe,
        )
        pipe.execute()

        logger.info("Enqueued task %s (priority=%d)", task_id, priority)
        return True

    def dequeue(self, count: int = 1) -> List[str]:
        """
        Atomically dequeue up to `count` tasks from the priority queue.

        Returns list of task IDs, ordered by priority then FIFO.
        """
        task_ids = []
        for _ in range(count):
            # ZPOPMIN returns the lowest-scored element (highest priority)
            result = self._redis.zpopmin(self._cfg.PRIORITY_QUEUE_KEY, count=1)
            if not result:
                break
            task_id = result[0][0]
            self._update_task_fields(
                task_id,
                {"status": TaskStatus.DISPATCHED.value, "updated_at": time.time()},
            )
            task_ids.append(task_id)

        return task_ids

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task record by ID."""
        task_key = f"{self._cfg.TASK_PREFIX}{task_id}"
        data = self._redis.get(task_key)
        if data is None:
            return None
        return json.loads(data)

    def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update fields on a task record."""
        updates["updated_at"] = time.time()
        return self._update_task_fields(task_id, updates)

    def _update_task_fields(
        self,
        task_id: str,
        fields: Dict[str, Any],
        pipe: Optional[redis.client.Pipeline] = None,
    ) -> bool:
        """Internal helper to update task fields in Redis."""
        task_key = f"{self._cfg.TASK_PREFIX}{task_id}"
        client = pipe or self._redis

        task_data = self._redis.get(task_key)
        if task_data is None:
            return False

        task = json.loads(task_data)
        task.update(fields)
        client.set(task_key, json.dumps(task), ex=86400)
        return True

    def mark_running(self, task_id: str, worker_id: str) -> bool:
        """Mark a task as running on a specific worker."""
        return self.update_task(
            task_id,
            {
                "status": TaskStatus.RUNNING.value,
                "worker_id": worker_id,
                "started_at": time.time(),
            },
        )

    def mark_completed(self, task_id: str, result: Any) -> bool:
        """Mark a task as successfully completed."""
        return self.update_task(
            task_id,
            {
                "status": TaskStatus.COMPLETED.value,
                "result": result,
                "completed_at": time.time(),
            },
        )

    def mark_failed(self, task_id: str, error: str) -> bool:
        """Mark a task as failed."""
        return self.update_task(
            task_id,
            {
                "status": TaskStatus.FAILED.value,
                "error": error,
                "completed_at": time.time(),
            },
        )

    def mark_retrying(self, task_id: str) -> bool:
        """Mark a task for retry and re-enqueue it."""
        task = self.get_task(task_id)
        if task is None:
            return False

        self.update_task(
            task_id,
            {
                "status": TaskStatus.RETRYING.value,
                "retry_count": task["retry_count"] + 1,
                "worker_id": None,
                "started_at": None,
                "error": None,
            },
        )
        return self.enqueue(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or queued task."""
        task = self.get_task(task_id)
        if task is None:
            return False

        if task["status"] in (TaskStatus.COMPLETED.value, TaskStatus.CANCELLED.value):
            return False

        # Remove from queue if present
        self._redis.zrem(self._cfg.PRIORITY_QUEUE_KEY, task_id)

        return self.update_task(
            task_id,
            {"status": TaskStatus.CANCELLED.value, "completed_at": time.time()},
        )

    def queue_depth(self) -> int:
        """Return the number of tasks in the priority queue."""
        return self._redis.zcard(self._cfg.PRIORITY_QUEUE_KEY)

    def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List tasks, optionally filtered by status.

        Note: This scans task keys. For production use at scale,
        maintain a secondary index.
        """
        pattern = f"{self._cfg.TASK_PREFIX}*"
        tasks = []
        cursor = 0

        while True:
            cursor, keys = self._redis.scan(cursor, match=pattern, count=200)
            for key in keys:
                data = self._redis.get(key)
                if data:
                    task = json.loads(data)
                    if status is None or task.get("status") == status:
                        tasks.append(task)
            if cursor == 0:
                break

        # Sort by created_at descending
        tasks.sort(key=lambda t: t.get("created_at", 0), reverse=True)
        return tasks[offset : offset + limit]

    def get_stats(self) -> Dict[str, Any]:
        """Return aggregate queue statistics."""
        all_tasks = self.list_tasks(limit=10000)

        status_counts = {}
        for task in all_tasks:
            s = task.get("status", "unknown")
            status_counts[s] = status_counts.get(s, 0) + 1

        completed = [t for t in all_tasks if t["status"] == TaskStatus.COMPLETED.value]
        latencies = []
        for t in completed:
            if t.get("started_at") and t.get("completed_at"):
                latencies.append(t["completed_at"] - t["started_at"])

        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0

        return {
            "total_tasks": len(all_tasks),
            "queue_depth": self.queue_depth(),
            "status_counts": status_counts,
            "completed_count": len(completed),
            "avg_execution_time": round(avg_latency, 4),
            "p95_execution_time": round(
                sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0.0, 4
            ),
        }
