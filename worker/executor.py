"""
Task executor on worker nodes.

Polls for assigned tasks, executes them with timeout enforcement,
and reports results back through the task queue. Integrates with the
retry mechanism and heartbeat reporter.
"""

import json
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Optional

import redis

from config import get_config
from scheduler.queue import TaskQueue, TaskStatus
from tasks.registry import registry
from worker.heartbeat import HeartbeatReporter
from worker.retry import RetryContext, RetryPolicy

logger = logging.getLogger(__name__)


class TaskExecutor:
    """
    Worker-side task executor.

    Pulls task assignments from its personal Redis queue, runs the
    corresponding handler, and reports success/failure back. Supports
    concurrent execution up to a configurable limit.
    """

    def __init__(
        self,
        worker_id: str,
        redis_client: Optional[redis.Redis] = None,
        max_concurrent: Optional[int] = None,
    ):
        self._cfg = get_config()
        self.worker_id = worker_id
        self._max_concurrent = max_concurrent or self._cfg.WORKER_MAX_CONCURRENT_TASKS

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

        self._queue = TaskQueue(self._redis)
        self._heartbeat = HeartbeatReporter(
            worker_id=worker_id,
            redis_client=self._redis,
            max_concurrent=self._max_concurrent,
        )
        self._pool = ThreadPoolExecutor(max_workers=self._max_concurrent)
        self._running = False
        self._poll_thread: Optional[threading.Thread] = None

    @property
    def heartbeat(self) -> HeartbeatReporter:
        return self._heartbeat

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the executor: heartbeat + task polling."""
        if self._running:
            return
        self._running = True
        self._heartbeat.start()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name=f"executor-{self.worker_id}"
        )
        self._poll_thread.start()
        logger.info(
            "Executor started: worker=%s, max_concurrent=%d",
            self.worker_id,
            self._max_concurrent,
        )

    def stop(self) -> None:
        """Gracefully stop the executor."""
        logger.info("Stopping executor %s...", self.worker_id)
        self._running = False
        self._heartbeat.set_draining()

        # Wait for poll thread
        if self._poll_thread:
            self._poll_thread.join(timeout=5)

        # Shut down thread pool (let running tasks finish)
        self._pool.shutdown(wait=True, cancel_futures=False)
        self._heartbeat.stop()
        logger.info("Executor %s stopped", self.worker_id)

    def _poll_loop(self) -> None:
        """Poll for assigned tasks and submit them for execution."""
        worker_queue_key = f"{self._cfg.WORKER_PREFIX}{self.worker_id}:tasks"

        while self._running:
            try:
                # Only poll if we have capacity
                if self._heartbeat.active_tasks >= self._max_concurrent:
                    time.sleep(self._cfg.WORKER_POLL_INTERVAL)
                    continue

                # Blocking pop with timeout so we can check _running periodically
                result = self._redis.blpop(worker_queue_key, timeout=1)
                if result is None:
                    continue

                _, assignment_data = result
                assignment = json.loads(assignment_data)
                task_id = assignment["task_id"]

                # Submit to thread pool
                self._pool.submit(self._execute_task, task_id)

            except redis.ConnectionError:
                logger.error("Redis connection lost in poll loop, retrying...")
                time.sleep(2)
            except Exception:
                logger.exception("Error in poll loop")
                time.sleep(1)

    # ------------------------------------------------------------------
    # Task execution with retry
    # ------------------------------------------------------------------

    def _execute_task(self, task_id: str) -> None:
        """Execute a single task with retry logic."""
        task = self._queue.get_task(task_id)
        if task is None:
            logger.error("Task %s not found, skipping", task_id)
            return

        task_type = task["task_type"]
        handler = registry.get_handler(task_type)
        if handler is None:
            logger.error("No handler for task type %s", task_type)
            self._queue.mark_failed(task_id, f"Unknown task type: {task_type}")
            return

        # Mark running
        self._queue.mark_running(task_id, self.worker_id)
        self._heartbeat.task_started()

        policy = RetryPolicy(max_retries=task.get("max_retries", 3))
        ctx = RetryContext(policy)
        timeout = task.get("timeout", 300)

        while ctx.should_continue:
            try:
                result = self._run_with_timeout(handler, task["payload"], timeout)
                ctx.mark_success(result)
            except Exception as e:
                ctx.mark_failure(e)

        # Report outcome
        if ctx.succeeded:
            self._queue.mark_completed(task_id, ctx.result)
            self._heartbeat.task_completed()
            logger.info(
                "Task %s completed successfully (attempts=%d, total_backoff=%.1fs)",
                task_id,
                ctx.attempt + 1,
                ctx.total_delay,
            )
        else:
            error_msg = str(ctx.last_error)[:500] if ctx.last_error else "Unknown error"
            self._queue.mark_failed(task_id, error_msg)
            self._heartbeat.task_failed()
            logger.error(
                "Task %s failed permanently after %d attempts: %s",
                task_id,
                ctx.attempt,
                error_msg[:200],
            )

    def _run_with_timeout(self, handler, payload: dict, timeout: int):
        """
        Run a handler with a timeout using a thread pool future.

        Note: This provides cooperative timeout. CPU-bound tasks that do not
        release the GIL cannot be forcibly interrupted, but the timeout will
        still trigger after the current operation completes.
        """
        future = self._pool.submit(handler, payload)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeout:
            future.cancel()
            raise TimeoutError(f"Task exceeded {timeout}s timeout")
