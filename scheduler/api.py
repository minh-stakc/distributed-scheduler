"""
Flask REST API for submitting and monitoring tasks.

Provides endpoints for task submission, status queries, worker listing,
and scheduler statistics.
"""

import logging

from flask import Flask, jsonify, request

from scheduler.dispatcher import Dispatcher
from scheduler.queue import TaskQueue, TaskStatus
from tasks.registry import registry

logger = logging.getLogger(__name__)


def create_api(dispatcher: Dispatcher) -> Flask:
    """Factory function to create the Flask API app."""

    app = Flask(__name__)
    queue: TaskQueue = dispatcher.task_queue

    # ------------------------------------------------------------------
    # Task endpoints
    # ------------------------------------------------------------------

    @app.route("/api/tasks", methods=["POST"])
    def submit_task():
        """Submit a new task for execution."""
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        task_type = data.get("task_type")
        if not task_type:
            return jsonify({"error": "task_type is required"}), 400

        if not registry.is_registered(task_type):
            available = [t["task_type"] for t in registry.list_task_types()]
            return jsonify({
                "error": f"Unknown task_type: {task_type}",
                "available_types": available,
            }), 400

        payload = data.get("payload", {})
        priority = data.get("priority", 0)
        max_retries = data.get("max_retries", 3)
        timeout = data.get("timeout", 300)

        task = queue.create_task(
            task_type=task_type,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            timeout=timeout,
        )
        queue.enqueue(task["task_id"])

        return jsonify({
            "task_id": task["task_id"],
            "status": task["status"],
            "message": "Task submitted and queued",
        }), 201

    @app.route("/api/tasks/<task_id>", methods=["GET"])
    def get_task(task_id):
        """Get the current status and details of a task."""
        task = queue.get_task(task_id)
        if task is None:
            return jsonify({"error": "Task not found"}), 404
        return jsonify(task), 200

    @app.route("/api/tasks", methods=["GET"])
    def list_tasks():
        """List tasks with optional status filter."""
        status = request.args.get("status")
        limit = min(int(request.args.get("limit", 100)), 1000)
        offset = int(request.args.get("offset", 0))

        if status and status not in [s.value for s in TaskStatus]:
            return jsonify({"error": f"Invalid status: {status}"}), 400

        tasks = queue.list_tasks(status=status, limit=limit, offset=offset)
        return jsonify({
            "tasks": tasks,
            "count": len(tasks),
            "limit": limit,
            "offset": offset,
        }), 200

    @app.route("/api/tasks/<task_id>", methods=["DELETE"])
    def cancel_task(task_id):
        """Cancel a pending or queued task."""
        task = queue.get_task(task_id)
        if task is None:
            return jsonify({"error": "Task not found"}), 404

        success = queue.cancel_task(task_id)
        if not success:
            return jsonify({
                "error": "Cannot cancel task",
                "status": task["status"],
            }), 409

        return jsonify({"task_id": task_id, "status": "cancelled"}), 200

    # ------------------------------------------------------------------
    # Worker endpoints
    # ------------------------------------------------------------------

    @app.route("/api/workers", methods=["GET"])
    def list_workers():
        """List all registered workers with their current state."""
        workers = dispatcher.get_workers()
        return jsonify({
            "workers": [
                {
                    "worker_id": w.worker_id,
                    "status": w.status,
                    "is_healthy": w.is_healthy,
                    "active_tasks": w.active_tasks,
                    "max_concurrent": w.max_concurrent,
                    "load_factor": round(w.load_factor, 3),
                    "cpu_percent": w.cpu_percent,
                    "memory_percent": w.memory_percent,
                    "completed_tasks": w.completed_tasks,
                    "failed_tasks": w.failed_tasks,
                    "last_heartbeat": w.last_heartbeat,
                }
                for w in workers
            ],
            "total": len(workers),
            "healthy": sum(1 for w in workers if w.is_healthy),
        }), 200

    # ------------------------------------------------------------------
    # Stats / Health
    # ------------------------------------------------------------------

    @app.route("/api/stats", methods=["GET"])
    def get_stats():
        """Return scheduler statistics."""
        stats = queue.get_stats()
        workers = dispatcher.get_workers()
        stats["workers_total"] = len(workers)
        stats["workers_healthy"] = sum(1 for w in workers if w.is_healthy)
        stats["algorithm"] = dispatcher._algorithm
        return jsonify(stats), 200

    @app.route("/api/task-types", methods=["GET"])
    def list_task_types():
        """List all available task types."""
        return jsonify({"task_types": registry.list_task_types()}), 200

    @app.route("/health", methods=["GET"])
    def health_check():
        """Health check endpoint."""
        redis_ok = queue.ping()
        return jsonify({
            "status": "healthy" if redis_ok else "degraded",
            "redis": "connected" if redis_ok else "disconnected",
        }), 200 if redis_ok else 503

    # ------------------------------------------------------------------
    # Error handlers
    # ------------------------------------------------------------------

    @app.errorhandler(404)
    def not_found(_):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def internal_error(_):
        return jsonify({"error": "Internal server error"}), 500

    return app
