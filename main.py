"""
Entry point for the Distributed Task Scheduling System.

Supports running in three modes:
  - scheduler: Starts the dispatcher, REST API, and metrics collector
  - worker:    Starts a worker executor with heartbeat reporting
  - dashboard: Starts the telemetry monitoring dashboard

Usage:
    python main.py --mode scheduler
    python main.py --mode worker --worker-id worker-1
    python main.py --mode dashboard
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
import uuid

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def run_scheduler(cfg):
    """Run the scheduler: dispatcher + REST API + metrics + alerts."""
    # Import task definitions to register them
    import tasks.definitions  # noqa: F401

    from monitoring.alerts import AlertManager
    from monitoring.dashboard import create_dashboard
    from monitoring.metrics import MetricsCollector
    from scheduler.api import create_api
    from scheduler.dispatcher import Dispatcher

    dispatcher = Dispatcher()
    dispatcher.start()

    # Start metrics collection
    metrics_collector = MetricsCollector(dispatcher=dispatcher)
    metrics_collector.start(expose_port=cfg.PROMETHEUS_PORT)

    # Start alert manager
    alert_manager = AlertManager(dispatcher=dispatcher)
    alert_manager.start()

    # Start dashboard in a background thread
    dashboard_app = create_dashboard(
        dispatcher=dispatcher, alert_manager=alert_manager
    )
    dashboard_thread = threading.Thread(
        target=lambda: dashboard_app.run(
            host=cfg.DASHBOARD_HOST, port=cfg.DASHBOARD_PORT, use_reloader=False
        ),
        daemon=True,
        name="dashboard",
    )
    dashboard_thread.start()
    logger.info("Dashboard available at http://%s:%d", cfg.DASHBOARD_HOST, cfg.DASHBOARD_PORT)

    # Start dead worker reaper
    def reaper_loop():
        while True:
            try:
                reaped = dispatcher.reap_dead_workers()
                if reaped:
                    logger.info("Reaped dead workers: %s", reaped)
            except Exception:
                logger.exception("Error in reaper loop")
            time.sleep(cfg.WORKER_HEARTBEAT_TIMEOUT)

    reaper_thread = threading.Thread(target=reaper_loop, daemon=True, name="reaper")
    reaper_thread.start()

    # Start Flask API (blocks)
    api_app = create_api(dispatcher)
    logger.info(
        "Scheduler API starting on http://%s:%d (algorithm=%s)",
        cfg.API_HOST,
        cfg.API_PORT,
        cfg.SCHEDULER_ALGORITHM,
    )

    # Graceful shutdown
    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received")
        dispatcher.stop()
        metrics_collector.stop()
        alert_manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    api_app.run(host=cfg.API_HOST, port=cfg.API_PORT, use_reloader=False)


def run_worker(cfg, worker_id: str):
    """Run a worker executor."""
    # Import task definitions to register them
    import tasks.definitions  # noqa: F401

    from worker.executor import TaskExecutor

    executor = TaskExecutor(worker_id=worker_id)

    def shutdown_handler(signum, frame):
        logger.info("Worker %s shutting down...", worker_id)
        executor.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    executor.start()
    logger.info("Worker %s started, waiting for tasks...", worker_id)

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        executor.stop()


def run_dashboard(cfg):
    """Run the standalone monitoring dashboard."""
    import redis

    from monitoring.alerts import AlertManager
    from monitoring.dashboard import create_dashboard
    from scheduler.dispatcher import Dispatcher

    redis_client = redis.Redis(
        host=cfg.REDIS_HOST,
        port=cfg.REDIS_PORT,
        db=cfg.REDIS_DB,
        password=cfg.REDIS_PASSWORD,
        decode_responses=True,
    )

    dispatcher = Dispatcher(redis_client=redis_client)
    alert_manager = AlertManager(dispatcher=dispatcher)
    alert_manager.start()

    dashboard_app = create_dashboard(
        dispatcher=dispatcher, alert_manager=alert_manager
    )
    logger.info("Dashboard starting on http://%s:%d", cfg.DASHBOARD_HOST, cfg.DASHBOARD_PORT)
    dashboard_app.run(
        host=cfg.DASHBOARD_HOST, port=cfg.DASHBOARD_PORT, use_reloader=False
    )


def main():
    parser = argparse.ArgumentParser(description="Distributed Task Scheduling System")
    parser.add_argument(
        "--mode",
        choices=["scheduler", "worker", "dashboard"],
        required=True,
        help="Component to run",
    )
    parser.add_argument(
        "--worker-id",
        default=None,
        help="Worker ID (auto-generated if not provided)",
    )
    parser.add_argument(
        "--algorithm",
        choices=["round_robin", "least_loaded", "priority"],
        default=None,
        help="Override scheduling algorithm",
    )

    args = parser.parse_args()
    cfg = get_config()

    if args.algorithm:
        os.environ["SCHEDULER_ALGORITHM"] = args.algorithm
        cfg.SCHEDULER_ALGORITHM = args.algorithm

    logger.info("Starting in %s mode", args.mode)
    logger.info("Redis: %s:%d", cfg.REDIS_HOST, cfg.REDIS_PORT)

    if args.mode == "scheduler":
        run_scheduler(cfg)

    elif args.mode == "worker":
        worker_id = args.worker_id or cfg.WORKER_ID or f"worker-{uuid.uuid4().hex[:8]}"
        run_worker(cfg, worker_id)

    elif args.mode == "dashboard":
        run_dashboard(cfg)


if __name__ == "__main__":
    main()
