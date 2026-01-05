"""
Alerting for failed tasks and unhealthy workers.

Monitors system health and fires alerts when thresholds are breached.
Supports logging-based alerts and optional webhook notifications.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional

import requests

from config import get_config

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents a single alert event."""

    alert_id: str
    severity: AlertSeverity
    title: str
    message: str
    source: str
    timestamp: float = field(default_factory=time.time)
    resolved: bool = False
    resolved_at: Optional[float] = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "source": self.source,
            "timestamp": self.timestamp,
            "resolved": self.resolved,
            "resolved_at": self.resolved_at,
            "metadata": self.metadata,
        }


class AlertManager:
    """
    Monitors system state and fires alerts when thresholds are exceeded.

    Alert conditions:
      - High failure rate (too many failed tasks in window)
      - Queue depth exceeding threshold
      - Worker utilization exceeding threshold
      - Workers going unhealthy
    """

    def __init__(self, dispatcher=None):
        self._cfg = get_config()
        self._dispatcher = dispatcher
        self._alerts: List[Alert] = []
        self._active_alerts: Dict[str, Alert] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._callbacks: List[Callable[[Alert], None]] = []

    def register_callback(self, callback: Callable[[Alert], None]) -> None:
        """Register a function to be called when an alert fires."""
        self._callbacks.append(callback)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="alert-manager"
        )
        self._thread.start()
        logger.info("Alert manager started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def get_alerts(self, active_only: bool = False, limit: int = 50) -> List[dict]:
        """Return recent alerts."""
        with self._lock:
            if active_only:
                alerts = list(self._active_alerts.values())
            else:
                alerts = list(self._alerts)
        return [a.to_dict() for a in alerts[-limit:]]

    def _fire_alert(self, alert_key: str, alert: Alert) -> None:
        """Fire an alert if not already active for this key."""
        with self._lock:
            if alert_key in self._active_alerts:
                return  # Already active, skip duplicate
            self._active_alerts[alert_key] = alert
            self._alerts.append(alert)

        logger.warning(
            "ALERT [%s] %s: %s", alert.severity.value.upper(), alert.title, alert.message
        )

        for cb in self._callbacks:
            try:
                cb(alert)
            except Exception:
                logger.exception("Alert callback failed")

        self._send_webhook(alert)

    def _resolve_alert(self, alert_key: str) -> None:
        """Resolve an active alert."""
        with self._lock:
            if alert_key not in self._active_alerts:
                return
            alert = self._active_alerts.pop(alert_key)
            alert.resolved = True
            alert.resolved_at = time.time()

        logger.info("RESOLVED [%s] %s", alert.severity.value, alert.title)

    def _send_webhook(self, alert: Alert) -> None:
        """Send alert to configured webhook URL."""
        url = self._cfg.ALERT_WEBHOOK_URL
        if not url:
            return
        try:
            requests.post(
                url,
                json=alert.to_dict(),
                timeout=5,
                headers={"Content-Type": "application/json"},
            )
        except Exception:
            logger.exception("Failed to send webhook alert")

    # ------------------------------------------------------------------
    # Monitor loop
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> None:
        while self._running:
            try:
                self._check_conditions()
            except Exception:
                logger.exception("Error in alert monitor loop")
            time.sleep(10)

    def _check_conditions(self) -> None:
        if self._dispatcher is None:
            return

        queue = self._dispatcher.task_queue
        stats = queue.get_stats()
        workers = self._dispatcher.get_workers()

        self._check_queue_depth(stats)
        self._check_failed_tasks(stats)
        self._check_worker_health(workers)
        self._check_worker_utilization(workers)

    def _check_queue_depth(self, stats: dict) -> None:
        depth = stats.get("queue_depth", 0)
        key = "queue_depth_high"
        if depth > self._cfg.ALERT_QUEUE_DEPTH_THRESHOLD:
            self._fire_alert(
                key,
                Alert(
                    alert_id=f"alert_{key}_{int(time.time())}",
                    severity=AlertSeverity.WARNING,
                    title="High Queue Depth",
                    message=f"Queue depth is {depth} (threshold: {self._cfg.ALERT_QUEUE_DEPTH_THRESHOLD})",
                    source="alert_manager",
                    metadata={"queue_depth": depth},
                ),
            )
        else:
            self._resolve_alert(key)

    def _check_failed_tasks(self, stats: dict) -> None:
        failed = stats.get("status_counts", {}).get("failed", 0)
        key = "failed_tasks_high"
        if failed > self._cfg.ALERT_FAILED_TASK_THRESHOLD:
            self._fire_alert(
                key,
                Alert(
                    alert_id=f"alert_{key}_{int(time.time())}",
                    severity=AlertSeverity.CRITICAL,
                    title="High Task Failure Rate",
                    message=f"{failed} tasks have failed (threshold: {self._cfg.ALERT_FAILED_TASK_THRESHOLD})",
                    source="alert_manager",
                    metadata={"failed_count": failed},
                ),
            )
        else:
            self._resolve_alert(key)

    def _check_worker_health(self, workers) -> None:
        unhealthy = [w for w in workers if not w.is_healthy]
        key = "workers_unhealthy"
        if unhealthy:
            self._fire_alert(
                key,
                Alert(
                    alert_id=f"alert_{key}_{int(time.time())}",
                    severity=AlertSeverity.WARNING,
                    title="Unhealthy Workers Detected",
                    message=f"{len(unhealthy)} worker(s) are unhealthy: {[w.worker_id for w in unhealthy]}",
                    source="alert_manager",
                    metadata={"unhealthy_workers": [w.worker_id for w in unhealthy]},
                ),
            )
        else:
            self._resolve_alert(key)

    def _check_worker_utilization(self, workers) -> None:
        threshold = self._cfg.ALERT_WORKER_UTILIZATION_THRESHOLD
        overloaded = [w for w in workers if w.load_factor > threshold]
        key = "workers_overloaded"
        if overloaded:
            self._fire_alert(
                key,
                Alert(
                    alert_id=f"alert_{key}_{int(time.time())}",
                    severity=AlertSeverity.WARNING,
                    title="Workers Overloaded",
                    message=f"{len(overloaded)} worker(s) above {threshold*100:.0f}% utilization",
                    source="alert_manager",
                    metadata={
                        "overloaded_workers": [
                            {"id": w.worker_id, "load": round(w.load_factor, 2)}
                            for w in overloaded
                        ]
                    },
                ),
            )
        else:
            self._resolve_alert(key)
