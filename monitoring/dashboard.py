"""
Flask telemetry dashboard for task latency, throughput, and node utilization.

Provides both JSON API endpoints and an HTML dashboard for monitoring
the distributed scheduler in real time.
"""

import logging
import time

from flask import Flask, Response, jsonify, render_template_string

from config import get_config
from monitoring.metrics import get_metrics

logger = logging.getLogger(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Distributed Scheduler - Telemetry Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: #0f1419; color: #e1e8ed; }
        .header { background: #1a2332; padding: 20px 30px; border-bottom: 2px solid #2d3748;
                   display: flex; justify-content: space-between; align-items: center; }
        .header h1 { font-size: 1.5rem; color: #63b3ed; }
        .header .status { padding: 6px 16px; border-radius: 20px; font-size: 0.85rem; }
        .status-healthy { background: #22543d; color: #68d391; }
        .status-degraded { background: #744210; color: #f6ad55; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .card { background: #1a2332; border-radius: 12px; padding: 20px; border: 1px solid #2d3748; }
        .card h2 { font-size: 1rem; color: #a0aec0; margin-bottom: 15px; text-transform: uppercase;
                    letter-spacing: 0.05em; }
        .metric { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 10px; }
        .metric-label { color: #a0aec0; font-size: 0.9rem; }
        .metric-value { font-size: 1.5rem; font-weight: 700; color: #63b3ed; }
        .metric-value.success { color: #68d391; }
        .metric-value.danger { color: #fc8181; }
        .metric-value.warning { color: #f6ad55; }
        .worker-row { display: flex; justify-content: space-between; align-items: center;
                      padding: 10px 0; border-bottom: 1px solid #2d3748; }
        .worker-row:last-child { border-bottom: none; }
        .worker-name { font-weight: 600; }
        .worker-status { padding: 3px 10px; border-radius: 10px; font-size: 0.8rem; }
        .bar-container { background: #2d3748; border-radius: 6px; height: 8px; flex: 1; margin: 0 10px; }
        .bar-fill { height: 100%; border-radius: 6px; transition: width 0.5s; }
        .bar-fill.low { background: #68d391; }
        .bar-fill.medium { background: #f6ad55; }
        .bar-fill.high { background: #fc8181; }
        .task-row { display: grid; grid-template-columns: 1fr 100px 100px 80px; gap: 10px;
                    padding: 8px 0; border-bottom: 1px solid #2d3748; font-size: 0.85rem; }
        .task-id { font-family: monospace; color: #63b3ed; overflow: hidden; text-overflow: ellipsis; }
        .badge { padding: 2px 8px; border-radius: 8px; font-size: 0.75rem; text-align: center; }
        .badge-completed { background: #22543d; color: #68d391; }
        .badge-running { background: #2a4365; color: #63b3ed; }
        .badge-failed { background: #742a2a; color: #fc8181; }
        .badge-pending, .badge-queued { background: #744210; color: #f6ad55; }
        .alert-row { padding: 10px; margin-bottom: 8px; border-radius: 8px; font-size: 0.85rem; }
        .alert-warning { background: #744210; border-left: 4px solid #f6ad55; }
        .alert-critical { background: #742a2a; border-left: 4px solid #fc8181; }
        .alert-info { background: #2a4365; border-left: 4px solid #63b3ed; }
        .refresh-info { text-align: center; color: #4a5568; font-size: 0.8rem; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Distributed Scheduler - Telemetry Dashboard</h1>
        <div class="status" id="system-status">Loading...</div>
    </div>
    <div class="container">
        <div class="grid" id="metrics-grid"></div>
        <div class="grid">
            <div class="card" id="workers-card">
                <h2>Worker Nodes</h2>
                <div id="workers-list">Loading...</div>
            </div>
            <div class="card" id="tasks-card">
                <h2>Recent Tasks</h2>
                <div id="tasks-list">Loading...</div>
            </div>
        </div>
        <div class="card" id="alerts-card" style="margin-bottom: 20px;">
            <h2>Active Alerts</h2>
            <div id="alerts-list">No active alerts</div>
        </div>
        <div class="refresh-info">Auto-refreshes every 5 seconds</div>
    </div>
    <script>
        const API_BASE = window.location.protocol + '//' + window.location.hostname + ':5000';

        function barClass(pct) {
            if (pct < 50) return 'low';
            if (pct < 80) return 'medium';
            return 'high';
        }

        function badgeClass(status) {
            return 'badge badge-' + status;
        }

        async function fetchJSON(url) {
            try {
                const r = await fetch(url);
                return await r.json();
            } catch(e) { return null; }
        }

        async function refresh() {
            const [stats, workers, tasks, alerts] = await Promise.all([
                fetchJSON(API_BASE + '/api/stats'),
                fetchJSON(API_BASE + '/api/workers'),
                fetchJSON(API_BASE + '/api/tasks?limit=10'),
                fetchJSON(API_BASE.replace(':5000', ':5001') + '/dashboard/api/alerts'),
            ]);

            // System status
            const statusEl = document.getElementById('system-status');
            if (stats) {
                const healthy = stats.workers_healthy > 0;
                statusEl.textContent = healthy ? 'System Healthy' : 'System Degraded';
                statusEl.className = 'status ' + (healthy ? 'status-healthy' : 'status-degraded');
            }

            // Metrics cards
            if (stats) {
                const sc = stats.status_counts || {};
                document.getElementById('metrics-grid').innerHTML = `
                    <div class="card">
                        <h2>Queue</h2>
                        <div class="metric"><span class="metric-label">Queue Depth</span>
                            <span class="metric-value">${stats.queue_depth}</span></div>
                        <div class="metric"><span class="metric-label">Total Tasks</span>
                            <span class="metric-value">${stats.total_tasks}</span></div>
                    </div>
                    <div class="card">
                        <h2>Throughput</h2>
                        <div class="metric"><span class="metric-label">Completed</span>
                            <span class="metric-value success">${sc.completed || 0}</span></div>
                        <div class="metric"><span class="metric-label">Failed</span>
                            <span class="metric-value danger">${sc.failed || 0}</span></div>
                        <div class="metric"><span class="metric-label">Running</span>
                            <span class="metric-value">${sc.running || 0}</span></div>
                    </div>
                    <div class="card">
                        <h2>Latency</h2>
                        <div class="metric"><span class="metric-label">Avg Execution</span>
                            <span class="metric-value">${stats.avg_execution_time.toFixed(2)}s</span></div>
                        <div class="metric"><span class="metric-label">P95 Execution</span>
                            <span class="metric-value">${stats.p95_execution_time.toFixed(2)}s</span></div>
                    </div>
                    <div class="card">
                        <h2>Workers</h2>
                        <div class="metric"><span class="metric-label">Total</span>
                            <span class="metric-value">${stats.workers_total}</span></div>
                        <div class="metric"><span class="metric-label">Healthy</span>
                            <span class="metric-value success">${stats.workers_healthy}</span></div>
                        <div class="metric"><span class="metric-label">Algorithm</span>
                            <span class="metric-value" style="font-size:1rem">${stats.algorithm}</span></div>
                    </div>`;
            }

            // Workers
            if (workers && workers.workers) {
                const whtml = workers.workers.map(w => `
                    <div class="worker-row">
                        <span class="worker-name">${w.worker_id}</span>
                        <span class="worker-status" style="background:${w.is_healthy?'#22543d':'#742a2a'};
                            color:${w.is_healthy?'#68d391':'#fc8181'}">${w.is_healthy?'Healthy':'Unhealthy'}</span>
                        <div class="bar-container">
                            <div class="bar-fill ${barClass(w.load_factor*100)}"
                                 style="width:${Math.max(w.load_factor*100,2)}%"></div>
                        </div>
                        <span style="font-size:0.8rem;color:#a0aec0">
                            ${w.active_tasks}/${w.max_concurrent} tasks | CPU ${w.cpu_percent.toFixed(0)}%
                        </span>
                    </div>`).join('');
                document.getElementById('workers-list').innerHTML = whtml || '<div style="color:#4a5568">No workers registered</div>';
            }

            // Tasks
            if (tasks && tasks.tasks) {
                const thtml = tasks.tasks.map(t => `
                    <div class="task-row">
                        <span class="task-id">${t.task_id.substring(0,12)}...</span>
                        <span>${t.task_type}</span>
                        <span class="${badgeClass(t.status)}">${t.status}</span>
                        <span style="color:#a0aec0">${t.started_at ? ((t.completed_at||Date.now()/1000) - t.started_at).toFixed(1)+'s' : '-'}</span>
                    </div>`).join('');
                document.getElementById('tasks-list').innerHTML = thtml || '<div style="color:#4a5568">No tasks</div>';
            }

            // Alerts
            if (alerts && alerts.alerts) {
                const ahtml = alerts.alerts.map(a => `
                    <div class="alert-row alert-${a.severity}">
                        <strong>${a.title}</strong>: ${a.message}
                    </div>`).join('');
                document.getElementById('alerts-list').innerHTML = ahtml || 'No active alerts';
            }
        }

        refresh();
        setInterval(refresh, 5000);
    </script>
</body>
</html>
"""


def create_dashboard(dispatcher=None, alert_manager=None) -> Flask:
    """Factory function to create the monitoring dashboard Flask app."""

    app = Flask(__name__)

    @app.route("/")
    def dashboard():
        """Serve the telemetry dashboard HTML."""
        return render_template_string(DASHBOARD_HTML)

    @app.route("/metrics")
    def metrics():
        """Prometheus metrics endpoint."""
        return Response(get_metrics(), mimetype="text/plain; charset=utf-8")

    @app.route("/dashboard/api/stats")
    def dashboard_stats():
        """Return scheduler stats as JSON for the dashboard."""
        if dispatcher is None:
            return jsonify({"error": "Dispatcher not available"}), 503
        queue = dispatcher.task_queue
        stats = queue.get_stats()
        workers = dispatcher.get_workers()
        stats["workers_total"] = len(workers)
        stats["workers_healthy"] = sum(1 for w in workers if w.is_healthy)
        return jsonify(stats), 200

    @app.route("/dashboard/api/workers")
    def dashboard_workers():
        """Return worker details for the dashboard."""
        if dispatcher is None:
            return jsonify({"error": "Dispatcher not available"}), 503
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
                }
                for w in workers
            ]
        }), 200

    @app.route("/dashboard/api/alerts")
    def dashboard_alerts():
        """Return active alerts."""
        if alert_manager is None:
            return jsonify({"alerts": []}), 200
        alerts = alert_manager.get_alerts(active_only=True)
        return jsonify({"alerts": alerts}), 200

    @app.route("/dashboard/api/throughput")
    def dashboard_throughput():
        """Return throughput data over time."""
        if dispatcher is None:
            return jsonify({"error": "Dispatcher not available"}), 503
        stats = dispatcher.task_queue.get_stats()
        return jsonify({
            "completed": stats.get("completed_count", 0),
            "avg_execution_time": stats.get("avg_execution_time", 0),
            "p95_execution_time": stats.get("p95_execution_time", 0),
            "queue_depth": stats.get("queue_depth", 0),
            "timestamp": time.time(),
        }), 200

    return app
