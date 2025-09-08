# Distributed Task Scheduling System

A production-grade distributed task scheduler that dispatches compute workloads
across worker nodes with retry mechanisms, health monitoring, and telemetry dashboards.

## Architecture

```
                    +-------------------+
                    |   Flask REST API  |
                    |   (Submit/Monitor)|
                    +--------+----------+
                             |
                    +--------v----------+
                    |    Dispatcher     |
                    | (Round-Robin /    |
                    |  Least-Loaded /   |
                    |  Priority)        |
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Redis Queue     |
                    +----+----+----+----+
                         |    |    |
                +--------+   |   +--------+
                |            |            |
         +------v---+ +-----v----+ +-----v----+
         | Worker 1 | | Worker 2 | | Worker 3 |
         | Executor | | Executor | | Executor |
         | Heartbeat| | Heartbeat| | Heartbeat|
         | Retry    | | Retry    | | Retry    |
         +------+---+ +-----+----+ +-----+----+
                |            |            |
                +------+-----+-----+------+
                       |           |
              +--------v---+ +----v-----------+
              | Prometheus | | Alert Manager  |
              +--------+---+ +----------------+
                       |
              +--------v-----------+
              | Telemetry Dashboard|
              | (Latency, Through- |
              |  put, Utilization) |
              +--------------------+
```

## Quick Start

### Using Docker Compose (recommended)

```bash
docker-compose up --build --scale worker=3
```

### Local Development

```bash
pip install -r requirements.txt

# Start Redis (required)
redis-server

# Start the scheduler + API
python main.py --mode scheduler

# Start a worker (in another terminal)
python main.py --mode worker --worker-id worker-1

# Start the monitoring dashboard (in another terminal)
python main.py --mode dashboard
```

## REST API

| Method | Endpoint               | Description                     |
|--------|------------------------|---------------------------------|
| POST   | /api/tasks             | Submit a new task               |
| GET    | /api/tasks/<task_id>   | Get task status                 |
| GET    | /api/tasks             | List all tasks (with filters)   |
| DELETE | /api/tasks/<task_id>   | Cancel a task                   |
| GET    | /api/workers           | List registered workers         |
| GET    | /api/stats             | Scheduler statistics            |
| GET    | /metrics               | Prometheus metrics endpoint     |

### Submit a Task

```bash
curl -X POST http://localhost:5000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "compute_heavy",
    "payload": {"iterations": 1000000},
    "priority": 5,
    "max_retries": 3
  }'
```

## Task Types

- **compute_heavy** -- CPU-intensive numerical computation
- **io_heavy** -- Simulated I/O-bound workload
- **data_transform** -- Data transformation/aggregation pipeline
- **health_check** -- Lightweight health verification task

## Scheduling Algorithms

- **round_robin** -- Distributes tasks evenly across workers in sequence
- **least_loaded** -- Routes to the worker with the fewest active tasks
- **priority** -- Processes highest-priority tasks first, with least-loaded placement

## Monitoring

- **Prometheus metrics** at `http://localhost:5000/metrics`
- **Telemetry dashboard** at `http://localhost:5001`
- Tracks: task latency, throughput, queue depth, worker utilization, error rates

## Configuration

All configuration is managed via environment variables or `config.py`. Key settings:

| Variable                  | Default       | Description                    |
|---------------------------|---------------|--------------------------------|
| REDIS_HOST                | localhost     | Redis server hostname          |
| REDIS_PORT                | 6379          | Redis server port              |
| SCHEDULER_ALGORITHM       | least_loaded  | Scheduling algorithm           |
| WORKER_HEARTBEAT_INTERVAL | 5             | Heartbeat interval (seconds)   |
| WORKER_HEARTBEAT_TIMEOUT  | 15            | Worker timeout (seconds)       |
| MAX_RETRIES               | 3             | Default max task retries       |
| RETRY_BASE_DELAY          | 1.0           | Base delay for exp. backoff    |
| PROMETHEUS_PORT           | 8000          | Prometheus metrics port        |
