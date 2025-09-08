"""
Centralized configuration for the Distributed Task Scheduling System.

All settings can be overridden via environment variables.
"""

import os


class Config:
    """Base configuration."""

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
    REDIS_URL = os.getenv(
        "REDIS_URL",
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    )

    # Scheduler
    SCHEDULER_ALGORITHM = os.getenv("SCHEDULER_ALGORITHM", "least_loaded")
    TASK_QUEUE_KEY = "distributed_scheduler:task_queue"
    PRIORITY_QUEUE_KEY = "distributed_scheduler:priority_queue"
    TASK_PREFIX = "distributed_scheduler:task:"
    WORKER_PREFIX = "distributed_scheduler:worker:"
    WORKER_SET_KEY = "distributed_scheduler:workers"
    STATS_KEY = "distributed_scheduler:stats"

    # Worker
    WORKER_ID = os.getenv("WORKER_ID", None)
    WORKER_HEARTBEAT_INTERVAL = int(os.getenv("WORKER_HEARTBEAT_INTERVAL", 5))
    WORKER_HEARTBEAT_TIMEOUT = int(os.getenv("WORKER_HEARTBEAT_TIMEOUT", 15))
    WORKER_MAX_CONCURRENT_TASKS = int(os.getenv("WORKER_MAX_CONCURRENT_TASKS", 4))
    WORKER_POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL", 0.5))

    # Retry
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", 1.0))
    RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", 60.0))
    RETRY_EXPONENTIAL_BASE = float(os.getenv("RETRY_EXPONENTIAL_BASE", 2.0))
    RETRY_JITTER = bool(os.getenv("RETRY_JITTER", "true").lower() == "true")

    # API
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 5000))
    API_DEBUG = os.getenv("API_DEBUG", "false").lower() == "true"

    # Monitoring
    DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", 5001))
    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8000))
    METRICS_COLLECTION_INTERVAL = int(os.getenv("METRICS_COLLECTION_INTERVAL", 10))

    # Alerting
    ALERT_FAILED_TASK_THRESHOLD = int(os.getenv("ALERT_FAILED_TASK_THRESHOLD", 5))
    ALERT_QUEUE_DEPTH_THRESHOLD = int(os.getenv("ALERT_QUEUE_DEPTH_THRESHOLD", 100))
    ALERT_WORKER_UTILIZATION_THRESHOLD = float(
        os.getenv("ALERT_WORKER_UTILIZATION_THRESHOLD", 0.9)
    )
    ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", None)


class DevelopmentConfig(Config):
    """Development configuration."""

    API_DEBUG = True


class ProductionConfig(Config):
    """Production configuration."""

    API_DEBUG = False


class DockerConfig(Config):
    """Docker configuration with service discovery defaults."""

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")


def get_config() -> Config:
    """Return the appropriate configuration based on environment."""
    env = os.getenv("APP_ENV", "development")
    configs = {
        "development": DevelopmentConfig,
        "production": ProductionConfig,
        "docker": DockerConfig,
    }
    return configs.get(env, DevelopmentConfig)()
