"""
Sample task definitions.

Provides compute-heavy, I/O-heavy, data transformation, and health check tasks.
Each task receives a payload dict and returns a result dict.
"""

import hashlib
import json
import logging
import math
import random
import time
from typing import Any, Dict

from tasks.registry import register_task

logger = logging.getLogger(__name__)


@register_task(
    task_type="compute_heavy",
    description="CPU-intensive numerical computation (prime sieve, hashing)",
    estimated_duration=10.0,
    resource_class="cpu",
)
def compute_heavy_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform CPU-intensive work: prime number sieve and iterative hashing.

    Payload:
        iterations (int): Number of hashing rounds (default: 100000)
        sieve_limit (int): Upper bound for prime sieve (default: 10000)
    """
    iterations = payload.get("iterations", 100_000)
    sieve_limit = payload.get("sieve_limit", 10_000)

    start = time.monotonic()

    # Sieve of Eratosthenes
    is_prime = [True] * (sieve_limit + 1)
    is_prime[0] = is_prime[1] = False
    for i in range(2, int(math.sqrt(sieve_limit)) + 1):
        if is_prime[i]:
            for j in range(i * i, sieve_limit + 1, i):
                is_prime[j] = False
    prime_count = sum(is_prime)

    # Iterative hashing
    data = b"distributed_scheduler_seed"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()

    elapsed = time.monotonic() - start
    final_hash = data.hex()

    logger.info(
        "compute_heavy completed: %d primes found, %d hash iterations in %.3fs",
        prime_count,
        iterations,
        elapsed,
    )

    return {
        "prime_count": prime_count,
        "sieve_limit": sieve_limit,
        "hash_iterations": iterations,
        "final_hash": final_hash[:32],
        "elapsed_seconds": round(elapsed, 4),
    }


@register_task(
    task_type="io_heavy",
    description="Simulated I/O-bound workload with sleeps and data serialization",
    estimated_duration=5.0,
    resource_class="io",
)
def io_heavy_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulate I/O-bound operations: network-like delays, data serialization.

    Payload:
        io_operations (int): Number of I/O operations to simulate (default: 10)
        delay_per_op (float): Seconds to sleep per operation (default: 0.2)
        data_size_kb (int): Size of data to serialize per op (default: 64)
    """
    io_operations = payload.get("io_operations", 10)
    delay_per_op = payload.get("delay_per_op", 0.2)
    data_size_kb = payload.get("data_size_kb", 64)

    start = time.monotonic()
    total_bytes_processed = 0

    for i in range(io_operations):
        # Simulate network/disk latency
        jitter = random.uniform(0.8, 1.2)
        time.sleep(delay_per_op * jitter)

        # Simulate data serialization/deserialization
        data = {
            "operation": i,
            "payload": "x" * (data_size_kb * 1024),
            "timestamp": time.time(),
        }
        serialized = json.dumps(data)
        json.loads(serialized)
        total_bytes_processed += len(serialized)

    elapsed = time.monotonic() - start

    logger.info(
        "io_heavy completed: %d ops, %d KB processed in %.3fs",
        io_operations,
        total_bytes_processed // 1024,
        elapsed,
    )

    return {
        "io_operations": io_operations,
        "total_bytes_processed": total_bytes_processed,
        "elapsed_seconds": round(elapsed, 4),
        "avg_op_time": round(elapsed / max(io_operations, 1), 4),
    }


@register_task(
    task_type="data_transform",
    description="Data transformation and aggregation pipeline",
    estimated_duration=3.0,
    resource_class="default",
)
def data_transform_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform data transformation: generate, filter, sort, aggregate.

    Payload:
        record_count (int): Number of records to generate (default: 10000)
        filter_threshold (float): Filter records above this value (default: 0.5)
    """
    record_count = payload.get("record_count", 10_000)
    filter_threshold = payload.get("filter_threshold", 0.5)

    start = time.monotonic()

    # Generate synthetic records
    records = [
        {
            "id": i,
            "value": random.random(),
            "category": random.choice(["A", "B", "C", "D"]),
            "score": random.gauss(50, 15),
        }
        for i in range(record_count)
    ]

    # Filter
    filtered = [r for r in records if r["value"] > filter_threshold]

    # Sort by score descending
    sorted_records = sorted(filtered, key=lambda r: r["score"], reverse=True)

    # Aggregate by category
    aggregation = {}
    for record in sorted_records:
        cat = record["category"]
        if cat not in aggregation:
            aggregation[cat] = {"count": 0, "total_score": 0.0, "min": float("inf"), "max": float("-inf")}
        agg = aggregation[cat]
        agg["count"] += 1
        agg["total_score"] += record["score"]
        agg["min"] = min(agg["min"], record["score"])
        agg["max"] = max(agg["max"], record["score"])

    for cat in aggregation:
        agg = aggregation[cat]
        agg["avg_score"] = round(agg["total_score"] / agg["count"], 2) if agg["count"] else 0
        agg["total_score"] = round(agg["total_score"], 2)
        agg["min"] = round(agg["min"], 2)
        agg["max"] = round(agg["max"], 2)

    elapsed = time.monotonic() - start

    logger.info(
        "data_transform completed: %d records -> %d filtered in %.3fs",
        record_count,
        len(filtered),
        elapsed,
    )

    return {
        "total_records": record_count,
        "filtered_records": len(filtered),
        "aggregation": aggregation,
        "top_5_scores": [round(r["score"], 2) for r in sorted_records[:5]],
        "elapsed_seconds": round(elapsed, 4),
    }


@register_task(
    task_type="health_check",
    description="Lightweight health verification task",
    estimated_duration=0.5,
    resource_class="default",
)
def health_check_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Quick health check that verifies the worker is operating correctly.

    Payload:
        echo (str): Optional string to echo back
    """
    start = time.monotonic()
    echo = payload.get("echo", "pong")

    # Basic arithmetic check
    assert 2 + 2 == 4, "Arithmetic verification failed"

    # Memory allocation check
    test_data = list(range(1000))
    assert len(test_data) == 1000, "Memory allocation check failed"

    elapsed = time.monotonic() - start

    return {
        "status": "healthy",
        "echo": echo,
        "elapsed_seconds": round(elapsed, 6),
    }
