"""
Retry mechanism with exponential backoff and jitter.

Provides configurable retry policies for handling transient task failures.
"""

import logging
import random
import time
from typing import Optional

from config import get_config

logger = logging.getLogger(__name__)


class RetryPolicy:
    """
    Configurable retry policy with exponential backoff and optional jitter.

    Backoff formula:
        delay = min(base_delay * (exponential_base ** attempt), max_delay)
        if jitter: delay = random.uniform(0, delay)
    """

    def __init__(
        self,
        max_retries: Optional[int] = None,
        base_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        exponential_base: Optional[float] = None,
        jitter: Optional[bool] = None,
    ):
        cfg = get_config()
        self.max_retries = max_retries if max_retries is not None else cfg.MAX_RETRIES
        self.base_delay = base_delay if base_delay is not None else cfg.RETRY_BASE_DELAY
        self.max_delay = max_delay if max_delay is not None else cfg.RETRY_MAX_DELAY
        self.exponential_base = (
            exponential_base if exponential_base is not None else cfg.RETRY_EXPONENTIAL_BASE
        )
        self.jitter = jitter if jitter is not None else cfg.RETRY_JITTER

    def should_retry(self, attempt: int, error: Optional[Exception] = None) -> bool:
        """
        Determine if a task should be retried.

        Args:
            attempt: Current attempt number (0-indexed).
            error: The exception that caused the failure.

        Returns:
            True if retry is warranted.
        """
        if attempt >= self.max_retries:
            return False

        # Non-retryable error types
        if error and isinstance(error, (KeyboardInterrupt, SystemExit)):
            return False

        return True

    def get_delay(self, attempt: int) -> float:
        """
        Compute the backoff delay for the given attempt number.

        Uses exponential backoff capped at max_delay, with optional jitter.
        """
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            delay = random.uniform(0, delay)

        return delay

    def wait(self, attempt: int) -> float:
        """Compute the delay and sleep for that duration. Returns the delay used."""
        delay = self.get_delay(attempt)
        logger.info(
            "Retry backoff: attempt %d, sleeping %.2fs (max_retries=%d)",
            attempt,
            delay,
            self.max_retries,
        )
        time.sleep(delay)
        return delay


class RetryContext:
    """
    Context manager / tracker for retry state around a single task.

    Usage:
        ctx = RetryContext(policy)
        while ctx.should_continue:
            try:
                result = do_work()
                ctx.mark_success(result)
            except Exception as e:
                ctx.mark_failure(e)
    """

    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()
        self.attempt = 0
        self.last_error: Optional[Exception] = None
        self.succeeded = False
        self.result = None
        self.total_delay = 0.0

    @property
    def should_continue(self) -> bool:
        """Whether another attempt should be made."""
        if self.succeeded:
            return False
        if self.attempt == 0:
            return True  # Always try at least once
        return self.policy.should_retry(self.attempt, self.last_error)

    def mark_failure(self, error: Exception) -> None:
        """Record a failed attempt and sleep if a retry will follow."""
        self.last_error = error
        self.attempt += 1

        if self.policy.should_retry(self.attempt, error):
            delay = self.policy.wait(self.attempt - 1)
            self.total_delay += delay
            logger.warning(
                "Task attempt %d failed: %s. Will retry.",
                self.attempt,
                str(error)[:200],
            )
        else:
            logger.error(
                "Task failed after %d attempts: %s",
                self.attempt,
                str(error)[:200],
            )

    def mark_success(self, result=None) -> None:
        """Record a successful execution."""
        self.succeeded = True
        self.result = result
        if self.attempt > 0:
            logger.info("Task succeeded on attempt %d", self.attempt + 1)
