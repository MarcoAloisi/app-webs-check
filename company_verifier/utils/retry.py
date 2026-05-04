"""Retry helpers with exponential backoff."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


class RetryableError(RuntimeError):
    """Raised when an operation may be retried."""



def retry_with_backoff(
    func: Callable[[], T],
    *,
    max_attempts: int = 4,
    base_delay: float = 1.2,
    max_delay: float = 8.0,
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Execute ``func`` with exponential backoff."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return func()
        except retryable_exceptions:
            if attempt >= max_attempts:
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            time.sleep(delay + random.uniform(0.0, 0.25))
