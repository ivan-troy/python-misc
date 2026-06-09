"""Retry with exponential backoff.

Deliberately not a decorator: retries are visible at the call site, not
hidden inside the callee's signature. That makes the runner's flow
readable — you can see which operations have retry behavior just by
reading the orchestration code.

The retry helper is sleep-blocking by design. The pipeline runs as a
short-lived process per tick (Windows Task Scheduler), so a few seconds
of blocking is fine. Don't reach for asyncio here — it would add
complexity for no benefit at this cadence.

Only :class:`TransientError` is retried by default. Any other exception
propagates immediately. Callers can override ``retry_on`` to widen the
set, but should think hard before doing so: retrying the wrong kind of
exception (a parse error, a config error) papers over real bugs.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

from csv_merger.pipeline._errors import TransientError

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Hook signature: ``(exception, attempt_number, delay_seconds) -> None``.
RetryHook = Callable[[BaseException, int, float], None]


def with_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: tuple[type[BaseException], ...] = (TransientError,),
    on_retry: RetryHook | None = None,
    jitter: bool = True,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn()`` with exponential backoff on retryable failures.

    Args:
        fn: Zero-argument callable to invoke. Use a lambda to bind args.
        max_attempts: Total attempts, including the first one. Must be ≥ 1.
        base_delay: Delay (seconds) after the first failure.
        max_delay: Upper bound on any single delay.
        retry_on: Exception classes that trigger a retry. Defaults to
            :class:`TransientError` only. Widen with care.
        on_retry: Optional callback fired before each sleep, with the
            exception, the 1-based attempt number that just failed, and
            the delay about to be slept. Useful for logging with context.
        jitter: When ``True`` (default), multiply the computed delay by
            a random factor in [0.5, 1.5] to spread retries across
            colliding callers.
        sleep: Sleep function, swappable for tests.

    Returns:
        Whatever ``fn()`` returns on the first successful attempt.

    Raises:
        ValueError: if ``max_attempts < 1``.
        BaseException: the last exception from ``fn()`` if every attempt
            failed, or any non-retryable exception immediately.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be ≥ 1, got {max_attempts}")

    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            last_exc = exc
            if attempt >= max_attempts:
                logger.debug(
                    "retry exhausted after %d attempts: %s",
                    attempt,
                    exc,
                )
                raise
            delay = _compute_delay(attempt, base_delay, max_delay, jitter)
            if on_retry is not None:
                on_retry(exc, attempt, delay)
            else:
                logger.info(
                    "attempt %d/%d failed with %s; sleeping %.2fs before retry",
                    attempt,
                    max_attempts,
                    type(exc).__name__,
                    delay,
                )
            sleep(delay)

    # Unreachable: the loop either returns or re-raises. Kept for type-checker.
    assert last_exc is not None  # pragma: no cover
    raise last_exc  # pragma: no cover


def _compute_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    jitter: bool,
) -> float:
    """Exponential backoff: ``base * 2**(attempt-1)``, capped, optionally jittered.

    Attempt 1 yields ``base_delay``; attempt 2 yields ``2 * base_delay``,
    attempt 3 yields ``4 * base_delay``, and so on up to ``max_delay``.
    """
    raw = base_delay * (2 ** (attempt - 1))
    capped = min(raw, max_delay)
    if jitter:
        # Half-to-one-and-a-half multiplier spreads load without ever
        # exceeding 1.5 * max_delay; clamp anyway as a belt-and-braces.
        factor: float = random.uniform(0.5, 1.5)
        capped = min(capped * factor, max_delay * 1.5)
    return float(capped)
