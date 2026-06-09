"""Tests for csv_merger.pipeline.retry."""

from __future__ import annotations

import unittest

from csv_merger.pipeline._errors import PermanentError, TransientError
from csv_merger.pipeline.retry import _compute_delay, with_retry


class WithRetryTests(unittest.TestCase):
    def test_first_attempt_success_does_not_retry(self) -> None:
        calls = []

        def fn() -> str:
            calls.append(1)
            return "ok"

        result = with_retry(
            fn,
            max_attempts=3,
            base_delay=0.01,
            max_delay=0.1,
            sleep=lambda _: None,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 1)

    def test_retries_on_transient_then_succeeds(self) -> None:
        attempt = [0]
        sleeps: list[float] = []

        def fn() -> str:
            attempt[0] += 1
            if attempt[0] < 3:
                raise TransientError("blip")
            return "ok"

        result = with_retry(
            fn,
            max_attempts=5,
            base_delay=0.01,
            max_delay=0.1,
            jitter=False,
            sleep=sleeps.append,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(attempt[0], 3)
        self.assertEqual(len(sleeps), 2)  # 2 retries → 2 sleeps

    def test_exhausts_attempts_and_raises_last(self) -> None:
        attempt = [0]

        def fn() -> str:
            attempt[0] += 1
            raise TransientError(f"fail {attempt[0]}")

        with self.assertRaises(TransientError) as ctx:
            with_retry(
                fn,
                max_attempts=3,
                base_delay=0.01,
                max_delay=0.1,
                sleep=lambda _: None,
            )
        self.assertIn("fail 3", str(ctx.exception))
        self.assertEqual(attempt[0], 3)

    def test_non_retryable_exception_propagates_immediately(self) -> None:
        attempt = [0]

        def fn() -> str:
            attempt[0] += 1
            raise PermanentError("nope")

        with self.assertRaises(PermanentError):
            with_retry(
                fn,
                max_attempts=5,
                base_delay=0.01,
                max_delay=0.1,
                sleep=lambda _: None,
            )
        # Critical: did NOT retry — permanent errors must not be retried.
        self.assertEqual(attempt[0], 1)

    def test_on_retry_hook_invoked_with_context(self) -> None:
        invocations: list[tuple[int, float]] = []

        def fn() -> str:
            raise TransientError("x")

        def hook(exc: BaseException, n: int, delay: float) -> None:
            invocations.append((n, delay))

        with self.assertRaises(TransientError):
            with_retry(
                fn,
                max_attempts=3,
                base_delay=0.01,
                max_delay=0.1,
                on_retry=hook,
                jitter=False,
                sleep=lambda _: None,
            )

        # 3 attempts → 2 retry hooks (hook fires *between* attempts).
        self.assertEqual([n for n, _ in invocations], [1, 2])

    def test_zero_max_attempts_rejected(self) -> None:
        with self.assertRaises(ValueError):
            with_retry(
                lambda: "x",
                max_attempts=0,
                base_delay=0.1,
                max_delay=1.0,
            )

    def test_custom_retry_on(self) -> None:
        """retry_on can widen the set of retried exceptions."""
        attempt = [0]

        def fn() -> str:
            attempt[0] += 1
            if attempt[0] < 2:
                raise ValueError("special case")
            return "ok"

        result = with_retry(
            fn,
            max_attempts=3,
            base_delay=0.01,
            max_delay=0.1,
            retry_on=(ValueError,),
            sleep=lambda _: None,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(attempt[0], 2)


class ComputeDelayTests(unittest.TestCase):
    def test_no_jitter_exponential(self) -> None:
        # 0.1, 0.2, 0.4, 0.8, capped at 1.0
        self.assertEqual(_compute_delay(1, 0.1, 1.0, False), 0.1)
        self.assertEqual(_compute_delay(2, 0.1, 1.0, False), 0.2)
        self.assertEqual(_compute_delay(3, 0.1, 1.0, False), 0.4)
        self.assertEqual(_compute_delay(4, 0.1, 1.0, False), 0.8)
        self.assertEqual(_compute_delay(5, 0.1, 1.0, False), 1.0)  # capped
        self.assertEqual(_compute_delay(10, 0.1, 1.0, False), 1.0)  # still

    def test_jitter_stays_within_bounds(self) -> None:
        """Jitter must keep delay within [0.5*raw, 1.5*max_delay]."""
        for _ in range(100):
            d = _compute_delay(5, 0.1, 1.0, True)
            self.assertGreaterEqual(d, 0)
            self.assertLessEqual(d, 1.5)  # max_delay * 1.5


if __name__ == "__main__":
    unittest.main()
