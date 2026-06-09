"""Tests for csv_merger logging configuration and emission."""

from __future__ import annotations

import io
import logging
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger._logging import LOG_FORMAT, configure, level_for_verbosity
from csv_merger.cli import main as cli_main
from tests._helpers import SAMPLE_DIR


class LevelMappingTests(unittest.TestCase):
    """level_for_verbosity should map -v counts as documented."""

    def test_zero_is_warning(self) -> None:
        self.assertEqual(level_for_verbosity(0), logging.WARNING)

    def test_one_is_info(self) -> None:
        self.assertEqual(level_for_verbosity(1), logging.INFO)

    def test_two_is_debug(self) -> None:
        self.assertEqual(level_for_verbosity(2), logging.DEBUG)

    def test_above_max_clamps_to_debug(self) -> None:
        for n in (3, 5, 100):
            self.assertEqual(
                level_for_verbosity(n),
                logging.DEBUG,
                msg=f"verbosity {n}",
            )

    def test_negative_clamps_to_warning(self) -> None:
        self.assertEqual(level_for_verbosity(-1), logging.WARNING)


class ConfigureTests(unittest.TestCase):
    """``configure`` should set the root level and use the project format."""

    def setUp(self) -> None:
        # Snapshot the root logger state so each test is isolated.
        self._original_level = logging.root.level
        self._original_handlers = list(logging.root.handlers)

    def tearDown(self) -> None:
        logging.root.handlers = self._original_handlers
        logging.root.setLevel(self._original_level)

    def test_configure_sets_level(self) -> None:
        configure(logging.DEBUG)
        self.assertEqual(logging.root.level, logging.DEBUG)
        configure(logging.WARNING)
        self.assertEqual(logging.root.level, logging.WARNING)

    def test_configure_does_not_stack_handlers(self) -> None:
        """Re-calling ``configure`` should not duplicate handlers."""
        configure(logging.INFO)
        n1 = len(logging.root.handlers)
        configure(logging.DEBUG)
        n2 = len(logging.root.handlers)
        self.assertEqual(n1, n2)

    def test_format_string_has_levelname_and_name(self) -> None:
        # Sanity check on the format string itself (not via emission, which
        # depends on a non-empty handler stream that pytest captures).
        self.assertIn("%(levelname)s", LOG_FORMAT)
        self.assertIn("%(name)s", LOG_FORMAT)
        self.assertIn("%(message)s", LOG_FORMAT)


class CliVerbosityTests(unittest.TestCase):
    """The CLI should produce the expected log volume per verbosity flag."""

    def setUp(self) -> None:
        self._original_level = logging.root.level
        self._original_handlers = list(logging.root.handlers)

    def tearDown(self) -> None:
        logging.root.handlers = self._original_handlers
        logging.root.setLevel(self._original_level)

    def _run_cli(self, *flags: str) -> tuple[int, str, str]:
        """Run the CLI against the sample data with extra flags.

        Returns ``(return_code, stderr_text, stdout_text)``. The CLI's
        log handler is configured by ``logging.basicConfig`` to write to
        ``sys.stderr``, so we capture from there.
        """
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                rc = cli_main(
                    [
                        "--inputs", str(SAMPLE_DIR),
                        "--output", str(output),
                        *flags,
                    ]
                )
            return rc, stderr.getvalue(), stdout.getvalue()

    def test_default_emits_no_info_or_debug(self) -> None:
        """No -v: INFO and DEBUG records should be suppressed."""
        rc, logs, _ = self._run_cli()
        self.assertEqual(rc, 0)
        self.assertNotIn("INFO", logs)
        self.assertNotIn("DEBUG", logs)

    def test_dash_v_emits_info_but_not_debug(self) -> None:
        rc, logs, _ = self._run_cli("-v")
        self.assertEqual(rc, 0)
        self.assertIn("INFO", logs)
        self.assertNotIn("DEBUG", logs)
        # Should mention the pipeline milestones we promise at INFO level.
        self.assertIn("starting merge", logs)
        self.assertIn("merge complete", logs)

    def test_dash_vv_emits_debug(self) -> None:
        rc, logs, _ = self._run_cli("-vv")
        self.assertEqual(rc, 0)
        self.assertIn("DEBUG", logs)
        # DEBUG should expose per-record/per-row internals.
        self.assertIn("matcher index built", logs)
        self.assertIn("atomic write", logs)

    def test_log_level_overrides_verbose(self) -> None:
        rc, logs, _ = self._run_cli("--log-level", "ERROR")
        self.assertEqual(rc, 0)
        # ERROR threshold suppresses our usual INFO/WARNING traffic.
        # The sample data emits one WARNING ("no matching batch row" is INFO,
        # not WARNING — confirm nothing leaks at this level).
        self.assertNotIn("INFO", logs)
        self.assertNotIn("DEBUG", logs)
        self.assertNotIn("WARNING", logs)

    def test_log_level_accepts_numeric(self) -> None:
        rc, logs, _ = self._run_cli("--log-level", "10")  # 10 == DEBUG
        self.assertEqual(rc, 0)
        self.assertIn("DEBUG", logs)

    def test_invalid_log_level_rejected(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit):
                    cli_main(
                        [
                            "--inputs", str(SAMPLE_DIR),
                            "--output", str(output),
                            "--log-level", "BOGUS",
                        ]
                    )
            self.assertIn("BOGUS", stderr.getvalue())

    def test_v_and_log_level_are_mutually_exclusive(self) -> None:
        """argparse should refuse the combination."""
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit):
                    cli_main(
                        [
                            "--inputs", str(SAMPLE_DIR),
                            "--output", str(output),
                            "-v",
                            "--log-level", "DEBUG",
                        ]
                    )


if __name__ == "__main__":
    unittest.main()
