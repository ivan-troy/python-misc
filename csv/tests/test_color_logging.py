"""Tests for ANSI color logging support."""

from __future__ import annotations

import io
import logging
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger._logging import (
    LOG_FORMAT,
    _ColorFormatter,
    _LEVEL_COLORS,
    _RESET,
    should_use_color,
)
from csv_merger.cli import main as cli_main
from tests._helpers import SAMPLE_DIR


# --------------------------------------------------------------------------- #
# Decision logic                                                              #
# --------------------------------------------------------------------------- #


class _FakeStream:
    """Minimal stream that lets tests dictate ``isatty()`` per instance."""

    def __init__(self, *, isatty: bool) -> None:
        self._isatty = isatty

    def isatty(self) -> bool:
        return self._isatty


class ShouldUseColorModeTests(unittest.TestCase):
    """``mode`` argument always wins over env and TTY state."""

    def test_always_returns_true_even_for_pipe(self) -> None:
        self.assertTrue(
            should_use_color(_FakeStream(isatty=False), mode="always", env={})
        )

    def test_always_wins_over_no_color_env(self) -> None:
        self.assertTrue(
            should_use_color(
                _FakeStream(isatty=True),
                mode="always",
                env={"NO_COLOR": "1"},
            )
        )

    def test_never_returns_false_even_on_tty(self) -> None:
        self.assertFalse(
            should_use_color(_FakeStream(isatty=True), mode="never", env={})
        )

    def test_never_wins_over_force_color_env(self) -> None:
        self.assertFalse(
            should_use_color(
                _FakeStream(isatty=False),
                mode="never",
                env={"FORCE_COLOR": "1"},
            )
        )


class ShouldUseColorAutoTests(unittest.TestCase):
    """In ``auto`` mode, env vars beat TTY state, and TTY state wins by default."""

    def test_auto_on_tty_with_clean_env_returns_true(self) -> None:
        self.assertTrue(
            should_use_color(_FakeStream(isatty=True), mode="auto", env={})
        )

    def test_auto_on_pipe_with_clean_env_returns_false(self) -> None:
        self.assertFalse(
            should_use_color(_FakeStream(isatty=False), mode="auto", env={})
        )

    def test_no_color_env_disables_on_tty(self) -> None:
        self.assertFalse(
            should_use_color(
                _FakeStream(isatty=True),
                mode="auto",
                env={"NO_COLOR": "1"},
            )
        )

    def test_no_color_any_nonempty_value_disables(self) -> None:
        # no-color.org: any non-empty value, even '0', disables.
        for value in ("1", "0", "true", "false", "yes"):
            with self.subTest(value=value):
                self.assertFalse(
                    should_use_color(
                        _FakeStream(isatty=True),
                        mode="auto",
                        env={"NO_COLOR": value},
                    )
                )

    def test_no_color_empty_string_does_not_disable(self) -> None:
        self.assertTrue(
            should_use_color(
                _FakeStream(isatty=True),
                mode="auto",
                env={"NO_COLOR": ""},
            )
        )

    def test_force_color_env_enables_on_pipe(self) -> None:
        self.assertTrue(
            should_use_color(
                _FakeStream(isatty=False),
                mode="auto",
                env={"FORCE_COLOR": "1"},
            )
        )

    def test_force_color_beats_no_color(self) -> None:
        """FORCE_COLOR is checked before NO_COLOR, so it wins."""
        self.assertTrue(
            should_use_color(
                _FakeStream(isatty=False),
                mode="auto",
                env={"FORCE_COLOR": "1", "NO_COLOR": "1"},
            )
        )

    def test_none_stream_returns_false(self) -> None:
        self.assertFalse(should_use_color(None, mode="auto", env={}))


# --------------------------------------------------------------------------- #
# Formatter behaviour                                                         #
# --------------------------------------------------------------------------- #


class ColorFormatterTests(unittest.TestCase):
    def _record(self, level: int, msg: str = "hello") -> logging.LogRecord:
        return logging.LogRecord(
            name="test",
            level=level,
            pathname=__file__,
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_wraps_levelname_in_ansi(self) -> None:
        rec = self._record(logging.WARNING)
        formatted = _ColorFormatter(LOG_FORMAT).format(rec)
        self.assertIn(_LEVEL_COLORS[logging.WARNING], formatted)
        self.assertIn("WARNING", formatted)
        self.assertIn(_RESET, formatted)

    def test_each_level_uses_its_documented_color(self) -> None:
        for level, color in _LEVEL_COLORS.items():
            with self.subTest(level=logging.getLevelName(level)):
                rec = self._record(level)
                formatted = _ColorFormatter(LOG_FORMAT).format(rec)
                self.assertIn(color, formatted)
                self.assertIn(_RESET, formatted)

    def test_reset_immediately_follows_levelname(self) -> None:
        """The reset must close the color BEFORE the rest of the line.

        Otherwise terminals carry the color into the logger name and
        message, which is exactly the bleed we want to avoid.
        """
        rec = self._record(logging.ERROR, "msg here")
        formatted = _ColorFormatter(LOG_FORMAT).format(rec)
        # The format string puts a space then logger name after levelname,
        # so the reset must appear before that space.
        reset_pos = formatted.index(_RESET)
        first_space_pos = formatted.index(" ")
        self.assertLess(
            reset_pos,
            first_space_pos,
            f"reset should precede first space; got {formatted!r}",
        )

    def test_unknown_level_falls_back_to_plain(self) -> None:
        """A level not in the map renders without ANSI."""
        rec = self._record(level=5, msg="custom")  # below DEBUG
        formatted = _ColorFormatter(LOG_FORMAT).format(rec)
        self.assertNotIn("\x1b[", formatted)

    def test_levelname_restored_after_format(self) -> None:
        """Subsequent handlers must see the original levelname.

        This is the behaviour that makes _ColorFormatter safe to combine
        with other handlers on the same logger.
        """
        rec = self._record(logging.INFO)
        original = rec.levelname
        _ColorFormatter(LOG_FORMAT).format(rec)
        self.assertEqual(rec.levelname, original)


# --------------------------------------------------------------------------- #
# CLI plumbing                                                                #
# --------------------------------------------------------------------------- #


class CliColorFlagTests(unittest.TestCase):
    """The --color flag should reach the formatter."""

    def setUp(self) -> None:
        self._original_level = logging.root.level
        self._original_handlers = list(logging.root.handlers)

    def tearDown(self) -> None:
        # Tear down: restore the root logger to whatever it was.
        for h in list(logging.root.handlers):
            logging.root.removeHandler(h)
        for h in self._original_handlers:
            logging.root.addHandler(h)
        logging.root.setLevel(self._original_level)

    def _run_cli(self, *flags: str) -> tuple[int, str]:
        """Run the CLI; return (rc, stderr).

        We always pass ``-v`` so at least one INFO line is emitted,
        because color-on-a-suppressed-line is unobservable.
        """
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                rc = cli_main(
                    [
                        "--inputs", str(SAMPLE_DIR),
                        "--output", str(output),
                        "-v",
                        *flags,
                    ]
                )
            return rc, stderr.getvalue()

    def test_color_always_emits_ansi(self) -> None:
        rc, logs = self._run_cli("--color", "always")
        self.assertEqual(rc, 0)
        self.assertIn("\x1b[", logs)

    def test_color_never_emits_no_ansi(self) -> None:
        rc, logs = self._run_cli("--color", "never")
        self.assertEqual(rc, 0)
        self.assertNotIn("\x1b[", logs)

    def test_color_auto_on_redirected_stderr_emits_no_ansi(self) -> None:
        """``redirect_stderr`` substitutes a StringIO, which is not a TTY.

        Auto mode should therefore disable color — the well-behaved
        outcome for "user piped stderr to a file".
        """
        rc, logs = self._run_cli("--color", "auto")
        self.assertEqual(rc, 0)
        self.assertNotIn("\x1b[", logs)

    def test_invalid_color_choice_rejected(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit):
                    cli_main(
                        [
                            "--inputs", str(SAMPLE_DIR),
                            "--output", str(output),
                            "--color", "rainbow",
                        ]
                    )


if __name__ == "__main__":
    unittest.main()
