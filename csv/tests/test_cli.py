"""Tests for the command-line interface."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.cli import main
from tests._helpers import SAMPLE_DIR


class CliTests(unittest.TestCase):
    def test_runs_against_sample_data(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                rc = main(
                    [
                        "--inputs",
                        str(SAMPLE_DIR),
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(rc, 0, msg=stderr.getvalue())
            self.assertTrue(output.exists())
            self.assertIn("wrote", stdout.getvalue())

    def test_invalid_inputs_dir_returns_nonzero(self) -> None:
        with TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as ctx:
                    main(
                        [
                            "--inputs",
                            "/no/such/dir",
                            "--output",
                            str(Path(tmp) / "out.txt"),
                        ]
                    )
            self.assertNotEqual(ctx.exception.code, 0)

    def test_date_range_argument_parses(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                rc = main(
                    [
                        "--inputs",
                        str(SAMPLE_DIR),
                        "--output",
                        str(output),
                        "--start",
                        "05/02/2026",
                        "--end",
                        "05/03/2026",
                    ]
                )
            self.assertEqual(rc, 0, msg=stderr.getvalue())
            content = output.read_text(encoding="utf-8")
        # Only batches 2 and 3 should appear (dates 05/02 and 05/03).
        self.assertIn("12,abc", content)
        self.assertIn("13,xyz", content)
        self.assertNotIn("14,abc", content)


if __name__ == "__main__":
    unittest.main()
