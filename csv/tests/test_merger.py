"""End-to-end tests for csv_merger.merger.

These exercise the full pipeline against the bundled ``sample_data/``
directory and compare the output to the supplied reference output.
"""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.merger import CsvMerger
from tests._helpers import SAMPLE_DIR


def _normalise(text: str) -> list[list[str]]:
    """Strip blank lines + whitespace so we can compare semantic content."""
    rows: list[list[str]] = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        rows.append([c.strip() for c in cleaned.split(",")])
    return rows


class MergeEndToEndTests(unittest.TestCase):
    def test_full_run_matches_expected_output(self) -> None:
        expected_path = SAMPLE_DIR / "expected-output.txt"
        self.assertTrue(expected_path.exists(), "sample expected output missing")

        with TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.txt"
            report = CsvMerger(
                input_dir=SAMPLE_DIR,
                output_path=output_path,
            ).run()

            actual = output_path.read_text(encoding="utf-8")

        expected = expected_path.read_text(encoding="utf-8")
        self.assertEqual(_normalise(actual), _normalise(expected))

        # Additional report-level invariants:
        self.assertEqual(len(report.batches_in_range), 4)
        # Sample data has 12 batch data rows that should all be overridden,
        # plus record-13 which is an orphan with no matching row.
        self.assertEqual(report.overrides_applied, 12)
        self.assertEqual(len(report.unmatched_records), 1)
        self.assertEqual(
            report.unmatched_records[0].path.name, "record-13.txt"
        )

    def test_date_range_filters_batches(self) -> None:
        with TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.txt"
            report = CsvMerger(
                input_dir=SAMPLE_DIR,
                output_path=output_path,
                start_date=date(2026, 5, 2),
                end_date=date(2026, 5, 3),
            ).run()
        self.assertEqual(len(report.batches_in_range), 2)
        self.assertEqual(len(report.batches_out_of_range), 2)

    def test_no_batches_in_range_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.txt"
            with self.assertRaises(ValueError):
                CsvMerger(
                    input_dir=SAMPLE_DIR,
                    output_path=output_path,
                    start_date=date(2030, 1, 1),
                ).run()

    def test_missing_input_directory_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            empty = Path(tmp) / "empty"
            empty.mkdir()
            with self.assertRaises(FileNotFoundError):
                CsvMerger(
                    input_dir=empty,
                    output_path=Path(tmp) / "out.txt",
                ).run()


if __name__ == "__main__":
    unittest.main()
