"""Tests for csv_merger.writer."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from csv_merger.models import BatchFile
from csv_merger.parsers import parse_batch_file
from csv_merger.writer import write_output
from tests._helpers import BATCH_1, write_file


class WriterAtomicTests(unittest.TestCase):
    def _parse_batch(self, tmp: Path) -> BatchFile:
        path = write_file(tmp, "batch-1.txt", BATCH_1)
        return parse_batch_file(path)

    def test_writes_output_when_no_overrides(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch = self._parse_batch(tmp_path)
            output = tmp_path / "out.txt"

            write_output(output, [batch], overrides={})

            self.assertTrue(output.exists())
            content = output.read_text(encoding="utf-8")
            # The original (un-overridden) field_a value should appear.
            self.assertIn("0.15,0.32", content)

    def test_existing_output_is_not_modified_on_failure(self) -> None:
        """If serialisation raises mid-write, the previous file survives."""
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch = self._parse_batch(tmp_path)
            output = tmp_path / "out.txt"
            output.write_text("original content", encoding="utf-8")

            # Use an override key that points at a row index out of range.
            # This causes write_output to raise mid-iteration. We feed in
            # one valid override so the writer reaches the row loop, plus a
            # bad header replacement that triggers KeyError via
            # with_replacements (the writer uses
            # ``row.with_replacements({field_a, field_b})`` so we have to
            # break that path differently — patch the row method).
            with patch.object(
                type(batch.data_rows[0]),
                "with_replacements",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(RuntimeError):
                    write_output(
                        output,
                        [batch],
                        overrides={(str(batch.path), 0): ("X", "Y")},
                    )

            self.assertEqual(
                output.read_text(encoding="utf-8"),
                "original content",
                "atomic write must not corrupt the existing file on error",
            )

    def test_no_temp_files_left_after_failure(self) -> None:
        """The temp file used for atomic write is cleaned up on error."""
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch = self._parse_batch(tmp_path)
            output = tmp_path / "out.txt"

            with patch.object(
                type(batch.data_rows[0]),
                "with_replacements",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(RuntimeError):
                    write_output(
                        output,
                        [batch],
                        overrides={(str(batch.path), 0): ("X", "Y")},
                    )

            stragglers = [
                p
                for p in tmp_path.iterdir()
                if p.name.startswith(".") and p.suffix == ".tmp"
            ]
            self.assertEqual(stragglers, [], "temp file was not cleaned up")


if __name__ == "__main__":
    unittest.main()
