"""Tests for csv_merger.parsers."""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.parsers import (
    MalformedBatchFile,
    MalformedRecordFile,
    parse_batch_file,
    parse_record_file,
)
from tests._helpers import BATCH_1, RECORD_1, write_file


class ParseBatchFileTests(unittest.TestCase):
    def test_parses_header_fields(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-1.txt", BATCH_1)
            batch = parse_batch_file(path)
        self.assertEqual(batch.header.date, date(2026, 5, 1))
        self.assertEqual(batch.header.key_1, "value_1")
        self.assertEqual(batch.header.key_2, "value_2")
        self.assertEqual(batch.header.batch, "1")

    def test_picks_include_section_only(self) -> None:
        """The 'do not include' section must not appear in the parsed rows."""
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-1.txt", BATCH_1)
            batch = parse_batch_file(path)
        # Include block has 4 rows (3 data + trailer); exclude block has
        # different field_a values like "0.57" — those must not appear.
        field_a_header = batch.roles.field_a
        field_a_values = [row.cells[field_a_header] for row in batch.rows]
        self.assertEqual(field_a_values, ["0.15", "0.87", "0.71", "4"])

    def test_columns_count_seven(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-1.txt", BATCH_1)
            batch = parse_batch_file(path)
        self.assertEqual(len(batch.column_headers), 7)

    def test_missing_records_section_raises(self) -> None:
        bad = BATCH_1.replace("records\n", "RECORDS_NOT_FOUND\n")
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-bad.txt", bad)
            with self.assertRaises(MalformedBatchFile):
                parse_batch_file(path)

    def test_invalid_date_raises(self) -> None:
        bad = BATCH_1.replace("05/01/2026", "not-a-date")
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-bad.txt", bad)
            with self.assertRaises(MalformedBatchFile):
                parse_batch_file(path)

    def test_missing_required_role_raises(self) -> None:
        """If the column headers don't include type1 the parser must error."""
        bad = BATCH_1.replace(
            "field_1,field_2,field_a,field_b,type1(a),type2(b),field_5",
            "field_1,field_2,field_a,field_b,renamed1,type2(b),field_5",
        )
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-bad.txt", bad)
            with self.assertRaises(MalformedBatchFile) as ctx:
                parse_batch_file(path)
            self.assertIn("type1", str(ctx.exception))

    def test_duplicate_column_headers_raises(self) -> None:
        """Duplicate headers are ambiguous for dict-keyed lookup."""
        bad = BATCH_1.replace(
            "field_1,field_2,field_a,field_b,type1(a),type2(b),field_5",
            "field_1,field_1,field_a,field_b,type1(a),type2(b),field_5",
        )
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "batch-bad.txt", bad)
            with self.assertRaises(MalformedBatchFile):
                parse_batch_file(path)


class ParseRecordFileTests(unittest.TestCase):
    def test_parses_record_fields(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-1.txt", RECORD_1)
            record = parse_record_file(path)
        self.assertEqual(record.key_2, "value_2")
        self.assertEqual(record.field_2, "abc")
        self.assertEqual(record.xy_pos, ("0.94", "0.97"))
        self.assertEqual(record.type1_value, "0.33")
        self.assertEqual(record.type2_value, "0.38")

    def test_file_index_extracted_from_name(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-7.txt", RECORD_1)
            record = parse_record_file(path)
        self.assertEqual(record.file_index, 7)

    def test_missing_xy_pos_raises(self) -> None:
        bad = RECORD_1.replace('"XY_POS",0.94,0.97\n', "")
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-bad.txt", bad)
            with self.assertRaises(MalformedRecordFile):
                parse_record_file(path)

    def test_handles_trailing_tabs_gracefully(self) -> None:
        """record-12.txt in the sample data has stray tabs after some
        values; the parser must strip them."""
        sample = (
            '"fielda",value a\n'
            '"key_2",value_2\n'
            '"field_2",abc\n'
            '"count",1\n'
            '"XY_POS",0.54,0.64\t\n'
            '"record_1",type1,a,0.74,0.74\t\n'
            '"record_2",type2,b,0.63,0.63\n'
            '"record_3",type3,c,9.4,9.4\n'
        )
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-12.txt", sample)
            record = parse_record_file(path)
        self.assertEqual(record.xy_pos, ("0.54", "0.64"))
        self.assertEqual(record.type1_value, "0.74")


class FileIndexTests(unittest.TestCase):
    """The trailing-ordinal regex must not over-match dates etc."""

    def test_simple_trailing_number(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-7.txt", RECORD_1)
            self.assertEqual(parse_record_file(path).file_index, 7)

    def test_no_ordinal_returns_none(self) -> None:
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-orphan.txt", RECORD_1)
            self.assertIsNone(parse_record_file(path).file_index)

    def test_does_not_concatenate_internal_digits(self) -> None:
        """Old behaviour: ``record-2024-01.txt`` -> 202401 (wrong).

        New behaviour: only the *trailing* ``-N`` is used, so this returns 1.
        """
        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "record-2024-01.txt", RECORD_1)
            self.assertEqual(parse_record_file(path).file_index, 1)


class EncodingAndSizeTests(unittest.TestCase):
    """Defence-in-depth: BOM tolerance and file-size guard."""

    def test_utf8_bom_is_not_attached_to_first_cell(self) -> None:
        """A UTF-8 BOM at the start of the file must be silently consumed."""
        bom = "\ufeff"
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            # Write the file ourselves so we can prepend the BOM byte.
            path = tmp_path / "batch-1.txt"
            path.write_text(bom + BATCH_1, encoding="utf-8")
            batch = parse_batch_file(path)
        # title is the first cell of the first row; it must not contain
        # the BOM character.
        self.assertEqual(batch.header.title, "title")

    def test_oversized_input_raises(self) -> None:
        """A file above the size limit is rejected up-front."""
        from csv_merger._constants import InputTooLargeError
        from csv_merger.parsers import _check_size

        with TemporaryDirectory() as tmp:
            path = write_file(Path(tmp), "huge.txt", "x" * 1024)
            # Use a tiny per-call limit to make the test fast.
            with self.assertRaises(InputTooLargeError):
                _check_size(path, max_bytes=512)


if __name__ == "__main__":
    unittest.main()
