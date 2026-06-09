"""Tests for csv_merger.batch_type2."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent

from csv_merger._constants import MalformedBatchFile
from csv_merger.batch_type2 import (
    parse_batch_file_type2,
    write_type2_csv,
)
from tests._helpers import SAMPLE_DIR

TYPE2_DIR = SAMPLE_DIR / "type2"


class EndToEndTests(unittest.TestCase):
    """Parsing the supplied sample and writing it yields the expected CSV."""

    def test_sample_round_trip_matches_expected(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.csv"
            write_type2_csv(report, output)
            actual = output.read_text(encoding="utf-8")
        expected = (
            (TYPE2_DIR / "batch-type2-expected.csv")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(actual, expected)


class ParserStructureTests(unittest.TestCase):
    """Verify the parsed Type2Report has the right shape."""

    def test_header_fields_extracted(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        self.assertEqual(report.title, "title")
        self.assertEqual(report.header["file name"], "file name value")
        self.assertEqual(report.header["process name"], "process name value")
        self.assertEqual(report.header["process type"], "process type value")

    def test_column_names_joined_across_sections(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        self.assertEqual(
            report.column_names,
            (
                "value1(cps)", "value2(cps)", "value3(cps)",
                "value4(cps)", "value5(cps)",
                "value6(nm)", "value7(nm)", "value8(nm)",
            ),
        )

    def test_numeric_rows_labelled_by_sequence(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        numeric_rows = [r for r in report.rows if r.r != "blank"]
        self.assertEqual(
            [r.rec_label for r in numeric_rows],
            ["1", "2", "3", "4", "5"],
        )

    def test_calc_rows_labelled_literally(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        calc_rows = [r for r in report.rows if r.r == "blank"]
        self.assertEqual(
            [r.rec_label for r in calc_rows],
            ["Calc 1", "Calc 2", "Calc 3", "Calc 4"],
        )

    def test_calc_rows_have_blank_index_columns(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        calc_rows = [r for r in report.rows if r.rec_label.startswith("Calc")]
        for row in calc_rows:
            self.assertEqual(row.r, "blank")
            self.assertEqual(row.phi, "blank")


class MalformedInputTests(unittest.TestCase):
    """The parser raises MalformedBatchFile on structural problems."""

    def _write(self, tmp: Path, content: str) -> Path:
        path = tmp / "input.txt"
        path.write_text(content, encoding="utf-8")
        return path

    def test_missing_result_banner_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "title\nfile name : x\n")
            with self.assertRaises(MalformedBatchFile):
                parse_batch_file_type2(path)

    def test_section_row_count_mismatch_raises(self) -> None:
        """Section A has 5 numeric rows, section B has 4 → fail loudly."""
        content = dedent("""\
            ===== result =====

            title
            process name : pn

                            v1   v2
              r  Phi  (u)  (u)
            ----
            0.1  0.2  1.0  2.0
            0.3  0.4  1.0  2.0
            ----

                    v3   v4
            r  Phi  (u)  (u)
            ----
            0.1  0.2  3.0  4.0
            ----
        """)
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), content)
            with self.assertRaises(MalformedBatchFile) as ctx:
                parse_batch_file_type2(path)
            self.assertIn("row counts differ", str(ctx.exception))

    def test_section_rPhi_mismatch_raises(self) -> None:
        """If the two sections list different (r, Phi) pairs, fail loudly."""
        content = dedent("""\
            ===== result =====

            title
            process name : pn

                            v1
              r  Phi  (u)
            ----
            0.1  0.2  1.0
            ----

                    v2
            r  Phi  (u)
            ----
            0.9  0.8  2.0
            ----
        """)
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), content)
            with self.assertRaises(MalformedBatchFile) as ctx:
                parse_batch_file_type2(path)
            self.assertIn("(r, Phi) mismatch", str(ctx.exception))


class WriterOptionsTests(unittest.TestCase):
    """The writer's configurable knobs work as documented."""

    def test_custom_processname_key(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.csv"
            write_type2_csv(
                report,
                output,
                processname_key="process type",  # use a different header
            )
            content = output.read_text(encoding="utf-8")
        # First data row should carry the process_type value, not process_name.
        self.assertIn("process type value", content)

    def test_custom_placeholder_columns(self) -> None:
        report = parse_batch_file_type2(TYPE2_DIR / "batch-type2.txt")
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.csv"
            write_type2_csv(
                report,
                output,
                placeholder_columns=("a", "b", "c"),
            )
            lines = output.read_text(encoding="utf-8").splitlines()
        self.assertTrue(lines[0].endswith(",a,b,c"))
        # Every data row should end with three 'na' values.
        self.assertTrue(lines[1].endswith(",na,na,na"))


if __name__ == "__main__":
    unittest.main()
