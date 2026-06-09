"""Tests for csv_merger.matcher."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.matcher import RecordMatcher
from csv_merger.parsers import parse_batch_file, parse_record_file
from tests._helpers import BATCH_1, RECORD_1, write_file


class MatchSingleBatchTests(unittest.TestCase):
    def test_record_1_binds_to_first_data_row(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", BATCH_1)
            record_path = write_file(tmp_path, "record-1.txt", RECORD_1)
            batch = parse_batch_file(batch_path)
            record = parse_record_file(record_path)

            result = RecordMatcher([batch], [record]).match()

        self.assertEqual(result.unmatched_records, [])
        self.assertEqual(len(result.bindings), 1)
        ((path, idx), bound) = next(iter(result.bindings.items()))
        self.assertEqual(idx, 0)  # first data row
        self.assertEqual(bound.path.name, "record-1.txt")

    def test_trailer_row_never_receives_binding(self) -> None:
        """Trailer is excluded by **position**, not by inspecting cell values.

        We construct a batch whose trailer is fully numeric and could in
        principle match a record's type1/type2 values. Under a value-based
        heuristic this would (incorrectly) bind. Under the positional rule
        the last row is excluded regardless of contents.
        """
        numeric_trailer_batch = (
            "title\n"
            "date,05/01/2026\n"
            "key_1,value_1\n"
            "key_2,value_2\n"
            "batch,1\n"
            "\n"
            "records\n"
            "data,2\n"
            "field_1,field_2,field_a,field_b,type1(a),type2(b),field_5\n"
            "11,abc,0.15,0.32,0.33,0.38,1\n"
            "11,abc,0.99,0.99,0.55,0.66,9\n"  # numeric trailer
        )
        # Record targets the trailer's type1/type2 values exactly.
        rec_targets_trailer = RECORD_1.replace(
            '"record_1",type1,a,0.33,0.33',
            '"record_1",type1,a,0.55,0.55',
        ).replace(
            '"record_2",type2,b,0.38,0.38',
            '"record_2",type2,b,0.66,0.66',
        )
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", numeric_trailer_batch)
            record_path = write_file(tmp_path, "record-x.txt", rec_targets_trailer)
            batch = parse_batch_file(batch_path)
            record = parse_record_file(record_path)

            result = RecordMatcher([batch], [record]).match()

        # The trailer is the last row; it must not be bound even though its
        # type1/type2 values match the record exactly.
        self.assertEqual(result.bindings, {})
        self.assertEqual(len(result.unmatched_records), 1)

    def test_duplicate_type_values_resolved_by_first_unbound_row(self) -> None:
        """Batch-1 has two rows with type1=0.66 (rows 2 and 3). Two records
        targeting type1=0.66/type2=0.44 and type1=0.66/type2=0.1 should bind
        to the right rows respectively."""
        rec_a = RECORD_1.replace(
            '"record_1",type1,a,0.33,0.33',
            '"record_1",type1,a,0.66,0.66',
        ).replace(
            '"record_2",type2,b,0.38,0.38',
            '"record_2",type2,b,0.44,0.44',
        )
        rec_b = RECORD_1.replace(
            '"record_1",type1,a,0.33,0.33',
            '"record_1",type1,a,0.66,0.66',
        ).replace(
            '"record_2",type2,b,0.38,0.38',
            '"record_2",type2,b,0.1,0.1',
        )
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", BATCH_1)
            a_path = write_file(tmp_path, "record-2.txt", rec_a)
            b_path = write_file(tmp_path, "record-3.txt", rec_b)
            batch = parse_batch_file(batch_path)
            records = [parse_record_file(a_path), parse_record_file(b_path)]

            result = RecordMatcher([batch], records).match()

        self.assertEqual(len(result.bindings), 2)
        idx_to_name = {
            idx: rec.path.name for (_, idx), rec in result.bindings.items()
        }
        self.assertEqual(idx_to_name[1], "record-2.txt")  # type2=0.44 row
        self.assertEqual(idx_to_name[2], "record-3.txt")  # type2=0.1 row

    def test_orphan_record_reported_as_unmatched(self) -> None:
        orphan = RECORD_1.replace(
            '"record_1",type1,a,0.33,0.33',
            '"record_1",type1,a,9.99,9.99',
        )
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", BATCH_1)
            orphan_path = write_file(tmp_path, "record-99.txt", orphan)
            batch = parse_batch_file(batch_path)
            record = parse_record_file(orphan_path)

            result = RecordMatcher([batch], [record]).match()

        self.assertEqual(result.bindings, {})
        self.assertEqual(len(result.unmatched_records), 1)

    def test_matches_when_columns_are_reordered(self) -> None:
        """Swap field_a/field_b with type1(a)/type2(b) in the column order.

        With value-by-position parsing, a record targeting type1=0.33 would
        silently bind to whatever happens to live in column index 4 — i.e.
        field_a in the reordered file. The dict-keyed model resolves cells
        by header name, so the record correctly lands on the row whose
        type1 column actually holds 0.33 regardless of column order.
        """
        reordered_batch = (
            "title\n"
            "date,05/01/2026\n"
            "key_1,value_1\n"
            "key_2,value_2\n"
            "batch,1\n"
            "\n"
            "records\n"
            "data,1\n"
            # type1/type2 moved before field_a/field_b
            "field_1,field_2,type1(a),type2(b),field_a,field_b,field_5\n"
            "11,abc,0.33,0.38,0.15,0.32,1\n"
            "11,abc,h,i,4,8,j\n"  # trailer
        )
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", reordered_batch)
            record_path = write_file(tmp_path, "record-1.txt", RECORD_1)
            batch = parse_batch_file(batch_path)
            record = parse_record_file(record_path)

            result = RecordMatcher([batch], [record]).match()

        self.assertEqual(len(result.bindings), 1)
        ((_path, idx), bound) = next(iter(result.bindings.items()))
        self.assertEqual(idx, 0)
        self.assertEqual(bound.path.name, "record-1.txt")

    def test_tolerates_extra_columns(self) -> None:
        """An extra column in the records block must not break matching."""
        batch_with_extra_col = (
            "title\n"
            "date,05/01/2026\n"
            "key_1,value_1\n"
            "key_2,value_2\n"
            "batch,1\n"
            "\n"
            "records\n"
            "data,1\n"
            "field_1,field_2,field_a,field_b,type1(a),type2(b),field_5,extra\n"
            "11,abc,0.15,0.32,0.33,0.38,1,X\n"
            "11,abc,4,8,h,i,j,Y\n"  # trailer
        )
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_path = write_file(tmp_path, "batch-1.txt", batch_with_extra_col)
            record_path = write_file(tmp_path, "record-1.txt", RECORD_1)
            batch = parse_batch_file(batch_path)
            record = parse_record_file(record_path)

            result = RecordMatcher([batch], [record]).match()

        self.assertEqual(len(result.bindings), 1)
        # And the extra column survives the round trip via to_csv_row.
        ((_path, idx), _bound) = next(iter(result.bindings.items()))
        self.assertEqual(batch.data_rows[idx].cells["extra"], "X")


class CrossBatchMatchTests(unittest.TestCase):
    """Verify the matcher index finds the right batch even with shared key_2."""

    def test_record_targets_correct_batch_when_key_2_shared(self) -> None:
        """All sample batches share key_2='value_2' but type values differ
        per batch. A record's (type1, type2) must select the right batch.
        """
        from tests._helpers import make_batch, make_record

        # Two batches, both key_2=value_2, but with different type values.
        batch_a = make_batch(
            data_rows=("11,abc,0.10,0.20,AA,BB,1",),
        )
        batch_b = make_batch(
            data_rows=("12,abc,0.30,0.40,CC,DD,2",),
        )
        # Record targets type1=CC, type2=DD — should land in batch_b only.
        record = make_record(type1_value="CC", type2_value="DD")

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            ap = write_file(tmp_path, "batch-1.txt", batch_a)
            bp = write_file(tmp_path, "batch-2.txt", batch_b)
            rp = write_file(tmp_path, "record-1.txt", record)

            result = RecordMatcher(
                [parse_batch_file(ap), parse_batch_file(bp)],
                [parse_record_file(rp)],
            ).match()

        self.assertEqual(len(result.bindings), 1)
        ((bound_path, idx), _) = next(iter(result.bindings.items()))
        self.assertTrue(bound_path.endswith("batch-2.txt"))
        self.assertEqual(idx, 0)


if __name__ == "__main__":
    unittest.main()
