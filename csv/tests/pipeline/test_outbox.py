"""Tests for csv_merger.pipeline.outbox."""

from __future__ import annotations

import os
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.pipeline._errors import PipelineError
from csv_merger.pipeline.outbox import (
    OutboxManifest,
    cleanup_old_sent,
    cleanup_staging_debris,
    list_pending,
    manifest_path_for,
    mark_published,
    outbox_filename,
    read_manifest,
    write_to_outbox,
)


_SIG = "a" * 64  # plausible-looking sha256


class FilenameTests(unittest.TestCase):
    def test_filename_uses_run_id_and_short_sig(self) -> None:
        self.assertEqual(
            outbox_filename(42, _SIG),
            f"42-{'a' * 16}.txt",
        )

    def test_custom_suffix(self) -> None:
        self.assertEqual(
            outbox_filename(1, _SIG, suffix=".csv"),
            f"1-{'a' * 16}.csv",
        )

    def test_short_signature_rejected(self) -> None:
        with self.assertRaises(PipelineError):
            outbox_filename(1, "tooshort")


class WriteToOutboxTests(unittest.TestCase):
    def _make_source(self, tmp: Path, content: bytes = b"merged") -> Path:
        src = tmp / "merged.txt"
        src.write_bytes(content)
        return src

    def _files(self) -> list[tuple[str, str]]:
        return [("a.txt", "h1"), ("b.txt", "h2")]

    def test_writes_file_with_expected_name(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = self._make_source(tmp_path)
            pending = tmp_path / "pending"

            result = write_to_outbox(
                src, 7, _SIG, pending, source_files=self._files()
            )

            self.assertEqual(result.name, f"7-{'a' * 16}.txt")
            self.assertTrue(result.exists())
            self.assertEqual(result.read_bytes(), b"merged")

    def test_writes_atomically_no_tmp_debris(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = self._make_source(tmp_path)
            pending = tmp_path / "pending"

            write_to_outbox(
                src, 1, _SIG, pending, source_files=self._files()
            )

            tmp_files = [
                p for p in pending.iterdir() if p.name.startswith(".")
            ]
            self.assertEqual(tmp_files, [])

    def test_existing_outbox_file_is_not_overwritten(self) -> None:
        """Crash-recovery property: same batch, same run_id stays as-is."""
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = self._make_source(tmp_path, b"original")
            pending = tmp_path / "pending"

            first = write_to_outbox(
                src, 1, _SIG, pending, source_files=self._files()
            )
            # Now "merge" a different file with the same signature
            # (this would be a re-run of the same batch).
            src.write_bytes(b"different content")
            second = write_to_outbox(
                src, 1, _SIG, pending, source_files=self._files()
            )

            self.assertEqual(first, second)
            self.assertEqual(first.read_bytes(), b"original")

    def test_manifest_sidecar_is_written(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = self._make_source(tmp_path)
            pending = tmp_path / "pending"

            result = write_to_outbox(
                src, 7, _SIG, pending, source_files=self._files()
            )

            manifest_path = result.with_suffix(".manifest.json")
            self.assertTrue(manifest_path.exists())
            text = manifest_path.read_text(encoding="utf-8")
            self.assertIn(_SIG, text)
            self.assertIn("a.txt", text)
            self.assertIn("b.txt", text)


class ListPendingTests(unittest.TestCase):
    def test_empty_dir_returns_empty(self) -> None:
        with TemporaryDirectory() as tmp:
            self.assertEqual(list_pending(Path(tmp)), [])

    def test_missing_dir_returns_empty(self) -> None:
        self.assertEqual(list_pending(Path("/no/such/path")), [])

    def test_returns_only_conformant_filenames(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / f"1-{'a' * 16}.txt").write_text("ok")
            (tmp_path / f"2-{'b' * 16}.txt").write_text("ok")
            (tmp_path / "random.txt").write_text("ignored")
            (tmp_path / ".hidden.tmp").write_text("ignored")
            # Manifest sidecars must not appear in the pending list.
            (tmp_path / f"1-{'a' * 16}.manifest.json").write_text("{}")

            result = list_pending(tmp_path)

            self.assertEqual(len(result), 2)
            self.assertTrue(
                all(r.name.endswith(".txt") for r in result)
            )

    def test_returns_sorted_order(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            names = [
                f"5-{'a' * 16}.txt",
                f"2-{'b' * 16}.txt",
                f"10-{'c' * 16}.txt",
            ]
            for n in names:
                (tmp_path / n).write_text("ok")

            result = list_pending(tmp_path)
            # Lexicographic sort: "10..." comes after "2..."
            self.assertEqual(
                [p.name for p in result],
                sorted(names),
            )


class MarkPublishedTests(unittest.TestCase):
    def test_moves_to_sent_dir(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            pending = tmp_path / "pending"
            sent = tmp_path / "sent"
            pending.mkdir()
            f = pending / f"1-{'a' * 16}.txt"
            f.write_text("hi")

            result = mark_published(f, sent)

            self.assertFalse(f.exists())
            self.assertEqual(result.parent, sent)
            self.assertEqual(result.read_text(), "hi")

    def test_moves_manifest_alongside(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            pending = tmp_path / "pending"
            sent = tmp_path / "sent"
            pending.mkdir()
            f = pending / f"1-{'a' * 16}.txt"
            f.write_text("hi")
            manifest = pending / f"1-{'a' * 16}.manifest.json"
            manifest.write_text("{}")

            mark_published(f, sent)

            self.assertFalse(manifest.exists())
            self.assertTrue((sent / manifest.name).exists())


class ManifestTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        m = OutboxManifest(
            batch_signature="abc" * 22,  # ~64-ish
            run_id=42,
            source_files=(("a.txt", "h1"), ("b.txt", "h2")),
        )
        roundtripped = OutboxManifest.from_json(m.to_json())
        self.assertEqual(roundtripped, m)

    def test_read_manifest_finds_sidecar(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = tmp_path / "merged.txt"
            src.write_text("merged")
            pending = tmp_path / "pending"

            out = write_to_outbox(
                src, 7, _SIG, pending,
                source_files=[("a.txt", "h1"), ("b.txt", "h2")],
            )
            manifest = read_manifest(out)
            self.assertEqual(manifest.run_id, 7)
            self.assertEqual(manifest.batch_signature, _SIG)
            self.assertEqual(
                manifest.source_files,
                (("a.txt", "h1"), ("b.txt", "h2")),
            )

    def test_read_manifest_missing_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            data = tmp_path / f"1-{'a' * 16}.txt"
            data.write_text("hi")
            with self.assertRaises(PipelineError):
                read_manifest(data)

    def test_read_manifest_malformed_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            data = tmp_path / f"1-{'a' * 16}.txt"
            data.write_text("hi")
            manifest_path_for(data).write_text("{not valid json")
            with self.assertRaises(PipelineError):
                read_manifest(data)


class CleanupOldSentTests(unittest.TestCase):
    def test_deletes_files_older_than_retention(self) -> None:
        with TemporaryDirectory() as tmp:
            sent_dir = Path(tmp)
            old = sent_dir / "old.txt"
            new = sent_dir / "new.txt"
            old.write_text("old")
            new.write_text("new")

            # Backdate `old` by 30 days.
            old_time = time.time() - 30 * 86400
            os.utime(old, (old_time, old_time))

            deleted = cleanup_old_sent(sent_dir, retention_days=7)

            self.assertEqual(deleted, 1)
            self.assertFalse(old.exists())
            self.assertTrue(new.exists())

    def test_zero_retention_deletes_everything(self) -> None:
        with TemporaryDirectory() as tmp:
            sent_dir = Path(tmp)
            (sent_dir / "a.txt").write_text("a")
            (sent_dir / "b.txt").write_text("b")

            # We need to ensure mtime is < now; the just-written files
            # have mtime ≈ now. Use a future ``now`` to compare against.
            future = datetime.now(timezone.utc) + timedelta(seconds=10)
            deleted = cleanup_old_sent(sent_dir, retention_days=0, now=future)

            self.assertEqual(deleted, 2)

    def test_missing_dir_is_noop(self) -> None:
        self.assertEqual(
            cleanup_old_sent(Path("/no/such/dir"), retention_days=7),
            0,
        )


class CleanupStagingDebrisTests(unittest.TestCase):
    def test_removes_tmp_files(self) -> None:
        with TemporaryDirectory() as tmp:
            staging = Path(tmp)
            (staging / "junk.tmp").write_text("x")
            (staging / "legit.txt").write_text("y")

            removed = cleanup_staging_debris(staging)

            self.assertEqual(removed, 1)
            self.assertFalse((staging / "junk.tmp").exists())
            self.assertTrue((staging / "legit.txt").exists())

    def test_missing_dir_is_noop(self) -> None:
        self.assertEqual(
            cleanup_staging_debris(Path("/no/such/dir")),
            0,
        )


if __name__ == "__main__":
    unittest.main()
