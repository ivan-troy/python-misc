"""Outbox pattern: durable publish queue.

The outbox isolates the merge step from the publish step. After
:mod:`csv_merger.merger` produces a merged file, the runner writes it
to ``outbox/pending/`` *before* attempting to publish. If the publish
fails (network blip, endpoint down), the file stays in ``pending/``
and the next run's first action is to drain it. The merge work is
never re-done.

File naming convention:

    outbox/pending/<run_id>-<sig16>.txt
    outbox/pending/<run_id>-<sig16>.manifest.json
    outbox/sent/<run_id>-<sig16>.txt
    outbox/sent/<run_id>-<sig16>.manifest.json

where ``sig16`` is the first 16 hex characters of the batch signature.
The signature in the filename lets the runner detect "this exact batch
is already in pending" and skip re-writing (which would otherwise
duplicate work after a crash between merge and processed_files
marking).

Manifest sidecar: each ``.txt`` is paired with a ``.manifest.json``
containing the full batch signature and the list of source files
``(path, content_hash)`` that produced it. The drain step uses the
manifest to mark files processed after a successful publish — without
it, the drain step would have no idea which source files were
represented by the merged payload.

Retention: ``sent/`` files are pruned by age, not by count. Default
7 days, configurable per :class:`~csv_merger.pipeline.config.OutboxConfig`.
This gives operators a recent-history audit trail without unbounded
disk growth.

Atomicity: every write goes to a ``.tmp`` file first, then ``os.replace``.
Every move from pending → sent is an ``os.replace``. There is no point
in the pipeline where a partially-written or partially-moved outbox
file is visible to the next run.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from csv_merger.pipeline._errors import PipelineError

logger = logging.getLogger(__name__)

#: Number of hex chars from the batch signature used in filenames.
_SIG_PREFIX_LEN = 16

#: Filename pattern: ``<run_id>-<sig16>.<ext>``. We accept any
#: extension because the merged-file extension is set by the writer
#: layer, not by us.
_FILENAME_RE = re.compile(
    r"^(?P<run_id>\d+)-(?P<sig>[0-9a-f]{" + str(_SIG_PREFIX_LEN) + r"})\."
    r"(?!manifest\.)"  # exclude the manifest sidecar
)

#: Manifest sidecar extension.
_MANIFEST_SUFFIX = ".manifest.json"


# --------------------------------------------------------------------------- #
# Manifest                                                                    #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class OutboxManifest:
    """Sidecar metadata for an outbox file.

    Tracks the full batch signature and the source files that produced
    the merged payload. The drain step reads this to mark processed
    files in state after a successful publish.
    """

    batch_signature: str
    run_id: int
    source_files: tuple[tuple[str, str], ...]  # (path, content_hash) pairs

    def to_json(self) -> str:
        return json.dumps(
            {
                "batch_signature": self.batch_signature,
                "run_id": self.run_id,
                "source_files": [
                    {"path": p, "hash": h} for p, h in self.source_files
                ],
            },
            indent=2,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> "OutboxManifest":
        data = json.loads(text)
        return cls(
            batch_signature=data["batch_signature"],
            run_id=int(data["run_id"]),
            source_files=tuple(
                (entry["path"], entry["hash"])
                for entry in data["source_files"]
            ),
        )


def outbox_filename(run_id: int, batch_signature: str, suffix: str = ".txt") -> str:
    """Return the outbox filename for a given run and batch signature."""
    short = batch_signature[:_SIG_PREFIX_LEN]
    if len(short) != _SIG_PREFIX_LEN:
        raise PipelineError(
            f"batch_signature too short ({len(batch_signature)} chars); "
            f"need at least {_SIG_PREFIX_LEN}"
        )
    return f"{run_id}-{short}{suffix}"


def manifest_path_for(outbox_file: Path) -> Path:
    """Return the manifest sidecar path for an outbox file.

    ``outbox/pending/7-abc...txt`` → ``outbox/pending/7-abc...manifest.json``.
    """
    # Drop the original suffix (e.g. .txt) and append the manifest suffix.
    return outbox_file.with_suffix(_MANIFEST_SUFFIX)


def write_to_outbox(
    source_file: Path,
    run_id: int,
    batch_signature: str,
    pending_dir: Path,
    *,
    source_files: list[tuple[str, str]],
    suffix: str = ".txt",
) -> Path:
    """Copy ``source_file`` into ``pending_dir`` atomically, with manifest.

    The destination filename is derived from ``run_id`` and
    ``batch_signature``. A sidecar ``.manifest.json`` is written
    alongside, listing the source files (path + content_hash) that
    produced the merged payload. The drain step uses the manifest to
    mark files processed after a successful publish.

    If a file with the same name already exists, this is a no-op and
    the existing path is returned — the same batch was already written
    by a previous run that crashed before publishing.

    Args:
        source_file: Path to the merged file produced by the merger.
        run_id: The current run's ID, used in the filename.
        batch_signature: Hex signature of the batch (full length;
            we slice the prefix internally and embed the full value
            in the manifest).
        pending_dir: Outbox/pending directory.
        source_files: List of ``(source_path, content_hash)`` tuples
            that produced the merged payload. Stored in the manifest.
        suffix: File extension to use in the outbox filename.

    Returns:
        The path of the file in ``pending_dir``.
    """
    pending_dir.mkdir(parents=True, exist_ok=True)
    target = pending_dir / outbox_filename(run_id, batch_signature, suffix)
    manifest_target = manifest_path_for(target)

    if target.exists():
        logger.info(
            "outbox already contains %s; skipping re-write", target.name
        )
        return target

    # Write to sibling .tmp files inside pending_dir so the final
    # ``os.replace`` is on the same filesystem (atomic guarantee).
    _atomic_copy(source_file, target, pending_dir)

    manifest = OutboxManifest(
        batch_signature=batch_signature,
        run_id=run_id,
        source_files=tuple(source_files),
    )
    _atomic_write_text(manifest.to_json(), manifest_target, pending_dir)

    logger.info(
        "wrote outbox file %s with manifest (%d source files)",
        target.name,
        len(source_files),
    )
    return target


def read_manifest(outbox_file: Path) -> OutboxManifest:
    """Read the manifest sidecar for ``outbox_file``.

    Raises:
        PipelineError: if the manifest is missing or malformed. A drain
            step can't safely proceed without knowing which source
            files the outbox file represents.
    """
    path = manifest_path_for(outbox_file)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PipelineError(
            f"missing manifest sidecar for {outbox_file}: {path}"
        ) from exc
    try:
        return OutboxManifest.from_json(text)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise PipelineError(
            f"malformed manifest at {path}: {exc}"
        ) from exc


def list_pending(pending_dir: Path) -> list[Path]:
    """List outbox-pending files in deterministic (filename) order.

    Manifest sidecars are excluded — the runner walks the data files
    and reads each one's manifest separately. Files with names that
    don't match the outbox naming convention are ignored.
    """
    if not pending_dir.is_dir():
        return []
    matches = [
        p for p in pending_dir.iterdir()
        if p.is_file() and _FILENAME_RE.match(p.name)
    ]
    matches.sort(key=lambda p: p.name)
    return matches


def mark_published(
    pending_path: Path,
    sent_dir: Path,
) -> Path:
    """Move a successfully-published file from ``pending/`` to ``sent/``.

    Moves both the data file and its manifest sidecar atomically. If
    the manifest is missing (shouldn't happen post-write_to_outbox)
    only the data file is moved and a warning is logged.
    """
    sent_dir.mkdir(parents=True, exist_ok=True)
    target = sent_dir / pending_path.name
    os.replace(pending_path, target)

    manifest_src = manifest_path_for(pending_path)
    if manifest_src.exists():
        manifest_target = manifest_path_for(target)
        os.replace(manifest_src, manifest_target)
    else:
        logger.warning(
            "no manifest sidecar to move alongside %s",
            pending_path.name,
        )

    logger.debug("moved to sent: %s", target)
    return target


def cleanup_old_sent(
    sent_dir: Path,
    retention_days: int,
    *,
    now: datetime | None = None,
) -> int:
    """Delete files in ``sent_dir`` older than ``retention_days``.

    Includes both data files and manifest sidecars. Returns the number
    of files deleted (data + manifest counted separately).
    """
    if not sent_dir.is_dir():
        return 0
    cutoff_now = now if now is not None else datetime.now(timezone.utc)
    cutoff = cutoff_now - timedelta(days=max(retention_days, 0))
    cutoff_timestamp = cutoff.timestamp()

    deleted = 0
    for entry in sent_dir.iterdir():
        if not entry.is_file():
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        if mtime < cutoff_timestamp:
            try:
                entry.unlink()
                deleted += 1
            except OSError as exc:
                logger.warning(
                    "failed to prune outbox/sent file %s: %s", entry, exc
                )
    if deleted:
        logger.info(
            "pruned %d outbox/sent file(s) older than %d day(s)",
            deleted,
            retention_days,
        )
    return deleted


def cleanup_staging_debris(staging_dir: Path) -> int:
    """Remove ``.tmp`` files left behind by crashed writes.

    Safe to call at run start; the runner does so. Returns the number
    of files cleaned up.
    """
    if not staging_dir.is_dir():
        return 0
    removed = 0
    for entry in staging_dir.iterdir():
        if entry.is_file() and entry.suffix == ".tmp":
            try:
                entry.unlink()
                removed += 1
            except OSError as exc:
                logger.warning(
                    "failed to clean staging debris %s: %s", entry, exc
                )
    if removed:
        logger.info(
            "cleaned %d staging .tmp file(s) from previous run",
            removed,
        )
    return removed


# --------------------------------------------------------------------------- #
# Internal: atomic write helpers                                              #
# --------------------------------------------------------------------------- #


def _atomic_copy(src: Path, target: Path, scratch_dir: Path) -> None:
    """Copy ``src`` → ``target`` atomically via a sibling ``.tmp`` file."""
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(scratch_dir),
    )
    tmp_path = Path(tmp_name)
    os.close(tmp_fd)
    try:
        shutil.copyfile(src, tmp_path)
        with tmp_path.open("rb+") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp_path, target)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _atomic_write_text(text: str, target: Path, scratch_dir: Path) -> None:
    """Write ``text`` → ``target`` atomically via a sibling ``.tmp`` file."""
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(scratch_dir),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise
