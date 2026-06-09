"""Network I/O: fetch files from a source folder, publish to an HTTP endpoint.

This module is the operational boundary between "trust nothing" and "the
rest of the pipeline." Every call here can fail transiently; the helpers
classify failures into :class:`TransientError` (retry) and
:class:`PermanentError` (don't), and bound the work with timeouts.

Design notes:

* **Stdlib-only HTTP.** ``urllib.request`` carries the publish request.
  We don't need connection pooling (one PUT per run) or fancy header
  handling. The 30 lines of urllib code are simpler than adding a
  dependency.

* **Parallel fetch, serial publish.** The fetch step is I/O-bound and
  embarrassingly parallel; we use a ``ThreadPoolExecutor`` with a
  configurable worker count. The publish step is one merged file →
  one PUT, so serial is correct.

* **Fail-the-batch on any fetch failure.** The runner's contract is
  "data is critical; missing 1 of 500 files corrupts the output." So
  if any fetch fails after retries, the whole batch is aborted. The
  state store's ``processed_files`` is untouched, so the next run
  picks everything up.

* **Atomic local staging.** Files are downloaded to ``staging/<name>.tmp``,
  size-verified, fsynced, then atomically renamed to ``fetched/<name>``.
  A process crash mid-fetch leaves debris in ``staging/`` (cleanable
  on startup) and never produces a partial file in ``fetched/``.
"""

from __future__ import annotations

import logging
import os
import shutil
import socket
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http.client import HTTPException
from pathlib import Path
from typing import Mapping

from csv_merger.pipeline._errors import PermanentError, TransientError
from csv_merger.pipeline.retry import with_retry

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# HTTP status classification                                                  #
# --------------------------------------------------------------------------- #
#
# Standard "retry these" set. 408 Request Timeout, 429 Too Many Requests,
# and 5xx Server Errors are retryable per RFC + practical convention.
# Other 4xx are permanent (a 400 Bad Request won't succeed on retry).

_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


# --------------------------------------------------------------------------- #
# Public types                                                                #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FetchedFile:
    """A file successfully fetched into local staging.

    Attributes:
        source_path: Original path on the remote source (for state tracking).
        local_path: Where the file now lives on local disk (in ``fetched/``).
        size_bytes: Verified size; matches both source and local copy.
    """

    source_path: Path
    local_path: Path
    size_bytes: int


# --------------------------------------------------------------------------- #
# Fetch                                                                       #
# --------------------------------------------------------------------------- #


def fetch_files(
    sources: list[Path],
    staging_dir: Path,
    fetched_dir: Path,
    *,
    parallel_workers: int = 16,
    fetch_max_attempts: int = 3,
    fetch_base_delay: float = 0.5,
    fetch_max_delay: float = 5.0,
) -> list[FetchedFile]:
    """Copy ``sources`` into ``fetched_dir`` via ``staging_dir``, in parallel.

    Each file is downloaded to ``staging_dir/<name>.tmp``, size-verified
    against the source, fsynced, and atomically renamed into
    ``fetched_dir/<name>``. The single-file step is retried up to
    ``fetch_max_attempts`` times with exponential backoff.

    If any file fails after retries, **all in-flight fetches are
    cancelled and the whole batch fails**. The caller is expected to
    treat partial success as failure for the "data is critical"
    constraint.

    Args:
        sources: Source file paths (typically UNC paths on Windows).
        staging_dir: Local directory for in-flight ``.tmp`` files.
        fetched_dir: Local directory for committed files.
        parallel_workers: Thread pool size for the fetch.
        fetch_max_attempts: Per-file retry budget.

    Returns:
        List of :class:`FetchedFile`, in the same order as ``sources``.

    Raises:
        TransientError: if any file fails after all retries (the runner
            treats this as a run failure; next tick retries the batch).
        PermanentError: if a source file is missing or unreadable.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    fetched_dir.mkdir(parents=True, exist_ok=True)

    if not sources:
        return []

    logger.info(
        "fetching %d file(s) with %d workers",
        len(sources),
        parallel_workers,
    )
    results: dict[Path, FetchedFile] = {}

    with ThreadPoolExecutor(max_workers=parallel_workers) as pool:
        futures = {
            pool.submit(
                _fetch_one_with_retry,
                src,
                staging_dir,
                fetched_dir,
                fetch_max_attempts,
                fetch_base_delay,
                fetch_max_delay,
            ): src
            for src in sources
        }
        try:
            for future in as_completed(futures):
                src = futures[future]
                # ``.result()`` re-raises whatever the worker raised. We
                # surface the first failure and let context-manager exit
                # drain the rest.
                results[src] = future.result()
        except (TransientError, PermanentError):
            # Cancel remaining futures so we don't waste network time.
            for f in futures:
                f.cancel()
            raise

    # Preserve input order in the returned list.
    return [results[src] for src in sources]


def _fetch_one_with_retry(
    source: Path,
    staging_dir: Path,
    fetched_dir: Path,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
) -> FetchedFile:
    """Wrap :func:`_fetch_one` in the project's retry policy."""
    return with_retry(
        lambda: _fetch_one(source, staging_dir, fetched_dir),
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
    )


def _fetch_one(
    source: Path,
    staging_dir: Path,
    fetched_dir: Path,
) -> FetchedFile:
    """Copy one file with atomic staging.

    Raises:
        PermanentError: source is missing or unreadable.
        TransientError: copy or stat failed (network blip).
    """
    try:
        source_stat = source.stat()
    except FileNotFoundError as exc:
        raise PermanentError(f"source file vanished: {source}") from exc
    except OSError as exc:
        # Could be network-flaky; treat as transient.
        raise TransientError(f"cannot stat {source}: {exc}") from exc

    expected_size = source_stat.st_size

    # ``mkstemp`` inside staging_dir guarantees uniqueness even under
    # concurrent fetches; we don't keep the fd because we want to use
    # shutil.copyfile (which opens its own handles) and we'll fsync via
    # a re-open at the end.
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{source.name}.",
        suffix=".tmp",
        dir=str(staging_dir),
    )
    tmp_path = Path(tmp_name)
    os.close(tmp_fd)

    final_path = fetched_dir / source.name

    try:
        try:
            shutil.copyfile(source, tmp_path)
        except FileNotFoundError as exc:
            raise PermanentError(
                f"source vanished mid-copy: {source}"
            ) from exc
        except (OSError, shutil.SameFileError) as exc:
            raise TransientError(
                f"copy failed: {source} -> {tmp_path}: {exc}"
            ) from exc

        # Size check: a partial network read can produce a short copy
        # with no error from shutil. The size mismatch is the most
        # reliable signal we have without a checksum from the source.
        actual_size = tmp_path.stat().st_size
        if actual_size != expected_size:
            raise TransientError(
                f"size mismatch for {source}: "
                f"expected {expected_size}, got {actual_size}"
            )

        # Durability: flush the file's contents to disk before rename
        # so a power loss after rename leaves a complete file.
        with tmp_path.open("rb+") as fh:
            os.fsync(fh.fileno())

        # Atomic rename — replaces fetched/<name> if it already exists
        # (which can happen if a previous run was killed between fetch
        # and processed_files marking).
        os.replace(tmp_path, final_path)
    except BaseException:
        # Clean up the staging file on any failure. Ignore unlink errors
        # since the file may already be gone (rename succeeded but
        # something later raised) or never have been created.
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise

    logger.debug(
        "fetched %s -> %s (%d bytes)", source, final_path, expected_size
    )
    return FetchedFile(
        source_path=source,
        local_path=final_path,
        size_bytes=expected_size,
    )


# --------------------------------------------------------------------------- #
# Publish                                                                     #
# --------------------------------------------------------------------------- #


def publish_file(
    local_path: Path,
    url: str,
    *,
    idempotency_key: str,
    extra_headers: Mapping[str, str] | None = None,
    method: str = "PUT",
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    connect_timeout: float = 10.0,
    read_timeout: float = 60.0,
) -> None:
    """Upload ``local_path`` to ``url`` with retry on transient failures.

    The HTTP request carries an ``Idempotency-Key`` header (the
    ``idempotency_key`` argument). The server is expected to use this
    to de-duplicate retried requests. Generate the key from the batch
    signature so the same batch retried always uses the same key.

    Args:
        local_path: File to upload (whole file in memory; sized for
            merged outputs of a few hundred KB to low MB).
        url: Destination URL.
        idempotency_key: Value of the ``Idempotency-Key`` header.
        extra_headers: Any additional headers to send.
        method: ``"PUT"`` (default) or ``"POST"``.
        max_attempts: Total HTTP attempts, including the first.
        connect_timeout: Socket connect timeout in seconds.
        read_timeout: Socket read timeout in seconds.

    Raises:
        TransientError: after exhausting retries on transient HTTP errors.
        PermanentError: on permanent HTTP errors (most 4xx, malformed URL).
    """
    body = local_path.read_bytes()
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(body)),
        "Idempotency-Key": idempotency_key,
    }
    if extra_headers:
        headers.update(extra_headers)

    def attempt() -> None:
        request = urllib.request.Request(
            url=url,
            data=body,
            headers=headers,
            method=method.upper(),
        )
        # urllib uses the socket-level timeout for both connect and
        # read. We pass the larger of the two so the connection has a
        # chance to establish; the per-call timeout is enforced by a
        # custom opener if we ever need to distinguish.
        timeout = max(connect_timeout, read_timeout)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                status = resp.status
                # 2xx → success. Drain the body so the connection can be
                # cleanly closed (some servers misbehave if we don't).
                resp.read()
                if 200 <= status < 300:
                    logger.debug(
                        "publish ok: %s -> %d", url, status
                    )
                    return
                # urllib only raises on >=400; a 3xx without redirect
                # following or a weird 1xx would land here. Treat as
                # permanent — the server is doing something we can't
                # transparently retry.
                raise PermanentError(
                    f"unexpected status {status} from {url}"
                )
        except urllib.error.HTTPError as exc:
            _classify_http_error(exc, url)
        except (urllib.error.URLError, socket.timeout, HTTPException, OSError) as exc:
            # URLError wraps socket errors; OSError catches transport
            # issues (DNS, connection refused, network unreachable).
            raise TransientError(
                f"transport error contacting {url}: {exc}"
            ) from exc

    with_retry(
        attempt,
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
    )


def _classify_http_error(
    exc: urllib.error.HTTPError, url: str
) -> None:
    """Convert ``HTTPError`` into the right pipeline exception.

    ``HTTPError`` is the urllib type for >=400 status codes. We split
    them into transient (retry) and permanent (give up) buckets.
    """
    status = exc.code
    # Try to read the body for diagnostics but don't fail if we can't.
    try:
        body_snippet = exc.read().decode("utf-8", errors="replace")[:200]
    except Exception:  # pragma: no cover — defensive only
        body_snippet = "<body unreadable>"

    msg = f"HTTP {status} from {url}: {body_snippet}"
    if status in _RETRYABLE_STATUS_CODES:
        raise TransientError(msg) from exc
    raise PermanentError(msg) from exc
