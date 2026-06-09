"""Pipeline configuration.

Loaded from a TOML file (Python 3.11+ stdlib :mod:`tomllib`). The shape
is a single frozen dataclass with nested frozen dataclasses for each
logical section. Loading validates that required fields are present and
that paths can be created; nothing about the *contents* of the source
folder is checked here — that's the runner's job.

Sensitive values (SMTP password) are loaded from environment variables,
never from the TOML file. The env-var name is configurable per
deployment but defaults to ``CSV_MERGER_SMTP_PASSWORD``.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from csv_merger.pipeline._errors import ConfigError

#: Default name of the environment variable holding the SMTP password.
DEFAULT_SMTP_PASSWORD_ENV = "CSV_MERGER_SMTP_PASSWORD"


# --------------------------------------------------------------------------- #
# Nested config sections                                                      #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FoldersConfig:
    """Filesystem locations the pipeline reads and writes."""

    source: Path
    staging: Path
    fetched: Path
    outbox_pending: Path
    outbox_sent: Path
    dead_letter: Path


@dataclass(frozen=True)
class StateConfig:
    """Where pipeline run-state lives on disk."""

    db_path: Path
    lock_path: Path


@dataclass(frozen=True)
class QuiescenceConfig:
    """Settings for "wait until the source folder is no longer changing"."""

    quiet_seconds: int = 30
    max_wait_seconds: int = 90
    poll_interval_seconds: int = 5


@dataclass(frozen=True)
class FetchConfig:
    """Settings for the parallel file-fetch step."""

    parallel_workers: int = 16


@dataclass(frozen=True)
class PublishConfig:
    """Settings for the HTTP publish step."""

    url: str
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 60.0


@dataclass(frozen=True)
class RetryPolicyConfig:
    """How many consecutive batch failures before quarantine."""

    max_attempts_per_batch: int = 3


@dataclass(frozen=True)
class OutboxConfig:
    """Outbox file retention policy."""

    sent_retention_days: int = 7


@dataclass(frozen=True)
class EmailConfig:
    """SMTP settings for alerting."""

    smtp_host: str
    smtp_port: int
    from_address: str
    to_addresses: tuple[str, ...]
    smtp_username: str = ""
    smtp_use_starttls: bool = True
    smtp_password_env: str = DEFAULT_SMTP_PASSWORD_ENV
    rate_limit_per_hour: int = 1

    @property
    def smtp_password(self) -> str:
        """Resolve the password from the environment.

        Returns the empty string if the env var is unset; the alerting
        module decides whether that's acceptable (e.g. unauthenticated
        relay) or a failure.
        """
        return os.environ.get(self.smtp_password_env, "")


# --------------------------------------------------------------------------- #
# Top-level config                                                            #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration."""

    folders: FoldersConfig
    state: StateConfig
    publish: PublishConfig
    email: EmailConfig
    quiescence: QuiescenceConfig = field(default_factory=QuiescenceConfig)
    fetch: FetchConfig = field(default_factory=FetchConfig)
    retry_policy: RetryPolicyConfig = field(default_factory=RetryPolicyConfig)
    outbox: OutboxConfig = field(default_factory=OutboxConfig)


# --------------------------------------------------------------------------- #
# Loading                                                                     #
# --------------------------------------------------------------------------- #


def load_config(path: Path) -> PipelineConfig:
    """Load a :class:`PipelineConfig` from a TOML file.

    Args:
        path: Path to the TOML file.

    Returns:
        A validated :class:`PipelineConfig`.

    Raises:
        ConfigError: if the file is missing, unreadable, or missing
            required keys.
    """
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")

    try:
        with path.open("rb") as fh:
            raw = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"{path}: cannot parse TOML: {exc}") from exc

    try:
        return _build_config(raw)
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"{path}: {exc}") from exc


def _build_config(raw: Mapping[str, Any]) -> PipelineConfig:
    """Construct a :class:`PipelineConfig` from a parsed TOML dict.

    Separated from :func:`load_config` so tests can build configs from
    in-memory dicts without going through disk.
    """
    folders = FoldersConfig(
        source=Path(_require(raw, "folders", "source")),
        staging=Path(_require(raw, "folders", "staging")),
        fetched=Path(_require(raw, "folders", "fetched")),
        outbox_pending=Path(_require(raw, "folders", "outbox_pending")),
        outbox_sent=Path(_require(raw, "folders", "outbox_sent")),
        dead_letter=Path(_require(raw, "folders", "dead_letter")),
    )
    state = StateConfig(
        db_path=Path(_require(raw, "state", "db_path")),
        lock_path=Path(_require(raw, "state", "lock_path")),
    )
    publish_raw = _require_section(raw, "publish")
    publish = PublishConfig(
        url=_require(raw, "publish", "url"),
        **{
            key: publish_raw[key]
            for key in (
                "max_attempts",
                "base_delay_seconds",
                "max_delay_seconds",
                "connect_timeout_seconds",
                "read_timeout_seconds",
            )
            if key in publish_raw
        },
    )
    email_raw = _require_section(raw, "email")
    email = EmailConfig(
        smtp_host=_require(raw, "email", "smtp_host"),
        smtp_port=_require(raw, "email", "smtp_port"),
        from_address=_require(raw, "email", "from_address"),
        to_addresses=tuple(_require(raw, "email", "to_addresses")),
        **{
            key: email_raw[key]
            for key in (
                "smtp_username",
                "smtp_use_starttls",
                "smtp_password_env",
                "rate_limit_per_hour",
            )
            if key in email_raw
        },
    )

    quiescence = _build_optional(
        raw, "quiescence", QuiescenceConfig
    )
    fetch = _build_optional(raw, "fetch", FetchConfig)
    retry_policy = _build_optional(raw, "retry_policy", RetryPolicyConfig)
    outbox = _build_optional(raw, "outbox", OutboxConfig)

    return PipelineConfig(
        folders=folders,
        state=state,
        publish=publish,
        email=email,
        quiescence=quiescence,
        fetch=fetch,
        retry_policy=retry_policy,
        outbox=outbox,
    )


def _require(raw: Mapping[str, Any], section: str, key: str) -> Any:
    sect = _require_section(raw, section)
    if key not in sect:
        raise ConfigError(f"[{section}] missing required key {key!r}")
    return sect[key]


def _require_section(raw: Mapping[str, Any], section: str) -> Mapping[str, Any]:
    if section not in raw:
        raise ConfigError(f"missing required section [{section}]")
    value = raw[section]
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{section}] must be a TOML table")
    return value


def _build_optional(
    raw: Mapping[str, Any],
    section: str,
    cls: type,
) -> Any:
    """Build an optional section dataclass, defaulting to ``cls()``.

    Unknown keys raise — better to fail fast than silently ignore a
    typo'd config knob.
    """
    if section not in raw:
        return cls()
    sect = raw[section]
    if not isinstance(sect, Mapping):
        raise ConfigError(f"[{section}] must be a TOML table")
    try:
        return cls(**sect)
    except TypeError as exc:
        raise ConfigError(f"[{section}]: {exc}") from exc
