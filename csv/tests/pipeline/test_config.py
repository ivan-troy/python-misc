"""Tests for csv_merger.pipeline.config."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent

from csv_merger.pipeline._errors import ConfigError
from csv_merger.pipeline.config import (
    DEFAULT_SMTP_PASSWORD_ENV,
    PipelineConfig,
    _build_config,
    load_config,
)


def _minimal_raw() -> dict:
    return {
        "folders": {
            "source": "/tmp/src",
            "staging": "/tmp/st",
            "fetched": "/tmp/f",
            "outbox_pending": "/tmp/op",
            "outbox_sent": "/tmp/os",
            "dead_letter": "/tmp/dl",
        },
        "state": {
            "db_path": "/tmp/pipeline.db",
            "lock_path": "/tmp/pipeline.lock",
        },
        "publish": {"url": "https://example.com/ingest"},
        "email": {
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "from_address": "p@example.com",
            "to_addresses": ["ops@example.com"],
        },
    }


class BuildConfigTests(unittest.TestCase):
    def test_minimal_valid_config_constructs(self) -> None:
        cfg = _build_config(_minimal_raw())
        self.assertIsInstance(cfg, PipelineConfig)
        self.assertEqual(cfg.publish.url, "https://example.com/ingest")
        self.assertEqual(cfg.email.to_addresses, ("ops@example.com",))
        self.assertEqual(cfg.email.smtp_password_env, DEFAULT_SMTP_PASSWORD_ENV)

    def test_defaults_applied_for_omitted_sections(self) -> None:
        cfg = _build_config(_minimal_raw())
        self.assertEqual(cfg.quiescence.quiet_seconds, 30)
        self.assertEqual(cfg.fetch.parallel_workers, 16)
        self.assertEqual(cfg.retry_policy.max_attempts_per_batch, 3)
        self.assertEqual(cfg.outbox.sent_retention_days, 7)

    def test_optional_section_overrides_default(self) -> None:
        raw = _minimal_raw()
        raw["fetch"] = {"parallel_workers": 32}
        raw["quiescence"] = {"quiet_seconds": 60, "max_wait_seconds": 120}
        cfg = _build_config(raw)
        self.assertEqual(cfg.fetch.parallel_workers, 32)
        self.assertEqual(cfg.quiescence.quiet_seconds, 60)
        self.assertEqual(cfg.quiescence.max_wait_seconds, 120)

    def test_missing_required_section_raises(self) -> None:
        raw = _minimal_raw()
        del raw["publish"]
        with self.assertRaises(ConfigError) as ctx:
            _build_config(raw)
        self.assertIn("publish", str(ctx.exception))

    def test_missing_required_key_raises(self) -> None:
        raw = _minimal_raw()
        del raw["publish"]["url"]
        with self.assertRaises(ConfigError) as ctx:
            _build_config(raw)
        self.assertIn("url", str(ctx.exception))

    def test_unknown_optional_key_raises(self) -> None:
        """Typos in optional sections must fail loudly."""
        raw = _minimal_raw()
        raw["fetch"] = {"paralel_workers": 16}  # note the typo
        with self.assertRaises(ConfigError):
            _build_config(raw)

    def test_to_addresses_becomes_tuple(self) -> None:
        """Mutable list from TOML becomes immutable tuple in the dataclass."""
        cfg = _build_config(_minimal_raw())
        self.assertIsInstance(cfg.email.to_addresses, tuple)


class LoadConfigTests(unittest.TestCase):
    def test_missing_file_raises(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            load_config(Path("/nonexistent/path.toml"))
        self.assertIn("not found", str(ctx.exception))

    def test_invalid_toml_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text("[invalid syntax here", encoding="utf-8")
            with self.assertRaises(ConfigError) as ctx:
                load_config(path)
            self.assertIn("TOML", str(ctx.exception))

    def test_minimal_toml_loads(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text(
                dedent("""\
                    [folders]
                    source = "/tmp/src"
                    staging = "/tmp/st"
                    fetched = "/tmp/f"
                    outbox_pending = "/tmp/op"
                    outbox_sent = "/tmp/os"
                    dead_letter = "/tmp/dl"

                    [state]
                    db_path = "/tmp/pipeline.db"
                    lock_path = "/tmp/pipeline.lock"

                    [publish]
                    url = "https://example.com/ingest"

                    [email]
                    smtp_host = "smtp.example.com"
                    smtp_port = 587
                    from_address = "p@example.com"
                    to_addresses = ["ops@example.com"]
                """),
                encoding="utf-8",
            )
            cfg = load_config(path)
            self.assertEqual(cfg.publish.url, "https://example.com/ingest")


if __name__ == "__main__":
    unittest.main()
