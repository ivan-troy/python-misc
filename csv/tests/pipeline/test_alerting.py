"""Tests for csv_merger.pipeline.alerting.

SMTP is mocked at the ``_smtp_send`` boundary so we don't need to stand
up a real SMTP server. The mock receives the fully-constructed
``EmailMessage``, which lets us assert on subject, body, recipients,
and headers — most of what matters for "did we form the message
correctly."
"""

from __future__ import annotations

import smtplib
import unittest
from email.message import EmailMessage
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from csv_merger.pipeline.alerting import (
    ALERT_CATASTROPHIC,
    ALERT_QUARANTINE,
    Alerter,
)
from csv_merger.pipeline.config import EmailConfig
from csv_merger.pipeline.state import StateStore


def _email_config(**overrides) -> EmailConfig:
    defaults = dict(
        smtp_host="smtp.example.com",
        smtp_port=587,
        from_address="pipeline@example.com",
        to_addresses=("ops@example.com",),
        smtp_username="",  # no auth → simpler test
        rate_limit_per_hour=1,
    )
    defaults.update(overrides)
    return EmailConfig(**defaults)


def _state(tmp: str) -> StateStore:
    return StateStore(Path(tmp) / "p.db")


class AlerterSendTests(unittest.TestCase):
    def test_first_send_succeeds_and_records(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(_email_config(), state)
            sent_messages: list[EmailMessage] = []

            def fake_send(cfg, msg):
                sent_messages.append(msg)

            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=fake_send,
            ):
                result = alerter.send(
                    ALERT_QUARANTINE,
                    "Batch X quarantined",
                    "Details about batch X.",
                )

            self.assertTrue(result.sent)
            self.assertFalse(result.suppressed_by_rate_limit)
            self.assertIsNone(result.error)

            # Message shape
            self.assertEqual(len(sent_messages), 1)
            msg = sent_messages[0]
            self.assertEqual(msg["From"], "pipeline@example.com")
            self.assertEqual(msg["To"], "ops@example.com")
            self.assertEqual(msg["Subject"], "Batch X quarantined")
            self.assertIn("Details about batch X.", msg.get_content())

            # State recorded
            self.assertIsNotNone(
                state.seconds_since_last_alert(ALERT_QUARANTINE)
            )
            state.close()

    def test_send_within_rate_limit_is_suppressed(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(_email_config(rate_limit_per_hour=1), state)
            sent_messages: list[EmailMessage] = []

            def fake_send(cfg, msg):
                sent_messages.append(msg)

            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=fake_send,
            ):
                r1 = alerter.send(ALERT_QUARANTINE, "First", "body")
                r2 = alerter.send(ALERT_QUARANTINE, "Second", "body")

            self.assertTrue(r1.sent)
            self.assertFalse(r2.sent)
            self.assertTrue(r2.suppressed_by_rate_limit)
            # Only one SMTP call.
            self.assertEqual(len(sent_messages), 1)
            state.close()

    def test_different_categories_have_independent_budgets(self) -> None:
        """Quarantine and catastrophic don't suppress each other."""
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(_email_config(rate_limit_per_hour=1), state)
            sent_messages: list[EmailMessage] = []

            def fake_send(cfg, msg):
                sent_messages.append(msg)

            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=fake_send,
            ):
                r1 = alerter.send(ALERT_QUARANTINE, "Q", "body")
                r2 = alerter.send(ALERT_CATASTROPHIC, "C", "body")

            self.assertTrue(r1.sent)
            self.assertTrue(r2.sent)
            self.assertEqual(len(sent_messages), 2)
            state.close()

    def test_smtp_failure_returns_error_does_not_raise(self) -> None:
        """The no-raise contract: alerting failures never break the run."""
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(_email_config(), state)

            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=smtplib.SMTPException("server down"),
            ):
                result = alerter.send(
                    ALERT_QUARANTINE, "Test", "body"
                )

            self.assertFalse(result.sent)
            self.assertIsNotNone(result.error)
            self.assertIn("server down", result.error or "")

            # Critical: nothing recorded in state — a failed send
            # should leave the rate-limit budget intact so the next
            # call can try again.
            self.assertIsNone(
                state.seconds_since_last_alert(ALERT_QUARANTINE)
            )
            state.close()

    def test_unknown_category_returns_error(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(_email_config(), state)
            result = alerter.send("bogus", "x", "y")
            self.assertFalse(result.sent)
            self.assertIn("unknown category", result.error or "")
            state.close()

    def test_higher_rate_limit_allows_more_sends(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            # 3600 per hour → min interval 1 second, but our tests run
            # in microseconds, so a value high enough should let two
            # immediate calls through.
            alerter = Alerter(
                _email_config(rate_limit_per_hour=360000),
                state,
            )
            sent: list[EmailMessage] = []
            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=lambda c, m: sent.append(m),
            ):
                alerter.send(ALERT_QUARANTINE, "1", "body")
                alerter.send(ALERT_QUARANTINE, "2", "body")
            self.assertEqual(len(sent), 2)
            state.close()


class MessageContentTests(unittest.TestCase):
    def test_multiple_recipients_comma_joined(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _state(tmp)
            alerter = Alerter(
                _email_config(
                    to_addresses=("a@example.com", "b@example.com"),
                ),
                state,
            )
            captured: list[EmailMessage] = []
            with patch(
                "csv_merger.pipeline.alerting._smtp_send",
                side_effect=lambda c, m: captured.append(m),
            ):
                alerter.send(ALERT_QUARANTINE, "subj", "body")
            self.assertEqual(
                captured[0]["To"], "a@example.com, b@example.com"
            )
            state.close()


if __name__ == "__main__":
    unittest.main()
