"""Email alerting via SMTP, rate-limited and persisted.

The pipeline emits emails for two operational events:

* **quarantine** — a batch has failed enough consecutive runs to be
  quarantined (see :class:`~csv_merger.pipeline.config.RetryPolicyConfig`).
  Actionable: an operator needs to look at the failing batch.
* **catastrophic** — pipeline-level failure that prevents any work
  (state DB corruption, config error, repeated lock contention).
  Actionable: the pipeline can't make progress until a human helps.

Rate limiting: at most one email per category per
``EmailConfig.rate_limit_per_hour`` (default 1/hour). Subsequent
occurrences within the window are logged but suppressed. The state
store's ``alert_history`` table is the source of truth — survives
process restarts.

Failure mode: if SMTP delivery itself fails, we log a warning and
proceed. We do NOT raise — alerting must never be the reason the
pipeline fails. A failed email send means the operator misses one
notification; raising would mean missing many because the run would
crash.
"""

from __future__ import annotations

import logging
import smtplib
import socket
import ssl
from dataclasses import dataclass
from email.message import EmailMessage

from csv_merger.pipeline.config import EmailConfig
from csv_merger.pipeline.state import StateStore

logger = logging.getLogger(__name__)


#: Alert categories. Add new ones cautiously — every new category gets
#: its own rate-limit budget, which means more total emails.
ALERT_QUARANTINE = "quarantine"
ALERT_CATASTROPHIC = "catastrophic"
_VALID_CATEGORIES: frozenset[str] = frozenset({ALERT_QUARANTINE, ALERT_CATASTROPHIC})

# How many seconds in a "per hour" window.
_SECONDS_PER_HOUR = 3600.0


@dataclass(frozen=True)
class AlertResult:
    """Outcome of an :meth:`Alerter.send` call.

    Attributes:
        sent: ``True`` if the SMTP transaction completed.
        suppressed_by_rate_limit: ``True`` if the call was suppressed
            because a recent alert in this category exists.
        error: Set if the SMTP transaction failed.
    """

    sent: bool
    suppressed_by_rate_limit: bool = False
    error: str | None = None


class Alerter:
    """Send rate-limited alert emails.

    Construct once per process. Holds a reference to the state store
    for rate-limit decisions and post-send recording; does not own the
    store's lifetime.
    """

    def __init__(
        self,
        config: EmailConfig,
        state: StateStore,
    ) -> None:
        self._config = config
        self._state = state

    def send(
        self,
        category: str,
        subject: str,
        body: str,
    ) -> AlertResult:
        """Send an alert email if rate-limit allows.

        Args:
            category: One of :data:`ALERT_QUARANTINE` or
                :data:`ALERT_CATASTROPHIC`.
            subject: Email subject line.
            body: Plain-text email body.

        Returns:
            An :class:`AlertResult` describing the outcome. **Never
            raises** — failures are logged and reported via the result.
        """
        if category not in _VALID_CATEGORIES:
            # Caller bug, not an operational condition. We don't raise
            # (per the no-raise contract) but the log makes it obvious.
            logger.error(
                "Alerter.send called with unknown category %r; "
                "valid categories: %s",
                category,
                sorted(_VALID_CATEGORIES),
            )
            return AlertResult(sent=False, error=f"unknown category {category!r}")

        # Rate limit check.
        elapsed = self._state.seconds_since_last_alert(category)
        min_interval = _SECONDS_PER_HOUR / max(
            self._config.rate_limit_per_hour, 1
        )
        if elapsed is not None and elapsed < min_interval:
            logger.warning(
                "alert suppressed by rate limit "
                "(category=%s, %.0fs since last, %.0fs minimum): %s",
                category,
                elapsed,
                min_interval,
                subject,
            )
            return AlertResult(sent=False, suppressed_by_rate_limit=True)

        # Build the message.
        msg = EmailMessage()
        msg["From"] = self._config.from_address
        msg["To"] = ", ".join(self._config.to_addresses)
        msg["Subject"] = subject
        msg.set_content(body)

        # Send.
        try:
            _smtp_send(self._config, msg)
        except (smtplib.SMTPException, OSError, socket.error) as exc:
            logger.error(
                "failed to send alert email (category=%s, subject=%r): %s",
                category,
                subject,
                exc,
            )
            return AlertResult(
                sent=False,
                error=f"{type(exc).__name__}: {exc}",
            )

        # Record only after a successful send. A crash between send and
        # record means the next call may send a duplicate; that's the
        # preferred failure mode (false positive over false negative).
        try:
            self._state.record_alert_sent(category, subject)
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "alert sent but failed to record in state: %s", exc
            )

        logger.info(
            "sent alert email (category=%s): %s", category, subject
        )
        return AlertResult(sent=True)


def _smtp_send(config: EmailConfig, msg: EmailMessage) -> None:
    """Perform the SMTP transaction.

    Extracted as a free function so tests can patch one symbol
    (``alerting._smtp_send``) instead of mocking ``smtplib.SMTP`` and
    all its methods.
    """
    # Reasonable timeout — alerting should never block the pipeline for
    # long. 15s connect+read is generous for any real mail server.
    timeout = 15.0

    if config.smtp_use_starttls:
        with smtplib.SMTP(
            config.smtp_host, config.smtp_port, timeout=timeout
        ) as client:
            client.ehlo()
            client.starttls(context=ssl.create_default_context())
            client.ehlo()
            _maybe_login(client, config)
            client.send_message(msg)
    else:
        # Either plain SMTP or SMTPS — we pick by port convention: 465
        # is SMTPS, anything else is plain.
        if config.smtp_port == 465:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                config.smtp_host,
                config.smtp_port,
                timeout=timeout,
                context=ctx,
            ) as client:
                _maybe_login(client, config)
                client.send_message(msg)
        else:
            with smtplib.SMTP(
                config.smtp_host,
                config.smtp_port,
                timeout=timeout,
            ) as client:
                _maybe_login(client, config)
                client.send_message(msg)


def _maybe_login(client: smtplib.SMTP, config: EmailConfig) -> None:
    """Authenticate if a username is configured.

    Empty username → relay without auth (some internal SMTP relays
    don't require it). Username set but password missing → log warning
    and try anyway; SMTP will reject and we'll surface the error.
    """
    if not config.smtp_username:
        return
    password = config.smtp_password
    if not password:
        logger.warning(
            "smtp_username set but %s env var is empty",
            config.smtp_password_env,
        )
    client.login(config.smtp_username, password)
