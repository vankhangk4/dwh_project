"""
etl/utils/monitoring.py
Phase 11: Alerting and monitoring utilities for ETL pipeline.

Provides:
    - send_email_alert()  : Send email alert via SMTP
    - send_slack_alert()   : Send Slack webhook notification
    - alert()             : Unified alert function (tries email, then Slack)

Supported alert levels:
    - INFO    : Pipeline started/completed normally
    - WARNING : Non-critical issues (e.g. low data volume, watermark gap)
    - ERROR   : ETL step failed
    - CRITICAL: Pipeline failed completely, data quality issue

Author: Nguyen Van Khang
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert levels
# ---------------------------------------------------------------------------

class AlertLevel:
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Unified alert function
# ---------------------------------------------------------------------------

def alert(
    message: str,
    level: str = AlertLevel.INFO,
    tenant_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    rows_processed: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    error_detail: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
    send_email: bool = True,
    send_slack: bool = True,
) -> bool:
    """
    Send unified alert via configured channels (email, Slack).

    This is the primary entry point for all ETL alerts.

    Args:
        message:        Human-readable alert message.
        level:          Alert level (INFO/WARNING/ERROR/CRITICAL).
        tenant_id:      Tenant identifier. (optional)
        pipeline_name:  Pipeline/step name. (optional)
        rows_processed: Number of rows processed. (optional)
        duration_seconds: ETL run duration in seconds. (optional)
        error_detail:  Detailed error message (for ERROR/CRITICAL). (optional)
        context:        Additional context dict (appended to alert body). (optional)
        send_email:     Send via email. (default True)
        send_slack:     Send via Slack. (default True)

    Returns:
        True if at least one alert channel succeeded, False if all failed.

    Example:
        alert(
            message="ETL pipeline completed successfully",
            level=AlertLevel.INFO,
            tenant_id="STORE_HN",
            pipeline_name="run_all_tenants",
            rows_processed=15000,
            duration_seconds=340,
        )
    """
    prefix = f"[{tenant_id}] " if tenant_id else ""
    full_message = f"{prefix}{message}"

    log_level_map = {
        AlertLevel.INFO: logging.INFO,
        AlertLevel.WARNING: logging.WARNING,
        AlertLevel.ERROR: logging.ERROR,
        AlertLevel.CRITICAL: logging.CRITICAL,
    }
    logger.log(log_level_map.get(level, logging.INFO), full_message)

    if error_detail:
        logger.debug("Error detail: %s", error_detail)

    email_ok = False
    slack_ok = False

    if send_email:
        try:
            send_email_alert(
                message=full_message,
                level=level,
                tenant_id=tenant_id,
                pipeline_name=pipeline_name,
                rows_processed=rows_processed,
                duration_seconds=duration_seconds,
                error_detail=error_detail,
                context=context,
            )
            email_ok = True
        except Exception as ex:
            logger.warning("Email alert failed: %s", ex)

    if send_slack:
        try:
            send_slack_alert(
                message=full_message,
                level=level,
                tenant_id=tenant_id,
                pipeline_name=pipeline_name,
                rows_processed=rows_processed,
                duration_seconds=duration_seconds,
                error_detail=error_detail,
                context=context,
            )
            slack_ok = True
        except Exception as ex:
            logger.warning("Slack alert failed: %s", ex)

    return email_ok or slack_ok


# ---------------------------------------------------------------------------
# Email alerting
# ---------------------------------------------------------------------------

def send_email_alert(
    message: str,
    level: str = AlertLevel.INFO,
    tenant_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    rows_processed: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    error_detail: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """
    Send email alert via SMTP.

    Required environment variables:
        SMTP_HOST     : SMTP server host
        SMTP_PORT     : SMTP port (default 587)
        SMTP_USER     : SMTP username
        SMTP_PASS     : SMTP password
        ALERT_FROM_EMAIL: Sender email address
        ALERT_TO_EMAIL : Recipient email address

    Args:
        message:         Alert message.
        level:           Alert level.
        tenant_id:       Tenant identifier. (optional)
        pipeline_name:   Pipeline name. (optional)
        rows_processed:  Rows processed. (optional)
        duration_seconds: Run duration. (optional)
        error_detail:    Error details. (optional)
        context:         Additional context. (optional)

    Raises:
        RuntimeError: If SMTP configuration is missing or send fails.
    """
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    from_email = os.environ.get("ALERT_FROM_EMAIL")
    to_email = os.environ.get("ALERT_TO_EMAIL")

    if not all([smtp_host, smtp_user, smtp_pass, from_email, to_email]):
        logger.warning(
            "SMTP configuration incomplete. Skipping email alert. "
            "Check SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, "
            "ALERT_FROM_EMAIL, ALERT_TO_EMAIL env vars."
        )
        return

    subject_prefix_map = {
        AlertLevel.INFO: "✅",
        AlertLevel.WARNING: "⚠️",
        AlertLevel.ERROR: "❌",
        AlertLevel.CRITICAL: "🚨",
    }
    prefix = subject_prefix_map.get(level, "ℹ️")

    subject_parts = [prefix, level]
    if tenant_id:
        subject_parts.append(f"Tenant:{tenant_id}")
    if pipeline_name:
        subject_parts.append(pipeline_name)
    subject = " ".join(subject_parts)

    body_html = _build_email_html(
        message=message,
        level=level,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        rows_processed=rows_processed,
        duration_seconds=duration_seconds,
        error_detail=error_detail,
        context=context,
    )

    body_text = _build_email_text(
        message=message,
        level=level,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        rows_processed=rows_processed,
        duration_seconds=duration_seconds,
        error_detail=error_detail,
        context=context,
    )

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Date"] = datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0700")

        part_text = MIMEText(body_text, "plain", "utf-8")
        part_html = MIMEText(body_html, "html", "utf-8")

        msg.attach(part_text)
        msg.attach(part_html)

        context_ssl = ssl.create_default_context()

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls(context=context_ssl)
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, to_email.split(","), msg.as_string())

        logger.info(
            "Email alert sent successfully | To: %s | Subject: %s",
            to_email, subject
        )

    except smtplib.SMTPAuthenticationError as ex:
        logger.error("SMTP authentication failed: %s", ex)
        raise RuntimeError(f"SMTP authentication failed: {ex}") from ex
    except smtplib.SMTPException as ex:
        logger.error("SMTP error: %s", ex)
        raise RuntimeError(f"SMTP send failed: {ex}") from ex
    except Exception as ex:
        logger.error("Email alert failed: %s", ex)
        raise RuntimeError(f"Email alert failed: {ex}") from ex


def _build_email_html(
    message: str,
    level: str,
    tenant_id: Optional[str],
    pipeline_name: Optional[str],
    rows_processed: Optional[int],
    duration_seconds: Optional[int],
    error_detail: Optional[str],
    context: Optional[dict[str, Any]],
) -> str:
    """Build HTML email body with ETL run details."""
    level_color_map = {
        AlertLevel.INFO: "#28a745",
        AlertLevel.WARNING: "#ffc107",
        AlertLevel.ERROR: "#dc3545",
        AlertLevel.CRITICAL: "#7b0000",
    }
    bg_color = level_color_map.get(level, "#6c757d")

    context_rows = ""
    if context:
        context_rows = "\n".join(
            f"      <tr><td><strong>{k}</strong></td><td>{v}</td></tr>"
            for k, v in context.items()
        )

    rows_str = f"{rows_processed:,}" if rows_processed is not None else "N/A"
    duration_str = (
        f"{duration_seconds:,}s ({duration_seconds // 60}m {duration_seconds % 60}s)"
        if duration_seconds is not None
        else "N/A"
    )

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; margin: 20px; }}
  .header {{ background-color: {bg_color}; color: white; padding: 15px; border-radius: 5px; }}
  .content {{ margin-top: 15px; }}
  .alert-box {{ background-color: #f8f9fa; border-left: 5px solid {bg_color}; padding: 15px; margin: 10px 0; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
  th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
  th {{ background-color: #f1f1f1; }}
  .error {{ background-color: #fff0f0; border: 1px solid #dc3545; padding: 10px; margin-top: 15px; }}
  .footer {{ margin-top: 20px; font-size: 12px; color: #666; }}
</style>
</head>
<body>
<div class="header">
  <h2>ETL Alert: {level}</h2>
</div>
<div class="content">
  <div class="alert-box">
    <strong>Message:</strong><br>{message}
  </div>

  <h3>Run Details</h3>
  <table>
    <tr><th>Field</th><th>Value</th></tr>
    <tr><td>Alert Level</td><td><strong>{level}</strong></td></tr>
    <tr><td>Tenant</td><td>{tenant_id or 'N/A'}</td></tr>
    <tr><td>Pipeline</td><td>{pipeline_name or 'N/A'}</td></tr>
    <tr><td>Rows Processed</td><td>{rows_str}</td></tr>
    <tr><td>Duration</td><td>{duration_str}</td></tr>
    <tr><td>Timestamp</td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
{context_rows}
  </table>
"""
    if error_detail:
        html += f"""
  <div class="error">
    <strong>Error Detail:</strong><br>
    <pre>{error_detail}</pre>
  </div>
"""

    html += """
  <div class="footer">
    <p>This is an automated alert from the Data Warehouse ETL pipeline.<br>
    Do not reply to this email.</p>
  </div>
</div>
</body>
</html>"""

    return html


def _build_email_text(
    message: str,
    level: str,
    tenant_id: Optional[str],
    pipeline_name: Optional[str],
    rows_processed: Optional[int],
    duration_seconds: Optional[int],
    error_detail: Optional[str],
    context: Optional[dict[str, Any]],
) -> str:
    """Build plain-text email body."""
    lines = [
        "=" * 60,
        f"ETL ALERT: {level}",
        "=" * 60,
        f"Message: {message}",
        "",
        "Run Details:",
        f"  Tenant:          {tenant_id or 'N/A'}",
        f"  Pipeline:       {pipeline_name or 'N/A'}",
        f"  Rows Processed: {rows_processed if rows_processed is not None else 'N/A'}",
        f"  Duration:        {duration_seconds if duration_seconds is not None else 'N/A'}s",
        f"  Timestamp:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    if context:
        lines.append("")
        lines.append("Additional Context:")
        for k, v in context.items():
            lines.append(f"  {k}: {v}")

    if error_detail:
        lines.append("")
        lines.append("Error Detail:")
        lines.append(error_detail)

    lines.extend([
        "",
        "=" * 60,
        "This is an automated alert. Do not reply.",
    ])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Slack alerting
# ---------------------------------------------------------------------------

def send_slack_alert(
    message: str,
    level: str = AlertLevel.INFO,
    tenant_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    rows_processed: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    error_detail: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """
    Send Slack notification via webhook.

    Required environment variable:
        SLACK_WEBHOOK_URL : Slack incoming webhook URL

    Args:
        message:         Alert message.
        level:          Alert level (determines emoji and color).
        tenant_id:      Tenant identifier. (optional)
        pipeline_name:  Pipeline name. (optional)
        rows_processed: Rows processed. (optional)
        duration_seconds: Run duration. (optional)
        error_detail:  Error details. (optional)
        context:        Additional context fields. (optional)

    Raises:
        RuntimeError: If Slack webhook URL is not configured or send fails.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not configured. Skipping Slack alert.")
        return

    level_emoji_map = {
        AlertLevel.INFO: ":large_green_circle:",
        AlertLevel.WARNING: ":large_yellow_circle:",
        AlertLevel.ERROR: ":red_circle:",
        AlertLevel.CRITICAL: ":fire:",
    }

    level_color_map = {
        AlertLevel.INFO: "#36a64f",
        AlertLevel.WARNING: "#ff9900",
        AlertLevel.ERROR: "#dc3545",
        AlertLevel.CRITICAL: "#7b0000",
    }

    emoji = level_emoji_map.get(level, ":information_source:")
    color = level_color_map.get(level, "#6c757d")

    header_fields = []
    if tenant_id:
        header_fields.append({"type": "mrkdwn", "text": f"*Tenant:* `{tenant_id}`"})
    if pipeline_name:
        header_fields.append({"type": "mrkdwn", "text": f"*Pipeline:* `{pipeline_name}`"})

    detail_fields = []
    if rows_processed is not None:
        detail_fields.append(
            {"type": "mrkdwn", "text": f"*Rows:* `{rows_processed:,}`"}
        )
    if duration_seconds is not None:
        mins = duration_seconds // 60
        secs = duration_seconds % 60
        dur_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
        detail_fields.append(
            {"type": "mrkdwn", "text": f"*Duration:* `{dur_str}`"}
        )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} ETL Alert: {level}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{message}*",
            },
        },
    ]

    if header_fields:
        blocks.append({
            "type": "section",
            "fields": header_fields,
        })

    if detail_fields:
        blocks.append({
            "type": "section",
            "fields": detail_fields,
        })

    if context:
        ctx_lines = "\n".join(f"• *{k}:* {v}" for k, v in context.items())
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ctx_lines},
        })

    if error_detail:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Detail:*\n```\n{error_detail[:1500]}\n```",
            },
        })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"Sent at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | DWH ETL Pipeline",
            }
        ],
    })

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }

    try:
        req = Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=10) as response:
            if response.status in (200, 201):
                logger.info("Slack alert sent successfully | level=%s", level)
            else:
                logger.warning(
                    "Slack webhook returned status %d",
                    response.status
                )
    except HTTPError as ex:
        logger.error("Slack webhook HTTP error: %s", ex)
        raise RuntimeError(f"Slack webhook failed with HTTP {ex.code}: {ex.reason}") from ex
    except URLError as ex:
        logger.error("Slack webhook URL error: %s", ex)
        raise RuntimeError(f"Slack webhook failed: {ex.reason}") from ex
    except Exception as ex:
        logger.error("Slack alert failed: %s", ex)
        raise RuntimeError(f"Slack alert failed: {ex}") from ex


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def alert_pipeline_start(
    tenant_id: str,
    pipeline_name: str,
    context: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience: alert when a pipeline step starts."""
    return alert(
        message=f"Pipeline '{pipeline_name}' started for tenant {tenant_id}",
        level=AlertLevel.INFO,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        context=context,
        send_email=False,
        send_slack=True,
    )


def alert_pipeline_success(
    tenant_id: str,
    pipeline_name: str,
    rows_processed: int,
    duration_seconds: int,
    context: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience: alert when a pipeline step completes successfully."""
    return alert(
        message=f"Pipeline '{pipeline_name}' completed successfully",
        level=AlertLevel.INFO,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        rows_processed=rows_processed,
        duration_seconds=duration_seconds,
        context=context,
        send_email=True,
        send_slack=True,
    )


def alert_pipeline_failure(
    tenant_id: str,
    pipeline_name: str,
    error_detail: str,
    context: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience: alert when a pipeline step fails."""
    return alert(
        message=f"Pipeline '{pipeline_name}' FAILED for tenant {tenant_id}",
        level=AlertLevel.ERROR,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        error_detail=error_detail,
        context=context,
        send_email=True,
        send_slack=True,
    )


def alert_pipeline_warning(
    message: str,
    tenant_id: str,
    pipeline_name: str,
    context: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience: send a warning alert."""
    return alert(
        message=message,
        level=AlertLevel.WARNING,
        tenant_id=tenant_id,
        pipeline_name=pipeline_name,
        context=context,
        send_email=False,
        send_slack=True,
    )
