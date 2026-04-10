"""
etl/utils/__init__.py
Phase 11: ETL utility modules.

Author: Nguyen Van Khang
"""

from etl.utils.logging_config import (
    setup_logging,
    get_logger,
    TenantContextFilter,
    ETLContextFilter,
    set_log_context,
)

from etl.utils.monitoring import (
    alert,
    alert_pipeline_start,
    alert_pipeline_success,
    alert_pipeline_failure,
    alert_pipeline_warning,
    send_email_alert,
    send_slack_alert,
    AlertLevel,
)

__all__ = [
    # Logging
    "setup_logging",
    "get_logger",
    "TenantContextFilter",
    "ETLContextFilter",
    "set_log_context",
    # Monitoring
    "alert",
    "alert_pipeline_start",
    "alert_pipeline_success",
    "alert_pipeline_failure",
    "alert_pipeline_warning",
    "send_email_alert",
    "send_slack_alert",
    "AlertLevel",
]
