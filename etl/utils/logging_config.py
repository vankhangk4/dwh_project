"""
etl/utils/logging_config.py
Phase 11: Structured logging configuration with tenant context.

Provides:
    - setup_logging()      : Configure root logger with structured format
    - get_logger()         : Get tenant-scoped logger with prefix
    - TenantContextFilter  : Filter that adds TenantID to every log record
    - ETLContextFilter     : Filter that adds TenantID + PipelineName to every record

Log format:
    2024-03-15 10:30:45 [STORE_HN] etl.pipeline INFO  - Starting ETL for tenant STORE_HN

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Optional

try:
    from pythonjsonlogger import jsonlogger
    _HAS_JSONLOGGER = True
except ImportError:
    jsonlogger = None
    _HAS_JSONLOGGER = False

# Default format string (standard console output)
DEFAULT_FORMAT = (
    "%(asctime)s [%(tenant_id)s] %(name)s %(levelname)s - %(message)s"
)

# JSON format (for file output / structured logging)
JSON_FORMAT = (
    "%(asctime)s %(tenant_id)s %(pipeline)s %(name)s %(levelname)s "
    "%(message)s %(duration_seconds)s %(rows_processed)s"
)


class TenantContextFilter(logging.Filter):
    """
    Logging filter that injects TenantID into every log record.

    Usage:
        logger = logging.getLogger("etl.pipeline")
        tenant_filter = TenantContextFilter(tenant_id="STORE_HN")
        logger.addFilter(tenant_filter)
        logger.info("Starting ETL")  # -> "Starting ETL [STORE_HN]"
    """

    def __init__(self, tenant_id: Optional[str] = None):
        super().__init__()
        self.tenant_id = tenant_id or "NO_TENANT"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "tenant_id"):
            record.tenant_id = self.tenant_id
        return True


class ETLContextFilter(logging.Filter):
    """
    Logging filter that injects TenantID + PipelineName into every log record.

    Adds fields:
        - tenant_id: Tenant identifier
        - pipeline:  Pipeline name (e.g. "extract", "transform", "load")
        - duration_seconds: ETL run duration (set via set_duration)
        - rows_processed: Number of rows processed (set via set_rows)
    """

    def __init__(
        self,
        tenant_id: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ):
        super().__init__()
        self.tenant_id = tenant_id or "NO_TENANT"
        self.pipeline_name = pipeline_name or "unknown"
        self._duration: Optional[int] = None
        self._rows: Optional[int] = None

    def set_duration(self, seconds: int) -> None:
        self._duration = seconds

    def set_rows(self, rows: int) -> None:
        self._rows = rows

    def filter(self, record: logging.LogRecord) -> bool:
        record.tenant_id = self.tenant_id
        record.pipeline = self.pipeline_name
        record.duration_seconds = self._duration if self._duration is not None else "-"
        record.rows_processed = self._rows if self._rows is not None else "-"
        return True


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_path: Optional[str] = None,
    json_format: bool = False,
    tenant_id: Optional[str] = None,
) -> logging.Logger:
    """
    Configure the root ETL logger with console and optional file output.

    Args:
        log_level:    Log level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
        log_file:     Optional log file name (appended to log_path).
        log_path:     Directory for log files. Default: ./logs.
        json_format:  If True, use JSON formatter for file output. Default: False.
        tenant_id:    Default tenant context for new loggers. (optional)

    Returns:
        Configured root logger.

    Example:
        logger = setup_logging(
            log_level="INFO",
            log_file="etl.log",
            log_path="./logs",
        )
        logger.info("ETL started")
    """
    if log_level is None:
        log_level = os.environ.get("ETL_LOG_LEVEL", "INFO").upper()

    if log_path is None:
        log_path = os.environ.get("LOG_PATH", "./logs")

    log_level_num = getattr(logging, log_level, logging.INFO)

    root_logger = logging.getLogger("etl")
    root_logger.setLevel(log_level_num)

    if root_logger.handlers:
        root_logger.handlers.clear()

    root_handler = logging.StreamHandler(sys.stdout)
    root_handler.setLevel(log_level_num)
    root_handler.addFilter(TenantContextFilter(tenant_id=tenant_id))
    root_handler.setFormatter(_build_console_formatter())
    root_logger.addHandler(root_handler)

    if log_file:
        os.makedirs(log_path, exist_ok=True)
        file_path = os.path.join(log_path, log_file)
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setLevel(log_level_num)
        file_handler.addFilter(TenantContextFilter(tenant_id=tenant_id))
        if json_format:
            file_handler.setFormatter(_build_json_formatter())
        else:
            file_handler.setFormatter(_build_console_formatter())
        root_logger.addHandler(file_handler)

    root_logger.propagate = False

    root_logger.info(
        "Logging initialized | level=%s | log_file=%s | log_path=%s",
        log_level, log_file or "(console only)", log_path
    )

    return root_logger


def _build_console_formatter(tenant_id: Optional[str] = None) -> logging.Formatter:
    """
    Build a colored/formatted console log formatter.
    """
    fmt = DEFAULT_FORMAT
    if tenant_id:
        fmt = fmt.replace("[%(tenant_id)s]", f"[{tenant_id}]")

    formatter = logging.Formatter(
        fmt=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return formatter


def _build_json_formatter() -> logging.Formatter:
    """
    Build a JSON log formatter for structured log aggregation.

    Falls back to standard formatter if pythonjsonlogger is not installed.
    """
    if not _HAS_JSONLOGGER:
        logging.warning(
            "pythonjsonlogger not installed. Using standard formatter for JSON logs."
        )
        return _build_console_formatter()

    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def add_fields(self, log_record, record, message_dict):
            super().add_fields(log_record, record, message_dict)
            log_record["timestamp"] = datetime.utcnow().isoformat()
            log_record["level"] = record.levelname
            log_record["logger"] = record.name
            if hasattr(record, "tenant_id"):
                log_record["tenant_id"] = record.tenant_id
            if hasattr(record, "pipeline"):
                log_record["pipeline"] = record.pipeline
            if hasattr(record, "duration_seconds"):
                log_record["duration_seconds"] = record.duration_seconds
            if hasattr(record, "rows_processed"):
                log_record["rows_processed"] = record.rows_processed

    formatter = CustomJsonFormatter(
        fmt=JSON_FORMAT,
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return formatter


def get_logger(
    name: str,
    tenant_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> logging.Logger:
    """
    Get a tenant-scoped logger with ETL context filter.

    Args:
        name:          Logger name (e.g. "etl.pipeline.sales").
        tenant_id:     Tenant identifier for log prefix. (optional)
        pipeline_name: Pipeline name (extract, transform, load, etc.). (optional)

    Returns:
        Configured logger with TenantContextFilter and ETLContextFilter.

    Example:
        logger = get_logger("etl.pipeline", tenant_id="STORE_HN", pipeline_name="extract")
        logger.info("Extracting sales data")
        # -> "Extracting sales data [STORE_HN] [extract]"
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        root_etl = logging.getLogger("etl")
        if not root_etl.handlers:
            setup_logging()

        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            _build_console_formatter(tenant_id=tenant_id)
        )
        logger.addHandler(handler)
        logger.propagate = False

    logger.filters.clear()

    if pipeline_name:
        ctx_filter = ETLContextFilter(tenant_id=tenant_id, pipeline_name=pipeline_name)
    else:
        ctx_filter = TenantContextFilter(tenant_id=tenant_id)

    logger.addFilter(ctx_filter)

    return logger


def set_log_context(
    logger: logging.Logger,
    tenant_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    duration_seconds: Optional[int] = None,
    rows_processed: Optional[int] = None,
) -> None:
    """
    Update log context on an existing ETLContextFilter logger.

    Useful for updating metadata after a step completes:
        set_log_context(logger, rows_processed=5000, duration_seconds=120)

    Args:
        logger:           Logger with ETLContextFilter.
        tenant_id:        Update tenant ID. (optional)
        pipeline_name:    Update pipeline name. (optional)
        duration_seconds: Update duration. (optional)
        rows_processed:   Update row count. (optional)
    """
    for f in logger.filters:
        if isinstance(f, ETLContextFilter):
            if tenant_id is not None:
                f.tenant_id = tenant_id
            if pipeline_name is not None:
                f.pipeline_name = pipeline_name
            if duration_seconds is not None:
                f.set_duration(duration_seconds)
            if rows_processed is not None:
                f.set_rows(rows_processed)
