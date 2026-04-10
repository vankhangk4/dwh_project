"""
etl/orchestrator/config.py
Phase 11: Configuration loader for ETL orchestrator.

Loads all environment variables and file paths needed by the ETL pipeline.
All configuration is read from environment variables (no hardcoding).

Environment variables required:
    CONN_STR          : SQL Server connection string
    DATA_ROOT_PATH    : Root directory for source data files
    LOG_PATH          : Directory for log files
    ETL_BATCH_SIZE    : Batch size for bulk inserts (default 5000)
    ETL_LOG_LEVEL     : Log level (DEBUG, INFO, WARNING, ERROR)
    JWT_SECRET_KEY    : JWT secret for auth gateway

File path patterns:
    DATA_ROOT_PATH/tenant_id/BaoCaoDoanhThu.xlsx    -> Sales data
    DATA_ROOT_PATH/tenant_id/QuanLyKho.xlsx           -> Inventory data
    DATA_ROOT_PATH/tenant_id/PhieuNhapKho.xlsx       -> Purchase data
    DATA_ROOT_PATH/tenant_id/DanhSachKhachHang.xlsx   -> Customer data
    DATA_ROOT_PATH/tenant_id/DanhSachNhanVien.xlsx   -> Employee data
    DATA_ROOT_PATH/tenant_id/DanhSachCuaHang.xlsx    -> Store data
    DATA_ROOT_PATH/DanhMucSanPham.csv                -> Product catalog (shared)
    DATA_ROOT_PATH/DanhMucNhaCungCap.csv             -> Supplier catalog (shared)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DBConfig:
    """Database connection configuration."""
    conn_str: str = ""
    batch_size: int = 5000
    timeout: int = 30

    @classmethod
    def from_env(cls) -> "DBConfig":
        conn_str = os.environ.get("CONN_STR", "")
        batch_size = int(os.environ.get("ETL_BATCH_SIZE", "5000"))
        timeout = int(os.environ.get("DB_TIMEOUT", "30"))
        return cls(conn_str=conn_str, batch_size=batch_size, timeout=timeout)

    def validate(self) -> list[str]:
        """Validate config and return list of errors."""
        errors = []
        if not self.conn_str:
            errors.append("CONN_STR environment variable is not set")
        return errors


@dataclass
class FilePathConfig:
    """File path configuration for source data files."""
    data_root: str = "./data"
    log_path: str = "./logs"

    # Sheet names (standardized)
    sales_sheet: str = "DanhSachHoaDon"
    inventory_sheet: str = "QuanLyKho"
    purchase_sheet: str = "PhieuNhapKho"
    customer_sheet: str = "DanhSachKhachHang"
    employee_sheet: str = "DanhSachNhanVien"
    store_sheet: str = "DanhSachCuaHang"

    # File patterns
    sales_file: str = "BaoCaoDoanhThu.xlsx"
    inventory_file: str = "QuanLyKho.xlsx"
    purchase_file: str = "PhieuNhapKho.xlsx"
    customer_file: str = "DanhSachKhachHang.xlsx"
    employee_file: str = "DanhSachNhanVien.xlsx"
    store_file: str = "DanhSachCuaHang.xlsx"
    product_file: str = "DanhMucSanPham.csv"
    supplier_file: str = "DanhMucNhaCungCap.csv"

    @classmethod
    def from_env(cls) -> "FilePathConfig":
        data_root = os.environ.get("DATA_ROOT_PATH", "./data")
        log_path = os.environ.get("LOG_PATH", "./logs")
        return cls(data_root=data_root, log_path=log_path)

    def get_sales_path(self, tenant_id: str) -> str:
        """Get full path to sales file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.sales_file)

    def get_inventory_path(self, tenant_id: str) -> str:
        """Get full path to inventory file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.inventory_file)

    def get_purchase_path(self, tenant_id: str) -> str:
        """Get full path to purchase file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.purchase_file)

    def get_customer_path(self, tenant_id: str) -> str:
        """Get full path to customer file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.customer_file)

    def get_employee_path(self, tenant_id: str) -> str:
        """Get full path to employee file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.employee_file)

    def get_store_path(self, tenant_id: str) -> str:
        """Get full path to store file for a tenant."""
        return os.path.join(self.data_root, tenant_id, self.store_file)

    def get_product_path(self) -> str:
        """Get full path to shared product catalog."""
        return os.path.join(self.data_root, self.product_file)

    def get_supplier_path(self) -> str:
        """Get full path to shared supplier catalog."""
        return os.path.join(self.data_root, self.supplier_file)

    def list_tenant_dirs(self) -> list[str]:
        """List all tenant directories in data root."""
        if not os.path.exists(self.data_root):
            return []
        return [
            d for d in os.listdir(self.data_root)
            if os.path.isdir(os.path.join(self.data_root, d))
            and not d.startswith(".")
        ]


@dataclass
class LogConfig:
    """Logging configuration."""
    log_level: str = "INFO"
    log_file: str = "etl.log"
    json_format: bool = False

    @classmethod
    def from_env(cls) -> "LogConfig":
        log_level = os.environ.get("ETL_LOG_LEVEL", "INFO").upper()
        log_file = os.environ.get("ETL_LOG_FILE", "etl.log")
        json_format = os.environ.get("ETL_LOG_JSON", "false").lower() == "true"
        return cls(
            log_level=log_level,
            log_file=log_file,
            json_format=json_format,
        )


@dataclass
class AlertConfig:
    """Alerting configuration."""
    enabled: bool = True
    email_enabled: bool = True
    slack_enabled: bool = True

    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_pass: Optional[str] = None
    from_email: Optional[str] = None
    to_email: Optional[str] = None

    slack_webhook_url: Optional[str] = None

    @classmethod
    def from_env(cls) -> "AlertConfig":
        enabled = os.environ.get("ETL_ALERTS_ENABLED", "true").lower() == "true"
        email_enabled = all([
            os.environ.get("SMTP_HOST"),
            os.environ.get("SMTP_USER"),
            os.environ.get("ALERT_TO_EMAIL"),
        ])
        slack_enabled = bool(os.environ.get("SLACK_WEBHOOK_URL"))

        return cls(
            enabled=enabled,
            email_enabled=email_enabled,
            slack_enabled=slack_enabled,
            smtp_host=os.environ.get("SMTP_HOST"),
            smtp_port=int(os.environ.get("SMTP_PORT", "587")),
            smtp_user=os.environ.get("SMTP_USER"),
            smtp_pass=os.environ.get("SMTP_PASS"),
            from_email=os.environ.get("ALERT_FROM_EMAIL"),
            to_email=os.environ.get("ALERT_TO_EMAIL"),
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"),
        )


@dataclass
class ETLConfig:
    """
    Root configuration container for ETL orchestrator.

    Aggregates all sub-configurations into a single object.
    """
    db: DBConfig = field(default_factory=DBConfig.from_env)
    files: FilePathConfig = field(default_factory=FilePathConfig.from_env)
    log: LogConfig = field(default_factory=LogConfig.from_env)
    alerts: AlertConfig = field(default_factory=AlertConfig.from_env)

    # Pipeline step ordering
    transform_steps: list[str] = field(default_factory=lambda: [
        "product",     # Shared dimension first
        "supplier",   # Shared dimension
        "store",      # Tenant-specific dimension
        "employee",   # Tenant-specific dimension
        "customer",   # Tenant-specific dimension
        "sales",      # Transactional facts
        "inventory",  # Transactional facts
        "purchase",   # Transactional facts
    ])

    # Data Mart refresh steps (after facts are loaded)
    datamart_steps: list[str] = field(default_factory=lambda: [
        "sales_summary",
        "inventory_alert",
        "customer_rfm",
        "employee_performance",
        "purchase_summary",
    ])

    @classmethod
    def load(cls) -> "ETLConfig":
        """
        Load all configuration from environment variables.

        Returns:
            ETLConfig instance with all sub-configs populated.
        """
        return cls(
            db=DBConfig.from_env(),
            files=FilePathConfig.from_env(),
            log=LogConfig.from_env(),
            alerts=AlertConfig.from_env(),
        )

    def validate(self) -> dict[str, list[str]]:
        """
        Validate all configuration and return errors grouped by section.

        Returns:
            Dict of section -> list of error strings.
            Empty dict if all valid.
        """
        errors: dict[str, list[str]] = {}

        db_errors = self.db.validate()
        if db_errors:
            errors["database"] = db_errors

        if not os.path.exists(self.files.data_root):
            errors.setdefault("files", []).append(
                f"DATA_ROOT_PATH does not exist: {self.files.data_root}"
            )

        return errors


# ---------------------------------------------------------------------------
# Global singleton config (lazy-loaded)
# ---------------------------------------------------------------------------

_config: Optional[ETLConfig] = None


def get_config() -> ETLConfig:
    """
    Get the global ETL configuration singleton.

    Loads from environment on first call, returns cached instance on subsequent calls.

    Returns:
        ETLConfig instance.

    Example:
        config = get_config()
        config.db.conn_str  # Access DB connection string
        config.files.get_sales_path("STORE_HN")  # Get sales file path
    """
    global _config
    if _config is None:
        _config = ETLConfig.load()
    return _config


def reload_config() -> ETLConfig:
    """
    Force-reload configuration from environment variables.

    Useful when env vars change at runtime.

    Returns:
        Fresh ETLConfig instance.
    """
    global _config
    _config = ETLConfig.load()
    return _config
