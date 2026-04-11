# ============================================================================
# superset/superset_config.py
# Phase 13: Apache Superset configuration for Data Warehouse Multi-Tenant.
#
# This configuration is loaded when Superset starts.
# It enables:
#   - Row-level security (RLS) for multi-tenant data isolation.
#   - Embedded dashboards (standalone browser mode).
#   - Guest token authentication.
#   - Secure session management.
#   - Custom branding.
#
# IMPORTANT: Keep SECRET_KEY and WTF_CSRF_ENABLED secure in production.
#   In production, load these from environment variables, not hardcoded here.
#
# Usage:
#   - Bundled with docker-compose-superset.yml (mounted as volume).
#   - Can also be used with a native Superset installation:
#       export SUPERSET_CONFIG_PATH=/path/to/superset_config.py
#
# Author: Nguyen Van Khang
# ============================================================================

from __future__ import annotations

import os
from typing import Any

# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

# Random secret key used for signing session cookies and CSRF tokens.
# CHANGE THIS IN PRODUCTION! Use a long random string (min 32 characters).
# For Docker/Kubernetes: pass via SUPERSET_SECRET_KEY environment variable.
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "YourSuperSecretKeyHere_ChangeThisInProduction_AtLeast64CharsLong!"
)

# Enable CSRF protection for all forms and AJAX requests.
WTF_CSRF_ENABLED = True

# CSRF token expiration time (in seconds). Default: 8 hours (28800s).
# Matches JWT access token expiry in Auth Gateway (Phase 12).
WTF_CSRF_TIME_LIMIT = 28800

# Require valid CSRF token for all state-changing requests.
WTF_CSRF_EXEMPT_ROUTES = [
    # Routes exempt from CSRF (typically API-only endpoints used by scripts)
    "api.v1.security.login",
    "api.v1.security.refresh",
    "api.v1 Override",
]

# Session cookie settings
SESSION_COOKIE_SECURE = os.environ.get("ENV", "development").lower() != "development"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# PostgreSQL connection string for Superset metadata.
# Default: use the containerized PostgreSQL from docker-compose-superset.yml.
# Override via SUPERSET_DATABASE_URI environment variable.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DATABASE_URI",
    "postgresql+psycopg2://superset:SupersetDBPass123!@superset_db:5432/superset"
)

# PostgreSQL connection options (stringified for Docker env var compatibility).
# For native installs, use the dict directly in local_config.py.
_DATABASE_CONNECTION_OPTIONS: dict[str, Any] = {
    "connect_args": {
        "sslmode": "disable",
        "options": "-c timezone=utc",
    },
    "pool_size": 5,
    "pool_recycle": 3600,
    "max_overflow": 10,
}
DATABASE_CONNECTION_OPTIONS_STRINGIFIED = True

# Superset database name (for SQLAlchemy)
SUPERSET_DATABASE_NAME = "superset"

# ---------------------------------------------------------------------------
# Cache (Redis)
# ---------------------------------------------------------------------------

# Redis connection for caching and Celery broker.
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "SupersetRedisPass123!")
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_DB_CODES = int(os.environ.get("REDIS_DB_CODES", "1"))
REDIS_DB_RESULTS = int(os.environ.get("REDIS_DB_RESULTS", "2"))

# Cache configuration using Redis.
# Stringified JSON for Docker environment variable compatibility.
_CACHE_CONFIG: dict[str, Any] = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_PASSWORD": REDIS_PASSWORD,
    "CACHE_REDIS_DB": REDIS_DB,
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_THRESHOLD": 10000,
}
CACHE_CONFIG_STRINGIFIED = True

# Data cache for chart/query results.
_DATA_CACHE_CONFIG: dict[str, Any] = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_PASSWORD": REDIS_PASSWORD,
    "CACHE_REDIS_DB": 1,
    "CACHE_DEFAULT_TIMEOUT": 3600,
}
DATA_CACHE_CONFIG_STRINGIFIED = True

# Celery configuration for async query execution.
CELERY_CONFIG: dict[str, Any] = {
    "broker_url": f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    "result_backend": f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_RESULTS}",
    "task_serializer": "json",
    "result_serializer": "json",
    "accept_content": ["json"],
    "timezone": "UTC",
    "enable_utc": True,
    "task_track_started": True,
    "task_time_limit": 3600,
    "task_soft_time_limit": 3000,
    "worker_prefetch_multiplier": 4,
    "result_expires": 3600,
}

# ---------------------------------------------------------------------------
# Authentication & User Registration
# ---------------------------------------------------------------------------

# Disable public user registration.
# Only admins can create users via the UI or API.
AUTH_USER_REGISTRATION = False

# Disable the "Invite" feature for public invites.
AUTH_USER_REGISTRATION_INVITE_ONLY = True

# Allow authenticated users to impersonate others (admin only).
AUTH_ALLOW_GUEST_CACHING = True
AUTH_ALLOW_GUEST_FALLBACK = True

# Disable anonymous login.
PUBLIC_ROLE_LIKE = None

# Default role assigned to new users (if registration is enabled).
# Default is "Gamma" — a read-only role.
AUTH_DEFAULT_ROLE = "Gamma"

# Username uniqueness: case-insensitive.
AUTH_ROLE_PUBLIC = None

# Enable REMOTE_USER (SSO) authentication (disabled by default).
AUTH_REMOTE_USER_BACKEND = None

# LDAP/OAuth configuration (disabled; add providers in production).
# from flask_appbuilder.security.manager import AUTH_OID
# from flask_appbuilder.security.manager import AUTH_OAUTH
# AUTH_TYPE = AUTH_DB  # Default: database authentication

# Guest token settings (for embedded dashboards).
GUEST_TOKEN_JWT_GUEST_SECRET = SECRET_KEY
GUEST_TOKEN_JWT_INSECURE_KEY = SECRET_KEY
GUEST_TOKEN_HEADER_NAME = "X-Guest-Token"
GUEST_TOKEN_REQUEST_HEADER = "X-Guest-Token"

# ---------------------------------------------------------------------------
# Row-Level Security (RLS) — Multi-Tenant Isolation
# ---------------------------------------------------------------------------

# Enable row-level security feature flag.
FEATURE_FLAGS = {
    # Enable dashboard RBAC (row-level security on dashboards)
    "DASHBOARD_RBAC": True,
    # Enable alert & report subscriptions
    "ALERT_REPORTS": True,
    # Enable explore drag-and-drop
    "ENABLE_EXPLORE_DRAG_DROP": True,
    # Enable template removal
    "ENABLE_TEMPLATE_REMOVAL": True,
    # Show columns by default in list view
    "LISTVIEW_DEFAULT_COLUMNS": True,
    # Enable row-level security
    "ROW_LEVEL_SECURITY": True,
    # Enable Embedded SDK
    "EMBEDDED_SUPERSET": True,
    # Enable async chart execution
    "THUMBNAILS": True,
    # Enable dashboards Caching
    "DASHBOARD_CACHE": True,
}

# Row-level security filters are applied via the Superset UI or API.
# The RLS rules are managed programmatically by:
#   superset/scripts/create_roles_rls.py (Phase 13)
#
# Example RLS clause applied per tenant:
#   "TenantID = 'STORE_HN'"
# This ensures each tenant only sees their own data.

# ---------------------------------------------------------------------------
# Session & Security
# ---------------------------------------------------------------------------

# Token expiry for guest tokens (in seconds). Default: 1 hour.
GUEST_TOKEN_EXPIRY_SECONDS = 3600

# Maximum number of rows a user can export.
# Set to 0 to disable export (for security).
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# Maximum query execution time (in seconds).
SQLLAB_TIMEOUT = 300

# CSV upload: limit rows to prevent large imports.
CSV_UPLOAD_MAX_SIZE = 100  # MB

# ---------------------------------------------------------------------------
# Embedded Dashboard (Standalone Mode)
# ---------------------------------------------------------------------------

# Allow embedded mode in iframes (for embedded Superset).
# Set to False in production if you don't use embedded dashboards.
TALISMAN_ENABLED = True
TALISMAN_CONTENT_SECURITY_POLICY = (
    "frame-ancestors * 'self' https://localhost:* http://localhost:* "
    "blob: data: https://*.superset.local http://*.superset.local"
)

# Standalone mode: disable requirejs (needed for embedded SDK).
SUPERSET_REQUIRES_JS = True

# ---------------------------------------------------------------------------
# UI & Branding
# ---------------------------------------------------------------------------

# Application name and branding.
APP_NAME = "DWH RetailTech — Analytics Portal"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"
APP_ICON_WIDTH = 126

# Hide the "Learn More" documentation links in charts.
HIDE_LEFT_NAVBAR = False
ENABLE_JAVASCRIPT_DEBUGGER = False

# Custom index template for branding.
INDEX_TEMPLATE = "appbuilder/navbar_menu.html"

# Default dashboard thumbnail settings.
THUMBNAIL_CACHE_TIMEOUT = 3600

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL.
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Custom log format with request context.
LOG_FORMAT = (
    "%(asctime)s:%(levelname)s:%(name)s:%(message)s "
    "[in %(pathname)s:%(lineno)d]"
)
LOG_FORMAT_CLASS_NAME = "default"

# Log to file in production.
if os.environ.get("ENV", "development").lower() == "production":
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": LOG_FORMAT,
            },
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "/app/superset/superset_home/superset.log",
                "maxBytes": 104857600,  # 100MB
                "backupCount": 5,
                "formatter": "default",
            },
        },
        "root": {
            "level": LOG_LEVEL,
            "handlers": ["file"],
        },
    }
else:
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": LOG_FORMAT,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            },
        },
        "root": {
            "level": LOG_LEVEL,
            "handlers": ["console"],
        },
    }

# ---------------------------------------------------------------------------
# Map & Visualization
# ---------------------------------------------------------------------------

# Mapbox API key (optional, for map charts).
# Set MAPBOX_API_KEY in environment variables.
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# ---------------------------------------------------------------------------
# Celery & Async Tasks
# ---------------------------------------------------------------------------

# Celery broker and result backend are configured via CELERY_CONFIG dict above.
# The Beat scheduler runs as a separate container in docker-compose-superset.yml.

# Import celery config (must happen after CELERY_CONFIG dict is defined).
try:
    from celery import Celery
    from celery.schedules import crontab

    broker_url = CELERY_CONFIG.get(
        "broker_url",
        f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    )
    result_backend = CELERY_CONFIG.get(
        "result_backend",
        f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_RESULTS}"
    )

    class CeleryConfig:
        broker_url = broker_url
        result_backend = result_backend
        task_serializer = CELERY_CONFIG.get("task_serializer", "json")
        result_serializer = CELERY_CONFIG.get("result_serializer", "json")
        accept_content = CELERY_CONFIG.get("accept_content", ["json"])
        timezone = CELERY_CONFIG.get("timezone", "UTC")
        enable_utc = CELERY_CONFIG.get("enable_utc", True)
        task_track_started = CELERY_CONFIG.get("task_track_started", True)
        task_time_limit = CELERY_CONFIG.get("task_time_limit", 3600)
        task_soft_time_limit = CELERY_CONFIG.get("task_soft_time_limit", 3000)
        worker_prefetch_multiplier = CELERY_CONFIG.get("worker_prefetch_multiplier", 4)
        result_expires = CELERY_CONFIG.get("result_expires", 3600)

    CELERYBEAT_SCHEDULE: dict[str, Any] = {
        # Schedule cleanup of expired query results
        "cleanup_expired_queries": {
            "task": "superset.tasks.scheduler.cleanup_expired_queries",
            "schedule": crontab(minute=0, hour=3),  # Run daily at 3 AM
        },
        # Schedule report generation (if ALERT_REPORTS is enabled)
        "reports.scheduler": {
            "task": "superset.tasks.scheduler.schedule_report",
            "schedule": crontab(minute="*/5"),  # Every 5 minutes
        },
    }

except ImportError:
    # Celery not available (e.g., during initial config validation)
    CeleryConfig = None
    CELERYBEAT_SCHEDULE = {}

# ---------------------------------------------------------------------------
# OIDC / OAuth (disabled by default)
# ---------------------------------------------------------------------------

# To enable OAuth, uncomment and configure below:
# from flask_appbuilder.security.manager import AUTH_OAUTH
# AUTH_TYPE = AUTH_OAUTH
#
# OAUTH_PROVIDERS = [
#     {
#         "name": "azure",
#         "token_key": "access_token",
#         "icon": "fa-windows",
#         "remote_app": {
#             "client_id": os.environ.get("OAUTH_CLIENT_ID", ""),
#             "client_secret": os.environ.get("OAUTH_CLIENT_SECRET", ""),
#             "api_base_url": "https://graph.microsoft.com",
#             "client_kwargs": {"scope": "openid profile email"},
#             "request_token_url": None,
#             "access_token_url": (
#                 "https://login.microsoftonline.com/"
#                 f"{os.environ.get('OAUTH_TENANT_ID', '')}/oauth2/v2.0/token"
#             ),
#             "authorize_url": (
#                 "https://login.microsoftonline.com/"
#                 f"{os.environ.get('OAUTH_TENANT_ID', '')}/oauth2/v2.0/authorize"
#             ),
#         },
#     },
# ]

# Map OAuth user info to Superset roles.
# AUTH_USER_REGISTRATION_ROLE = "Gamma"
# AUTH_ROLES_MAPPING = {
#     "azure": {
#         "member_of_NT AUTHORITY\\authenticated_users": ["Gamma"],
#     },
# }

# ---------------------------------------------------------------------------
# Additional Settings
# ---------------------------------------------------------------------------

# Disable automatic DB connection testing on startup (speed up init).
DB_CONNECTION_TESTING = False

# Maximum number of tabs/windows a user can open (for SQL Lab).
SUPERSET_DASHBOARD_POSITION_DATA_LIMIT = 65535

# Superset home directory (for uploads, exports, etc.).
SUPERSET_HOME = "/app/superset/superset_home"

# Default filter scope (apply to all charts).
DEFAULT_FILTER_SCOPE = "all"

# SIP-38: default chart engine (options: "sql_lab", "migrate_dashboards")
DEFAULT_CHART_ENGINE = "sql_lab"

# Hide footer version info (for security in production).
VERSION_INFO_VISIBLE = os.environ.get("ENV", "development").lower() == "development"

# Proxy fix for running behind a reverse proxy.
ENABLE_PROXY_FIX = True
