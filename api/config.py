"""
api/config.py
Phase 12: Configuration loader for Auth Gateway.

Loads all environment variables and application settings.
All configuration is read from environment variables (no hardcoding).

Environment variables required:
    JWT_SECRET_KEY       : Secret key for JWT token signing (min 32 chars)
    JWT_ALGORITHM        : JWT algorithm (default HS256)
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES : Token expiry in minutes (default 60)
    CONN_STR             : SQL Server connection string
    SUPERSET_URL         : Superset instance URL (default http://localhost:8088)
    SUPERSET_USERNAME    : Superset admin username (default admin)
    SUPERSET_PASSWORD    : Superset admin password
    ENV                  : Application environment (development/staging/production)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class JWTConfig:
    """JWT authentication configuration."""
    secret_key: str = ""
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    @classmethod
    def from_env(cls) -> "JWTConfig":
        secret_key = os.environ.get("JWT_SECRET_KEY", "")
        if not secret_key:
            raise ValueError(
                "JWT_SECRET_KEY environment variable is not set. "
                "Please set a strong secret key (min 32 characters) before running the API."
            )
        if len(secret_key) < 32:
            raise ValueError(
                f"JWT_SECRET_KEY is too short ({len(secret_key)} chars). "
                "Minimum 32 characters required for security."
            )
        algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
        expire_minutes = int(os.environ.get("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
        return cls(
            secret_key=secret_key,
            algorithm=algorithm,
            access_token_expire_minutes=expire_minutes,
        )

    def validate(self) -> list[str]:
        errors = []
        if not self.secret_key:
            errors.append("JWT_SECRET_KEY environment variable is not set")
        if len(self.secret_key) < 32:
            errors.append("JWT_SECRET_KEY must be at least 32 characters long")
        return errors


@dataclass
class SupersetConfig:
    """Superset integration configuration."""
    url: str = "http://localhost:8088"
    username: str = "admin"
    password: str = ""
    guest_token_expiry_seconds: int = 3600

    @classmethod
    def from_env(cls) -> "SupersetConfig":
        return cls(
            url=os.environ.get("SUPERSET_URL", "http://localhost:8088"),
            username=os.environ.get("SUPERSET_USERNAME", "admin"),
            password=os.environ.get("SUPERSET_PASSWORD", ""),
            guest_token_expiry_seconds=int(
                os.environ.get("SUPERSET_GUEST_TOKEN_EXPIRY", "3600")
            ),
        )

    def validate(self) -> list[str]:
        errors = []
        if not self.url:
            errors.append("SUPERSET_URL is not set")
        if not self.password:
            errors.append("SUPERSET_PASSWORD is not set (required for Superset integration)")
        return errors


@dataclass
class CORSConfig:
    """CORS configuration."""
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    allowed_methods: list[str] = field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
    )
    allowed_headers: list[str] = field(
        default_factory=lambda: ["*"]
    )
    allow_credentials: bool = True

    @classmethod
    def from_env(cls) -> "CORSConfig":
        origins_str = os.environ.get("CORS_ALLOWED_ORIGINS", "*")
        if origins_str == "*":
            allowed_origins = ["*"]
        else:
            allowed_origins = [
                o.strip()
                for o in origins_str.split(",")
                if o.strip()
            ]
        return cls(
            allowed_origins=allowed_origins,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
            allowed_headers=["*"],
            allow_credentials=os.environ.get("CORS_ALLOW_CREDENTIALS", "true").lower() == "true",
        )


@dataclass
class ServerConfig:
    """Server configuration."""
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False
    workers: int = 1

    @classmethod
    def from_env(cls) -> "ServerConfig":
        env = os.environ.get("ENV", "development").lower()
        return cls(
            host=os.environ.get("API_HOST", "0.0.0.0"),
            port=int(os.environ.get("API_PORT", "8000")),
            reload=env == "development",
            workers=int(os.environ.get("API_WORKERS", "1")),
        )


@dataclass
class AppConfig:
    """
    Root configuration container for Auth Gateway.

    Aggregates all sub-configurations into a single object.
    """
    jwt: JWTConfig = field(default_factory=JWTConfig.from_env)
    superset: SupersetConfig = field(default_factory=SupersetConfig.from_env)
    cors: CORSConfig = field(default_factory=CORSConfig.from_env)
    server: ServerConfig = field(default_factory=ServerConfig.from_env)
    env: str = field(default_factory=lambda: os.environ.get("ENV", "development"))
    app_name: str = "DWH Auth Gateway"
    app_version: str = "1.0.0"
    debug: bool = field(
        default_factory=lambda: os.environ.get("ENV", "development").lower() == "development"
    )

    @classmethod
    def load(cls) -> "AppConfig":
        return cls(
            jwt=JWTConfig.from_env(),
            superset=SupersetConfig.from_env(),
            cors=CORSConfig.from_env(),
            server=ServerConfig.from_env(),
            env=os.environ.get("ENV", "development"),
            app_name="DWH Auth Gateway",
            app_version="1.0.0",
            debug=os.environ.get("ENV", "development").lower() == "development",
        )

    def validate(self) -> dict[str, list[str]]:
        errors: dict[str, list[str]] = {}
        jwt_errors = self.jwt.validate()
        if jwt_errors:
            errors["jwt"] = jwt_errors
        superset_errors = self.superset.validate()
        if superset_errors:
            errors["superset"] = superset_errors
        return errors


# ---------------------------------------------------------------------------
# Global singleton config (lazy-loaded)
# ---------------------------------------------------------------------------

_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """
    Get the global API configuration singleton.

    Loads from environment on first call, returns cached instance on subsequent calls.

    Returns:
        AppConfig instance.
    """
    global _config
    if _config is None:
        _config = AppConfig.load()
    return _config


def reload_config() -> AppConfig:
    """
    Force-reload configuration from environment variables.

    Returns:
        Fresh AppConfig instance.
    """
    global _config
    _config = AppConfig.load()
    return _config
