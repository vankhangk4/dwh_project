"""
api/routes/__init__.py
Phase 12: API Routes Package

This package contains all FastAPI route modules.

Author: Nguyen Van Khang
"""

from api.routes.tenants import router as tenants_router
from api.routes.etl import router as etl_router

__all__ = ["tenants_router", "etl_router"]