"""
etl/transform/__init__.py
Phase 10: ETL Transform Module

This module contains the data transformation logic for all staging tables.
Each transformation function takes a raw DataFrame from extract module
and returns a cleaned, validated DataFrame ready for staging load.

Modules:
    - base_transform  : Shared utility functions (clean_string, parse_date, etc.)
    - transform_sales  : Transform STG_SalesRaw -> business-ready sales data
    - transform_inventory : Transform STG_InventoryRaw -> inventory facts
    - transform_product  : Transform STG_ProductRaw -> product dimension
    - transform_customer : Transform STG_CustomerRaw -> customer dimension (SCD)
    - transform_employee : Transform STG_EmployeeRaw -> employee dimension
    - transform_purchase  : Transform STG_PurchaseRaw -> purchase facts
    - transform_store     : Transform STG_StoreRaw -> store dimension
    - transform_supplier  : Transform STG_SupplierRaw -> supplier dimension

Usage:
    from etl.transform import transform_sales, transform_inventory
    df_clean = transform_sales(df_raw, tenant_id="STORE_HN")

Author: Nguyen Van Khang
"""

from __future__ import annotations

from etl.transform.base_transform import (
    clean_string,
    parse_date,
    handle_null,
    safe_float,
    safe_int,
    normalize_phone,
    normalize_email,
    calculate_age,
    calculate_tenure_days,
)

from etl.transform.transform_sales import transform_sales
from etl.transform.transform_inventory import transform_inventory
from etl.transform.transform_product import transform_products
from etl.transform.transform_customer import transform_customers
from etl.transform.transform_employee import transform_employees
from etl.transform.transform_purchase import transform_purchases
from etl.transform.transform_store import transform_stores
from etl.transform.transform_supplier import transform_suppliers

__all__ = [
    # Base utilities
    "clean_string",
    "parse_date",
    "handle_null",
    "safe_float",
    "safe_int",
    "normalize_phone",
    "normalize_email",
    "calculate_age",
    "calculate_tenure_days",
    # Transform functions
    "transform_sales",
    "transform_inventory",
    "transform_products",
    "transform_customers",
    "transform_employees",
    "transform_purchases",
    "transform_stores",
    "transform_suppliers",
]