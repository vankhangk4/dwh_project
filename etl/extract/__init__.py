"""
etl/extract/__init__.py
Phase 9: ETL Extract Module — Data Warehouse Multi-Tenant

Modules:
    - extract_sales      : Extract sales transactions from Excel
    - extract_inventory  : Extract inventory snapshots from Excel
    - extract_product    : Extract product catalog from CSV
    - extract_customer   : Extract customer master from Excel
    - extract_employee   : Extract employee master from Excel
    - extract_purchase   : Extract purchase orders from Excel
    - extract_supplier   : Extract supplier master from CSV
    - extract_store      : Extract store master from Excel

Shared utilities:
    - db_utils           : Database connection, SP execution, staging load

Author: Nguyen Van Khang
"""

from . import db_utils
from .extract_sales import extract_sales_from_excel, get_last_watermark as get_watermark_sales
from .extract_inventory import extract_inventory_from_excel, get_last_watermark as get_watermark_inventory
from .extract_product import extract_products_from_csv, get_last_watermark as get_watermark_product
from .extract_customer import extract_customers_from_excel, get_last_watermark as get_watermark_customer
from .extract_employee import extract_employees_from_excel, get_last_watermark as get_watermark_employee
from .extract_purchase import extract_purchases_from_excel, get_last_watermark as get_watermark_purchase
from .extract_supplier import extract_suppliers_from_csv, get_last_watermark as get_watermark_supplier
from .extract_store import extract_stores_from_excel, get_last_watermark as get_watermark_store

__all__ = [
    # db_utils
    "db_utils",
    # extract functions
    "extract_sales_from_excel",
    "extract_inventory_from_excel",
    "extract_products_from_csv",
    "extract_customers_from_excel",
    "extract_employees_from_excel",
    "extract_purchases_from_excel",
    "extract_suppliers_from_csv",
    "extract_stores_from_excel",
    # watermark helpers
    "get_watermark_sales",
    "get_watermark_inventory",
    "get_watermark_product",
    "get_watermark_customer",
    "get_watermark_employee",
    "get_watermark_purchase",
    "get_watermark_supplier",
    "get_watermark_store",
]
