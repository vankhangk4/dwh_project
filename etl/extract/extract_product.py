"""
etl/extract/extract_product.py
Phase 9: Extract product catalog from CSV source files.

Functions:
    - extract_products_from_csv() : Read product CSV, enrich with TenantID context.
    - get_last_watermark()         : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Product_CSV'
    - Shared source — no TenantID isolation needed.
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .csv (Comma-separated values) — primary format for product master
    - .xlsx (Excel 2007+) — fallback

Expected columns in source CSV:
    - MaSP            : Product code (required, unique key)
    - TenSP           : Product name (required)
    - ThuongHieu      : Brand name (optional)
    - DanhMuc         : Category name (required)
    - PhanLoai        : Sub-category / classification (optional)
    - GiaVon          : Unit cost price (optional)
    - GiaNiemYet      : Unit list price (optional)
    - SKU             : Stock keeping unit (optional)
    - Barcode         : Barcode number (optional)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from .db_utils import get_last_watermark

logger = logging.getLogger(__name__)

# Source type identifier used for watermark
SOURCE_TYPE = "Product_CSV"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_products_from_csv(
    file_path: str,
    tenant_id: Optional[str] = None,
    watermark: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read product catalog from CSV file (shared dimension — no watermark filter).

    For product master, we always do full reload (replace) because:
    1. Product catalog changes infrequently and must be up-to-date always.
    2. SCD Type 2 logic is handled in the stored procedure usp_Load_DimProduct.

    Args:
        file_path:   Full path to the CSV/Excel file.
                    Supported: .csv (primary), .xlsx (fallback)
        tenant_id:   Tenant identifier (optional — product is a shared dimension,
                    but we still tag it for audit purposes).
        watermark:   Ignored for product master (always full reload).
                    Kept for API compatibility. (optional)

    Returns:
        DataFrame with columns matching STG_ProductRaw table.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[SHARED] Starting product catalog extraction from: %s", file_path)
    logger.info("[SHARED] TenantID context: %s", tenant_id or "N/A (Shared Dimension)")

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[SHARED] File not found: %s", file_path)
        raise FileNotFoundError(f"Product file not found: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in {".csv", ".xlsx", ".xls"}:
        raise ValueError(
            f"Unsupported file format: {file_ext}. "
            f"Supported: .csv, .xlsx, .xls"
        )

    try:
        # Step 1: Read file
        if file_ext == ".csv":
            df = _read_csv(file_path)
        else:
            df = _read_excel(file_path)

        logger.info(
            "[SHARED] Raw rows read from file: %d",
            len(df)
        )

        if df.empty:
            logger.warning("[SHARED] No data found in product file: %s", file_path)
            return df

        # Step 2: Normalize column names
        df = _normalize_columns(df)
        logger.info("[SHARED] Columns after normalization: %s", list(df.columns))

        # Step 3: Validate required columns
        df = _validate_required_columns(df)

        # Step 4: Tag TenantID (optional, for audit)
        if tenant_id:
            df["_TenantID"] = tenant_id

        # Step 5: Add audit columns
        df["STG_LoadDatetime"] = datetime.now()
        df["STG_SourceFile"] = file_path

        # Step 6: Clean data types
        df = _clean_data_types(df)

        # Step 7: Deduplicate on MaSP (keep last occurrence)
        rows_before = len(df)
        df = df.drop_duplicates(subset=["MaSP"], keep="last")
        rows_dedup = rows_before - len(df)
        if rows_dedup > 0:
            logger.warning(
                "[SHARED] Removed %d duplicate product codes (MaSP).",
                rows_dedup
            )

        # Step 8: Filter out invalid product codes
        df = df[df["MaSP"].notna() & (df["MaSP"] != "") & (df["MaSP"] != "NAN")]
        rows_invalid = rows_before - rows_before  # recalculate after dedup
        logger.info(
            "[SHARED] Product extraction completed. "
            "Rows to load: %d | Unique products: %d",
            len(df), df["MaSP"].nunique()
        )
        logger.info("[SHARED] Product extraction: DONE")
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[SHARED] Product extraction failed: %s",
            ex, exc_info=True
        )
        raise RuntimeError(
            f"Product extraction failed: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read CSV
# ---------------------------------------------------------------------------

def _read_csv(file_path: str) -> pd.DataFrame:
    """
    Read product data from CSV file.
    Auto-detects delimiter, encoding, and header row.
    """
    logger.debug("Reading CSV product file: %s", file_path)

    # Try common encodings and delimiters
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    delimiters = [",", ";", "\t", "|"]

    for encoding in encodings:
        for delimiter in delimiters:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    sep=delimiter,
                    dtype={"MaSP": str, "SKU": str, "Barcode": str},
                    on_bad_lines="skip",
                    engine="python",
                )
                logger.info(
                    "Read CSV with encoding=%s, delimiter='%s': %d rows, %d cols",
                    encoding, delimiter, len(df), len(df.columns)
                )
                # Validate: must have at least MaSP column
                if len(df.columns) >= 2:
                    return df
            except Exception:
                continue

    # Fallback: simple read with default settings
    logger.warning(
        "Could not auto-detect CSV format. Trying default read (utf-8, comma)."
    )
    df = pd.read_csv(
        file_path,
        dtype={"MaSP": str, "SKU": str, "Barcode": str},
        on_bad_lines="skip",
    )
    logger.info("Read CSV default: %d rows, %d cols", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str) -> pd.DataFrame:
    """
    Read product data from Excel file (fallback when CSV not available).
    """
    logger.debug("Reading Excel product file: %s", file_path)

    try:
        df = pd.read_excel(
            file_path,
            dtype={"MaSP": str, "SKU": str, "Barcode": str},
        )
        logger.info("Read Excel: %d rows, %d cols", len(df), len(df.columns))
        return df
    except Exception as ex:
        logger.warning(
            "Could not read product Excel file %s: %s. Trying first sheet.",
            file_path, ex
        )
        df = pd.read_excel(
            file_path,
            sheet_name=0,
            dtype={"MaSP": str, "SKU": str, "Barcode": str},
        )
        logger.info("Read Excel sheet 0: %d rows", len(df))
        return df


# ---------------------------------------------------------------------------
# Helper: Normalize column names
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to match STG_ProductRaw schema.

    Known column variations:
        - "Mã SP" / "MASP" / "ProductCode" -> "MaSP"
        - "Tên SP" / "TenSP" / "ProductName" -> "TenSP"
        - "Thương Hiệu" / "ThuongHieu" / "Brand" -> "ThuongHieu"
        - "Danh Mục" / "DanhMuc" / "Category" -> "DanhMuc"
        - "Phân Loại" / "PhanLoai" / "SubCategory" -> "PhanLoai"
        - "Giá Vốn" / "GiaVon" / "CostPrice" -> "GiaVon"
        - "Giá Niêm Yết" / "GiaNiemYet" / "ListPrice" -> "GiaNiemYet"
        - "SKU" / "SKU" / "ProductSKU" -> "SKU"
        - "Barcode" / "Barcode" / "BarCode" -> "Barcode"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Product Code
        "Mã SP": "MaSP",
        "MASP": "MaSP",
        "Product Code": "MaSP",
        "ProductCode": "MaSP",
        "Product": "MaSP",
        "MaSanPham": "MaSP",
        "MaSP": "MaSP",
        # Product Name
        "Tên SP": "TenSP",
        "TENSP": "TenSP",
        "Product Name": "TenSP",
        "ProductName": "TenSP",
        "SanPham": "TenSP",
        "TenSanPham": "TenSP",
        "Name": "TenSP",
        "Name_Vi": "TenSP",
        "TenSP": "TenSP",
        # Brand
        "Thương Hiệu": "ThuongHieu",
        "THUONGHIEU": "ThuongHieu",
        "Brand": "ThuongHieu",
        "BrandName": "ThuongHieu",
        "HangSX": "ThuongHieu",
        "NhaSanXuat": "ThuongHieu",
        "ThuongHieu": "ThuongHieu",
        # Category
        "Danh Mục": "DanhMuc",
        "DANHMUC": "DanhMuc",
        "Category": "DanhMuc",
        "CategoryName": "DanhMuc",
        "NhomSanPham": "DanhMuc",
        "LoaiSanPham": "DanhMuc",
        "DanhMuc": "DanhMuc",
        # Sub-category
        "Phân Loại": "PhanLoai",
        "PHANLOAI": "PhanLoai",
        "SubCategory": "PhanLoai",
        "Sub Category": "PhanLoai",
        "PhanLoai": "PhanLoai",
        "PhanLoaiSanPham": "PhanLoai",
        # Cost Price
        "Giá Vốn": "GiaVon",
        "GIAVON": "GiaVon",
        "CostPrice": "GiaVon",
        "UnitCost": "GiaVon",
        "GiaCost": "GiaVon",
        "DonGiaVon": "GiaVon",
        "GiaVon": "GiaVon",
        # List Price
        "Giá Niêm Yết": "GiaNiemYet",
        "GIANIEMYET": "GiaNiemYet",
        "ListPrice": "GiaNiemYet",
        "UnitPrice": "GiaNiemYet",
        "GiaBan": "GiaNiemYet",
        "DonGiaBan": "GiaNiemYet",
        "GiaNiemYet": "GiaNiemYet",
        # SKU
        "SKU": "SKU",
        "ProductSKU": "SKU",
        "Product SKU": "SKU",
        "SKUCode": "SKU",
        # Barcode
        "Barcode": "Barcode",
        "Bar Code": "Barcode",
        "BarCode": "Barcode",
        "EAN": "Barcode",
        "EAN13": "Barcode",
    }

    df = df.rename(columns=column_mapping)

    for col in df.columns:
        col_clean = col.strip()
        for key, value in column_mapping.items():
            if col_clean.upper() == key.upper():
                df = df.rename(columns={col: value})
                break

    return df


# ---------------------------------------------------------------------------
# Helper: Validate required columns
# ---------------------------------------------------------------------------

def _validate_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure required columns exist. Fill missing optional columns with defaults.

    Required: MaSP, TenSP, DanhMuc
    Optional: ThuongHieu, PhanLoai, GiaVon, GiaNiemYet, SKU, Barcode
    """
    required_cols = ["MaSP", "TenSP", "DanhMuc"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[SHARED] Missing required columns: %s. Available: %s",
            missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in product file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "ThuongHieu": None,
        "PhanLoai": None,
        "GiaVon": 0.0,
        "GiaNiemYet": 0.0,
        "SKU": None,
        "Barcode": None,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[SHARED] Added default column '%s' = %s",
                col, default
            )

    # Clean string columns
    string_cols = ["MaSP", "TenSP", "ThuongHieu", "DanhMuc", "PhanLoai", "SKU", "Barcode"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].replace({"NAN": None, "NONE": None, "": None})

    return df


# ---------------------------------------------------------------------------
# Helper: Clean data types
# ---------------------------------------------------------------------------

def _clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert numeric columns and clean string values.
    """
    numeric_cols = ["GiaVon", "GiaNiemYet"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: Optional[str] = None, conn=None) -> datetime:
    """
    Get last successful watermark for product extraction.

    Args:
        tenant_id: Ignored for shared dimension. (optional)
        conn:      Database connection. (optional)

    Returns:
        datetime of last successful extraction.
    """
    if conn is None:
        logger.warning(
            "[SHARED] get_last_watermark called without DB connection. "
            "Returning default 2020-01-01."
        )
        return datetime(2020, 1, 1, 0, 0, 0)

    # For shared dimension, use 'SHARED' as tenant sentinel
    effective_tenant = tenant_id or "SHARED"
    source_name = f"{effective_tenant}_{SOURCE_TYPE}"
    return get_last_watermark(conn, effective_tenant, SOURCE_TYPE)