"""
etl/extract/extract_supplier.py
Phase 9: Extract supplier master data from CSV source files.

Functions:
    - extract_suppliers_from_csv() : Read supplier CSV, enrich with TenantID context.
    - get_last_watermark()         : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Supplier_CSV'
    - Shared source — no TenantID isolation needed.
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .csv (Comma-separated values) — primary format for supplier master
    - .xlsx (Excel 2007+) — fallback

Expected columns in source CSV:
    - MaNCC           : Supplier code (required, unique key)
    - TenNCC          : Supplier name (required)
    - NguoiLienHe     : Contact person (optional)
    - ChucVu          : Contact title (optional)
    - DienThoai       : Phone number (optional)
    - Email           : Email address (optional)
    - DiaChi          : Address (optional)
    - ThanhPho        : City (optional)
    - QuocGia         : Country (optional)
    - MaSoThue        : Tax ID (optional)
    - DieuKhoanTT     : Payment terms (optional)

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
SOURCE_TYPE = "Supplier_CSV"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_suppliers_from_csv(
    file_path: str,
    tenant_id: Optional[str] = None,
    watermark: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read supplier catalog from CSV file (shared dimension — no watermark filter).

    For supplier master, we always do full reload (replace) because:
    1. Supplier catalog changes infrequently and must be up-to-date always.
    2. The stored procedure handles upsert logic.

    Args:
        file_path:   Full path to the CSV/Excel file.
                    Supported: .csv (primary), .xlsx (fallback)
        tenant_id:   Tenant identifier (optional — supplier is a shared dimension,
                    but we still tag it for audit purposes).
        watermark:   Ignored for supplier master (always full reload).
                    Kept for API compatibility. (optional)

    Returns:
        DataFrame with columns matching STG_SupplierRaw table.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[SHARED] Starting supplier extraction from: %s", file_path)
    logger.info("[SHARED] TenantID context: %s", tenant_id or "N/A (Shared Dimension)")

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[SHARED] File not found: %s", file_path)
        raise FileNotFoundError(f"Supplier file not found: {file_path}")

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
            logger.warning("[SHARED] No data found in supplier file: %s", file_path)
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

        # Step 7: Deduplicate on MaNCC (keep last occurrence)
        rows_before = len(df)
        df = df.drop_duplicates(subset=["MaNCC"], keep="last")
        rows_dedup = rows_before - len(df)
        if rows_dedup > 0:
            logger.warning(
                "[SHARED] Removed %d duplicate supplier codes (MaNCC).",
                rows_dedup
            )

        # Step 8: Filter out invalid supplier codes
        df = df[df["MaNCC"].notna() & (df["MaNCC"] != "") & (df["MaNCC"] != "NAN")]

        logger.info(
            "[SHARED] Supplier extraction completed. "
            "Rows to load: %d | Unique suppliers: %d",
            len(df),
            df["MaNCC"].nunique()
        )
        logger.info("[SHARED] Supplier extraction: DONE")
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[SHARED] Supplier extraction failed: %s",
            ex, exc_info=True
        )
        raise RuntimeError(
            f"Supplier extraction failed: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read CSV
# ---------------------------------------------------------------------------

def _read_csv(file_path: str) -> pd.DataFrame:
    """
    Read supplier data from CSV file.
    Auto-detects delimiter, encoding, and header row.
    """
    logger.debug("Reading CSV supplier file: %s", file_path)

    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    delimiters = [",", ";", "\t", "|"]

    for encoding in encodings:
        for delimiter in delimiters:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    sep=delimiter,
                    dtype={"MaNCC": str, "DienThoai": str, "MaSoThue": str},
                    on_bad_lines="skip",
                    engine="python",
                )
                logger.info(
                    "Read CSV with encoding=%s, delimiter='%s': %d rows, %d cols",
                    encoding, delimiter, len(df), len(df.columns)
                )
                if len(df.columns) >= 2:
                    return df
            except Exception:
                continue

    logger.warning(
        "Could not auto-detect CSV format. Trying default read (utf-8, comma)."
    )
    df = pd.read_csv(
        file_path,
        dtype={"MaNCC": str, "DienThoai": str, "MaSoThue": str},
        on_bad_lines="skip",
    )
    logger.info("Read CSV default: %d rows, %d cols", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str) -> pd.DataFrame:
    """
    Read supplier data from Excel file (fallback when CSV not available).
    """
    logger.debug("Reading Excel supplier file: %s", file_path)

    try:
        df = pd.read_excel(
            file_path,
            dtype={"MaNCC": str, "DienThoai": str, "MaSoThue": str},
        )
        logger.info("Read Excel: %d rows, %d cols", len(df), len(df.columns))
        return df
    except Exception as ex:
        logger.warning(
            "Could not read supplier Excel file %s: %s. Trying first sheet.",
            file_path, ex
        )
        df = pd.read_excel(
            file_path,
            sheet_name=0,
            dtype={"MaNCC": str, "DienThoai": str, "MaSoThue": str},
        )
        logger.info("Read Excel sheet 0: %d rows", len(df))
        return df


# ---------------------------------------------------------------------------
# Helper: Normalize column names
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to match STG_SupplierRaw schema.

    Known column variations:
        - "Mã NCC" / "MaNCC" / "SupplierCode" -> "MaNCC"
        - "Tên NCC" / "TenNCC" / "SupplierName" -> "TenNCC"
        - "Người Liên Hệ" / "NguoiLienHe" / "ContactPerson" -> "NguoiLienHe"
        - "Chức Vụ" / "ChucVu" / "ContactTitle" -> "ChucVu"
        - "Điện Thoại" / "DienThoai" / "Phone" -> "DienThoai"
        - "Email" / "Email" / "EmailAddress" -> "Email"
        - "Địa Chỉ" / "DiaChi" / "Address" -> "DiaChi"
        - "Thành Phố" / "ThanhPho" / "City" -> "ThanhPho"
        - "Quốc Gia" / "QuocGia" / "Country" -> "QuocGia"
        - "Mã Số Thuế" / "MaSoThue" / "TaxID" -> "MaSoThue"
        - "Điều Khoản TT" / "DieuKhoanTT" / "PaymentTerms" -> "DieuKhoanTT"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Supplier Code
        "Mã NCC": "MaNCC",
        "MaNCC": "MaNCC",
        "Supplier Code": "MaNCC",
        "SupplierCode": "MaNCC",
        "NhaCungCap": "MaNCC",
        "NCC": "MaNCC",
        "Code": "MaNCC",
        # Supplier Name
        "Tên NCC": "TenNCC",
        "TENNCC": "TenNCC",
        "Supplier Name": "TenNCC",
        "SupplierName": "TenNCC",
        "NhaCungCap": "TenNCC",
        "TenNCC": "TenNCC",
        "Name": "TenNCC",
        # Contact Person
        "Người Liên Hệ": "NguoiLienHe",
        "NGUOI LIEN HE": "NguoiLienHe",
        "Contact Person": "NguoiLienHe",
        "ContactPerson": "NguoiLienHe",
        "Contact": "NguoiLienHe",
        "NguoiLienHe": "NguoiLienHe",
        "LienHe": "NguoiLienHe",
        # Contact Title
        "Chức Vụ": "ChucVu",
        "CHUCVU": "ChucVu",
        "Contact Title": "ChucVu",
        "ContactTitle": "ChucVu",
        "ChucVu": "ChucVu",
        "Title": "ChucVu",
        # Phone
        "Điện Thoại": "DienThoai",
        "DIENTHOAI": "DienThoai",
        "Phone": "DienThoai",
        "PhoneNumber": "DienThoai",
        "Mobile": "DienThoai",
        "SDT": "DienThoai",
        # Email
        "Email": "Email",
        "Email Address": "Email",
        "EmailAddress": "Email",
        # Address
        "Địa Chỉ": "DiaChi",
        "DIACHI": "DiaChi",
        "Address": "DiaChi",
        "DiaChi": "DiaChi",
        # City
        "Thành Phố": "ThanhPho",
        "THANHPHO": "ThanhPho",
        "City": "ThanhPho",
        "Province": "ThanhPho",
        # Country
        "Quốc Gia": "QuocGia",
        "QUOCGIA": "QuocGia",
        "Country": "QuocGia",
        "QuocGia": "QuocGia",
        # Tax ID
        "Mã Số Thuế": "MaSoThue",
        "MASOTHUe": "MaSoThue",
        "Tax ID": "MaSoThue",
        "TaxID": "MaSoThue",
        "TaxCode": "MaSoThue",
        "MST": "MaSoThue",
        # Payment Terms
        "Điều Khoản TT": "DieuKhoanTT",
        "DIEUKHOANTT": "DieuKhoanTT",
        "Payment Terms": "DieuKhoanTT",
        "PaymentTerms": "DieuKhoanTT",
        "DieuKhoan": "DieuKhoanTT",
        "Terms": "DieuKhoanTT",
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

    Required: MaNCC, TenNCC
    Optional: NguoiLienHe, ChucVu, DienThoai, Email, DiaChi,
              ThanhPho, QuocGia, MaSoThue, DieuKhoanTT
    """
    required_cols = ["MaNCC", "TenNCC"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[SHARED] Missing required columns: %s. Available: %s",
            missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in supplier file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "NguoiLienHe": None,
        "ChucVu": None,
        "DienThoai": None,
        "Email": None,
        "DiaChi": None,
        "ThanhPho": None,
        "QuocGia": "Việt Nam",
        "MaSoThue": None,
        "DieuKhoanTT": None,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[SHARED] Added default column '%s' = %s",
                col, default
            )

    # Clean string columns
    string_cols = [
        "MaNCC", "TenNCC", "NguoiLienHe", "ChucVu",
        "DienThoai", "Email", "DiaChi", "ThanhPho", "QuocGia", "MaSoThue", "DieuKhoanTT"
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"NAN": None, "NONE": None, "": None})

    return df


# ---------------------------------------------------------------------------
# Helper: Clean data types
# ---------------------------------------------------------------------------

def _clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean string values — no numeric conversion needed for supplier.
    """
    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: Optional[str] = None, conn=None) -> datetime:
    """
    Get last successful watermark for supplier extraction.

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

    effective_tenant = tenant_id or "SHARED"
    source_name = f"{effective_tenant}_{SOURCE_TYPE}"
    return get_last_watermark(conn, effective_tenant, SOURCE_TYPE)