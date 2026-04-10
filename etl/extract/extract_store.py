"""
etl/extract/extract_store.py
Phase 9: Extract store master data from Excel/CSV source files.

Functions:
    - extract_stores_from_excel() : Read store Excel, tag TenantID.
    - get_last_watermark()         : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Store_Excel'
    - Source name format: '{TenantID}_Store_Excel'
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .xlsx (Excel 2007+)
    - .xls  (Excel 97-2003)
    - .csv  (Comma-separated values)

Expected columns in source file (CuaHang.xlsx):
    - MaCH            : Store code (required, unique per tenant)
    - TenCH           : Store name (required)
    - LoaiCH          : Store type (optional)
    - DiaChi          : Address (optional)
    - Phuong          : Ward (optional)
    - Quan            : District (optional)
    - ThanhPho        : City (required)
    - Vung            : Region/Zone (optional)
    - DienThoai       : Phone number (optional)
    - Email           : Email address (optional)
    - NguoiQuanLy     : Store manager name (optional)
    - NgayKhaiTruong  : Opening date (optional)
    - NgayDongCua     : Closing date (optional)

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
SOURCE_TYPE = "Store_Excel"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_stores_from_excel(
    file_path: str,
    tenant_id: str,
    watermark: Optional[datetime] = None,
    sheet_name: str = "DanhSachCuaHang",
) -> pd.DataFrame:
    """
    Read store master data from Excel/CSV file, tag TenantID.

    For store master, we do full reload (replace) because:
    1. Store data changes rarely.
    2. The stored procedure usp_Load_DimStore handles upsert logic.

    Args:
        file_path:   Full path to the Excel/CSV file.
                    Supported: .xlsx, .xls, .csv
        tenant_id:  Tenant identifier (e.g. 'STORE_HN').
        watermark:  Ignored for full-reload store master.
                    Kept for API compatibility. (optional)
        sheet_name: Name of the Excel sheet to read. (default: 'DanhSachCuaHang')

    Returns:
        DataFrame with columns matching STG_StoreRaw table.
        Each row is tagged with TenantID.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting store extraction from: %s", tenant_id, file_path)
    logger.info("[%s] Sheet name: %s", tenant_id, sheet_name)

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[%s] File not found: %s", tenant_id, file_path)
        raise FileNotFoundError(f"Store file not found: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in {".xlsx", ".xls", ".csv"}:
        raise ValueError(
            f"Unsupported file format: {file_ext}. "
            f"Supported: .xlsx, .xls, .csv"
        )

    try:
        # Step 1: Read file based on extension
        if file_ext == ".csv":
            df = _read_csv(file_path)
        else:
            df = _read_excel(file_path, sheet_name)

        logger.info(
            "[%s] Raw rows read from file: %d",
            tenant_id, len(df)
        )

        if df.empty:
            logger.warning("[%s] No data found in store file: %s", tenant_id, file_path)
            return df

        # Step 2: Normalize column names
        df = _normalize_columns(df)
        logger.info("[%s] Columns after normalization: %s", tenant_id, list(df.columns))

        # Step 3: Validate required columns
        df = _validate_required_columns(df, tenant_id)

        # Step 4: Tag TenantID
        df["TenantID"] = tenant_id

        # Step 5: Add audit columns
        df["STG_LoadDatetime"] = datetime.now()
        df["STG_SourceFile"] = file_path

        # Step 6: Clean data types
        df = _clean_data_types(df)

        # Step 7: Deduplicate on MaCH (keep last occurrence)
        rows_before = len(df)
        df = df.drop_duplicates(subset=["MaCH"], keep="last")
        rows_dedup = rows_before - len(df)
        if rows_dedup > 0:
            logger.warning(
                "[%s] Removed %d duplicate store codes (MaCH).",
                tenant_id, rows_dedup
            )

        # Step 8: Filter out invalid store codes
        df = df[df["MaCH"].notna() & (df["MaCH"] != "") & (df["MaCH"] != "NAN")]

        logger.info(
            "[%s] Store extraction completed. "
            "Rows to load: %d | Unique stores: %d",
            tenant_id,
            len(df),
            df["MaCH"].nunique()
        )
        logger.info("[%s] Store extraction: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[%s] Store extraction failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Store extraction failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read store data from Excel file with fallback sheet names.
    """
    logger.debug("Reading Excel file: %s (sheet: %s)", file_path, sheet_name)

    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype={
                "MaCH": str,
                "DienThoai": str,
                "Email": str,
            },
        )
        logger.info("Read Excel sheet '%s': %d rows", sheet_name, len(df))
        return df
    except Exception as ex:
        logger.warning(
            "Could not read sheet '%s': %s. Trying alternatives.",
            sheet_name, ex
        )

    fallback_sheets = [
        "DanhSachCuaHang",
        "CuaHang",
        "Store",
        "Shop",
        "Sheet1",
        0,
    ]

    for alt_sheet in fallback_sheets:
        if alt_sheet == sheet_name:
            continue
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=alt_sheet,
                dtype={
                    "MaCH": str,
                    "DienThoai": str,
                    "Email": str,
                },
            )
            logger.info(
                "Read Excel fallback sheet '%s': %d rows",
                alt_sheet, len(df)
            )
            return df
        except Exception:
            continue

    raise RuntimeError(
        f"Could not read any sheet from {file_path}. "
        f"Tried: {sheet_name}, {fallback_sheets}"
    )


# ---------------------------------------------------------------------------
# Helper: Read CSV
# ---------------------------------------------------------------------------

def _read_csv(file_path: str) -> pd.DataFrame:
    """
    Read store data from CSV file.
    """
    logger.debug("Reading CSV file: %s", file_path)

    try:
        df = pd.read_csv(
            file_path,
            dtype={
                "MaCH": str,
                "DienThoai": str,
                "Email": str,
            },
            encoding="utf-8",
        )
    except UnicodeDecodeError:
        logger.warning(
            "UTF-8 decode failed for %s. Trying 'latin-1'.",
            file_path
        )
        df = pd.read_csv(
            file_path,
            dtype={
                "MaCH": str,
                "DienThoai": str,
                "Email": str,
            },
            encoding="latin-1",
        )

    logger.info("Read CSV file: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Helper: Normalize column names
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to match STG_StoreRaw schema.

    Known column variations:
        - "Mã CH" / "MACH" / "StoreCode" -> "MaCH"
        - "Tên CH" / "TenCH" / "StoreName" -> "TenCH"
        - "Loại CH" / "LoaiCH" / "StoreType" -> "LoaiCH"
        - "Địa Chỉ" / "DiaChi" / "Address" -> "DiaChi"
        - "Phường" / "Phuong" / "Ward" -> "Phuong"
        - "Quận" / "Quan" / "District" -> "Quan"
        - "Thành Phố" / "ThanhPho" / "City" -> "ThanhPho"
        - "Vùng" / "Vung" / "Region" -> "Vung"
        - "Điện Thoại" / "DienThoai" / "Phone" -> "DienThoai"
        - "Email" / "Email" / "EmailAddress" -> "Email"
        - "Người Quản Lý" / "NguoiQuanLy" / "StoreManager" -> "NguoiQuanLy"
        - "Ngày Khai Trương" / "NgayKhaiTruong" / "OpenDate" -> "NgayKhaiTruong"
        - "Ngày Đóng Cửa" / "NgayDongCua" / "CloseDate" -> "NgayDongCua"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Store Code
        "Mã CH": "MaCH",
        "MACH": "MaCH",
        "Store Code": "MaCH",
        "StoreCode": "MaCH",
        "CuaHang": "MaCH",
        "ShopCode": "MaCH",
        # Store Name
        "Tên CH": "TenCH",
        "TENCH": "TenCH",
        "Store Name": "TenCH",
        "StoreName": "TenCH",
        "CuaHang": "TenCH",
        "Name": "TenCH",
        "TenCuaHang": "TenCH",
        # Store Type
        "Loại CH": "LoaiCH",
        "LOAICH": "LoaiCH",
        "Store Type": "LoaiCH",
        "StoreType": "LoaiCH",
        "LoaiCH": "LoaiCH",
        "Type": "LoaiCH",
        "LoaiCuaHang": "LoaiCH",
        # Address
        "Địa Chỉ": "DiaChi",
        "DIACHI": "DiaChi",
        "Address": "DiaChi",
        "DiaChi": "DiaChi",
        "AddressLine": "DiaChi",
        # Ward
        "Phường": "Phuong",
        "PHUONG": "Phuong",
        "Ward": "Phuong",
        "Phuong": "Phuong",
        # District
        "Quận": "Quan",
        "QUAN": "Quan",
        "District": "Quan",
        "Quan": "Quan",
        # City
        "Thành Phố": "ThanhPho",
        "THANHPHO": "ThanhPho",
        "City": "ThanhPho",
        "Province": "ThanhPho",
        "TinhThanh": "ThanhPho",
        # Region
        "Vùng": "Vung",
        "VUNG": "Vung",
        "Region": "Vung",
        "Vung": "Vung",
        "Zone": "Vung",
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
        # Store Manager
        "Người Quản Lý": "NguoiQuanLy",
        "NGUOIQUANLY": "NguoiQuanLy",
        "Store Manager": "NguoiQuanLy",
        "StoreManager": "NguoiQuanLy",
        "Manager": "NguoiQuanLy",
        "NguoiQuanLy": "NguoiQuanLy",
        "QuanLy": "NguoiQuanLy",
        # Opening Date
        "Ngày Khai Trương": "NgayKhaiTruong",
        "NGAYKHAITRUONG": "NgayKhaiTruong",
        "Open Date": "NgayKhaiTruong",
        "OpenDate": "NgayKhaiTruong",
        "KhaiTruong": "NgayKhaiTruong",
        "OpeningDate": "NgayKhaiTruong",
        "NgayKhaiTruong": "NgayKhaiTruong",
        # Closing Date
        "Ngày Đóng Cửa": "NgayDongCua",
        "NGAYDONGCUA": "NgayDongCua",
        "Close Date": "NgayDongCua",
        "CloseDate": "NgayDongCua",
        "DongCua": "NgayDongCua",
        "NgayDongCua": "NgayDongCua",
        "EndDate": "NgayDongCua",
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

def _validate_required_columns(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Ensure required columns exist. Fill missing optional columns with defaults.

    Required: MaCH, TenCH, ThanhPho
    Optional: LoaiCH, DiaChi, Phuong, Quan, Vung, DienThoai, Email,
              NguoiQuanLy, NgayKhaiTruong, NgayDongCua
    """
    required_cols = ["MaCH", "TenCH", "ThanhPho"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[%s] Missing required columns: %s. Available: %s",
            tenant_id, missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in store file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "LoaiCH": "Cửa hàng truyền thống",
        "DiaChi": None,
        "Phuong": None,
        "Quan": None,
        "Vung": None,
        "DienThoai": None,
        "Email": None,
        "NguoiQuanLy": None,
        "NgayKhaiTruong": None,
        "NgayDongCua": None,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[%s] Added default column '%s' = %s",
                tenant_id, col, default
            )

    # Clean string columns
    string_cols = [
        "MaCH", "TenCH", "LoaiCH", "DiaChi", "Phuong", "Quan",
        "ThanhPho", "Vung", "DienThoai", "Email", "NguoiQuanLy"
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
    Parse date columns to proper types.
    """
    date_cols = ["NgayKhaiTruong", "NgayDongCua"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: str, conn=None) -> datetime:
    """
    Get last successful watermark for store extraction.

    Args:
        tenant_id: Tenant identifier.
        conn:      Database connection. (optional)

    Returns:
        datetime of last successful extraction.
    """
    if conn is None:
        logger.warning(
            "[%s] get_last_watermark called without DB connection. "
            "Returning default 2020-01-01.",
            tenant_id
        )
        return datetime(2020, 1, 1, 0, 0, 0)

    source_name = f"{tenant_id}_{SOURCE_TYPE}"
    return get_last_watermark(conn, tenant_id, SOURCE_TYPE)