"""
etl/extract/extract_sales.py
Phase 9: Extract sales data from Excel source files.

Functions:
    - extract_sales_from_excel()  : Read sales Excel, filter by watermark, tag TenantID.
    - get_last_watermark()        : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Sales_Excel'
    - Source name format: '{TenantID}_Sales_Excel'
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .xlsx (Excel 2007+)
    - .xls  (Excel 97-2003)
    - .csv  (Comma-separated values)

Expected columns in source file:
    - MaHoaDon    : Invoice number (required)
    - NgayBan     : Sale date DD/MM/YYYY (required)
    - MaSP        : Product code (required)
    - SoLuong     : Quantity sold (required)
    - DonGiaBan   : Unit selling price (required)
    - MaKH        : Customer code (optional)
    - MaNV        : Employee code (optional)
    - MaCH        : Store code (required)
    - PhuongThucTT: Payment method (optional)
    - ChietKhau   : Discount amount (optional)
    - KenhBan     : Sales channel (optional)
    - NhomBanHang : Sales group (optional)
    - IsHoanTra   : Return flag 0/1 (optional)
    - LyDoHoanTra : Return reason (optional)
    - SoDong      : Invoice line number (optional)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

from .db_utils import get_last_watermark, load_dataframe_to_staging

logger = logging.getLogger(__name__)

# Source type identifier used for watermark
SOURCE_TYPE = "Sales_Excel"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_sales_from_excel(
    file_path: str,
    tenant_id: str,
    watermark: Optional[datetime] = None,
    sheet_name: str = "DanhSachHoaDon",
) -> pd.DataFrame:
    """
    Read sales data from Excel file, filter by watermark, and tag TenantID.

    Args:
        file_path:   Full path to the Excel/CSV file.
                    Supported: .xlsx, .xls, .csv
        tenant_id:  Tenant identifier (e.g. 'STORE_HN').
        watermark:  Extract only rows AFTER this datetime.
                    If None, fetches from DB via get_last_watermark(). (optional)
        sheet_name: Name of the Excel sheet to read. (default: 'DanhSachHoaDon')

    Returns:
        DataFrame with columns matching STG_SalesRaw table.
        Each row is tagged with TenantID.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting sales extraction from: %s", tenant_id, file_path)
    logger.info("[%s] Sheet name: %s | Watermark: %s", tenant_id, sheet_name, watermark)

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[%s] File not found: %s", tenant_id, file_path)
        raise FileNotFoundError(f"Sales file not found: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in {".xlsx", ".xls", ".csv"}:
        raise ValueError(
            f"Unsupported file format: {file_ext}. "
            f"Supported: .xlsx, .xls, .csv"
        )

    # Determine watermark
    if watermark is None:
        logger.info(
            "[%s] No watermark provided. "
            "Watermark must be passed from orchestrator (incremental).",
            tenant_id
        )
        watermark = datetime(2020, 1, 1, 0, 0, 0)

    watermark_ts = pd.Timestamp(watermark)

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
            logger.warning("[%s] No data found in file: %s", tenant_id, file_path)
            return df

        # Step 2: Normalize column names
        df = _normalize_columns(df)
        logger.info("[%s] Columns after normalization: %s", tenant_id, list(df.columns))

        # Step 3: Parse dates
        df = _parse_dates(df)

        # Step 4: Filter by watermark
        rows_before = len(df)
        df = df[df["NgayBan"] > watermark_ts]
        rows_filtered = rows_before - len(df)
        logger.info(
            "[%s] Watermark filter: removed %d old rows (before %s), "
            "kept %d new rows.",
            tenant_id, rows_filtered,
            watermark_ts.strftime("%Y-%m-%d %H:%M:%S"),
            len(df)
        )

        if df.empty:
            logger.info(
                "[%s] No new rows after watermark filter. "
                "Skipping extract for this cycle.",
                tenant_id
            )
            return df

        # Step 5: Validate required columns
        df = _validate_required_columns(df, tenant_id)

        # Step 6: Tag TenantID
        df["TenantID"] = tenant_id

        # Step 7: Add audit columns
        df["STG_LoadDatetime"] = datetime.now()
        df["STG_SourceFile"] = file_path

        # Step 8: Clean data types
        df = _clean_data_types(df)

        logger.info(
            "[%s] Sales extraction completed. "
            "Rows to load: %d | Date range: %s to %s",
            tenant_id,
            len(df),
            df["NgayBan"].min().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
            df["NgayBan"].max().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
        )
        logger.info("[%s] Sales extraction: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[%s] Sales extraction failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Sales extraction failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read Excel file with fallback sheet names.

    Tries the requested sheet first, then falls back to common alternatives.
    """
    logger.debug("Reading Excel file: %s (sheet: %s)", file_path, sheet_name)

    # Try requested sheet
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype={
                "MaHoaDon": str,
                "MaSP": str,
                "MaKH": str,
                "MaNV": str,
                "MaCH": str,
            },
        )
        logger.info("Read Excel sheet '%s': %d rows", sheet_name, len(df))
        return df
    except Exception as ex:
        logger.warning(
            "Could not read sheet '%s': %s. Trying alternatives.",
            sheet_name, ex
        )

    # Fallback: try common alternative sheet names
    fallback_sheets = [
        "DanhSachHoaDon",
        "Sheet1",
        "Sales",
        "DoanhThu",
        0,  # First sheet by index
    ]

    for alt_sheet in fallback_sheets:
        if alt_sheet == sheet_name:
            continue
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=alt_sheet,
                dtype={
                    "MaHoaDon": str,
                    "MaSP": str,
                    "MaKH": str,
                    "MaNV": str,
                    "MaCH": str,
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
    Read sales data from CSV file.
    """
    logger.debug("Reading CSV file: %s", file_path)

    try:
        # Try UTF-8 first
        df = pd.read_csv(
            file_path,
            dtype={
                "MaHoaDon": str,
                "MaSP": str,
                "MaKH": str,
                "MaNV": str,
                "MaCH": str,
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
                "MaHoaDon": str,
                "MaSP": str,
                "MaKH": str,
                "MaNV": str,
                "MaCH": str,
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
    Normalize column names: strip whitespace, handle common variations.

    Known column name variations from source files:
        - "Mã Hóa Đơn" / "MAHOADON" / "SoHoaDon" -> "MaHoaDon"
        - "Ngày Bán" / "NGAYBAN" / "Date" -> "NgayBan"
        - etc.
    """
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Column name mapping: common variations -> standard name
    column_mapping: dict[str, str] = {
        # Invoice
        "Mã Hóa Đơn": "MaHoaDon",
        "MAHOADON": "MaHoaDon",
        "SoHoaDon": "MaHoaDon",
        "So Hoa Don": "MaHoaDon",
        "Invoice Number": "MaHoaDon",
        "InvoiceNo": "MaHoaDon",
        "Invoice": "MaHoaDon",
        # Date
        "Ngày Bán": "NgayBan",
        "NGAYBAN": "NgayBan",
        "Date": "NgayBan",
        "Sale Date": "NgayBan",
        "SaleDate": "NgayBan",
        "Ngày": "NgayBan",
        "DateOfSale": "NgayBan",
        # Product
        "Mã SP": "MaSP",
        "MASP": "MaSP",
        "San Pham": "MaSP",
        "Product Code": "MaSP",
        "ProductCode": "MaSP",
        "Product": "MaSP",
        # Quantity
        "Số Lượng": "SoLuong",
        "SOLUONG": "SoLuong",
        "Quantity": "SoLuong",
        "Qty": "SoLuong",
        # Price
        "Đơn Giá": "DonGiaBan",
        "DONGIABAN": "DonGiaBan",
        "DonGia": "DonGiaBan",
        "Unit Price": "DonGiaBan",
        "UnitPrice": "DonGiaBan",
        "Price": "DonGiaBan",
        # Customer
        "Mã KH": "MaKH",
        "MAKH": "MaKH",
        "Customer Code": "MaKH",
        "CustomerCode": "MaKH",
        "KhachHang": "MaKH",
        # Employee
        "Mã NV": "MaNV",
        "MANV": "MaNV",
        "Employee Code": "MaNV",
        "EmployeeCode": "MaNV",
        "NhanVien": "MaNV",
        # Store
        "Mã CH": "MaCH",
        "MACH": "MaCH",
        "Store Code": "MaCH",
        "StoreCode": "MaCH",
        "CuaHang": "MaCH",
        # Payment
        "Phương Thức TT": "PhuongThucTT",
        "PhuongThucTT": "PhuongThucTT",
        "Payment Method": "PhuongThucTT",
        "Payment": "PhuongThucTT",
        "PTTT": "PhuongThucTT",
        # Discount
        "Chiết Khấu": "ChietKhau",
        "CHIETKHAU": "ChietKhau",
        "Discount": "ChietKhau",
        "ChietKhau": "ChietKhau",
        # Sales Channel
        "Kênh Bán": "KenhBan",
        "KENHBAN": "KenhBan",
        "Sales Channel": "KenhBan",
        "Channel": "KenhBan",
        # Sales Group
        "Nhóm Bán Hàng": "NhomBanHang",
        "NHOMBANHANG": "NhomBanHang",
        "Sales Group": "NhomBanHang",
        # Return
        "Is Hoan Tra": "IsHoanTra",
        "ISHOANTRA": "IsHoanTra",
        "HoanTra": "IsHoanTra",
        "IsReturn": "IsHoanTra",
        "Return": "IsHoanTra",
        # Return Reason
        "Lý Do Hoàn Trả": "LyDoHoanTra",
        "LyDoHoanTra": "LyDoHoanTra",
        "Return Reason": "LyDoHoanTra",
        # Line
        "Số Dòng": "SoDong",
        "SODONG": "SoDong",
        "Line": "SoDong",
        "SoDong": "SoDong",
    }

    # Apply mapping
    df = df.rename(columns=column_mapping)

    # Also rename columns that match case-insensitively
    for col in df.columns:
        col_clean = col.strip()
        for key, value in column_mapping.items():
            if col_clean.upper() == key.upper():
                df = df.rename(columns={col: value})
                break

    return df


# ---------------------------------------------------------------------------
# Helper: Parse dates
# ---------------------------------------------------------------------------

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'NgayBan' column to datetime.
    Supports formats: DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, YYYYMMDD.
    """
    if "NgayBan" not in df.columns:
        logger.warning(
            "Column 'NgayBan' not found. Available: %s",
            list(df.columns)
        )
        return df

    # Try multiple date formats common in Vietnam Excel exports
    date_formats = [
        "%d/%m/%Y",      # DD/MM/YYYY  (most common in Vietnam)
        "%d-%m-%Y",      # DD-MM-YYYY
        "%Y-%m-%d",      # YYYY-MM-DD
        "%m/%d/%Y",      # MM/DD/YYYY
        "%Y%m%d",        # YYYYMMDD as int
        "%d/%m/%Y %H:%M:%S",  # with time
    ]

    parsed = False
    for fmt in date_formats:
        try:
            if df["NgayBan"].dtype == "object" or str(df["NgayBan"].dtype).startswith("str"):
                df["NgayBan"] = pd.to_datetime(
                    df["NgayBan"], format=fmt, errors="raise"
                )
            else:
                # Already numeric (YYYYMMDD as int) or datetime
                df["NgayBan"] = pd.to_datetime(df["NgayBan"], errors="coerce")
            parsed = True
            logger.debug("Parsed dates with format: %s", fmt)
            break
        except (ValueError, TypeError):
            continue

    if not parsed:
        # Fallback: pandas auto-detection
        df["NgayBan"] = pd.to_datetime(
            df["NgayBan"], dayfirst=True, errors="coerce"
        )
        logger.info("Parsed dates using pandas auto-detection (dayfirst=True).")

    # Count invalid dates
    invalid_count = df["NgayBan"].isna().sum()
    if invalid_count > 0:
        logger.warning(
            "Found %d rows with invalid/unparseable dates in NgayBan column.",
            invalid_count
        )

    return df


# ---------------------------------------------------------------------------
# Helper: Validate required columns
# ---------------------------------------------------------------------------

def _validate_required_columns(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Ensure required columns exist. Fill missing optional columns with defaults.

    Required: MaHoaDon, NgayBan, MaSP, MaCH, SoLuong, DonGiaBan
    Optional: MaKH, MaNV, PhuongThucTT, ChietKhau, KenhBan, NhomBanHang,
             IsHoanTra, LyDoHoanTra, SoDong
    """
    required_cols = ["MaHoaDon", "NgayBan", "MaSP", "MaCH", "SoLuong", "DonGiaBan"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[%s] Missing required columns: %s. Available: %s",
            tenant_id, missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in sales file: {missing_required}"
        )

    # Fill optional columns with defaults
    optional_defaults: dict[str, Any] = {
        "MaKH": None,
        "MaNV": None,
        "PhuongThucTT": "Tiền mặt",
        "ChietKhau": 0.0,
        "KenhBan": "InStore",
        "NhomBanHang": "Bán lẻ",
        "IsHoanTra": 0,
        "LyDoHoanTra": None,
        "SoDong": 1,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[%s] Added default column '%s' = %s",
                tenant_id, col, default
            )

    # Strip whitespace from string columns
    string_cols = ["MaHoaDon", "MaSP", "MaKH", "MaNV", "MaCH"]
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
    Convert numeric columns to proper types.
    """
    # Numeric columns
    numeric_cols = ["SoLuong", "DonGiaBan", "ChietKhau"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Boolean
    if "IsHoanTra" in df.columns:
        df["IsHoanTra"] = pd.to_numeric(df["IsHoanTra"], errors="coerce").fillna(0)
        df["IsHoanTra"] = df["IsHoanTra"].apply(lambda x: 1 if x != 0 else 0)

    # Integer
    if "SoLuong" in df.columns:
        df["SoLuong"] = df["SoLuong"].astype(int)
    if "SoDong" in df.columns:
        df["SoDong"] = pd.to_numeric(df["SoDong"], errors="coerce").fillna(1).astype(int)

    return df


# ---------------------------------------------------------------------------
# Watermark helper (imported from db_utils, re-exported)
# ---------------------------------------------------------------------------

def get_watermark(tenant_id: str, conn=None) -> datetime:
    """
    Get last successful watermark for sales extraction.

    Args:
        tenant_id: Tenant identifier.
        conn:      Database connection. (optional, kept for compatibility)

    Returns:
        datetime of last successful extraction.
    """
    if conn is None:
        logger.warning(
            "get_watermark called without DB connection. "
            "Returning default 2020-01-01."
        )
        return datetime(2020, 1, 1, 0, 0, 0)

    source_name = f"{tenant_id}_{SOURCE_TYPE}"
    return get_last_watermark(conn, tenant_id, SOURCE_TYPE)
