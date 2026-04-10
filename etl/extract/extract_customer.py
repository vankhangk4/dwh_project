"""
etl/extract/extract_customer.py
Phase 9: Extract customer master data from Excel/CSV source files.

Functions:
    - extract_customers_from_excel() : Read customer Excel, tag TenantID.
    - get_last_watermark()           : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Customer_Excel'
    - Source name format: '{TenantID}_Customer_Excel'
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .xlsx (Excel 2007+)
    - .xls  (Excel 97-2003)
    - .csv  (Comma-separated values)

Expected columns in source file (KhachHang.xlsx):
    - MaKH            : Customer code (required, unique per tenant)
    - HoTen           : Full name (required)
    - GioiTinh        : Gender M/F (optional)
    - NgaySinh        : Date of birth DD/MM/YYYY (optional)
    - DienThoai       : Phone number (optional)
    - Email           : Email address (optional)
    - DiaChi          : Address (optional)
    - ThanhPho        : City (optional)
    - LoaiKH          : Customer type (optional)
    - HangTV          : Loyalty tier (optional)
    - DiemTichLuy     : Loyalty points (optional)
    - NgayDangKy      : Membership registration date (optional)

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
SOURCE_TYPE = "Customer_Excel"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_customers_from_excel(
    file_path: str,
    tenant_id: str,
    watermark: Optional[datetime] = None,
    sheet_name: str = "DanhSachKhachHang",
) -> pd.DataFrame:
    """
    Read customer master data from Excel/CSV file, tag TenantID.

    For customer master, we do full reload (replace) because:
    1. SCD Type 2 logic is handled in the stored procedure usp_Load_DimCustomer.
    2. Watermark is based on file modification time, not row-level date.

    Args:
        file_path:   Full path to the Excel/CSV file.
                    Supported: .xlsx, .xls, .csv
        tenant_id:  Tenant identifier (e.g. 'STORE_HN').
        watermark:  Ignored for full-reload customer master.
                    Kept for API compatibility. (optional)
        sheet_name: Name of the Excel sheet to read. (default: 'DanhSachKhachHang')

    Returns:
        DataFrame with columns matching STG_CustomerRaw table.
        Each row is tagged with TenantID.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting customer extraction from: %s", tenant_id, file_path)
    logger.info("[%s] Sheet name: %s", tenant_id, sheet_name)

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[%s] File not found: %s", tenant_id, file_path)
        raise FileNotFoundError(f"Customer file not found: {file_path}")

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
            logger.warning("[%s] No data found in customer file: %s", tenant_id, file_path)
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

        # Step 7: Deduplicate on MaKH (keep last occurrence)
        rows_before = len(df)
        df = df.drop_duplicates(subset=["MaKH"], keep="last")
        rows_dedup = rows_before - len(df)
        if rows_dedup > 0:
            logger.warning(
                "[%s] Removed %d duplicate customer codes (MaKH).",
                tenant_id, rows_dedup
            )

        # Step 8: Filter out invalid customer codes
        df = df[df["MaKH"].notna() & (df["MaKH"] != "") & (df["MaKH"] != "NAN")]

        logger.info(
            "[%s] Customer extraction completed. "
            "Rows to load: %d | Unique customers: %d",
            tenant_id,
            len(df),
            df["MaKH"].nunique()
        )
        logger.info("[%s] Customer extraction: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[%s] Customer extraction failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Customer extraction failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read customer data from Excel file with fallback sheet names.
    """
    logger.debug("Reading Excel file: %s (sheet: %s)", file_path, sheet_name)

    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype={
                "MaKH": str,
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
        "DanhSachKhachHang",
        "KhachHang",
        "Customer",
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
                    "MaKH": str,
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
    Read customer data from CSV file.
    """
    logger.debug("Reading CSV file: %s", file_path)

    try:
        df = pd.read_csv(
            file_path,
            dtype={
                "MaKH": str,
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
                "MaKH": str,
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
    Normalize column names to match STG_CustomerRaw schema.

    Known column variations:
        - "Mã KH" / "MAKH" / "CustomerCode" -> "MaKH"
        - "Họ Tên" / "HoTen" / "FullName" -> "HoTen"
        - "Giới Tính" / "GioiTinh" / "Gender" -> "GioiTinh"
        - "Ngày Sinh" / "NgaySinh" / "DateOfBirth" -> "NgaySinh"
        - "Điện Thoại" / "DienThoai" / "Phone" -> "DienThoai"
        - "Email" / "Email" / "EmailAddress" -> "Email"
        - "Địa Chỉ" / "DiaChi" / "Address" -> "DiaChi"
        - "Thành Phố" / "ThanhPho" / "City" -> "ThanhPho"
        - "Loại KH" / "LoaiKH" / "CustomerType" -> "LoaiKH"
        - "Hạng TV" / "HangTV" / "LoyaltyTier" -> "HangTV"
        - "Điểm Tích Lũy" / "DiemTichLuy" / "LoyaltyPoints" -> "DiemTichLuy"
        - "Ngày Đăng Ký" / "NgayDangKy" / "MemberSince" -> "NgayDangKy"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Customer Code
        "Mã KH": "MaKH",
        "MAKH": "MaKH",
        "Customer Code": "MaKH",
        "CustomerCode": "MaKH",
        "Customer": "MaKH",
        "MaKhachHang": "MaKH",
        "KhachHang": "MaKH",
        # Full Name
        "Họ Tên": "HoTen",
        "HOTEN": "HoTen",
        "Full Name": "HoTen",
        "FullName": "HoTen",
        "TenKhachHang": "HoTen",
        "Name": "HoTen",
        "KhachHang": "HoTen",
        # Gender
        "Giới Tính": "GioiTinh",
        "GIOITINH": "GioiTinh",
        "Gender": "GioiTinh",
        "Sex": "GioiTinh",
        # Date of Birth
        "Ngày Sinh": "NgaySinh",
        "NGAYSINH": "NgaySinh",
        "DateOfBirth": "NgaySinh",
        "BirthDate": "NgaySinh",
        "DOB": "NgaySinh",
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
        # Customer Type
        "Loại KH": "LoaiKH",
        "LOAIKH": "LoaiKH",
        "Customer Type": "LoaiKH",
        "CustomerType": "LoaiKH",
        "Type": "LoaiKH",
        # Loyalty Tier
        "Hạng TV": "HangTV",
        "HANGTV": "HangTV",
        "Loyalty Tier": "HangTV",
        "LoyaltyTier": "HangTV",
        "Rank": "HangTV",
        "Hang": "HangTV",
        # Loyalty Points
        "Điểm Tích Lũy": "DiemTichLuy",
        "DIEMTICHLUY": "DiemTichLuy",
        "Loyalty Points": "DiemTichLuy",
        "LoyaltyPoints": "DiemTichLuy",
        "Points": "DiemTichLuy",
        "Diem": "DiemTichLuy",
        # Registration Date
        "Ngày Đăng Ký": "NgayDangKy",
        "NGAYDANGKY": "NgayDangKy",
        "Member Since": "NgayDangKy",
        "MemberSince": "NgayDangKy",
        "RegistrationDate": "NgayDangKy",
        "JoinDate": "NgayDangKy",
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

    Required: MaKH, HoTen
    Optional: GioiTinh, NgaySinh, DienThoai, Email, DiaChi, ThanhPho,
              LoaiKH, HangTV, DiemTichLuy, NgayDangKy
    """
    required_cols = ["MaKH", "HoTen"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[%s] Missing required columns: %s. Available: %s",
            tenant_id, missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in customer file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "GioiTinh": None,
        "NgaySinh": None,
        "DienThoai": None,
        "Email": None,
        "DiaChi": None,
        "ThanhPho": None,
        "LoaiKH": "Khách lẻ",
        "HangTV": "Bronze",
        "DiemTichLuy": 0,
        "NgayDangKy": None,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[%s] Added default column '%s' = %s",
                tenant_id, col, default
            )

    # Clean string columns
    string_cols = ["MaKH", "HoTen", "DienThoai", "Email", "DiaChi", "ThanhPho"]
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
    Convert numeric and date columns to proper types.
    """
    # Numeric columns
    if "DiemTichLuy" in df.columns:
        df["DiemTichLuy"] = pd.to_numeric(df["DiemTichLuy"], errors="coerce").fillna(0)
        df["DiemTichLuy"] = df["DiemTichLuy"].astype(int)

    # Normalize gender values
    if "GioiTinh" in df.columns:
        df["GioiTinh"] = df["GioiTinh"].astype(str).str.strip().str.upper()
        df["GioiTinh"] = df["GioiTinh"].replace({
            "NAM": "Nam", "NỮ": "Nữ", "NU": "Nữ",
            "NAN": None, "NONE": None, "": None, "NAM": "Nam"
        })
        df["GioiTinh"] = df["GioiTinh"].map(lambda x: "Nam" if x == "NAM" else ("Nữ" if x in ("NỮ", "NU", "NỮ") else x))
        df["GioiTinh"] = df["GioiTinh"].replace({"NAN": None, "NONE": None, "": None})

    # Parse dates
    date_cols = ["NgaySinh", "NgayDangKy"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: str, conn=None) -> datetime:
    """
    Get last successful watermark for customer extraction.

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