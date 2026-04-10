"""
etl/extract/extract_inventory.py
Phase 9: Extract inventory snapshot data from Excel/CSV source files.

Functions:
    - extract_inventory_from_excel() : Read inventory Excel, tag TenantID.
    - get_last_watermark()           : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Inventory_Excel'
    - Source name format: '{TenantID}_Inventory_Excel'
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .xlsx (Excel 2007+)
    - .xls  (Excel 97-2003)
    - .csv  (Comma-separated values)

Expected columns in source file (QuanLyKho.xlsx):
    - MaCH            : Store code (required)
    - MaSP            : Product code (required)
    - NgayChot        : Snapshot date DD/MM/YYYY (required)
    - TonDauNgay      : Opening quantity (required)
    - NhapTrongNgay   : Received quantity (required)
    - BanTrongNgay    : Sold quantity (required)
    - TraLaiNhap      : Return to supplier (optional)
    - DieuChinh       : Adjustment quantity (optional)
    - DonGiaVon       : Unit cost price (required)
    - MucTonToiThieu  : Reorder level (optional)
    - LoaiChuyen      : Transaction type (optional)

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
SOURCE_TYPE = "Inventory_Excel"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_inventory_from_excel(
    file_path: str,
    tenant_id: str,
    watermark: Optional[datetime] = None,
    sheet_name: str = "QuanLyKho",
) -> pd.DataFrame:
    """
    Read inventory data from Excel/CSV file, filter by watermark, tag TenantID.

    Args:
        file_path:   Full path to the Excel/CSV file.
                    Supported: .xlsx, .xls, .csv
        tenant_id:  Tenant identifier (e.g. 'STORE_HN').
        watermark:  Extract only rows AFTER this datetime.
                    If None, fetches from DB via get_last_watermark(). (optional)
        sheet_name: Name of the Excel sheet to read. (default: 'QuanLyKho')

    Returns:
        DataFrame with columns matching STG_InventoryRaw table.
        Each row is tagged with TenantID.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting inventory extraction from: %s", tenant_id, file_path)
    logger.info("[%s] Sheet name: %s | Watermark: %s", tenant_id, sheet_name, watermark)

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[%s] File not found: %s", tenant_id, file_path)
        raise FileNotFoundError(f"Inventory file not found: {file_path}")

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
        df = df[df["NgayChot"] > watermark_ts]
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
            "[%s] Inventory extraction completed. "
            "Rows to load: %d | Date range: %s to %s",
            tenant_id,
            len(df),
            df["NgayChot"].min().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
            df["NgayChot"].max().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
        )
        logger.info("[%s] Inventory extraction: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[%s] Inventory extraction failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Inventory extraction failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read Excel file with fallback sheet names.
    """
    logger.debug("Reading Excel file: %s (sheet: %s)", file_path, sheet_name)

    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype={
                "MaCH": str,
                "MaSP": str,
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
        "QuanLyKho",
        "Sheet1",
        "Inventory",
        "TonKho",
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
                    "MaSP": str,
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
    Read inventory data from CSV file.
    """
    logger.debug("Reading CSV file: %s", file_path)

    try:
        df = pd.read_csv(
            file_path,
            dtype={
                "MaCH": str,
                "MaSP": str,
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
                "MaSP": str,
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
    Normalize column names to match STG_InventoryRaw schema.

    Known column variations:
        - "Mã Cửa Hàng" / "MACH" / "StoreCode" -> "MaCH"
        - "Mã SP" / "MASP" / "ProductCode" -> "MaSP"
        - "Ngày Chốt" / "NGAYCHOT" / "Date" / "SnapshotDate" -> "NgayChot"
        - "Tồn Đầu Ngày" / "TonDauNgay" / "OpeningQty" -> "TonDauNgay"
        - "Nhập Trong Ngày" / "NhapTrongNgay" / "ReceivedQty" -> "NhapTrongNgay"
        - "Bán Trong Ngày" / "BanTrongNgay" / "SoldQty" -> "BanTrongNgay"
        - "Trả Lại Nhập" / "TraLaiNhap" / "ReturnToSupplier" -> "TraLaiNhap"
        - "Điều Chỉnh" / "DieuChinh" / "Adjustment" -> "DieuChinh"
        - "Đơn Giá Vốn" / "DonGiaVon" / "UnitCost" -> "DonGiaVon"
        - "Mức Tồn Tối Thiểu" / "MucTonToiThieu" / "ReorderLevel" -> "MucTonToiThieu"
        - "Loại Chuyển" / "LoaiChuyen" / "TransType" -> "LoaiChuyen"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Store
        "Mã Cửa Hàng": "MaCH",
        "MACH": "MaCH",
        "Store Code": "MaCH",
        "StoreCode": "MaCH",
        "CuaHang": "MaCH",
        # Product
        "Mã SP": "MaSP",
        "MASP": "MaSP",
        "Product Code": "MaSP",
        "ProductCode": "MaSP",
        "Product": "MaSP",
        # Date
        "Ngày Chốt": "NgayChot",
        "NGAYCHOT": "NgayChot",
        "Snapshot Date": "NgayChot",
        "SnapshotDate": "NgayChot",
        "Date": "NgayChot",
        "Ngay": "NgayChot",
        "NgayChot": "NgayChot",
        # Opening Qty
        "Tồn Đầu Ngày": "TonDauNgay",
        "TONDAUNGAY": "TonDauNgay",
        "OpeningQty": "TonDauNgay",
        "Opening Qty": "TonDauNgay",
        "TonDau": "TonDauNgay",
        # Received Qty
        "Nhập Trong Ngày": "NhapTrongNgay",
        "NHAPTRONGNGAY": "NhapTrongNgay",
        "ReceivedQty": "NhapTrongNgay",
        "Received Qty": "NhapTrongNgay",
        "Nhap": "NhapTrongNgay",
        "SoLuongNhap": "NhapTrongNgay",
        # Sold Qty
        "Bán Trong Ngày": "BanTrongNgay",
        "BANTRONGNGAY": "BanTrongNgay",
        "SoldQty": "BanTrongNgay",
        "Sold Qty": "BanTrongNgay",
        "Ban": "BanTrongNgay",
        "SoLuongBan": "BanTrongNgay",
        # Return to Supplier
        "Trả Lại Nhập": "TraLaiNhap",
        "TRALAINHAP": "TraLaiNhap",
        "ReturnToSupplier": "TraLaiNhap",
        "Return To Supplier": "TraLaiNhap",
        "TraLai": "TraLaiNhap",
        # Adjustment
        "Điều Chỉnh": "DieuChinh",
        "DIEUCHINH": "DieuChinh",
        "Adjustment": "DieuChinh",
        "SoLuongDieuChinh": "DieuChinh",
        # Unit Cost
        "Đơn Giá Vốn": "DonGiaVon",
        "DONGIAVON": "DonGiaVon",
        "UnitCost": "DonGiaVon",
        "Unit Cost": "DonGiaVon",
        "GiaVon": "DonGiaVon",
        "CostPrice": "DonGiaVon",
        # Reorder Level
        "Mức Tồn Tối Thiểu": "MucTonToiThieu",
        "MUCTONTOITHIEU": "MucTonToiThieu",
        "ReorderLevel": "MucTonToiThieu",
        "Reorder Level": "MucTonToiThieu",
        "MucTonToiThieu": "MucTonToiThieu",
        # Transaction Type
        "Loại Chuyển": "LoaiChuyen",
        "LOAICHUYEN": "LoaiChuyen",
        "TransType": "LoaiChuyen",
        "TransactionType": "LoaiChuyen",
        "LoaiChuyen": "LoaiChuyen",
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
# Helper: Parse dates
# ---------------------------------------------------------------------------

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'NgayChot' column to datetime.
    Supports formats: DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, YYYYMMDD.
    """
    if "NgayChot" not in df.columns:
        logger.warning(
            "Column 'NgayChot' not found. Available: %s",
            list(df.columns)
        )
        return df

    date_formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y%m%d",
        "%d/%m/%Y %H:%M:%S",
    ]

    parsed = False
    for fmt in date_formats:
        try:
            if df["NgayChot"].dtype == "object" or str(df["NgayChot"].dtype).startswith("str"):
                df["NgayChot"] = pd.to_datetime(
                    df["NgayChot"], format=fmt, errors="raise"
                )
            else:
                df["NgayChot"] = pd.to_datetime(df["NgayChot"], errors="coerce")
            parsed = True
            logger.debug("Parsed dates with format: %s", fmt)
            break
        except (ValueError, TypeError):
            continue

    if not parsed:
        df["NgayChot"] = pd.to_datetime(
            df["NgayChot"], dayfirst=True, errors="coerce"
        )
        logger.info("Parsed dates using pandas auto-detection (dayfirst=True).")

    invalid_count = df["NgayChot"].isna().sum()
    if invalid_count > 0:
        logger.warning(
            "Found %d rows with invalid/unparseable dates in NgayChot column.",
            invalid_count
        )

    return df


# ---------------------------------------------------------------------------
# Helper: Validate required columns
# ---------------------------------------------------------------------------

def _validate_required_columns(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Ensure required columns exist. Fill missing optional columns with defaults.

    Required: MaCH, MaSP, NgayChot, TonDauNgay, NhapTrongNgay, BanTrongNgay, DonGiaVon
    Optional: TraLaiNhap, DieuChinh, MucTonToiThieu, LoaiChuyen
    """
    required_cols = ["MaCH", "MaSP", "NgayChot", "TonDauNgay", "NhapTrongNgay", "BanTrongNgay", "DonGiaVon"]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[%s] Missing required columns: %s. Available: %s",
            tenant_id, missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in inventory file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "TraLaiNhap": 0,
        "DieuChinh": 0,
        "MucTonToiThieu": 0,
        "LoaiChuyen": "Daily Count",
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[%s] Added default column '%s' = %s",
                tenant_id, col, default
            )

    # Strip whitespace from string columns
    string_cols = ["MaCH", "MaSP"]
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
    numeric_cols = [
        "TonDauNgay", "NhapTrongNgay", "BanTrongNgay",
        "TraLaiNhap", "DieuChinh", "DonGiaVon", "MucTonToiThieu",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Convert quantities to int
    int_cols = ["TonDauNgay", "NhapTrongNgay", "BanTrongNgay", "TraLaiNhap", "DieuChinh", "MucTonToiThieu"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype(int)

    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: str, conn=None) -> datetime:
    """
    Get last successful watermark for inventory extraction.

    Args:
        tenant_id: Tenant identifier.
        conn:      Database connection. (optional)

    Returns:
        datetime of last successful extraction.
    """
    if conn is None:
        logger.warning(
            "get_last_watermark called without DB connection. "
            "Returning default 2020-01-01."
        )
        return datetime(2020, 1, 1, 0, 0, 0)

    source_name = f"{tenant_id}_{SOURCE_TYPE}"
    return get_last_watermark(conn, tenant_id, SOURCE_TYPE)