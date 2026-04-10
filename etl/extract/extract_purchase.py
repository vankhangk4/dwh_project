"""
etl/extract/extract_purchase.py
Phase 9: Extract purchase order data from Excel/CSV source files.

Functions:
    - extract_purchases_from_excel() : Read purchase Excel, filter by watermark, tag TenantID.
    - get_last_watermark()           : Get last successful extraction timestamp.

Watermark:
    - Source type: 'Purchase_Excel'
    - Source name format: '{TenantID}_Purchase_Excel'
    - Default: '2020-01-01' if no watermark exists.

Supported file formats:
    - .xlsx (Excel 2007+)
    - .xls  (Excel 97-2003)
    - .csv  (Comma-separated values)

Expected columns in source file (PhieuNhapKho.xlsx):
    - MaCH            : Store code (required)
    - MaNCC           : Supplier code (required)
    - MaSP            : Product code (required)
    - SoPhieuNhap      : Purchase order number (required)
    - SoDong          : Line number (required)
    - NgayNhap         : Purchase date DD/MM/YYYY (required)
    - SoLuong         : Quantity ordered (required)
    - DonGiaNhap       : Unit cost (required)
    - ChietKhau       : Discount amount (optional)
    - ThueGTGT        : VAT amount (optional)
    - SoGRN           : Goods Receipt Number (optional)
    - NgayGRN         : GRN date (optional)
    - SoLuongThucNhan  : Actual received quantity (optional)
    - NgayNhanHang     : Received date (optional)
    - TinhTrangChatLuong: Quality status (optional)
    - TinhTrangThanhToan: Payment status (optional)
    - PhuongThucTT     : Payment method (optional)
    - HanThanhToan     : Payment due date (optional)
    - GhiChu          : Notes (optional)

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
SOURCE_TYPE = "Purchase_Excel"


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_purchases_from_excel(
    file_path: str,
    tenant_id: str,
    watermark: Optional[datetime] = None,
    sheet_name: str = "PhieuNhapKho",
) -> pd.DataFrame:
    """
    Read purchase data from Excel/CSV file, filter by watermark, tag TenantID.

    Args:
        file_path:   Full path to the Excel/CSV file.
                    Supported: .xlsx, .xls, .csv
        tenant_id:  Tenant identifier (e.g. 'STORE_HN').
        watermark:  Extract only rows AFTER this datetime.
                    If None, fetches from DB via get_last_watermark(). (optional)
        sheet_name: Name of the Excel sheet to read. (default: 'PhieuNhapKho')

    Returns:
        DataFrame with columns matching STG_PurchaseRaw table.
        Each row is tagged with TenantID.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file format is not supported.
        RuntimeError: If extraction fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting purchase extraction from: %s", tenant_id, file_path)
    logger.info("[%s] Sheet name: %s | Watermark: %s", tenant_id, sheet_name, watermark)

    # Validate file exists
    if not os.path.exists(file_path):
        logger.error("[%s] File not found: %s", tenant_id, file_path)
        raise FileNotFoundError(f"Purchase file not found: {file_path}")

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
            logger.warning("[%s] No data found in purchase file: %s", tenant_id, file_path)
            return df

        # Step 2: Normalize column names
        df = _normalize_columns(df)
        logger.info("[%s] Columns after normalization: %s", tenant_id, list(df.columns))

        # Step 3: Parse dates
        df = _parse_dates(df)

        # Step 4: Filter by watermark
        rows_before = len(df)
        df = df[df["NgayNhap"] > watermark_ts]
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
            "[%s] Purchase extraction completed. "
            "Rows to load: %d | Date range: %s to %s",
            tenant_id,
            len(df),
            df["NgayNhap"].min().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
            df["NgayNhap"].max().strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
        )
        logger.info("[%s] Purchase extraction: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except FileNotFoundError:
        raise
    except Exception as ex:
        logger.error(
            "[%s] Purchase extraction failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Purchase extraction failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Helper: Read Excel
# ---------------------------------------------------------------------------

def _read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read purchase data from Excel file with fallback sheet names.
    """
    logger.debug("Reading Excel file: %s (sheet: %s)", file_path, sheet_name)

    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype={
                "MaCH": str,
                "MaNCC": str,
                "MaSP": str,
                "SoPhieuNhap": str,
                "SoGRN": str,
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
        "PhieuNhapKho",
        "Purchase",
        "NhapKho",
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
                    "MaNCC": str,
                    "MaSP": str,
                    "SoPhieuNhap": str,
                    "SoGRN": str,
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
    Read purchase data from CSV file.
    """
    logger.debug("Reading CSV file: %s", file_path)

    try:
        df = pd.read_csv(
            file_path,
            dtype={
                "MaCH": str,
                "MaNCC": str,
                "MaSP": str,
                "SoPhieuNhap": str,
                "SoGRN": str,
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
                "MaNCC": str,
                "MaSP": str,
                "SoPhieuNhap": str,
                "SoGRN": str,
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
    Normalize column names to match STG_PurchaseRaw schema.

    Known column variations:
        - "Mã CH" / "MACH" / "StoreCode" -> "MaCH"
        - "Mã NCC" / "MaNCC" / "SupplierCode" -> "MaNCC"
        - "Mã SP" / "MASP" / "ProductCode" -> "MaSP"
        - "Số Phiếu Nhập" / "SoPhieuNhap" / "PONumber" -> "SoPhieuNhap"
        - "Số Dòng" / "SoDong" / "LineNumber" -> "SoDong"
        - "Ngày Nhập" / "NgayNhap" / "PurchaseDate" -> "NgayNhap"
        - "Số Lượng" / "SoLuong" / "Quantity" -> "SoLuong"
        - "Đơn Giá Nhập" / "DonGiaNhap" / "UnitCost" -> "DonGiaNhap"
        - "Chiết Khấu" / "ChietKhau" / "Discount" -> "ChietKhau"
        - "Thuế GTGT" / "ThueGTGT" / "VAT" -> "ThueGTGT"
        - "Số GRN" / "SoGRN" / "GRNNumber" -> "SoGRN"
        - "Ngày GRN" / "NgayGRN" / "GRNDate" -> "NgayGRN"
        - "Số Lượng Thực Nhận" / "SoLuongThucNhan" / "ReceivedQty" -> "SoLuongThucNhan"
        - "Ngày Nhận Hàng" / "NgayNhanHang" / "ReceivedDate" -> "NgayNhanHang"
        - "Tình Trạng Chất Lượng" / "TinhTrangChatLuong" / "QualityStatus" -> "TinhTrangChatLuong"
        - "Tình Trạng Thanh Toán" / "TinhTrangThanhToan" / "PaymentStatus" -> "TinhTrangThanhToan"
        - "Phương Thức TT" / "PhuongThucTT" / "PaymentMethod" -> "PhuongThucTT"
        - "Hạn Thanh Toán" / "HanThanhToan" / "PaymentDueDate" -> "HanThanhToan"
        - "Ghi Chú" / "GhiChu" / "Notes" -> "GhiChu"
    """
    df.columns = df.columns.str.strip()

    column_mapping: dict[str, str] = {
        # Store
        "Mã CH": "MaCH",
        "MACH": "MaCH",
        "Store Code": "MaCH",
        "StoreCode": "MaCH",
        "CuaHang": "MaCH",
        # Supplier
        "Mã NCC": "MaNCC",
        "MaNCC": "MaNCC",
        "Supplier Code": "MaNCC",
        "SupplierCode": "MaNCC",
        "NhaCungCap": "MaNCC",
        "NCC": "MaNCC",
        # Product
        "Mã SP": "MaSP",
        "MASP": "MaSP",
        "Product Code": "MaSP",
        "ProductCode": "MaSP",
        "Product": "MaSP",
        # Purchase Order Number
        "Số Phiếu Nhập": "SoPhieuNhap",
        "SOPHIEUNHAP": "SoPhieuNhap",
        "Purchase Order": "SoPhieuNhap",
        "PONumber": "SoPhieuNhap",
        "PurchaseOrder": "SoPhieuNhap",
        "PhieuNhap": "SoPhieuNhap",
        "SoPhieu": "SoPhieuNhap",
        # Line Number
        "Số Dòng": "SoDong",
        "SODONG": "SoDong",
        "Line Number": "SoDong",
        "LineNumber": "SoDong",
        "SoDong": "SoDong",
        # Purchase Date
        "Ngày Nhập": "NgayNhap",
        "NGAYNHAP": "NgayNhap",
        "Purchase Date": "NgayNhap",
        "PurchaseDate": "NgayNhap",
        "Date": "NgayNhap",
        "NgayNhap": "NgayNhap",
        # Quantity
        "Số Lượng": "SoLuong",
        "SOLUONG": "SoLuong",
        "Quantity": "SoLuong",
        "Qty": "SoLuong",
        # Unit Cost
        "Đơn Giá Nhập": "DonGiaNhap",
        "DONGIANHAP": "DonGiaNhap",
        "Unit Cost": "DonGiaNhap",
        "UnitCost": "DonGiaNhap",
        "Cost": "DonGiaNhap",
        "GiaNhap": "DonGiaNhap",
        # Discount
        "Chiết Khấu": "ChietKhau",
        "CHIETKHAU": "ChietKhau",
        "Discount": "ChietKhau",
        "ChietKhau": "ChietKhau",
        # VAT
        "Thuế GTGT": "ThueGTGT",
        "THUEGTGT": "ThueGTGT",
        "VAT": "ThueGTGT",
        "Tax": "ThueGTGT",
        # GRN Number
        "Số GRN": "SoGRN",
        "SOGRN": "SoGRN",
        "GRN Number": "SoGRN",
        "GRNNumber": "SoGRN",
        "GRN": "SoGRN",
        # GRN Date
        "Ngày GRN": "NgayGRN",
        "NGAYGRN": "NgayGRN",
        "GRN Date": "NgayGRN",
        "GRNDate": "NgayGRN",
        # Actual Received Qty
        "Số Lượng Thực Nhận": "SoLuongThucNhan",
        "SOLUONGTHUCNHAN": "SoLuongThucNhan",
        "Actual Received": "SoLuongThucNhan",
        "ReceivedQty": "SoLuongThucNhan",
        # Received Date
        "Ngày Nhận Hàng": "NgayNhanHang",
        "NGAYNHANHANG": "NgayNhanHang",
        "Received Date": "NgayNhanHang",
        "ReceivedDate": "NgayNhanHang",
        # Quality Status
        "Tình Trạng Chất Lượng": "TinhTrangChatLuong",
        "TINHTRANGCHATLUONG": "TinhTrangChatLuong",
        "Quality Status": "TinhTrangChatLuong",
        "QualityStatus": "TinhTrangChatLuong",
        "ChatLuong": "TinhTrangChatLuong",
        # Payment Status
        "Tình Trạng Thanh Toán": "TinhTrangThanhToan",
        "TINHTRANGTHANHTOAN": "TinhTrangThanhToan",
        "Payment Status": "TinhTrangThanhToan",
        "PaymentStatus": "TinhTrangThanhToan",
        "TinhTrangTT": "TinhTrangThanhToan",
        # Payment Method
        "Phương Thức TT": "PhuongThucTT",
        "PhuongThucTT": "PhuongThucTT",
        "Payment Method": "PhuongThucTT",
        "Payment": "PhuongThucTT",
        "PTTT": "PhuongThucTT",
        # Payment Due Date
        "Hạn Thanh Toán": "HanThanhToan",
        "HANTHANHTOAN": "HanThanhToan",
        "Payment Due Date": "HanThanhToan",
        "PaymentDueDate": "HanThanhToan",
        "DueDate": "HanThanhToan",
        # Notes
        "Ghi Chú": "GhiChu",
        "GHICHU": "GhiChu",
        "Notes": "GhiChu",
        "GhiChu": "GhiChu",
        "Comment": "GhiChu",
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
    Parse date columns to datetime.
    Supports formats: DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, YYYYMMDD.
    """
    date_columns = ["NgayNhap", "NgayGRN", "NgayNhanHang", "HanThanhToan"]
    date_formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y%m%d",
        "%d/%m/%Y %H:%M:%S",
    ]

    for date_col in date_columns:
        if date_col not in df.columns:
            continue

        parsed = False
        for fmt in date_formats:
            try:
                if df[date_col].dtype == "object" or str(df[date_col].dtype).startswith("str"):
                    df[date_col] = pd.to_datetime(
                        df[date_col], format=fmt, errors="raise"
                    )
                else:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                parsed = True
                logger.debug("Parsed %s with format: %s", date_col, fmt)
                break
            except (ValueError, TypeError):
                continue

        if not parsed:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
            logger.info("Parsed %s using pandas auto-detection (dayfirst=True).", date_col)

        invalid_count = df[date_col].isna().sum()
        if invalid_count > 0:
            logger.warning(
                "Found %d rows with invalid/unparseable dates in %s column.",
                invalid_count, date_col
            )

    return df


# ---------------------------------------------------------------------------
# Helper: Validate required columns
# ---------------------------------------------------------------------------

def _validate_required_columns(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Ensure required columns exist. Fill missing optional columns with defaults.

    Required: MaCH, MaNCC, MaSP, SoPhieuNhap, SoDong, NgayNhap, SoLuong, DonGiaNhap
    Optional: ChietKhau, ThueGTGT, SoGRN, NgayGRN, SoLuongThucNhan, NgayNhanHang,
              TinhTrangChatLuong, TinhTrangThanhToan, PhuongThucTT, HanThanhToan, GhiChu
    """
    required_cols = [
        "MaCH", "MaNCC", "MaSP", "SoPhieuNhap",
        "SoDong", "NgayNhap", "SoLuong", "DonGiaNhap"
    ]
    missing_required = [c for c in required_cols if c not in df.columns]

    if missing_required:
        logger.error(
            "[%s] Missing required columns: %s. Available: %s",
            tenant_id, missing_required, list(df.columns)
        )
        raise ValueError(
            f"Missing required columns in purchase file: {missing_required}"
        )

    optional_defaults: dict[str, Any] = {
        "ChietKhau": 0.0,
        "ThueGTGT": 0.0,
        "SoGRN": None,
        "NgayGRN": None,
        "SoLuongThucNhan": None,
        "NgayNhanHang": None,
        "TinhTrangChatLuong": "Passed",
        "TinhTrangThanhToan": "Pending",
        "PhuongThucTT": "Tiền mặt",
        "HanThanhToan": None,
        "GhiChu": None,
    }

    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default
            logger.debug(
                "[%s] Added default column '%s' = %s",
                tenant_id, col, default
            )

    # Strip whitespace from string columns
    string_cols = ["MaCH", "MaNCC", "MaSP", "SoPhieuNhap", "SoGRN"]
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
        "SoLuong", "DonGiaNhap", "ChietKhau", "ThueGTGT", "SoLuongThucNhan"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Convert quantities to int
    if "SoLuong" in df.columns:
        df["SoLuong"] = df["SoLuong"].astype(int)
    if "SoDong" in df.columns:
        df["SoDong"] = pd.to_numeric(df["SoDong"], errors="coerce").fillna(1).astype(int)
    if "SoLuongThucNhan" in df.columns:
        df["SoLuongThucNhan"] = df["SoLuongThucNhan"].astype(int)

    return df


# ---------------------------------------------------------------------------
# Watermark helper
# ---------------------------------------------------------------------------

def get_last_watermark(tenant_id: str, conn=None) -> datetime:
    """
    Get last successful watermark for purchase extraction.

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