"""
etl/transform/transform_purchase.py
Phase 10: Transform STG_PurchaseRaw data into purchase fact data.

This module transforms raw purchase staging data from STG_PurchaseRaw into
clean, validated data ready for fact table loading.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE)
    2. Parse and validate date columns (NgayNhap, NgayGRN, NgayNhanHang, HanThanhToan)
    3. Convert numeric columns (SoLuong, DonGiaNhap, ChietKhau, ThueGTGT)
    4. Calculate derived financial metrics:
         - TotalCost = SoLuong * DonGiaNhap
         - DiscountAmount = ChietKhau (validated >= 0)
         - NetCost = TotalCost - DiscountAmount
         - VATAmount = ThueGTGT (validated >= 0)
         - GrossCost = NetCost + VATAmount
    5. Normalize payment and quality status
    6. Deduplicate on (SoPhieuNhap, MaSP, SoDong)
    7. Filter invalid rows (null required fields, negative qty/price)

Expected input columns (from extract module):
    - TenantID, MaCH, MaNCC, MaSP, SoPhieuNhap, SoDong, NgayNhap
    - SoLuong, DonGiaNhap, ChietKhau, ThueGTGT
    - SoGRN, NgayGRN, SoLuongThucNhan, NgayNhanHang
    - TinhTrangChatLuong, TinhTrangThanhToan, PhuongThucTT, HanThanhToan, GhiChu
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for staging or SP call):
    - All normalized/typed columns above
    - DateKey (INT yyyyMMdd for NgayNhap)
    - TotalCost, DiscountAmount, NetCost, VATAmount, GrossCost

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from .base_transform import (
    clean_string,
    parse_date,
    safe_float,
    safe_int,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------

def transform_purchases(
    df: pd.DataFrame,
    tenant_id: str,
    drop_duplicates: bool = True,
    filter_invalid: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_PurchaseRaw data into clean, business-ready purchase facts.

    This function performs full ETL transformation on raw purchase data:
        - String normalization (trim, uppercase)
        - Date parsing (NgayNhap -> DateKey, NgayGRN, NgayNhanHang, HanThanhToan)
        - Numeric validation (SoLuong, DonGiaNhap, ChietKhau, ThueGTGT)
        - Financial calculations (TotalCost, NetCost, VATAmount, GrossCost)
        - Status normalization (TinhTrangChatLuong, TinhTrangThanhToan, PhuongThucTT)
        - Deduplication on (SoPhieuNhap, MaSP, SoDong)
        - Invalid row filtering

    Args:
        df:              Raw STG_PurchaseRaw DataFrame from extract module.
        tenant_id:       Tenant identifier (e.g. 'STORE_HN').
        drop_duplicates: Drop duplicate (SoPhieuNhap, MaSP, SoDong) rows. (default True)
        filter_invalid:  Remove rows with null required fields or negative values. (default True)

    Returns:
        DataFrame with transformed purchase data ready for staging load.
        Columns include computed TotalCost, NetCost, VATAmount, GrossCost.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting purchase transformation", tenant_id)
    logger.info("[%s] Input rows: %d", tenant_id, len(df))

    if df is None or df.empty:
        logger.warning("[%s] Input DataFrame is empty. Returning empty.", tenant_id)
        return pd.DataFrame()

    original_count = len(df)

    try:
        df = df.copy()
        df = df.reset_index(drop=True)

        if "TenantID" not in df.columns:
            df["TenantID"] = tenant_id
            logger.debug("[%s] TenantID column injected from parameter.", tenant_id)

        df = _normalize_strings(df, tenant_id)

        df = _parse_and_validate_dates(df, tenant_id)

        df = _convert_and_validate_numerics(df, tenant_id)

        df = _filter_invalid_rows(df, tenant_id, filter_invalid)

        if drop_duplicates:
            df = _deduplicate_purchases(df, tenant_id)

        df = _calculate_financial_metrics(df, tenant_id)

        df = _normalize_status_columns(df, tenant_id)

        df = _validate_foreign_keys(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[%s] Purchase transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            tenant_id, rows_out, rows_filtered
        )
        logger.info("[%s] Purchase transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[%s] Purchase transformation failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Purchase transformation failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize all string columns in the purchase DataFrame.
    """
    string_columns = [
        "MaCH", "MaNCC", "MaSP", "SoPhieuNhap", "SoGRN",
        "TinhTrangChatLuong", "TinhTrangThanhToan",
        "PhuongThucTT", "GhiChu", "STG_SourceFile",
    ]

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    if "PhuongThucTT" in df.columns:
        df["PhuongThucTT"] = df["PhuongThucTT"].apply(_normalize_payment_method)

    logger.debug("[%s] String normalization complete.", tenant_id)

    return df


def _normalize_payment_method(value: Any) -> Optional[str]:
    """
    Normalize payment method to standard Vietnamese terms.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Tiền mặt"

    if v in ("TIEN MAT", "CASH"):
        return "Tiền mặt"
    if v in ("CHUYEN KHOAN", "TRANSFER", "BANK", "BANKING"):
        return "Chuyển khoản"
    if v in ("THE", "CARD", "CREDIT CARD", "DEBIT CARD"):
        return "Thẻ"
    if v in ("COD", "TRA SAU"):
        return "COD"
    if v in ("MOMO", "ZALOPAY", "VNPAY"):
        return v

    return v


# ---------------------------------------------------------------------------
# Step 2: Parse and validate dates
# ---------------------------------------------------------------------------

def _parse_and_validate_dates(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Parse date columns to datetime, compute DateKey from NgayNhap.

    Date columns:
        - NgayNhap      -> DateKey (required)
        - NgayGRN       -> GRN date (optional)
        - NgayNhanHang  -> Actual received date (optional)
        - HanThanhToan  -> Payment due date (optional)
    """
    date_column_map = {
        "NgayNhap": True,
        "NgayGRN": False,
        "NgayNhanHang": False,
        "HanThanhToan": False,
    }

    for col, required in date_column_map.items():
        if col not in df.columns:
            if required:
                logger.error(
                    "[%s] Required date column '%s' not found.",
                    tenant_id, col
                )
                raise ValueError(f"Required date column '{col}' is missing.")
            continue

        df[col] = df[col].apply(
            lambda v: parse_date(v, dayfirst=True, default=None)
        )

        invalid_count = df[col].isna().sum()
        if invalid_count > 0 and required:
            logger.warning(
                "[%s] %d rows with invalid %s (required field).",
                tenant_id, invalid_count, col
            )
        elif invalid_count > 0:
            logger.debug(
                "[%s] %d rows with invalid %s (optional).",
                tenant_id, invalid_count, col
            )

    if "NgayNhap" in df.columns:
        df["DateKey"] = df["NgayNhap"].apply(_compute_date_key)
    else:
        df["DateKey"] = None

    logger.debug("[%s] Date parsing complete.", tenant_id)

    return df


def _compute_date_key(dt: Any) -> Optional[int]:
    """
    Convert datetime to DateKey integer (yyyymmdd format).
    """
    if dt is None:
        return None

    if isinstance(dt, datetime):
        return int(dt.strftime("%Y%m%d"))

    if isinstance(dt, pd.Timestamp):
        return int(dt.strftime("%Y%m%d"))

    return None


# ---------------------------------------------------------------------------
# Step 3: Convert and validate numeric columns
# ---------------------------------------------------------------------------

def _convert_and_validate_numerics(
    df: pd.DataFrame,
    tenant_id: str,
) -> pd.DataFrame:
    """
    Convert and validate all numeric columns in purchase data.

    Rules:
        - SoLuong: must be > 0, integer
        - DonGiaNhap: must be >= 0, float
        - ChietKhau: must be >= 0, float
        - ThueGTGT: must be >= 0, float
        - SoLuongThucNhan: integer, can be null
        - SoDong: positive integer, default 1
    """
    numeric_float_cols = ["DonGiaNhap", "ChietKhau", "ThueGTGT"]
    for col in numeric_float_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    if "DonGiaNhap" in df.columns:
        df["DonGiaNhap"] = df["DonGiaNhap"].apply(
            lambda v: safe_float(v, min_val=0)
        )

    if "ChietKhau" in df.columns:
        df["ChietKhau"] = df["ChietKhau"].apply(
            lambda v: safe_float(v, min_val=0)
        )

    if "ThueGTGT" in df.columns:
        df["ThueGTGT"] = df["ThueGTGT"].apply(
            lambda v: safe_float(v, min_val=0)
        )

    int_cols = ["SoLuong", "SoLuongThucNhan", "SoDong"]
    for col in int_cols:
        if col in df.columns:
            if col == "SoLuong":
                df[col] = df[col].apply(
                    lambda v: safe_int(v, default=1, min_val=1)
                )
            elif col == "SoDong":
                df[col] = df[col].apply(
                    lambda v: safe_int(v, default=1, min_val=1)
                )
            else:
                df[col] = df[col].apply(safe_int)

    logger.debug("[%s] Numeric conversion complete.", tenant_id)

    return df


# ---------------------------------------------------------------------------
# Step 4: Filter invalid rows
# ---------------------------------------------------------------------------

def _filter_invalid_rows(
    df: pd.DataFrame,
    tenant_id: str,
    filter_invalid: bool,
) -> pd.DataFrame:
    """
    Filter out invalid business records from purchase data.

    Rules:
        1. SoPhieuNhap is null/empty -> EXCLUDE
        2. MaSP is null/empty -> EXCLUDE
        3. MaCH is null/empty -> EXCLUDE
        4. NgayNhap is null -> EXCLUDE
        5. SoLuong <= 0 -> EXCLUDE
        6. DonGiaNhap < 0 -> EXCLUDE
    """
    if not filter_invalid:
        return df

    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)

    required_fk = [
        ("SoPhieuNhap", "Số phiếu nhập"),
        ("MaSP", "Mã sản phẩm"),
        ("MaCH", "Mã cửa hàng"),
    ]

    for col, label in required_fk:
        if col in df.columns:
            mask &= df[col].notna() & (df[col] != "")
            null_count = initial_count - mask.sum()
            if null_count > 0:
                logger.warning(
                    "[%s] Excluding %d rows with null/empty %s.",
                    tenant_id, null_count, label
                )

    if "NgayNhap" in df.columns:
        mask &= df["NgayNhap"].notna()
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null NgayNhap.",
                tenant_id, null_count
            )

    if "SoLuong" in df.columns:
        qty_invalid = (df["SoLuong"] <= 0).sum()
        mask &= (df["SoLuong"] > 0)
        if qty_invalid > 0:
            logger.warning(
                "[%s] Excluding %d rows with SoLuong <= 0.",
                tenant_id, qty_invalid
            )

    if "DonGiaNhap" in df.columns:
        price_invalid = (df["DonGiaNhap"] < 0).sum()
        mask &= (df["DonGiaNhap"] >= 0)
        if price_invalid > 0:
            logger.warning(
                "[%s] Excluding %d rows with DonGiaNhap < 0.",
                tenant_id, price_invalid
            )

    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)

    logger.info(
        "[%s] Filtered %d invalid rows. Remaining: %d",
        tenant_id, rows_removed, len(df)
    )

    return df


# ---------------------------------------------------------------------------
# Step 5: Deduplicate on (SoPhieuNhap, MaSP, SoDong)
# ---------------------------------------------------------------------------

def _deduplicate_purchases(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Remove duplicate rows based on (SoPhieuNhap, MaSP, SoDong) composite key.

    Business logic:
        - Grain: 1 row = 1 line item in 1 purchase order
        - Composite key: SoPhieuNhap + MaSP + SoDong
        - If duplicates found, keep LAST occurrence
        - In most ERP systems, SoDong (line number) should prevent
          duplicates, but we deduplicate as a safety net
    """
    key_cols = ["SoPhieuNhap", "MaSP"]
    if "SoDong" in df.columns:
        key_cols.append("SoDong")

    missing_cols = [c for c in key_cols if c not in df.columns]
    if missing_cols:
        logger.warning(
            "[%s] Cannot deduplicate — missing columns: %s. Skipping.",
            tenant_id, missing_cols
        )
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=key_cols, keep="last")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[%s] Removed %d duplicate purchase lines.",
            tenant_id, rows_dedup
        )

    return df


# ---------------------------------------------------------------------------
# Step 6: Calculate financial metrics
# ---------------------------------------------------------------------------

def _calculate_financial_metrics(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Calculate derived financial columns for fact purchase.

    Computed columns:
        - TotalCost     = SoLuong * DonGiaNhap
        - DiscountAmount = ChietKhau (clamped >= 0)
        - NetCost       = TotalCost - DiscountAmount
        - VATAmount     = ThueGTGT (clamped >= 0)
        - GrossCost     = NetCost + VATAmount

    Note:
        - ChietKhau is treated as a flat discount amount per line
        - ThueGTGT is the VAT amount (not rate) per line
        - All monetary values in VND
    """
    df["SoLuong"] = df.get("SoLuong", pd.Series([1] * len(df))).fillna(1)
    df["DonGiaNhap"] = df.get("DonGiaNhap", pd.Series([0.0] * len(df))).fillna(0.0)

    df["TotalCost"] = df["SoLuong"] * df["DonGiaNhap"]
    df["TotalCost"] = df["TotalCost"].apply(lambda v: max(0, v))

    df["ChietKhau"] = df.get("ChietKhau", pd.Series([0.0] * len(df))).fillna(0.0)
    df["DiscountAmount"] = df["ChietKhau"].apply(
        lambda v: max(0, safe_float(v, min_val=0))
    )

    df["NetCost"] = df["TotalCost"] - df["DiscountAmount"]
    df["NetCost"] = df["NetCost"].apply(lambda v: max(0, v))

    df["ThueGTGT"] = df.get("ThueGTGT", pd.Series([0.0] * len(df))).fillna(0.0)
    df["VATAmount"] = df["ThueGTGT"].apply(
        lambda v: max(0, safe_float(v, min_val=0))
    )

    df["GrossCost"] = df["NetCost"] + df["VATAmount"]
    df["GrossCost"] = df["GrossCost"].apply(lambda v: max(0, v))

    if "SoLuongThucNhan" in df.columns:
        df["SoLuongThucNhan"] = df["SoLuongThucNhan"].fillna(df["SoLuong"])
        df["SoLuongThucNhan"] = df["SoLuongThucNhan"].apply(safe_int)

    total_cost = df["TotalCost"].sum()
    total_discount = df["DiscountAmount"].sum()
    total_net = df["NetCost"].sum()
    total_vat = df["VATAmount"].sum()
    total_gross = df["GrossCost"].sum()

    logger.info(
        "[%s] Financial metrics calculated. "
        "TotalCost: %,.0f | Discount: %,.0f | NetCost: %,.0f | VAT: %,.0f | GrossCost: %,.0f",
        tenant_id, total_cost, total_discount, total_net, total_vat, total_gross
    )

    return df


# ---------------------------------------------------------------------------
# Step 7: Normalize status columns
# ---------------------------------------------------------------------------

def _normalize_status_columns(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize quality status and payment status columns.

    Quality Status (TinhTrangChatLuong):
        - "PASSED", "OK", "DAT" -> "Passed"
        - "FAILED", "LOI", "KHONG DAT" -> "Failed"
        - "PENDING", "CHO" -> "Pending"
        - "CONDITIONAL", "CO DIEU KIEN" -> "Conditional"

    Payment Status (TinhTrangThanhToan):
        - "PAID", "DA THANH TOAN", "DA TT" -> "Paid"
        - "PENDING", "CHO", "CHUA TT" -> "Pending"
        - "PARTIAL", "MOT PHAN" -> "Partial"
        - "OVERDUE", "QUA HAN" -> "Overdue"
        - "CANCELLED", "DA HUY" -> "Cancelled"
    """
    if "TinhTrangChatLuong" in df.columns:
        df["TinhTrangChatLuong"] = df["TinhTrangChatLuong"].apply(
            _normalize_quality_status
        )

    if "TinhTrangThanhToan" in df.columns:
        df["TinhTrangThanhToan"] = df["TinhTrangThanhToan"].apply(
            _normalize_payment_status
        )

    logger.debug("[%s] Status columns normalized.", tenant_id)

    return df


def _normalize_quality_status(value: Any) -> Optional[str]:
    """
    Normalize quality status to standard values.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Pending"

    if v in ("PASSED", "OK", "DAT", "GOOD", "QUALIFIED", "DAT"):
        return "Passed"
    if v in ("FAILED", "LOI", "KHONG DAT", "NOT OK", "REJECTED"):
        return "Failed"
    if v in ("PENDING", "CHO", "DANG CHO", "INSPECTING"):
        return "Pending"
    if v in ("CONDITIONAL", "CO DIEU KIEN", "CONDITIONALLY PASSED"):
        return "Conditional"

    return v


def _normalize_payment_status(value: Any) -> Optional[str]:
    """
    Normalize payment status to standard values.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Pending"

    if v in ("PAID", "DA THANH TOAN", "DA TT", "COMPLETED", "COMPLETE"):
        return "Paid"
    if v in ("PENDING", "CHO", "CHUA TT", "UNPAID"):
        return "Pending"
    if v in ("PARTIAL", "MOT PHAN", "PARTIALLY PAID"):
        return "Partial"
    if v in ("OVERDUE", "QUA HAN", "LATE"):
        return "Overdue"
    if v in ("CANCELLED", "DA HUY", "CANCELED"):
        return "Cancelled"
    if v in ("REFUNDED", "DA HOAN", "REFUND"):
        return "Refunded"

    return v


# ---------------------------------------------------------------------------
# Step 8: Validate foreign keys
# ---------------------------------------------------------------------------

def _validate_foreign_keys(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Validate that required foreign key columns are present and non-null.

    FK columns that MUST be present:
        - TenantID (inherited from extract)
        - DateKey (computed from NgayNhap)
        - MaSP (for DimProduct lookup)
        - MaCH (for DimStore lookup)
        - MaNCC (for DimSupplier lookup)

    FK columns that MAY be null:
        - SoPhieuNhap (purchase order number — required for deduplication)
    """
    required_fk = ["TenantID", "DateKey", "MaSP", "MaCH"]
    for col in required_fk:
        if col not in df.columns:
            logger.error(
                "[%s] Required FK column '%s' is missing from transformed data.",
                tenant_id, col
            )
            raise ValueError(
                f"Required FK column '{col}' is missing after transformation."
            )

    if "MaNCC" in df.columns:
        null_ncc = df["MaNCC"].isna().sum()
        if null_ncc > 0:
            logger.warning(
                "[%s] %d rows have null MaNCC (optional FK).",
                tenant_id, null_ncc
            )

    logger.debug(
        "[%s] FK validation passed. All required keys present.",
        tenant_id
    )

    return df


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def get_purchase_summary(df: pd.DataFrame, tenant_id: str) -> dict[str, Any]:
    """
    Generate a summary dict of the purchase transformation results.
    """
    summary: dict[str, Any] = {
        "tenant_id": tenant_id,
        "total_rows": len(df),
        "date_range": {"min": None, "max": None},
        "financials": {
            "total_cost": 0.0,
            "total_discount": 0.0,
            "total_net_cost": 0.0,
            "total_vat": 0.0,
            "total_gross_cost": 0.0,
        },
        "flags": {
            "quality_passed": 0,
            "quality_failed": 0,
            "quality_pending": 0,
            "payment_paid": 0,
            "payment_pending": 0,
            "payment_overdue": 0,
        },
        "unique_suppliers": 0,
        "unique_products": 0,
        "unique_stores": 0,
        "unique_orders": 0,
    }

    if df.empty:
        return summary

    if "NgayNhap" in df.columns:
        valid_dates = df["NgayNhap"].dropna()
        if len(valid_dates) > 0:
            summary["date_range"]["min"] = valid_dates.min().strftime("%Y-%m-%d")
            summary["date_range"]["max"] = valid_dates.max().strftime("%Y-%m-%d")

    for col, key in [
        ("TotalCost", "total_cost"),
        ("DiscountAmount", "total_discount"),
        ("NetCost", "total_net_cost"),
        ("VATAmount", "total_vat"),
        ("GrossCost", "total_gross_cost"),
    ]:
        if col in df.columns:
            summary["financials"][key] = float(df[col].sum())

    if "TinhTrangChatLuong" in df.columns:
        for status, key in [
            ("Passed", "quality_passed"),
            ("Failed", "quality_failed"),
            ("Pending", "quality_pending"),
        ]:
            summary["flags"][key] = int((df["TinhTrangChatLuong"] == status).sum())

    if "TinhTrangThanhToan" in df.columns:
        for status, key in [
            ("Paid", "payment_paid"),
            ("Pending", "payment_pending"),
            ("Overdue", "payment_overdue"),
        ]:
            summary["flags"][key] = int((df["TinhTrangThanhToan"] == status).sum())

    for col, key in [
        ("MaNCC", "unique_suppliers"),
        ("MaSP", "unique_products"),
        ("MaCH", "unique_stores"),
        ("SoPhieuNhap", "unique_orders"),
    ]:
        if col in df.columns:
            summary[key] = int(df[col].nunique())

    return summary
