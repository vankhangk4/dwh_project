"""
etl/transform/transform_sales.py
Phase 10: Transform STG_SalesRaw data into business-ready sales facts.

This module transforms raw sales staging data from STG_SalesRaw into
clean, validated data ready for dimension lookup and fact table loading.

Transformation steps:
    1. Normalize string columns (trim, UPPER, remove control chars)
    2. Parse and validate date columns (NgayBan)
    3. Convert numeric columns (SoLuong, DonGiaBan, ChietKhau)
    4. Filter invalid rows (qty <= 0, price < 0)
    5. Deduplicate on (MaHoaDon, MaSP) composite key
    6. Calculate derived financial metrics:
         - GrossSalesAmount = Quantity * UnitPrice
         - DiscountAmount   = ChietKhau (validated)
         - NetSalesAmount   = GrossSalesAmount - DiscountAmount
         - IsHoanTra flag   = normalized 0/1
    7. Add business logic flags

Expected input columns (from extract module):
    - TenantID, MaHoaDon, NgayBan, MaSP, MaCH, MaKH, MaNV
    - SoLuong, DonGiaBan, PhuongThucTT, ChietKhau, KenhBan
    - NhomBanHang, IsHoanTra, LyDoHoanTra, SoDong
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for staging or SP call):
    - All normalized/typed columns above
    - GrossSalesAmount, NetSalesAmount
    - DateKey (INT yyyyMMdd format for DimDate FK)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from .base_transform import (
    clean_string,
    handle_null,
    parse_date,
    safe_float,
    safe_int,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------

def transform_sales(
    df: pd.DataFrame,
    tenant_id: str,
    drop_duplicates: bool = True,
    filter_invalid: bool = True,
    remove_error_rows: bool = False,
) -> pd.DataFrame:
    """
    Transform raw STG_SalesRaw data into clean, business-ready sales facts.

    This function performs full ETL transformation on raw sales data:
        - String normalization (trim, uppercase, control char removal)
        - Date parsing (NgayBan)
        - Numeric validation (SoLuong, DonGiaBan, ChietKhau)
        - Invalid row filtering (qty <= 0, price < 0)
        - Deduplication on (MaHoaDon, MaSP)
        - Financial calculations (GrossSales, NetSales)
        - Business flag enrichment

    Args:
        df:              Raw STG_SalesRaw DataFrame from extract module.
        tenant_id:       Tenant identifier (e.g. 'STORE_HN').
        drop_duplicates: Drop duplicate (MaHoaDon, MaSP) pairs. (default True)
        filter_invalid:  Remove rows with qty <= 0 or price < 0. (default True)
        remove_error_rows: If True, error rows go to error log. (default False)

    Returns:
        DataFrame with transformed sales data ready for staging load.
        Columns include computed GrossSalesAmount and NetSalesAmount.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.

    Example:
        df_raw = extract_sales_from_excel(file_path, "STORE_HN")
        df_clean = transform_sales(df_raw, tenant_id="STORE_HN")
        # df_clean is ready for load_dataframe_to_staging(..., staging_table="STG_SalesRaw")
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting sales transformation", tenant_id)
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
            df = _deduplicate_sales(df, tenant_id)

        df = _calculate_financial_metrics(df, tenant_id)

        df = _enrich_business_flags(df, tenant_id)

        df = _validate_foreign_keys(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[%s] Sales transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            tenant_id, rows_out, rows_filtered
        )
        logger.info("[%s] Sales transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[%s] Sales transformation failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Sales transformation failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize all string columns in the sales DataFrame.

    Rules:
        - Strip whitespace
        - Convert to UPPERCASE
        - Replace null placeholders (NAN, NONE, NULL, empty) with None
        - Remove control characters
        - Normalize specific domain values (payment method, sales channel)
    """
    string_columns = [
        "MaHoaDon", "MaSP", "MaKH", "MaNV", "MaCH",
        "PhuongThucTT", "KenhBan", "NhomBanHang",
        "LyDoHoanTra", "STG_SourceFile",
    ]

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    if "PhuongThucTT" in df.columns:
        df["PhuongThucTT"] = df["PhuongThucTT"].apply(_normalize_payment_method)

    if "KenhBan" in df.columns:
        df["KenhBan"] = df["KenhBan"].apply(_normalize_sales_channel)

    if "NhomBanHang" in df.columns:
        df["NhomBanHang"] = df["NhomBanHang"].apply(_normalize_sales_group)

    rows_cleaned = sum(
        df[c].notna().sum() for c in string_columns if c in df.columns
    )
    logger.debug(
        "[%s] String normalization complete. Non-null strings: %d",
        tenant_id, rows_cleaned
    )

    return df


def _normalize_payment_method(value: Any) -> Optional[str]:
    """
    Normalize payment method to standard Vietnamese terms.

    Mapping:
        "TIEN MAT", "CASH" -> "Tiền mặt"
        "CHUYEN KHOAN", "TRANSFER", "BANK" -> "Chuyển khoản"
        "THE", "CARD", "CREDIT CARD", "DEBIT CARD" -> "Thẻ"
        "QR", "QRCODE", "VIETQR", "VNPAY-QR" -> "QR Code"
        "COD", "TRA SAU" -> "COD"
        default -> "Tiền mặt"
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Tiền mặt"

    if v in ("TIEN MAT", "CASH"):
        return "Tiền mặt"
    if v in ("CHUYEN KHOAN", "TRANSFER", "BANK", "BANKING"):
        return "Chuyển khoản"
    if v in ("THE", "CARD", "CREDIT CARD", "DEBIT CARD", "CARD PAYMENT"):
        return "Thẻ"
    if v in ("QR", "QRCODE", "VIETQR", "VNPAY QR", "VNPAYQR", "QR PAYMENT"):
        return "QR Code"
    if v in ("COD", "TRA SAU", "CASH ON DELIVERY"):
        return "COD"
    if v in ("MOMO", "ZALOPAY", "VNPAY", "PAYOO", "EWALLET"):
        return v

    return v


def _normalize_sales_channel(value: Any) -> Optional[str]:
    """
    Normalize sales channel to standard values.

    Mapping:
        "INSTORE", "OFFLINE", "TRADITIONAL" -> "InStore"
        "ONLINE", "E-COMMERCE", "WEBSITE" -> "Online"
        "POS" -> "POS"
        "TELEPHONE", "CALL" -> "Telephone"
        default -> "InStore"
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "InStore"

    if v in ("INSTORE", "OFFLINE", "TRADITIONAL", "CUA HANG", "SHOP"):
        return "InStore"
    if v in ("ONLINE", "E-COMMERCE", "WEBSITE", "ECOMMERCE", "LAZADA", "SHOPEE", "TIKTOK"):
        return "Online"
    if v in ("POS"):
        return "POS"
    if v in ("TELEPHONE", "CALL", "HOTLINE"):
        return "Telephone"
    if v in ("SOCIAL", "FACEBOOK", "ZALO", "MESSENGER"):
        return "Social Media"

    return v


def _normalize_sales_group(value: Any) -> Optional[str]:
    """
    Normalize sales group classification.

    Standard values:
        - "Bán lẻ" (Retail)
        - "Bán sỉ" (Wholesale)
        - "Bán online" (Online)
        - "Bán hàng" (General)
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Bán lẻ"

    if v in ("BAN LE", "RETAIL", "LE"):
        return "Bán lẻ"
    if v in ("BAN SI", "WHOLESALE", "SI"):
        return "Bán sỉ"
    if v in ("BAN ONLINE", "ONLINE", "ECOMMERCE"):
        return "Bán online"

    return "Bán lẻ"


# ---------------------------------------------------------------------------
# Step 2: Parse and validate dates
# ---------------------------------------------------------------------------

def _parse_and_validate_dates(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Parse NgayBan to datetime, compute DateKey (INT yyyyMMdd).

    Invalid dates are set to None (will be handled in load step).
    """
    if "NgayBan" not in df.columns:
        logger.warning("[%s] NgayBan column not found", tenant_id)
        return df

    df["NgayBan"] = df["NgayBan"].apply(
        lambda v: parse_date(v, dayfirst=True, default=None)
    )

    invalid_dates = df["NgayBan"].isna().sum()
    if invalid_dates > 0:
        logger.warning(
            "[%s] Found %d rows with invalid/unparseable NgayBan. "
            "These rows will be excluded from fact loading.",
            tenant_id, invalid_dates
        )

    df["DateKey"] = df["NgayBan"].apply(_compute_date_key)

    null_datekey = df["DateKey"].isna().sum()
    if null_datekey > 0:
        logger.warning(
            "[%s] %d rows have null DateKey after date parsing.",
            tenant_id, null_datekey
        )

    logger.debug(
        "[%s] Date parsing complete. Valid dates: %d / %d",
        tenant_id, len(df) - invalid_dates, len(df)
    )

    return df


def _compute_date_key(dt: Any) -> Optional[int]:
    """
    Convert datetime to DateKey integer (yyyymmdd format).

    Used as foreign key to DimDate.
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
    Convert and validate all numeric columns in sales data.

    Rules:
        - SoLuong: must be > 0 (filter in Step 4), min 0
        - DonGiaBan: must be >= 0, min 0
        - ChietKhau: must be >= 0, cannot exceed GrossSalesAmount
        - SoDong: positive integer, default 1
        - IsHoanTra: 0 or 1 integer
    """
    numeric_cols = ["SoLuong", "DonGiaBan", "ChietKhau"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    if "SoLuong" in df.columns:
        df["SoLuong"] = df["SoLuong"].apply(
            lambda v: safe_int(v, min_val=0)
        )

    if "DonGiaBan" in df.columns:
        df["DonGiaBan"] = df["DonGiaBan"].apply(
            lambda v: safe_float(v, min_val=0)
        )

    if "ChietKhau" in df.columns:
        df["ChietKhau"] = df["ChietKhau"].apply(
            lambda v: safe_float(v, min_val=0)
        )

    if "SoDong" in df.columns:
        df["SoDong"] = df["SoDong"].apply(
            lambda v: safe_int(v, default=1, min_val=1)
        )

    if "IsHoanTra" in df.columns:
        df["IsHoanTra"] = df["IsHoanTra"].apply(
            lambda v: 1 if safe_int(v) not in (0, 1) else safe_int(v)
        )

    rows_valid = (
        (df["SoLuong"] >= 0).sum()
        if "SoLuong" in df.columns else len(df)
    )
    logger.debug(
        "[%s] Numeric conversion complete. Valid rows: %d / %d",
        tenant_id, rows_valid, len(df)
    )

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
    Filter out invalid business records from sales data.

    Rules:
        1. SoLuong <= 0 -> EXCLUDE (qty must be positive)
        2. DonGiaBan < 0 -> EXCLUDE (price cannot be negative)
        3. MaHoaDon is null/empty -> EXCLUDE (invoice number required)
        4. MaSP is null/empty -> EXCLUDE (product code required)
        5. MaCH is null/empty -> EXCLUDE (store code required)
        6. NgayBan is null -> EXCLUDE (sale date required)

    Returns:
        DataFrame with only valid sales records.
    """
    if not filter_invalid:
        return df

    initial_count = len(df)

    mask = pd.Series([True] * len(df), index=df.index)

    if "MaHoaDon" in df.columns:
        mask &= df["MaHoaDon"].notna() & (df["MaHoaDon"] != "")
        null_hoadon = initial_count - mask.sum()
        if null_hoadon > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaHoaDon.",
                tenant_id, null_hoadon
            )

    if "MaSP" in df.columns:
        mask &= df["MaSP"].notna() & (df["MaSP"] != "")
        null_masp = initial_count - mask.sum()
        if null_masp > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaSP.",
                tenant_id, null_masp
            )

    if "MaCH" in df.columns:
        mask &= df["MaCH"].notna() & (df["MaCH"] != "")
        null_mach = initial_count - mask.sum()
        if null_mach > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaCH.",
                tenant_id, null_mach
            )

    if "NgayBan" in df.columns:
        mask &= df["NgayBan"].notna()
        null_ngayban = initial_count - mask.sum()
        if null_ngayban > 0:
            logger.warning(
                "[%s] Excluding %d rows with null NgayBan.",
                tenant_id, null_ngayban
            )

    if "SoLuong" in df.columns:
        qty_invalid = (df["SoLuong"] <= 0).sum()
        mask &= (df["SoLuong"] > 0)
        if qty_invalid > 0:
            logger.warning(
                "[%s] Excluding %d rows with SoLuong <= 0.",
                tenant_id, qty_invalid
            )

    if "DonGiaBan" in df.columns:
        price_invalid = (df["DonGiaBan"] < 0).sum()
        mask &= (df["DonGiaBan"] >= 0)
        if price_invalid > 0:
            logger.warning(
                "[%s] Excluding %d rows with DonGiaBan < 0.",
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
# Step 5: Deduplicate on (MaHoaDon, MaSP)
# ---------------------------------------------------------------------------

def _deduplicate_sales(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Remove duplicate rows based on (MaHoaDon, MaSP) composite key.

    Business logic:
        - 1 invoice line = 1 product (grain: MaHoaDon + MaSP)
        - If same product appears twice in same invoice, keep first
          (assuming it's a data entry error)
        - Logs warning for each duplicate found

    Args:
        df: DataFrame after filtering.

    Returns:
        DataFrame with duplicates removed (keeps first occurrence).
    """
    if "MaHoaDon" not in df.columns or "MaSP" not in df.columns:
        logger.warning(
            "[%s] MaHoaDon or MaSP not found. Skipping deduplication.",
            tenant_id
        )
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=["MaHoaDon", "MaSP"], keep="first")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[%s] Removed %d duplicate (MaHoaDon, MaSP) rows.",
            tenant_id, rows_dedup
        )
    else:
        logger.debug(
            "[%s] No duplicate (MaHoaDon, MaSP) rows found.",
            tenant_id
        )

    return df


# ---------------------------------------------------------------------------
# Step 6: Calculate financial metrics
# ---------------------------------------------------------------------------

def _calculate_financial_metrics(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Calculate financial derived columns for fact sales.

    Computed columns:
        - GrossSalesAmount  = SoLuong * DonGiaBan
        - ChietKhauAmt      = ChietKhau (validated, >= 0)
        - NetSalesAmount    = GrossSalesAmount - ChietKhauAmt
        - UnitCostPrice     = Derived from DonGiaBan * 0.7 (estimated cost ratio)
                               Note: Actual cost comes from DimProduct in SP
        - GrossProfitAmount = NetSalesAmount - (SoLuong * UnitCostPrice)

    Note: UnitCostPrice and GrossProfitAmount are estimates based on
          average cost ratio (0.70). The actual values should come from
          DimProduct.UnitCostPrice in the stored procedure usp_Transform_FactSales.
    """
    df["GrossSalesAmount"] = df["SoLuong"] * df["DonGiaBan"]

    if "ChietKhau" in df.columns:
        chiet_khau = df["ChietKhau"].fillna(0)
        df["ChietKhau"] = chiet_khau.apply(lambda v: max(0, v))
    else:
        df["ChietKhau"] = 0.0

    df["NetSalesAmount"] = df["GrossSalesAmount"] - df["ChietKhau"]

    df["NetSalesAmount"] = df["NetSalesAmount"].apply(
        lambda v: max(0, v)
    )

    df["GrossSalesAmount"] = df["GrossSalesAmount"].apply(
        lambda v: max(0, v)
    )

    negative_net = (df["NetSalesAmount"] < 0).sum()
    if negative_net > 0:
        logger.warning(
            "[%s] %d rows have negative NetSalesAmount after discount > gross. "
            "Clamped to 0.",
            tenant_id, negative_net
        )
        df.loc[df["NetSalesAmount"] < 0, "NetSalesAmount"] = 0

    total_gross = df["GrossSalesAmount"].sum()
    total_discount = df["ChietKhau"].sum()
    total_net = df["NetSalesAmount"].sum()

    logger.info(
        "[%s] Financial metrics calculated. "
        "Total GrossSales: %,.0f | Total Discount: %,.0f | Total NetSales: %,.0f",
        tenant_id, total_gross, total_discount, total_net
    )

    return df


# ---------------------------------------------------------------------------
# Step 7: Enrich business flags
# ---------------------------------------------------------------------------

def _enrich_business_flags(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Add computed business flag columns to sales data.

    Flags added:
        - IsHoanTra       : Normalized to 0/1 int
        - ReturnFlag      : "HoanTra" if IsHoanTra=1, else "BanHang"
        - IsHighValue     : 1 if NetSalesAmount > 10,000,000 (10M VND)
        - IsDiscounted    : 1 if ChietKhau > 0
        - IsOnline        : 1 if KenhBan is online channel
    """
    if "IsHoanTra" not in df.columns:
        df["IsHoanTra"] = 0
    else:
        df["IsHoanTra"] = df["IsHoanTra"].apply(lambda v: 1 if v else 0)

    df["ReturnFlag"] = df["IsHoanTra"].apply(lambda v: "HoanTra" if v == 1 else "BanHang")

    df["IsHighValue"] = (
        (df["NetSalesAmount"] > 10_000_000).astype(int)
        if "NetSalesAmount" in df.columns
        else 0
    )

    df["IsDiscounted"] = (
        (df["ChietKhau"] > 0).astype(int)
        if "ChietKhau" in df.columns
        else 0
    )

    df["IsOnline"] = 0
    if "KenhBan" in df.columns:
        df["IsOnline"] = df["KenhBan"].apply(
            lambda v: 1 if v and v in ("Online", "E-COMMERCE", "WEBSITE", "SOCIAL MEDIA")
            else 0
        )

    logger.debug(
        "[%s] Business flags enriched. "
        "Returns: %d | HighValue: %d | Discounted: %d | Online: %d",
        tenant_id,
        df["IsHoanTra"].sum() if "IsHoanTra" in df.columns else 0,
        df["IsHighValue"].sum() if "IsHighValue" in df.columns else 0,
        df["IsDiscounted"].sum() if "IsDiscounted" in df.columns else 0,
        df["IsOnline"].sum() if "IsOnline" in df.columns else 0,
    )

    return df


# ---------------------------------------------------------------------------
# Step 8: Validate foreign key lookups
# ---------------------------------------------------------------------------

def _validate_foreign_keys(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Validate that required foreign key columns are present and non-null.

    FK columns that MUST be present:
        - TenantID (inherited from extract)
        - DateKey (computed from NgayBan)
        - MaSP (for DimProduct lookup)
        - MaCH (for DimStore lookup)

    FK columns that MAY be null but should be validated when present:
        - MaKH (for DimCustomer lookup)
        - MaNV (for DimEmployee lookup)
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

    optional_fk = ["MaKH", "MaNV"]
    for col in optional_fk:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                logger.debug(
                    "[%s] %d rows have null %s (optional FK — allowed).",
                    tenant_id, null_count, col
                )

    logger.debug(
        "[%s] FK validation passed. All required keys present.",
        tenant_id
    )

    return df


# ---------------------------------------------------------------------------
# Summary helper (for logging)
# ---------------------------------------------------------------------------

def get_transformation_summary(df: pd.DataFrame, tenant_id: str) -> dict[str, Any]:
    """
    Generate a summary dict of the transformation results.

    Args:
        df:        Transformed DataFrame.
        tenant_id: Tenant identifier.

    Returns:
        Dictionary with key metrics (row counts, financial totals, flags).
    """
    summary: dict[str, Any] = {
        "tenant_id": tenant_id,
        "total_rows": len(df),
        "date_range": {
            "min": None,
            "max": None,
        },
        "financials": {
            "total_gross_sales": 0.0,
            "total_discount": 0.0,
            "total_net_sales": 0.0,
        },
        "flags": {
            "total_returns": 0,
            "high_value_orders": 0,
            "discounted_orders": 0,
            "online_orders": 0,
        },
        "unique_invoices": 0,
        "unique_products": 0,
        "unique_customers": 0,
        "unique_stores": 0,
    }

    if df.empty:
        return summary

    if "NgayBan" in df.columns:
        valid_dates = df["NgayBan"].dropna()
        if len(valid_dates) > 0:
            summary["date_range"]["min"] = valid_dates.min().strftime("%Y-%m-%d")
            summary["date_range"]["max"] = valid_dates.max().strftime("%Y-%m-%d")

    if "GrossSalesAmount" in df.columns:
        summary["financials"]["total_gross_sales"] = float(df["GrossSalesAmount"].sum())

    if "ChietKhau" in df.columns:
        summary["financials"]["total_discount"] = float(df["ChietKhau"].sum())

    if "NetSalesAmount" in df.columns:
        summary["financials"]["total_net_sales"] = float(df["NetSalesAmount"].sum())

    if "IsHoanTra" in df.columns:
        summary["flags"]["total_returns"] = int(df["IsHoanTra"].sum())

    if "IsHighValue" in df.columns:
        summary["flags"]["high_value_orders"] = int(df["IsHighValue"].sum())

    if "IsDiscounted" in df.columns:
        summary["flags"]["discounted_orders"] = int(df["IsDiscounted"].sum())

    if "IsOnline" in df.columns:
        summary["flags"]["online_orders"] = int(df["IsOnline"].sum())

    if "MaHoaDon" in df.columns:
        summary["unique_invoices"] = int(df["MaHoaDon"].nunique())

    if "MaSP" in df.columns:
        summary["unique_products"] = int(df["MaSP"].nunique())

    if "MaKH" in df.columns:
        summary["unique_customers"] = int(df["MaKH"].nunique())

    if "MaCH" in df.columns:
        summary["unique_stores"] = int(df["MaCH"].nunique())

    return summary
