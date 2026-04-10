"""
etl/transform/transform_inventory.py
Phase 10: Transform STG_InventoryRaw data into inventory fact data.

This module transforms raw inventory staging data from STG_InventoryRaw into
clean, validated data ready for fact table loading.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE, control char removal)
    2. Parse and validate date columns (NgayChot)
    3. Convert numeric columns (TonDauNgay, NhapTrongNgay, BanTrongNgay,
       TraLaiNhap, DieuChinh, DonGiaVon, MucTonToiThieu)
    4. Calculate derived inventory metrics:
         - ClosingQty = TonDauNgay + NhapTrongNgay - BanTrongNgay - TraLaiNhap + DieuChinh
         - StockValue = ClosingQty * DonGiaVon
    5. Add business flags (AlertLevel, StockStatus)
    6. Deduplicate on (MaCH, MaSP, NgayChot)

Expected input columns (from extract module):
    - TenantID, MaCH, MaSP, NgayChot
    - TonDauNgay, NhapTrongNgay, BanTrongNgay, TraLaiNhap, DieuChinh
    - DonGiaVon, MucTonToiThieu, LoaiChuyen
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for staging or SP call):
    - All normalized/typed columns above
    - DateKey (INT yyyyMMdd format)
    - ClosingQty, StockValue
    - AlertLevel, StockStatus flags

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

def transform_inventory(
    df: pd.DataFrame,
    tenant_id: str,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_InventoryRaw data into clean inventory facts.

    This function performs full ETL transformation on raw inventory data:
        - String normalization (trim, uppercase)
        - Date parsing (NgayChot) -> DateKey
        - Numeric validation and conversion
        - Inventory calculations (ClosingQty, StockValue)
        - Business flag enrichment (AlertLevel, StockStatus)
        - Deduplication on (MaCH, MaSP, NgayChot)

    Args:
        df:              Raw STG_InventoryRaw DataFrame from extract module.
        tenant_id:       Tenant identifier (e.g. 'STORE_HN').
        drop_duplicates: Drop duplicate (MaCH, MaSP, NgayChot) rows. (default True)

    Returns:
        DataFrame with transformed inventory data ready for staging load.
        Columns include computed ClosingQty and StockValue.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting inventory transformation", tenant_id)
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

        df = _filter_invalid_rows(df, tenant_id)

        if drop_duplicates:
            df = _deduplicate_inventory(df, tenant_id)

        df = _calculate_inventory_metrics(df, tenant_id)

        df = _enrich_business_flags(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[%s] Inventory transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            tenant_id, rows_out, rows_filtered
        )
        logger.info("[%s] Inventory transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[%s] Inventory transformation failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Inventory transformation failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize all string columns in the inventory DataFrame.
    """
    string_columns = [
        "MaCH", "MaSP", "LoaiChuyen", "STG_SourceFile",
    ]

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    if "LoaiChuyen" in df.columns:
        df["LoaiChuyen"] = df["LoaiChuyen"].apply(_normalize_transaction_type)

    logger.debug("[%s] String normalization complete.", tenant_id)

    return df


def _normalize_transaction_type(value: Any) -> Optional[str]:
    """
    Normalize inventory transaction type.

    Standard values:
        - "Daily Count" — daily stocktake
        - "Adjustment"   — manual adjustment
        - "Transfer In"  — incoming transfer
        - "Transfer Out" — outgoing transfer
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Daily Count"

    if v in ("DAILY COUNT", "COUNT", "INVENTORY COUNT", "KIEM KE", "DAILY"):
        return "Daily Count"
    if v in ("ADJUSTMENT", "ADJUST", "ADJUSTED", "DIEU CHINH"):
        return "Adjustment"
    if v in ("TRANSFER IN", "TRANSFERIN", "NHAP CHUYEN", "RECEIVE"):
        return "Transfer In"
    if v in ("TRANSFER OUT", "TRANSFEROUT", "XUAT CHUYEN", "SEND"):
        return "Transfer Out"
    if v in ("PURCHASE", "NHAP KHO", "RECEIVING"):
        return "Purchase Receipt"
    if v in ("SALE", "BAN", "SALES"):
        return "Sales Deduction"

    return v


# ---------------------------------------------------------------------------
# Step 2: Parse and validate dates
# ---------------------------------------------------------------------------

def _parse_and_validate_dates(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Parse NgayChot to datetime, compute DateKey (INT yyyyMMdd).
    """
    if "NgayChot" not in df.columns:
        logger.warning("[%s] NgayChot column not found", tenant_id)
        return df

    df["NgayChot"] = df["NgayChot"].apply(
        lambda v: parse_date(v, dayfirst=True, default=None)
    )

    invalid_dates = df["NgayChot"].isna().sum()
    if invalid_dates > 0:
        logger.warning(
            "[%s] Found %d rows with invalid/unparseable NgayChot.",
            tenant_id, invalid_dates
        )

    df["DateKey"] = df["NgayChot"].apply(_compute_date_key)

    logger.debug(
        "[%s] Date parsing complete. Valid dates: %d / %d",
        tenant_id, len(df) - invalid_dates, len(df)
    )

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
    Convert and validate all numeric columns in inventory data.

    Rules:
        - TonDauNgay: >= 0 (opening stock)
        - NhapTrongNgay: >= 0 (received quantity)
        - BanTrongNgay: >= 0 (sold quantity)
        - TraLaiNhap: >= 0 (returned to supplier)
        - DieuChinh: can be negative (adjustment)
        - DonGiaVon: >= 0 (unit cost)
        - MucTonToiThieu: >= 0 (reorder level)
    """
    qty_columns = [
        "TonDauNgay", "NhapTrongNgay", "BanTrongNgay",
        "TraLaiNhap", "DieuChinh", "MucTonToiThieu",
    ]
    for col in qty_columns:
        if col in df.columns:
            df[col] = df[col].apply(safe_int)

    if "DonGiaVon" in df.columns:
        df["DonGiaVon"] = df["DonGiaVon"].apply(safe_float)

    logger.debug("[%s] Numeric conversion complete.", tenant_id)

    return df


# ---------------------------------------------------------------------------
# Step 4: Filter invalid rows
# ---------------------------------------------------------------------------

def _filter_invalid_rows(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Filter out invalid business records from inventory data.

    Rules:
        1. MaCH is null/empty -> EXCLUDE
        2. MaSP is null/empty -> EXCLUDE
        3. NgayChot is null -> EXCLUDE
        4. TonDauNgay < 0 -> EXCLUDE
        5. DonGiaVon < 0 -> EXCLUDE
    """
    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)

    if "MaCH" in df.columns:
        mask &= df["MaCH"].notna() & (df["MaCH"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaCH.",
                tenant_id, null_count
            )

    if "MaSP" in df.columns:
        mask &= df["MaSP"].notna() & (df["MaSP"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaSP.",
                tenant_id, null_count
            )

    if "NgayChot" in df.columns:
        mask &= df["NgayChot"].notna()
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null NgayChot.",
                tenant_id, null_count
            )

    if "TonDauNgay" in df.columns:
        negative_count = (df["TonDauNgay"] < 0).sum()
        mask &= (df["TonDauNgay"] >= 0)
        if negative_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with TonDauNgay < 0.",
                tenant_id, negative_count
            )

    if "DonGiaVon" in df.columns:
        negative_count = (df["DonGiaVon"] < 0).sum()
        mask &= (df["DonGiaVon"] >= 0)
        if negative_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with DonGiaVon < 0.",
                tenant_id, negative_count
            )

    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)

    logger.info(
        "[%s] Filtered %d invalid rows. Remaining: %d",
        tenant_id, rows_removed, len(df)
    )

    return df


# ---------------------------------------------------------------------------
# Step 5: Deduplicate on (MaCH, MaSP, NgayChot)
# ---------------------------------------------------------------------------

def _deduplicate_inventory(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Remove duplicate rows based on (MaCH, MaSP, NgayChot) composite key.

    Business logic:
        - Grain: 1 row = 1 product in 1 store on 1 day
        - If duplicates found, keep the LAST record (most recent entry)
        - This handles the case where the same product/store/date
          appears multiple times due to multiple data entry sessions
    """
    if "MaCH" not in df.columns or "MaSP" not in df.columns or "NgayChot" not in df.columns:
        logger.warning(
            "[%s] Cannot deduplicate — required columns missing.",
            tenant_id
        )
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=["MaCH", "MaSP", "NgayChot"], keep="last")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[%s] Removed %d duplicate (MaCH, MaSP, NgayChot) rows.",
            tenant_id, rows_dedup
        )

    return df


# ---------------------------------------------------------------------------
# Step 6: Calculate inventory metrics
# ---------------------------------------------------------------------------

def _calculate_inventory_metrics(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Calculate derived inventory columns.

    Formula:
        ClosingQty = TonDauNgay + NhapTrongNgay - BanTrongNgay - TraLaiNhap + DieuChinh

        StockValue = ClosingQty * DonGiaVon

    Notes:
        - ClosingQty can be negative (oversold scenario)
        - StockValue = 0 if ClosingQty <= 0
        - MucTonToiThieu used for alert threshold (default 10 if not set)
    """
    df["TonDauNgay"] = df["TonDauNgay"].fillna(0)
    df["NhapTrongNgay"] = df["NhapTrongNgay"].fillna(0)
    df["BanTrongNgay"] = df["BanTrongNgay"].fillna(0)
    df["TraLaiNhap"] = df["TraLaiNhap"].fillna(0)
    df["DieuChinh"] = df["DieuChinh"].fillna(0)

    df["ClosingQty"] = (
        df["TonDauNgay"]
        + df["NhapTrongNgay"]
        - df["BanTrongNgay"]
        - df["TraLaiNhap"]
        + df["DieuChinh"]
    )

    df["ClosingQty"] = df["ClosingQty"].apply(safe_int)

    df["DonGiaVon"] = df["DonGiaVon"].fillna(0)
    df["StockValue"] = (
        df["ClosingQty"].clip(lower=0) * df["DonGiaVon"]
    ).apply(safe_float)

    if "MucTonToiThieu" not in df.columns:
        df["MucTonToiThieu"] = 10
    else:
        df["MucTonToiThieu"] = df["MucTonToiThieu"].fillna(10)
        df["MucTonToiThieu"] = df["MucTonToiThieu"].apply(
            lambda v: max(0, safe_int(v))
        )

    total_closing = df["ClosingQty"].sum()
    total_stock_value = df["StockValue"].sum()
    negative_closing = (df["ClosingQty"] < 0).sum()

    logger.info(
        "[%s] Inventory metrics calculated. "
        "Total ClosingQty: %d | Total StockValue: %,.0f | Negative ClosingQty: %d",
        tenant_id, total_closing, total_stock_value, negative_closing
    )

    return df


# ---------------------------------------------------------------------------
# Step 7: Enrich business flags
# ---------------------------------------------------------------------------

def _enrich_business_flags(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Add computed business flag columns to inventory data.

    Flags added:
        - AlertLevel: "Critical" | "Low" | "Normal" | "Overstock"
          Critical: ClosingQty < MucTonToiThieu
          Low: ClosingQty < MucTonToiThieu * 2
          Normal: MucTonToiThieu <= ClosingQty <= MucTonToiThieu * 5
          Overstock: ClosingQty > MucTonToiThieu * 5
        - StockStatus: "InStock" | "OutOfStock" | "LowStock" | "Overstock"
        - DaysOfStock: Estimated days until stockout (BanTrongNgay > 0 required)
    """
    df["MucTonToiThieu"] = df["MucTonToiThieu"].apply(
        lambda v: max(1, safe_int(v, default=10))
    )

    def _compute_alert_level(row) -> str:
        closing = row.get("ClosingQty", 0)
        reorder = row.get("MucTonToiThieu", 10)
        if closing < reorder:
            return "Critical"
        elif closing < reorder * 2:
            return "Low"
        elif closing > reorder * 5:
            return "Overstock"
        else:
            return "Normal"

    def _compute_stock_status(row) -> str:
        closing = row.get("ClosingQty", 0)
        reorder = row.get("MucTonToiThieu", 10)
        if closing <= 0:
            return "OutOfStock"
        elif closing < reorder:
            return "LowStock"
        elif closing > reorder * 5:
            return "Overstock"
        else:
            return "InStock"

    df["AlertLevel"] = df.apply(_compute_alert_level, axis=1)
    df["StockStatus"] = df.apply(_compute_stock_status, axis=1)

    def _compute_days_of_stock(row) -> float:
        sold_per_day = row.get("BanTrongNgay", 0)
        closing_qty = row.get("ClosingQty", 0)
        if sold_per_day > 0 and closing_qty > 0:
            days = closing_qty / sold_per_day
            return round(days, 1)
        return -1.0

    df["DaysOfStock"] = df.apply(_compute_days_of_stock, axis=1)
    df["DaysOfStock"] = df["DaysOfStock"].apply(safe_float)

    alert_counts = df["AlertLevel"].value_counts().to_dict()
    stock_status_counts = df["StockStatus"].value_counts().to_dict()

    logger.info(
        "[%s] Business flags enriched. "
        "AlertLevels: %s | StockStatus: %s",
        tenant_id, alert_counts, stock_status_counts
    )

    return df


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def get_inventory_summary(df: pd.DataFrame, tenant_id: str) -> dict[str, Any]:
    """
    Generate a summary dict of the inventory transformation results.
    """
    summary: dict[str, Any] = {
        "tenant_id": tenant_id,
        "total_rows": len(df),
        "date_range": {"min": None, "max": None},
        "metrics": {
            "total_closing_qty": 0,
            "total_stock_value": 0.0,
            "avg_closing_qty": 0.0,
            "avg_unit_cost": 0.0,
        },
        "flags": {
            "critical_count": 0,
            "low_count": 0,
            "normal_count": 0,
            "overstock_count": 0,
            "out_of_stock_count": 0,
        },
        "unique_stores": 0,
        "unique_products": 0,
    }

    if df.empty:
        return summary

    if "NgayChot" in df.columns:
        valid_dates = df["NgayChot"].dropna()
        if len(valid_dates) > 0:
            summary["date_range"]["min"] = valid_dates.min().strftime("%Y-%m-%d")
            summary["date_range"]["max"] = valid_dates.max().strftime("%Y-%m-%d")

    if "ClosingQty" in df.columns:
        summary["metrics"]["total_closing_qty"] = int(df["ClosingQty"].sum())
        summary["metrics"]["avg_closing_qty"] = float(df["ClosingQty"].mean())

    if "StockValue" in df.columns:
        summary["metrics"]["total_stock_value"] = float(df["StockValue"].sum())

    if "DonGiaVon" in df.columns:
        avg_cost = df["DonGiaVon"].replace(0, pd.NA).mean()
        summary["metrics"]["avg_unit_cost"] = float(avg_cost) if pd.notna(avg_cost) else 0.0

    if "AlertLevel" in df.columns:
        for level in ["Critical", "Low", "Normal", "Overstock"]:
            summary["flags"][f"{level.lower()}_count"] = int(
                (df["AlertLevel"] == level).sum()
            )

    if "StockStatus" in df.columns:
        summary["flags"]["out_of_stock_count"] = int(
            (df["StockStatus"] == "OutOfStock").sum()
        )

    if "MaCH" in df.columns:
        summary["unique_stores"] = int(df["MaCH"].nunique())

    if "MaSP" in df.columns:
        summary["unique_products"] = int(df["MaSP"].nunique())

    return summary
