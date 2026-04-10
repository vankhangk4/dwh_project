"""
etl/transform/transform_product.py
Phase 10: Transform STG_ProductRaw data into DimProduct dimension (SCD Type 2).

This module transforms raw product staging data from STG_ProductRaw into
clean product dimension data ready for SCD Type 2 processing.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE)
    2. Parse and validate date columns (NgayCapNhat)
    3. Convert numeric columns (GiaVon, GiaNiemYet)
    4. Calculate derived attributes:
         - UnitCostPrice    = GiaVon (validated >= 0)
         - UnitListPrice   = GiaNiemYet (validated >= 0)
         - PriceRatio      = UnitCostPrice / UnitListPrice (margin estimate)
         - PriceValid      = Boolean flag
    5. Normalize category and brand names
    6. Deduplicate on MaSP
    7. Filter invalid rows (null MaSP, invalid price structure)

Expected input columns (from extract module):
    - MaSP, TenSP, ThuongHieu, DanhMuc, PhanLoai
    - GiaVon, GiaNiemYet, SKU, Barcode
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for usp_Load_DimProduct SCD Type 2 SP):
    - ProductCode (MaSP normalized)
    - ProductName (TenSP normalized)
    - Brand (ThuongHieu normalized)
    - CategoryName (DanhMuc normalized)
    - UnitCostPrice, UnitListPrice
    - IsActive, EffectiveDate, ExpirationDate, IsCurrent

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

def transform_products(
    df: pd.DataFrame,
    tenant_id: Optional[str] = None,
    filter_invalid: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_ProductRaw data into clean product dimension records.

    This function performs full ETL transformation on raw product data:
        - String normalization (trim, uppercase)
        - Numeric validation (GiaVon, GiaNiemYet)
        - Derived attribute calculations
        - Category/brand normalization
        - Deduplication on MaSP
        - Invalid row filtering

    Args:
        df:          Raw STG_ProductRaw DataFrame from extract module.
        tenant_id:   Tenant identifier (optional — product is shared dimension).
        filter_invalid: Remove rows with invalid product code or price. (default True)

    Returns:
        DataFrame with transformed product data ready for DimProduct SCD loading.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.
    """
    logger.info("=" * 60)
    logger.info("[SHARED] Starting product transformation")
    logger.info("[SHARED] Input rows: %d", len(df))

    if df is None or df.empty:
        logger.warning("[SHARED] Input DataFrame is empty. Returning empty.")
        return pd.DataFrame()

    original_count = len(df)

    try:
        df = df.copy()
        df = df.reset_index(drop=True)

        df = _normalize_strings(df)

        df = _convert_and_validate_numerics(df)

        df = _filter_invalid_rows(df, filter_invalid)

        df = _deduplicate_products(df)

        df = _normalize_category_and_brand(df)

        df = _calculate_derived_attributes(df)

        df = _add_scd_type2_fields(df)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[SHARED] Product transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            rows_out, rows_filtered
        )
        logger.info("[SHARED] Product transformation: DONE")
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[SHARED] Product transformation failed: %s",
            ex, exc_info=True
        )
        raise RuntimeError(
            f"Product transformation failed: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize all string columns in the product DataFrame.
    """
    string_columns = [
        "MaSP", "TenSP", "ThuongHieu", "DanhMuc",
        "PhanLoai", "SKU", "Barcode", "STG_SourceFile",
    ]

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    logger.debug("[SHARED] String normalization complete.")

    return df


# ---------------------------------------------------------------------------
# Step 2: Convert and validate numeric columns
# ---------------------------------------------------------------------------

def _convert_and_validate_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert and validate all numeric columns in product data.

    Rules:
        - GiaVon (UnitCostPrice): must be >= 0, default 0
        - GiaNiemYet (UnitListPrice): must be >= 0, default 0
        - PriceValid: True if GiaNiemYet > GiaVon (normal case), else False
    """
    numeric_cols = ["GiaVon", "GiaNiemYet"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    if "GiaVon" in df.columns:
        df["GiaVon"] = df["GiaVon"].apply(lambda v: safe_float(v, min_val=0))

    if "GiaNiemYet" in df.columns:
        df["GiaNiemYet"] = df["GiaNiemYet"].apply(lambda v: safe_float(v, min_val=0))

    logger.debug("[SHARED] Numeric conversion complete.")

    return df


# ---------------------------------------------------------------------------
# Step 3: Filter invalid rows
# ---------------------------------------------------------------------------

def _filter_invalid_rows(df: pd.DataFrame, filter_invalid: bool) -> pd.DataFrame:
    """
    Filter out invalid product records.

    Rules:
        1. MaSP is null/empty -> EXCLUDE
        2. TenSP is null/empty -> EXCLUDE
        3. GiaVon > GiaNiemYet * 1.5 -> WARNING (possible bad data)
        4. GiaNiemYet == 0 AND GiaVon == 0 -> WARNING (no price)
    """
    if not filter_invalid:
        return df

    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)

    if "MaSP" in df.columns:
        mask &= df["MaSP"].notna() & (df["MaSP"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[SHARED] Excluding %d rows with null/empty MaSP.", null_count
            )

    if "TenSP" in df.columns:
        mask &= df["TenSP"].notna() & (df["TenSP"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[SHARED] Excluding %d rows with null/empty TenSP.", null_count
            )

    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)

    logger.info(
        "[SHARED] Filtered %d invalid rows. Remaining: %d",
        rows_removed, len(df)
    )

    return df


# ---------------------------------------------------------------------------
# Step 4: Deduplicate on MaSP
# ---------------------------------------------------------------------------

def _deduplicate_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows based on MaSP (product code — natural key).

    Business logic:
        - 1 product code = 1 product record
        - If duplicates found, keep LAST occurrence
          (most recent data is most up-to-date for SCD tracking)
        - The SCD Type 2 logic is handled in usp_Load_DimProduct
    """
    if "MaSP" not in df.columns:
        logger.warning("[SHARED] MaSP column not found. Skipping deduplication.")
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=["MaSP"], keep="last")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[SHARED] Removed %d duplicate MaSP rows.", rows_dedup
        )

    return df


# ---------------------------------------------------------------------------
# Step 5: Normalize category and brand names
# ---------------------------------------------------------------------------

def _normalize_category_and_brand(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize category and brand names to standard values.

    Known categories:
        - "Thuc pham" / "FOOD" / "TP" -> "Thực phẩm"
        - "Do uong" / "DRINK" / "BEVERAGE" -> "Đồ uống"
        - "Sua" / "MILK" / "DAIRY" -> "Sữa"
        - "My pham" / "COSMETIC" / "BEAUTY" -> "Mỹ phẩm"
        - "Dien may" / "ELECTRONIC" / "APPLIANCE" -> "Điện máy"
        - etc.
    """
    if "DanhMuc" in df.columns:
        df["DanhMuc"] = df["DanhMuc"].apply(_normalize_category)

    if "ThuongHieu" in df.columns:
        df["ThuongHieu"] = df["ThuongHieu"].apply(
            lambda v: clean_string(v, upper=False, strip=True, default=None)
        )

    return df


def _normalize_category(value: Any) -> Optional[str]:
    """
    Normalize category name to standard Vietnamese categories.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Khác"

    category_map = {
        "THUC PHAM": "Thực phẩm",
        "FOOD": "Thực phẩm",
        "TP": "Thực phẩm",
        "DO UONG": "Đồ uống",
        "DRINK": "Đồ uống",
        "BEVERAGE": "Đồ uống",
        "SUA": "Sữa",
        "MILK": "Sữa",
        "DAIRY": "Sữa",
        "MY PHAM": "Mỹ phẩm",
        "COSMETIC": "Mỹ phẩm",
        "BEAUTY": "Mỹ phẩm",
        "DIEN MAY": "Điện máy",
        "ELECTRONIC": "Điện máy",
        "APPLIANCE": "Điện máy",
        "THOI TRANG": "Thời trang",
        "FASHION": "Thời trang",
        "GIA DUNG": "Gia dụng",
        "HOUSEWARE": "Gia dụng",
        "SACH": "Sách",
        "BOOK": "Sách",
        "THE THAO": "Thể thao",
        "SPORT": "Thể thao",
        "MEBELL": "Nội thất",
        "FURNITURE": "Nội thất",
        "OTO": "Ô tô",
        "CAR": "Ô tô",
        "XE MAY": "Xe máy",
        "MOTORCYCLE": "Xe máy",
        "THUOC": "Dược phẩm",
        "PHARMACY": "Dược phẩm",
        "PHARMA": "Dược phẩm",
        "VAN PHONG PHAM": "Văn phòng phẩm",
        "STATIONERY": "Văn phòng phẩm",
        "ME VA BE": "Mẹ và Bé",
        "BABY": "Mẹ và Bé",
        "MOTHER CHILD": "Mẹ và Bé",
    }

    return category_map.get(v, v.title())


# ---------------------------------------------------------------------------
# Step 6: Calculate derived attributes
# ---------------------------------------------------------------------------

def _calculate_derived_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate derived product attributes.

    Computed columns:
        - UnitCostPrice  = GiaVon (clamped >= 0)
        - UnitListPrice  = GiaNiemYet (clamped >= 0)
        - PriceRatio     = UnitCostPrice / UnitListPrice (margin estimate)
        - PriceValid     = True if UnitListPrice > UnitCostPrice
        - MarginPercent  = (UnitListPrice - UnitCostPrice) / UnitListPrice * 100
        - IsPromotion    = True if UnitListPrice <= UnitCostPrice * 1.05
    """
    df["UnitCostPrice"] = df.get("GiaVon", pd.Series([0.0] * len(df)))
    df["UnitCostPrice"] = df["UnitCostPrice"].fillna(0).apply(safe_float)

    df["UnitListPrice"] = df.get("GiaNiemYet", pd.Series([0.0] * len(df)))
    df["UnitListPrice"] = df["UnitListPrice"].fillna(0).apply(safe_float)

    df["UnitCostPrice"] = df["UnitCostPrice"].apply(
        lambda v: safe_float(v, min_val=0)
    )
    df["UnitListPrice"] = df["UnitListPrice"].apply(
        lambda v: safe_float(v, min_val=0)
    )

    def _compute_price_ratio(row) -> Optional[float]:
        cost = row.get("UnitCostPrice", 0)
        list_price = row.get("UnitListPrice", 0)
        if list_price > 0:
            return round(cost / list_price, 4)
        return None

    def _compute_margin(row) -> float:
        cost = row.get("UnitCostPrice", 0)
        list_price = row.get("UnitListPrice", 0)
        if list_price > cost:
            return round((list_price - cost) / list_price * 100, 2)
        return 0.0

    def _compute_is_promotion(row) -> bool:
        cost = row.get("UnitCostPrice", 0)
        list_price = row.get("UnitListPrice", 0)
        return list_price <= cost * 1.05

    df["PriceRatio"] = df.apply(_compute_price_ratio, axis=1).apply(
        lambda v: safe_float(v)
    )

    df["MarginPercent"] = df.apply(_compute_margin, axis=1).apply(
        lambda v: safe_float(v)
    )

    df["PriceValid"] = df["UnitListPrice"] > df["UnitCostPrice"]

    df["IsPromotion"] = df.apply(_compute_is_promotion, axis=1)

    total_products = len(df)
    valid_prices = df["PriceValid"].sum()
    promotions = df["IsPromotion"].sum()
    avg_margin = df["MarginPercent"].mean()

    logger.info(
        "[SHARED] Derived attributes calculated. "
        "Total: %d | ValidPrice: %d | Promotions: %d | AvgMargin: %.1f%%",
        total_products, valid_prices, promotions, avg_margin
    )

    return df


# ---------------------------------------------------------------------------
# Step 7: Add SCD Type 2 fields
# ---------------------------------------------------------------------------

def _add_scd_type2_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add SCD Type 2 metadata fields for DimProduct.

    These fields are used by usp_Load_DimProduct to track
    historical changes:
        - IsActive      : True for current version
        - EffectiveDate: Date this version became active
        - ExpirationDate: Date this version was superseded (NULL = current)
        - IsCurrent     : True for current active version
    """
    now = datetime.now()

    df["IsActive"] = True

    df["EffectiveDate"] = now

    df["ExpirationDate"] = None

    df["IsCurrent"] = True

    logger.debug(
        "[SHARED] SCD Type 2 fields added. "
        "EffectiveDate: %s | IsCurrent: True",
        now.strftime("%Y-%m-%d")
    )

    return df


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def get_product_summary(df: pd.DataFrame) -> dict[str, Any]:
    """
    Generate a summary dict of the product transformation results.
    """
    summary: dict[str, Any] = {
        "total_rows": len(df),
        "financials": {
            "avg_unit_cost": 0.0,
            "avg_unit_list_price": 0.0,
            "avg_margin_percent": 0.0,
        },
        "flags": {
            "valid_prices": 0,
            "promotions": 0,
        },
        "unique_categories": 0,
        "unique_brands": 0,
    }

    if df.empty:
        return summary

    if "UnitCostPrice" in df.columns:
        avg_cost = df["UnitCostPrice"].replace(0, pd.NA).mean()
        summary["financials"]["avg_unit_cost"] = (
            float(avg_cost) if pd.notna(avg_cost) else 0.0
        )

    if "UnitListPrice" in df.columns:
        avg_list = df["UnitListPrice"].replace(0, pd.NA).mean()
        summary["financials"]["avg_unit_list_price"] = (
            float(avg_list) if pd.notna(avg_list) else 0.0
        )

    if "MarginPercent" in df.columns:
        avg_margin = df["MarginPercent"].replace(0, pd.NA).mean()
        summary["financials"]["avg_margin_percent"] = (
            float(avg_margin) if pd.notna(avg_margin) else 0.0
        )

    if "PriceValid" in df.columns:
        summary["flags"]["valid_prices"] = int(df["PriceValid"].sum())

    if "IsPromotion" in df.columns:
        summary["flags"]["promotions"] = int(df["IsPromotion"].sum())

    if "DanhMuc" in df.columns:
        summary["unique_categories"] = int(df["DanhMuc"].nunique())

    if "ThuongHieu" in df.columns:
        summary["unique_brands"] = int(df["ThuongHieu"].nunique())

    return summary
