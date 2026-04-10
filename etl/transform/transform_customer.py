"""
etl/transform/transform_customer.py
Phase 10: Transform STG_CustomerRaw data into DimCustomer dimension (SCD Type 2).

This module transforms raw customer staging data from STG_CustomerRaw into
clean, validated customer dimension data ready for SCD Type 2 processing.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE for codes, Title Case for names)
    2. Parse and validate date columns (NgaySinh, NgayDangKy)
    3. Clean and validate numeric columns (DiemTichLuy)
    4. Calculate derived attributes:
         - Age (from NgaySinh)
         - LoyaltyTier (Bronze/Silver/Gold/Platinum based on DiemTichLuy)
         - MembershipYears (from NgayDangKy)
         - City (normalized)
    5. Normalize gender values
    6. Deduplicate on MaKH
    7. Filter invalid rows (null MaKH, invalid age)

Expected input columns (from extract module):
    - TenantID, MaKH, HoTen, GioiTinh, NgaySinh, DienThoai
    - Email, DiaChi, ThanhPho, LoaiKH, HangTV, DiemTichLuy, NgayDangKy
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for usp_Load_DimCustomer SCD Type 2 SP):
    - CustomerCode (MaKH normalized)
    - FullName (HoTen normalized)
    - Gender, DateOfBirth, Phone, Email, Address, City
    - CustomerType, LoyaltyTier, LoyaltyPoints
    - MemberSince, Age, MembershipYears
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
    safe_int,
    safe_float,
    calculate_age,
    normalize_phone,
    normalize_email,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------

def transform_customers(
    df: pd.DataFrame,
    tenant_id: str,
    filter_invalid: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_CustomerRaw data into clean customer dimension records.

    This function performs full ETL transformation on raw customer data:
        - String normalization (trim, uppercase for codes, title case for names)
        - Date parsing (NgaySinh, NgayDangKy)
        - Numeric validation (DiemTichLuy)
        - Derived attribute calculations (Age, LoyaltyTier, MembershipYears)
        - Gender normalization
        - Deduplication on MaKH
        - Invalid row filtering

    Args:
        df:            Raw STG_CustomerRaw DataFrame from extract module.
        tenant_id:     Tenant identifier (e.g. 'STORE_HN').
        filter_invalid: Remove rows with invalid customer code or name. (default True)

    Returns:
        DataFrame with transformed customer data ready for DimCustomer SCD loading.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting customer transformation", tenant_id)
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

        df = _deduplicate_customers(df, tenant_id)

        df = _calculate_derived_attributes(df, tenant_id)

        df = _enrich_customer_classification(df, tenant_id)

        df = _add_scd_type2_fields(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[%s] Customer transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            tenant_id, rows_out, rows_filtered
        )
        logger.info("[%s] Customer transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[%s] Customer transformation failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Customer transformation failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize all string columns in the customer DataFrame.

    Rules:
        - MaKH: UPPERCASE (code identifier)
        - HoTen: Title Case (person name)
        - GioiTinh: normalized M/F
        - ThanhPho, DiaChi, DienThoai, Email: trimmed
        - LoaiKH, HangTV: UPPERCASE
    """
    code_columns = ["MaKH"]
    for col in code_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    name_columns = ["HoTen"]
    for col in name_columns:
        if col in df.columns:
            df[col] = df[col].apply(_normalize_name)

    string_columns = [
        "DienThoai", "Email", "DiaChi", "ThanhPho",
        "LoaiKH", "HangTV", "STG_SourceFile",
    ]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )

    if "DienThoai" in df.columns:
        df["DienThoai"] = df["DienThoai"].apply(normalize_phone)

    if "Email" in df.columns:
        df["Email"] = df["Email"].apply(normalize_email)

    logger.debug("[%s] String normalization complete.", tenant_id)

    return df


def _normalize_name(value: Any) -> Optional[str]:
    """
    Normalize person name to Title Case with proper handling
    of Vietnamese names and common particles.

    Rules:
        - Strip whitespace
        - Title case each word
        - Preserve common Vietnamese particles (Van, Thi, etc.)
        - Common titles removed: "ÔNG", "BÀ", "ANH", "CHỊ"
    """
    v = clean_string(value, upper=False, strip=True, default=None)
    if v is None:
        return None

    v = v.strip()

    prefixes_to_remove = ["ÔNG ", "BÀ ", "ANH ", "CHỊ ", "ÔNG.", "BÀ.", "MR.", "MRS.", "MS."]
    for prefix in prefixes_to_remove:
        if v.upper().startswith(prefix.strip()):
            v = v[len(prefix):].strip()

    if not v:
        return None

    words = v.split()
    result = []
    for word in words:
        if len(word) > 0:
            result.append(word.capitalize())

    return " ".join(result) if result else None


# ---------------------------------------------------------------------------
# Step 2: Parse and validate dates
# ---------------------------------------------------------------------------

def _parse_and_validate_dates(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Parse NgaySinh and NgayDangKy to datetime.

    Invalid dates are set to None and handled downstream.
    """
    date_columns = ["NgaySinh", "NgayDangKy"]

    for col in date_columns:
        if col not in df.columns:
            continue

        df[col] = df[col].apply(
            lambda v: parse_date(v, dayfirst=True, default=None)
        )

        invalid_count = df[col].isna().sum()
        if invalid_count > 0:
            logger.debug(
                "[%s] %s: %d invalid dates set to None.",
                tenant_id, col, invalid_count
            )

    logger.debug("[%s] Date parsing complete.", tenant_id)

    return df


# ---------------------------------------------------------------------------
# Step 3: Convert and validate numerics
# ---------------------------------------------------------------------------

def _convert_and_validate_numerics(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Convert and validate all numeric columns in customer data.

    Rules:
        - DiemTichLuy: must be >= 0, integer
    """
    if "DiemTichLuy" in df.columns:
        df["DiemTichLuy"] = df["DiemTichLuy"].apply(
            lambda v: safe_int(v, min_val=0)
        )

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
    Filter out invalid customer records.

    Rules:
        1. MaKH is null/empty -> EXCLUDE
        2. HoTen is null/empty -> EXCLUDE
        3. NgaySinh is in the future -> EXCLUDE
        4. Age < 0 or Age > 120 -> EXCLUDE
        5. DiemTichLuy < 0 -> EXCLUDE
    """
    if not filter_invalid:
        return df

    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)

    if "MaKH" in df.columns:
        mask &= df["MaKH"].notna() & (df["MaKH"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaKH.",
                tenant_id, null_count
            )

    if "HoTen" in df.columns:
        mask &= df["HoTen"].notna() & (df["HoTen"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty HoTen.",
                tenant_id, null_count
            )

    if "NgaySinh" in df.columns:
        future_count = (
            (df["NgaySinh"] > datetime.now()).sum()
        )
        mask &= df["NgaySinh"].isna() | (df["NgaySinh"] <= datetime.now())
        if future_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with future NgaySinh.",
                tenant_id, future_count
            )

    if "DiemTichLuy" in df.columns:
        negative_count = (df["DiemTichLuy"] < 0).sum()
        mask &= (df["DiemTichLuy"] >= 0)
        if negative_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with negative DiemTichLuy.",
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
# Step 5: Deduplicate on MaKH
# ---------------------------------------------------------------------------

def _deduplicate_customers(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Remove duplicate rows based on MaKH (customer code — natural key).

    Business logic:
        - 1 customer code = 1 customer record per tenant
        - If duplicates found, keep LAST occurrence
          (most recent data is most up-to-date)
        - The SCD Type 2 logic is handled in usp_Load_DimCustomer
    """
    if "MaKH" not in df.columns:
        logger.warning(
            "[%s] MaKH column not found. Skipping deduplication.",
            tenant_id
        )
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=["MaKH"], keep="last")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[%s] Removed %d duplicate MaKH rows.",
            tenant_id, rows_dedup
        )

    return df


# ---------------------------------------------------------------------------
# Step 6: Calculate derived attributes
# ---------------------------------------------------------------------------

def _calculate_derived_attributes(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Calculate derived customer attributes.

    Computed columns:
        - Age          = calculate_age(NgaySinh)
        - MembershipYears = years since NgayDangKy
        - City         = normalized ThanhPho
        - CustomerCode = MaKH (alias)
        - FullName     = HoTen (alias)
        - DateOfBirth  = NgaySinh (alias)
        - MemberSince  = NgayDangKy (alias)
    """
    if "NgaySinh" in df.columns:
        df["Age"] = df["NgaySinh"].apply(calculate_age)
        invalid_age = ((df["Age"] < 0) | (df["Age"] > 120)).sum()
        if invalid_age > 0:
            df.loc[(df["Age"] < 0) | (df["Age"] > 120), "Age"] = None
            logger.debug(
                "[%s] %d rows with unrealistic Age set to None.",
                tenant_id, invalid_age
            )

    if "NgayDangKy" in df.columns:
        now = datetime.now()
        def _calc_membership_years(reg_date: Any) -> int:
            if reg_date is None:
                return 0
            parsed = parse_date(reg_date)
            if parsed is None:
                return 0
            delta = now - parsed
            years = int(delta.days / 365.25)
            return max(0, min(years, 50))

        df["MembershipYears"] = df["NgayDangKy"].apply(_calc_membership_years)

    if "ThanhPho" in df.columns:
        df["City"] = df["ThanhPho"].apply(
            lambda v: _normalize_city(v) if v else None
        )

    df["CustomerCode"] = df.get("MaKH", None)
    df["FullName"] = df.get("HoTen", None)
    df["DateOfBirth"] = df.get("NgaySinh", None)
    df["MemberSince"] = df.get("NgayDangKy", None)

    logger.debug(
        "[%s] Derived attributes calculated. "
        "Avg Age: %s | Avg Membership Years: %s",
        tenant_id,
        round(df["Age"].mean(), 1) if "Age" in df.columns else "N/A",
        round(df["MembershipYears"].mean(), 1) if "MembershipYears" in df.columns else "N/A",
    )

    return df


def _normalize_city(value: Any) -> Optional[str]:
    """
    Normalize city/province names to standard Vietnamese names.

    Known mappings:
        - "HN", "HA NOI", "HANOI" -> "Hà Nội"
        - "HCM", "HO CHI MINH", "TP HCM", "TPHCM" -> "Hồ Chí Minh"
        - "DN", "DA NANG", "DANANG" -> "Đà Nẵng"
        - etc.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return None

    city_map = {
        "HA NOI": "Hà Nội",
        "HANOI": "Hà Nội",
        "HN": "Hà Nội",
        "HO CHI MINH": "Hồ Chí Minh",
        "HOCHIMINH": "Hồ Chí Minh",
        "TP HCM": "Hồ Chí Minh",
        "TPHCM": "Hồ Chí Minh",
        "HCM": "Hồ Chí Minh",
        "DA NANG": "Đà Nẵng",
        "DANANG": "Đà Nẵng",
        "DN": "Đà Nẵng",
        "CAN THO": "Cần Thơ",
        "CANTHO": "Cần Thơ",
        "HAIPHONG": "Hải Phòng",
        "HP": "Hải Phòng",
        "HAIPHONG": "Hải Phòng",
        "BRVT": "Bà Rịa - Vũng Tàu",
        "VUNGT AU": "Bà Rịa - Vũng Tàu",
        "LONG AN": "Long An",
        "BINH DUONG": "Bình Dương",
        "DONG NAI": "Đồng Nai",
    }

    return city_map.get(v, v.title())


# ---------------------------------------------------------------------------
# Step 7: Enrich customer classification
# ---------------------------------------------------------------------------

def _enrich_customer_classification(
    df: pd.DataFrame,
    tenant_id: str,
) -> pd.DataFrame:
    """
    Enrich customer data with derived classification attributes.

    Classification rules:
        - LoyaltyTier: based on DiemTichLuy
            Bronze:   0 - 99,999
            Silver:   100,000 - 499,999
            Gold:     500,000 - 1,999,999
            Platinum: 2,000,000+
        - CustomerType: normalized from LoaiKH
        - Gender: normalized to "Nam" / "Nữ"
    """
    if "DiemTichLuy" in df.columns:
        df["DiemTichLuy"] = df["DiemTichLuy"].fillna(0)

        def _compute_loyalty_tier(points: int) -> str:
            if points >= 2_000_000:
                return "Platinum"
            elif points >= 500_000:
                return "Gold"
            elif points >= 100_000:
                return "Silver"
            else:
                return "Bronze"

        df["LoyaltyTier"] = df["DiemTichLuy"].apply(_compute_loyalty_tier)

        tier_counts = df["LoyaltyTier"].value_counts().to_dict()
        logger.info(
            "[%s] Loyalty tier distribution: %s",
            tenant_id, tier_counts
        )

    if "GioiTinh" in df.columns:
        df["Gender"] = df["GioiTinh"].apply(_normalize_gender)

    if "LoaiKH" in df.columns:
        df["CustomerType"] = df["LoaiKH"].apply(_normalize_customer_type)

    logger.debug(
        "[%s] Customer classification enriched.",
        tenant_id
    )

    return df


def _normalize_gender(value: Any) -> Optional[str]:
    """
    Normalize gender values to standard Vietnamese terms.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return None

    if v in ("NAM", "M", "MALE", "0", "TRUE"):
        return "Nam"
    elif v in ("NU", "NỮ", "NỮ", "FEMALE", "F", "1", "FALSE"):
        return "Nữ"
    else:
        return None


def _normalize_customer_type(value: Any) -> Optional[str]:
    """
    Normalize customer type to standard values.

    Standard types:
        - "Khách lẻ" (Retail)
        - "Khách sỉ" (Wholesale)
        - "Khách VIP" (VIP)
        - "Khách online" (Online)
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Khách lẻ"

    type_map = {
        "KHACH LE": "Khách lẻ",
        "RETAIL": "Khách lẻ",
        "LE": "Khách lẻ",
        "INDIVIDUAL": "Khách lẻ",
        "KHACH SI": "Khách sỉ",
        "WHOLESALE": "Khách sỉ",
        "SI": "Khách sỉ",
        "BUSINESS": "Khách sỉ",
        "KHACH VIP": "Khách VIP",
        "VIP": "Khách VIP",
        "PREMIUM": "Khách VIP",
        "KHACH ONLINE": "Khách online",
        "ONLINE": "Khách online",
        "CORPORATE": "Khách doanh nghiệp",
    }

    return type_map.get(v, "Khách lẻ")


# ---------------------------------------------------------------------------
# Step 8: Add SCD Type 2 fields
# ---------------------------------------------------------------------------

def _add_scd_type2_fields(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Add SCD Type 2 metadata fields for DimCustomer.
    """
    now = datetime.now()

    df["IsActive"] = True

    df["EffectiveDate"] = now

    df["ExpirationDate"] = None

    df["IsCurrent"] = True

    logger.debug(
        "[%s] SCD Type 2 fields added. EffectiveDate: %s",
        tenant_id, now.strftime("%Y-%m-%d")
    )

    return df


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def get_customer_summary(df: pd.DataFrame, tenant_id: str) -> dict[str, Any]:
    """
    Generate a summary dict of the customer transformation results.
    """
    summary: dict[str, Any] = {
        "tenant_id": tenant_id,
        "total_rows": len(df),
        "demographics": {
            "avg_age": 0.0,
            "male_count": 0,
            "female_count": 0,
            "unknown_gender_count": 0,
        },
        "loyalty": {
            "total_loyalty_points": 0,
            "avg_loyalty_points": 0.0,
        },
        "tier_distribution": {},
        "type_distribution": {},
    }

    if df.empty:
        return summary

    if "Age" in df.columns:
        avg_age = df["Age"].replace(0, pd.NA).mean()
        summary["demographics"]["avg_age"] = (
            float(avg_age) if pd.notna(avg_age) else 0.0
        )

    if "Gender" in df.columns:
        summary["demographics"]["male_count"] = int((df["Gender"] == "Nam").sum())
        summary["demographics"]["female_count"] = int((df["Gender"] == "Nữ").sum())
        summary["demographics"]["unknown_gender_count"] = int(df["Gender"].isna().sum())

    if "DiemTichLuy" in df.columns:
        summary["loyalty"]["total_loyalty_points"] = int(df["DiemTichLuy"].sum())
        avg_pts = df["DiemTichLuy"].replace(0, pd.NA).mean()
        summary["loyalty"]["avg_loyalty_points"] = (
            float(avg_pts) if pd.notna(avg_pts) else 0.0
        )

    if "LoyaltyTier" in df.columns:
        summary["tier_distribution"] = df["LoyaltyTier"].value_counts().to_dict()

    if "CustomerType" in df.columns:
        summary["type_distribution"] = df["CustomerType"].value_counts().to_dict()

    return summary