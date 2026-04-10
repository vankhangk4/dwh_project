"""
etl/transform/transform_store.py
Phase 11: Transform STG_StoreRaw data into DimStore dimension.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE)
    2. Parse and validate date columns (NgayKhaiTruong, NgayDongCua)
    3. Normalize city, district, ward values
    4. Calculate derived attributes (StoreAge, IsActive)
    5. Deduplicate on MaCH
    6. Filter invalid rows

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
)

logger = logging.getLogger(__name__)


def transform_stores(
    df: pd.DataFrame,
    tenant_id: str,
    filter_invalid: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_StoreRaw data into clean store dimension records.

    Args:
        df:            Raw STG_StoreRaw DataFrame from extract module.
        tenant_id:     Tenant identifier (e.g. 'STORE_HN').
        filter_invalid: Remove rows with invalid store code or name. (default True)

    Returns:
        DataFrame with transformed store data ready for DimStore loading.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting store transformation", tenant_id)
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

        df = _normalize_strings(df, tenant_id)

        df = _parse_and_validate_dates(df, tenant_id)

        df = _filter_invalid_rows(df, tenant_id, filter_invalid)

        df = _deduplicate_stores(df, tenant_id)

        df = _calculate_derived_attributes(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        logger.info(
            "[%s] Store transformation completed. Output rows: %d",
            tenant_id, rows_out
        )
        logger.info("[%s] Store transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error("[%s] Store transformation failed: %s", tenant_id, ex, exc_info=True)
        raise RuntimeError(f"Store transformation failed for tenant {tenant_id}: {ex}") from ex


def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    string_cols = ["MaCH", "TenCH", "LoaiCH", "DiaChi", "Phuong", "Quan",
                   "ThanhPho", "Vung", "DienThoai", "Email", "NguoiQuanLy"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )
    if "ThanhPho" in df.columns:
        df["ThanhPho"] = df["ThanhPho"].apply(_normalize_city)
    if "Quan" in df.columns:
        df["Quan"] = df["Quan"].apply(lambda v: clean_string(v, upper=False, strip=True, default=None))
    if "Phuong" in df.columns:
        df["Phuong"] = df["Phuong"].apply(lambda v: clean_string(v, upper=False, strip=True, default=None))
    return df


def _normalize_city(value: Any) -> Optional[str]:
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return None
    city_map = {
        "HA NOI": "Hà Nội", "HANOI": "Hà Nội", "HN": "Hà Nội",
        "HO CHI MINH": "Hồ Chí Minh", "HOCHIMINH": "Hồ Chí Minh",
        "TP HCM": "Hồ Chí Minh", "TPHCM": "Hồ Chí Minh", "HCM": "Hồ Chí Minh",
        "DA NANG": "Đà Nẵng", "DANANG": "Đà Nẵng", "DN": "Đà Nẵng",
        "CAN THO": "Cần Thơ", "CANTHO": "Cần Thơ",
    }
    return city_map.get(v, v.title())


def _parse_and_validate_dates(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    for col in ["NgayKhaiTruong", "NgayDongCua"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: parse_date(v, dayfirst=True, default=None)
            )
    return df


def _filter_invalid_rows(df: pd.DataFrame, tenant_id: str, filter_invalid: bool) -> pd.DataFrame:
    if not filter_invalid:
        return df
    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)
    for col, label in [("MaCH", "MaCH"), ("TenCH", "TenCH"), ("ThanhPho", "Thành phố")]:
        if col in df.columns:
            mask &= df[col].notna() & (df[col] != "")
    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)
    logger.info("[%s] Filtered %d invalid rows. Remaining: %d", tenant_id, rows_removed, len(df))
    return df


def _deduplicate_stores(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    if "MaCH" not in df.columns:
        return df
    initial_count = len(df)
    df = df.drop_duplicates(subset=["MaCH"], keep="last").reset_index(drop=True)
    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning("[%s] Removed %d duplicate MaCH rows.", tenant_id, rows_dedup)
    return df


def _calculate_derived_attributes(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    now = datetime.now()
    if "NgayKhaiTruong" in df.columns:
        df["StoreOpenDate"] = df["NgayKhaiTruong"]
        df["StoreAgeDays"] = df["NgayKhaiTruong"].apply(
            lambda v: max(0, (now - v).days) if v is not None else None
        )
    if "NgayDongCua" in df.columns:
        df["IsActive"] = df["NgayDongCua"].isna()
    else:
        df["IsActive"] = True
    logger.debug("[%s] Store derived attributes calculated.", tenant_id)
    return df