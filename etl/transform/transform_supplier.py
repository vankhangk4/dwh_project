"""
etl/transform/transform_supplier.py
Phase 11: Transform STG_SupplierRaw data into DimSupplier dimension.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from .base_transform import (
    clean_string,
    parse_date,
)

logger = logging.getLogger(__name__)


def transform_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw STG_SupplierRaw data into clean supplier dimension records.

    Args:
        df: Raw STG_SupplierRaw DataFrame from extract module.

    Returns:
        DataFrame with transformed supplier data ready for DimSupplier loading.
    """
    logger.info("[SHARED] Starting supplier transformation")
    logger.info("[SHARED] Input rows: %d", len(df))

    if df is None or df.empty:
        logger.warning("[SHARED] Input DataFrame is empty. Returning empty.")
        return pd.DataFrame()

    original_count = len(df)

    try:
        df = df.copy()
        df = df.reset_index(drop=True)

        df = _normalize_strings(df)

        df = _parse_and_validate_dates(df)

        df = _filter_invalid_rows(df)

        df = _deduplicate_suppliers(df)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        logger.info(
            "[SHARED] Supplier transformation completed. Output rows: %d",
            rows_out
        )
        logger.info("[SHARED] Supplier transformation: DONE")

        return df

    except Exception as ex:
        logger.error("[SHARED] Supplier transformation failed: %s", ex, exc_info=True)
        raise RuntimeError(f"Supplier transformation failed: {ex}") from ex


def _normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    string_cols = ["MaNCC", "TenNCC", "NguoiLienHe", "ChucVu",
                   "DienThoai", "Email", "DiaChi", "ThanhPho", "QuocGia",
                   "MaSoThue", "DieuKhoanTT"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=True, strip=True, default=None)
            )
    if "NguoiLienHe" in df.columns:
        df["NguoiLienHe"] = df["NguoiLienHe"].apply(
            lambda v: clean_string(v, upper=False, strip=True, default=None)
        )
    return df


def _parse_and_validate_dates(df: pd.DataFrame) -> pd.DataFrame:
    return df


def _filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)
    for col in ["MaNCC", "TenNCC"]:
        if col in df.columns:
            mask &= df[col].notna() & (df[col] != "")
    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)
    logger.info("[SHARED] Filtered %d invalid rows. Remaining: %d", rows_removed, len(df))
    return df


def _deduplicate_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    if "MaNCC" not in df.columns:
        return df
    initial_count = len(df)
    df = df.drop_duplicates(subset=["MaNCC"], keep="last").reset_index(drop=True)
    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning("[SHARED] Removed %d duplicate MaNCC rows.", rows_dedup)
    return df