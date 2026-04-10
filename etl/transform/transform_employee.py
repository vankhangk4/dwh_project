"""
etl/transform/transform_employee.py
Phase 10: Transform STG_EmployeeRaw data into DimEmployee dimension.

This module transforms raw employee staging data from STG_EmployeeRaw into
clean, validated employee dimension data ready for dimension loading.

Transformation steps:
    1. Normalize string columns (trim, UPPERCASE)
    2. Parse and validate date columns (NgaySinh, NgayVaoLam, NgayNghiViec)
    3. Normalize gender values
    4. Calculate derived attributes:
         - Age (from NgaySinh)
         - TenureYears (from NgayVaoLam)
         - TenureDays (from NgayVaoLam to today or NgayNghiViec)
         - IsActive (if NgayNghiViec is NULL, active; else inactive)
    5. Normalize position and department names
    6. Deduplicate on MaNV
    7. Filter invalid rows (null MaNV, invalid age)

Expected input columns (from extract module):
    - TenantID, MaNV, HoTen, GioiTinh, NgaySinh, DienThoai, Email
    - ChucVu, PhongBan, CaLamViec, NgayVaoLam, NgayNghiViec
    - STG_LoadDatetime, STG_SourceFile

Output columns (ready for usp_Load_DimEmployee SP):
    - EmployeeCode (MaNV normalized)
    - FullName (HoTen normalized)
    - Gender, DateOfBirth, Phone, Email
    - Position, Department, Shift
    - HireDate, TerminationDate
    - Age, TenureYears, TenureDays
    - IsActive

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
    calculate_age,
    calculate_tenure_days,
    normalize_phone,
    normalize_email,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------

def transform_employees(
    df: pd.DataFrame,
    tenant_id: str,
    filter_invalid: bool = True,
) -> pd.DataFrame:
    """
    Transform raw STG_EmployeeRaw data into clean employee dimension records.

    This function performs full ETL transformation on raw employee data:
        - String normalization (trim, uppercase)
        - Date parsing (NgaySinh, NgayVaoLam, NgayNghiViec)
        - Gender normalization
        - Derived attribute calculations (Age, TenureDays, IsActive)
        - Position and department normalization
        - Deduplication on MaNV
        - Invalid row filtering

    Args:
        df:            Raw STG_EmployeeRaw DataFrame from extract module.
        tenant_id:     Tenant identifier (e.g. 'STORE_HN').
        filter_invalid: Remove rows with invalid employee code or name. (default True)

    Returns:
        DataFrame with transformed employee data ready for DimEmployee loading.

    Raises:
        ValueError: If required columns are missing.
        RuntimeError: If transformation fails.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting employee transformation", tenant_id)
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

        df = _filter_invalid_rows(df, tenant_id, filter_invalid)

        df = _deduplicate_employees(df, tenant_id)

        df = _calculate_derived_attributes(df, tenant_id)

        df = _normalize_position_and_department(df, tenant_id)

        df = _add_employee_aliases(df, tenant_id)

        df["_TransformDatetime"] = datetime.now()
        df["_TransformStatus"] = "OK"

        rows_out = len(df)
        rows_filtered = original_count - rows_out
        logger.info(
            "[%s] Employee transformation completed. "
            "Output rows: %d | Filtered invalid/duplicate: %d",
            tenant_id, rows_out, rows_filtered
        )
        logger.info("[%s] Employee transformation: DONE", tenant_id)
        logger.info("=" * 60)

        return df

    except Exception as ex:
        logger.error(
            "[%s] Employee transformation failed: %s",
            tenant_id, ex, exc_info=True
        )
        raise RuntimeError(
            f"Employee transformation failed for tenant {tenant_id}: {ex}"
        ) from ex


# ---------------------------------------------------------------------------
# Step 1: Normalize string columns
# ---------------------------------------------------------------------------

def _normalize_strings(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Normalize all string columns in the employee DataFrame.

    Rules:
        - MaNV: UPPERCASE (code identifier)
        - HoTen: Title Case (person name)
        - GioiTinh: normalized Nam/Nu
        - ChucVu, PhongBan, CaLamViec: UPPERCASE
        - DienThoai, Email: trimmed
    """
    code_columns = ["MaNV"]
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
        "ChucVu", "PhongBan", "CaLamViec",
        "DienThoai", "Email", "STG_SourceFile",
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
    """
    v = clean_string(value, upper=False, strip=True, default=None)
    if v is None:
        return None

    v = v.strip()

    prefixes_to_remove = [
        "ÔNG ", "BÀ ", "ANH ", "CHỊ ",
        "ÔNG.", "BÀ.", "MR.", "MRS.", "MS.",
    ]
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
    Parse NgaySinh, NgayVaoLam, NgayNghiViec to datetime.

    Rules:
        - NgaySinh: must be before today (no future dates)
        - NgayVaoLam: must be before today (join date cannot be in future)
        - NgayNghiViec: can be NULL (active employee), or before today
    """
    date_columns = ["NgaySinh", "NgayVaoLam", "NgayNghiViec"]

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

    now = datetime.now()

    if "NgaySinh" in df.columns:
        future_dob = (df["NgaySinh"] > now).sum()
        if future_dob > 0:
            logger.warning(
                "[%s] Found %d rows with future NgaySinh. Setting to None.",
                tenant_id, future_dob
            )
            df.loc[df["NgaySinh"] > now, "NgaySinh"] = None

    if "NgayVaoLam" in df.columns:
        future_join = (df["NgayVaoLam"] > now).sum()
        if future_join > 0:
            logger.warning(
                "[%s] Found %d rows with future NgayVaoLam. Setting to None.",
                tenant_id, future_join
            )
            df.loc[df["NgayVaoLam"] > now, "NgayVaoLam"] = None

    if "NgayNghiViec" in df.columns:
        future_term = (df["NgayNghiViec"] > now).sum()
        if future_term > 0:
            logger.warning(
                "[%s] Found %d rows with future NgayNghiViec. Setting to None.",
                tenant_id, future_term
            )
            df.loc[df["NgayNghiViec"] > now, "NgayNghiViec"] = None

    if "NgayVaoLam" in df.columns and "NgayNghiViec" in df.columns:
        invalid_date_order = (
            (df["NgayNghiViec"].notna())
            & (df["NgayVaoLam"].notna())
            & (df["NgayNghiViec"] < df["NgayVaoLam"])
        ).sum()
        if invalid_date_order > 0:
            logger.warning(
                "[%s] Found %d rows where NgayNghiViec < NgayVaoLam. "
                "This is invalid — will be handled by IsActive flag.",
                tenant_id, invalid_date_order
            )

    logger.debug("[%s] Date parsing complete.", tenant_id)

    return df


# ---------------------------------------------------------------------------
# Step 3: Filter invalid rows
# ---------------------------------------------------------------------------

def _filter_invalid_rows(
    df: pd.DataFrame,
    tenant_id: str,
    filter_invalid: bool,
) -> pd.DataFrame:
    """
    Filter out invalid employee records.

    Rules:
        1. MaNV is null/empty -> EXCLUDE
        2. HoTen is null/empty -> EXCLUDE
    """
    if not filter_invalid:
        return df

    initial_count = len(df)
    mask = pd.Series([True] * len(df), index=df.index)

    if "MaNV" in df.columns:
        mask &= df["MaNV"].notna() & (df["MaNV"] != "")
        null_count = initial_count - mask.sum()
        if null_count > 0:
            logger.warning(
                "[%s] Excluding %d rows with null/empty MaNV.",
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

    rows_removed = initial_count - mask.sum()
    df = df[mask].reset_index(drop=True)

    logger.info(
        "[%s] Filtered %d invalid rows. Remaining: %d",
        tenant_id, rows_removed, len(df)
    )

    return df


# ---------------------------------------------------------------------------
# Step 4: Deduplicate on MaNV
# ---------------------------------------------------------------------------

def _deduplicate_employees(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Remove duplicate rows based on MaNV (employee code — natural key).

    Business logic:
        - 1 employee code = 1 employee record per tenant
        - If duplicates found, keep LAST occurrence
    """
    if "MaNV" not in df.columns:
        logger.warning(
            "[%s] MaNV column not found. Skipping deduplication.",
            tenant_id
        )
        return df

    initial_count = len(df)

    df = df.drop_duplicates(subset=["MaNV"], keep="last")
    df = df.reset_index(drop=True)

    rows_dedup = initial_count - len(df)
    if rows_dedup > 0:
        logger.warning(
            "[%s] Removed %d duplicate MaNV rows.",
            tenant_id, rows_dedup
        )

    return df


# ---------------------------------------------------------------------------
# Step 5: Calculate derived attributes
# ---------------------------------------------------------------------------

def _calculate_derived_attributes(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Calculate derived employee attributes.

    Computed columns:
        - Age           = calculate_age(NgaySinh)
        - TenureYears   = TenureDays / 365
        - TenureDays    = calculate_tenure_days(NgayVaoLam, NgayNghiViec)
        - IsActive      = True if NgayNghiViec is NULL, else False
    """
    now = datetime.now()

    if "NgaySinh" in df.columns:
        df["Age"] = df["NgaySinh"].apply(
            lambda v: calculate_age(v, reference_date=now)
        )
        valid_ages = df["Age"].replace(0, pd.NA)
        avg_age = valid_ages.mean()
        logger.debug(
            "[%s] Age calculated. Avg age: %s",
            tenant_id,
            round(float(avg_age), 1) if pd.notna(avg_age) else "N/A"
        )

    if "NgayVaoLam" in df.columns and "NgayNghiViec" in df.columns:
        def _calc_tenure(row) -> int:
            start = row.get("NgayVaoLam", None)
            end = row.get("NgayNghiViec", None)
            if start is None:
                return 0
            return calculate_tenure_days(start, end_date=end, reference_date=now)

        df["TenureDays"] = df.apply(_calc_tenure, axis=1)
        df["TenureYears"] = (df["TenureDays"] / 365.25).apply(
            lambda v: round(max(0, min(v, 50)), 1)
        )

        active_count = df["NgayNghiViec"].isna().sum()
        inactive_count = len(df) - active_count
        logger.info(
            "[%s] Tenure calculated. Active: %d | Inactive: %d",
            tenant_id, active_count, inactive_count
        )

    if "NgayNghiViec" in df.columns:
        df["IsActive"] = df["NgayNghiViec"].isna()
    else:
        df["IsActive"] = True

    logger.debug("[%s] Derived attributes calculated.", tenant_id)

    return df


# ---------------------------------------------------------------------------
# Step 6: Normalize position and department
# ---------------------------------------------------------------------------

def _normalize_position_and_department(
    df: pd.DataFrame,
    tenant_id: str,
) -> pd.DataFrame:
    """
    Normalize position (ChucVu) and department (PhongBan) names.

    Known position mappings:
        - "Quan ly", "QL", "Manager", "MANAGER" -> "Quản lý"
        - "Ban hang", "BH", "Sale", "SALES" -> "Bán hàng"
        - "Thu ngan", "TN", "Cashier", "CASHIER" -> "Thu ngân"
        - "Ke toan", "KT", "Accountant" -> "Kế toán"
        - "Kho", "Warehouse", "WAREHOUSE" -> "Kho"
        - "Giao hang", "Delivery" -> "Giao hàng"
    """
    if "ChucVu" in df.columns:
        df["ChucVu"] = df["ChucVu"].apply(_normalize_position)

    if "PhongBan" in df.columns:
        df["PhongBan"] = df["PhongBan"].apply(_normalize_department)

    if "CaLamViec" in df.columns:
        df["CaLamViec"] = df["CaLamViec"].apply(_normalize_shift)

    logger.debug("[%s] Position and department normalized.", tenant_id)

    return df


def _normalize_position(value: Any) -> Optional[str]:
    """
    Normalize position title to standard values.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return None

    pos_map = {
        "QUAN LY": "Quản lý",
        "QL": "Quản lý",
        "MANAGER": "Quản lý",
        "GM": "Giám đốc",
        "DIRECTOR": "Giám đốc",
        "BAN HANG": "Bán hàng",
        "BH": "Bán hàng",
        "SALE": "Bán hàng",
        "SALES": "Bán hàng",
        "SALESMAN": "Bán hàng",
        "THU NGAN": "Thu ngân",
        "TN": "Thu ngân",
        "CASHIER": "Thu ngân",
        "KE TOAN": "Kế toán",
        "KT": "Kế toán",
        "ACCOUNTANT": "Kế toán",
        "KHO": "Kho",
        "WAREHOUSE": "Kho",
        "INVENTORY": "Kho",
        "GIAO HANG": "Giao hàng",
        "DELIVERY": "Giao hàng",
        "SHIPPER": "Giao hàng",
        "BAO VE": "Bảo vệ",
        "SECURITY": "Bảo vệ",
        "VE SINH": "Vệ sinh",
        "CLEANER": "Vệ sinh",
        "HR": "Nhân sự",
        "NHAN SU": "Nhân sự",
        "HUMAN RESOURCES": "Nhân sự",
        "MARKETING": "Marketing",
        "IT": "IT",
        "IT Support": "IT",
        "RECEPTIONIST": "Lễ tân",
        "LE TAN": "Lễ tân",
        "RECEPTION": "Lễ tân",
    }

    return pos_map.get(v, v.title())


def _normalize_department(value: Any) -> Optional[str]:
    """
    Normalize department name to standard values.
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return None

    dept_map = {
        "BAN HANG": "Bán hàng",
        "SALES": "Bán hàng",
        "BH": "Bán hàng",
        "KE TOAN": "Kế toán",
        "ACCOUNTING": "Kế toán",
        "KT": "Kế toán",
        "KHO": "Kho",
        "WAREHOUSE": "Kho",
        "NHAN SU": "Nhân sự",
        "HR": "Nhân sự",
        "HUMAN RESOURCES": "Nhân sự",
        "MARKETING": "Marketing",
        "IT": "IT",
        "ADMIN": "Hành chính",
        "HANH CHINH": "Hành chính",
        "ADMINISTRATION": "Hành chính",
        "LOGISTICS": "Logistics",
        "LOGISTIC": "Logistics",
        "CHAM SOC KHACH HANG": "Chăm sóc khách hàng",
        "CSKH": "Chăm sóc khách hàng",
        "CUSTOMER SERVICE": "Chăm sóc khách hàng",
        "LE TAN": "Lễ tân",
        "RECEPTION": "Lễ tân",
        "BAO VE": "Bảo vệ",
        "SECURITY": "Bảo vệ",
    }

    return dept_map.get(v, v.title())


def _normalize_shift(value: Any) -> Optional[str]:
    """
    Normalize work shift to standard values.

    Standard shifts:
        - "Sáng" (Morning): 6:00 - 14:00
        - "Chiều" (Afternoon): 14:00 - 22:00
        - "Đêm" (Night): 22:00 - 6:00
        - "Full time": Ca hành chính
    """
    v = clean_string(value, upper=True, strip=True, default=None)
    if v is None:
        return "Sáng"

    shift_map = {
        "SANG": "Sáng",
        "MORNING": "Sáng",
        "DAY": "Sáng",
        "CHIEU": "Chiều",
        "AFTERNOON": "Chiều",
        "DEM": "Đêm",
        "NIGHT": "Đêm",
        "FULL TIME": "Hành chính",
        "CA HANH CHINH": "Hành chính",
        "REGULAR": "Hành chính",
        "ROTATING": "Luân phiên",
        "LUAN PHIEN": "Luân phiên",
    }

    return shift_map.get(v, v.title())


# ---------------------------------------------------------------------------
# Step 7: Add employee aliases
# ---------------------------------------------------------------------------

def _add_employee_aliases(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """
    Add standard DimEmployee column aliases.

    Aliases:
        - EmployeeCode = MaNV
        - FullName    = HoTen
        - Gender      = GioiTinh (normalized)
        - DateOfBirth = NgaySinh
        - Phone       = DienThoai
        - Position    = ChucVu
        - Department  = PhongBan
        - Shift       = CaLamViec
        - HireDate    = NgayVaoLam
        - TerminationDate = NgayNghiViec
    """
    df["EmployeeCode"] = df.get("MaNV", None)
    df["FullName"] = df.get("HoTen", None)

    if "GioiTinh" in df.columns:
        df["Gender"] = df["GioiTinh"].apply(_normalize_gender_employee)
    else:
        df["Gender"] = None

    df["DateOfBirth"] = df.get("NgaySinh", None)
    df["Phone"] = df.get("DienThoai", None)
    df["Position"] = df.get("ChucVu", None)
    df["Department"] = df.get("PhongBan", None)
    df["Shift"] = df.get("CaLamViec", None)
    df["HireDate"] = df.get("NgayVaoLam", None)
    df["TerminationDate"] = df.get("NgayNghiViec", None)

    logger.debug("[%s] Employee aliases added.", tenant_id)

    return df


def _normalize_gender_employee(value: Any) -> Optional[str]:
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


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def get_employee_summary(df: pd.DataFrame, tenant_id: str) -> dict[str, Any]:
    """
    Generate a summary dict of the employee transformation results.
    """
    summary: dict[str, Any] = {
        "tenant_id": tenant_id,
        "total_rows": len(df),
        "demographics": {
            "avg_age": 0.0,
            "avg_tenure_years": 0.0,
            "active_count": 0,
            "inactive_count": 0,
        },
        "gender_distribution": {},
        "position_distribution": {},
        "department_distribution": {},
    }

    if df.empty:
        return summary

    if "Age" in df.columns:
        avg_age = df["Age"].replace(0, pd.NA).mean()
        summary["demographics"]["avg_age"] = (
            float(avg_age) if pd.notna(avg_age) else 0.0
        )

    if "TenureYears" in df.columns:
        avg_tenure = df["TenureYears"].replace(0, pd.NA).mean()
        summary["demographics"]["avg_tenure_years"] = (
            float(avg_tenure) if pd.notna(avg_tenure) else 0.0
        )

    if "IsActive" in df.columns:
        summary["demographics"]["active_count"] = int(df["IsActive"].sum())
        summary["demographics"]["inactive_count"] = int((~df["IsActive"]).sum())

    if "Gender" in df.columns:
        summary["gender_distribution"] = df["Gender"].value_counts().to_dict()

    if "Position" in df.columns:
        summary["position_distribution"] = df["Position"].value_counts().head(10).to_dict()

    if "Department" in df.columns:
        summary["department_distribution"] = df["Department"].value_counts().head(10).to_dict()

    return summary
