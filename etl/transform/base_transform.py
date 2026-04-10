"""
etl/transform/base_transform.py
Phase 10: Shared utility functions for all transform modules.

This module provides reusable transformation primitives used across
all domain-specific transform modules:
    - clean_string()      : Strip, uppercase, remove control chars
    - parse_date()        : Parse date from multiple formats
    - handle_null()       : Replace null/placeholder values
    - safe_float()        : Safe numeric conversion to float
    - safe_int()          : Safe numeric conversion to int
    - normalize_phone()   : Standardize phone number format
    - normalize_email()   : Standardize email format
    - calculate_age()     : Calculate age from date of birth
    - calculate_tenure_days() : Calculate tenure in days from start date

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# String utilities
# ---------------------------------------------------------------------------

def clean_string(
    value: Any,
    upper: bool = True,
    strip: bool = True,
    remove_control_chars: bool = True,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Clean a string value with consistent rules.

    Args:
        value:         Input value of any type.
        upper:         Convert to UPPERCASE. (default True)
        strip:         Strip leading/trailing whitespace. (default True)
        remove_control_chars: Remove control characters. (default True)
        default:       Default value if cleaned result is empty. (optional)

    Returns:
        Cleaned string, or None if result is empty/invalid.

    Examples:
        clean_string("  Hello World  ") -> "HELLO WORLD"
        clean_string(None) -> None
        clean_string("") -> None
        clean_string("  ") -> None
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    try:
        s = str(value)
    except (ValueError, TypeError):
        return default

    if strip:
        s = s.strip()

    if remove_control_chars:
        s = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", s)

    if upper:
        s = s.upper()

    s = re.sub(r"\s+", " ", s)

    if s in ("", "NAN", "NONE", "NULL", "NA", "N/A", "#N/A"):
        return default

    return s if s else default


def clean_strings_in_df(
    df: pd.DataFrame,
    columns: list[str],
    upper: bool = True,
    strip: bool = True,
    default: Optional[str] = None,
) -> pd.DataFrame:
    """
    Apply clean_string() to multiple columns in a DataFrame.

    Args:
        df:       Input DataFrame.
        columns:  List of column names to clean.
        upper:    Convert to UPPERCASE. (default True)
        strip:    Strip whitespace. (default True)
        default:  Default value for nulls. (optional)

    Returns:
        DataFrame with cleaned columns (copy, does not mutate original).
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: clean_string(v, upper=upper, strip=strip, default=default)
            )
    return df


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y%m%d",
]


def parse_date(
    value: Any,
    dayfirst: bool = True,
    max_year: int = 2030,
    min_year: int = 1990,
    default: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    Parse a date value from multiple string/numeric formats.

    Supports formats:
        - DD/MM/YYYY  (most common in Vietnam)
        - DD-MM-YYYY
        - YYYY-MM-DD
        - MM/DD/YYYY
        - YYYYMMDD (int)
        - datetime / Timestamp objects
        - NaT / None

    Args:
        value:     Input date value (str, int, float, datetime, Timestamp).
        dayfirst:  If True, assume DD/MM when ambiguous. (default True)
        max_year:  Reject dates after this year. (default 2030)
        min_year:  Reject dates before this year. (default 1990)
        default:   Return value if parsing fails. (optional)

    Returns:
        datetime object, or default if parsing fails.
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()

    if isinstance(value, datetime):
        try_year = value.year
        if min_year <= try_year <= max_year:
            return value
        logger.debug(
            "Date %s out of range [%d-%d], returning default.",
            value, min_year, max_year
        )
        return default

    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())

    if isinstance(value, (int, float)):
        try:
            if value > 19000101 and value < 21000101:
                str_val = str(int(value))
                for fmt in ["%Y%m%d", "%Y-%m-%d"]:
                    try:
                        parsed = datetime.strptime(str_val, fmt)
                        if min_year <= parsed.year <= max_year:
                            return parsed
                    except ValueError:
                        pass
            if not pd.isna(value):
                parsed = pd.to_datetime(value, dayfirst=dayfirst).to_pydatetime()
                if min_year <= parsed.year <= max_year:
                    return parsed
            return default
        except (ValueError, TypeError, OverflowError):
            return default

    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() in ("NAN", "NONE", "NULL", "", "NA"):
            return default

        for fmt in _DATE_FORMATS:
            try:
                parsed = datetime.strptime(value, fmt)
                if min_year <= parsed.year <= max_year:
                    return parsed
            except ValueError:
                continue

        try:
            parsed = pd.to_datetime(value, dayfirst=dayfirst).to_pydatetime()
            if min_year <= parsed.year <= max_year:
                return parsed
        except (ValueError, TypeError):
            pass

    return default


def parse_dates_in_df(
    df: pd.DataFrame,
    columns: list[str],
    dayfirst: bool = True,
    default: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Apply parse_date() to multiple date columns in a DataFrame.

    Args:
        df:       Input DataFrame.
        columns:  List of date column names to parse.
        dayfirst: Assume DD/MM when ambiguous. (default True)
        default:  Default value for unparseable dates. (optional)

    Returns:
        DataFrame with parsed datetime columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: parse_date(v, dayfirst=dayfirst, default=default)
            )
    return df


# ---------------------------------------------------------------------------
# Numeric utilities
# ---------------------------------------------------------------------------

def safe_float(
    value: Any,
    default: float = 0.0,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> float:
    """
    Safely convert a value to float with bounds checking.

    Args:
        value:   Input value (int, float, str, None).
        default: Value to return if conversion fails. (default 0.0)
        min_val: Minimum allowed value. (optional)
        max_val: Maximum allowed value. (optional)

    Returns:
        Float value within bounds, or default.
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    try:
        result = float(value)
    except (ValueError, TypeError):
        return default

    if result != result:
        return default

    if min_val is not None and result < min_val:
        result = min_val
    if max_val is not None and result > max_val:
        result = max_val

    return result


def safe_int(
    value: Any,
    default: int = 0,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    """
    Safely convert a value to integer with bounds checking.

    Args:
        value:   Input value (int, float, str, None).
        default: Value to return if conversion fails. (default 0)
        min_val: Minimum allowed value. (optional)
        max_val: Maximum allowed value. (optional)

    Returns:
        Integer value within bounds, or default.
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    try:
        result = int(float(value))
    except (ValueError, TypeError):
        return default

    if min_val is not None and result < min_val:
        result = min_val
    if max_val is not None and result > max_val:
        result = max_val

    return result


def safe_floats_in_df(
    df: pd.DataFrame,
    columns: list[str],
    default: float = 0.0,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> pd.DataFrame:
    """
    Apply safe_float() to multiple numeric columns in a DataFrame.

    Args:
        df:       Input DataFrame.
        columns:  List of column names.
        default:  Default value for conversion failures.
        min_val:  Minimum allowed value. (optional)
        max_val:  Maximum allowed value. (optional)

    Returns:
        DataFrame with converted float columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: safe_float(v, default=default, min_val=min_val, max_val=max_val)
            )
    return df


def safe_ints_in_df(
    df: pd.DataFrame,
    columns: list[str],
    default: int = 0,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> pd.DataFrame:
    """
    Apply safe_int() to multiple columns in a DataFrame.

    Args:
        df:       Input DataFrame.
        columns:  List of column names.
        default:  Default value for conversion failures.
        min_val:  Minimum allowed value. (optional)
        max_val:  Maximum allowed value. (optional)

    Returns:
        DataFrame with converted int columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: safe_int(v, default=default, min_val=min_val, max_val=max_val)
            )
    return df


# ---------------------------------------------------------------------------
# Domain-specific utilities
# ---------------------------------------------------------------------------

def handle_null(
    value: Any,
    null_placeholder: Any = None,
    default: Any = None,
) -> Any:
    """
    Replace null/placeholder values with a sensible default.

    Args:
        value:            Input value.
        null_placeholder: Value to consider as null (e.g. 0, -1). (optional)
        default:          Default value to return. (optional)

    Returns:
        Original value if valid, default otherwise.
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    if isinstance(value, str):
        stripped = value.strip().upper()
        if stripped in ("", "NAN", "NONE", "NULL", "NA", "N/A", "#N/A", "NULL"):
            return default

    if null_placeholder is not None and value == null_placeholder:
        return default

    return value


def normalize_phone(value: Any) -> Optional[str]:
    """
    Normalize phone number to standard Vietnamese format.

    Removes all non-digit characters, then:
        - 10-digit mobile: 0XXXXXXXXX
        - 11-digit mobile: 00XXXXXXXXX
        - Landline with area code: kept as-is

    Args:
        value: Phone number string.

    Returns:
        Normalized phone string or None.
    """
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    try:
        s = str(value).strip()
        if not s or s.upper() in ("NAN", "NONE", "NULL", ""):
            return None

        digits_only = re.sub(r"[^\d]", "", s)

        if len(digits_only) < 9 or len(digits_only) > 15:
            return None

        if digits_only.startswith("84"):
            digits_only = "0" + digits_only[2:]

        if digits_only.startswith("0") and len(digits_only) == 10:
            return digits_only

        if len(digits_only) >= 9 and len(digits_only) <= 11:
            return digits_only

        return digits_only if digits_only else None

    except (ValueError, TypeError):
        return None


def normalize_email(value: Any) -> Optional[str]:
    """
    Normalize email address to lowercase and validate format.

    Args:
        value: Email address string.

    Returns:
        Lowercase, stripped email or None if invalid.
    """
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    try:
        s = str(value).strip().lower()
        if not s or s.upper() in ("NAN", "NONE", "NULL", ""):
            return None

        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if re.match(email_pattern, s):
            return s
        return None

    except (ValueError, TypeError):
        return None


def calculate_age(date_of_birth: Any, reference_date: Optional[datetime] = None) -> int:
    """
    Calculate age in years from date of birth.

    Args:
        date_of_birth: Date of birth (datetime, string, or int).
        reference_date: Reference date for calculation. (default: today)

    Returns:
        Age in full years, or 0 if calculation fails.
    """
    if reference_date is None:
        reference_date = datetime.now()

    dob = parse_date(date_of_birth)
    if dob is None:
        return 0

    age = reference_date.year - dob.year

    if (reference_date.month, reference_date.day) < (dob.month, dob.day):
        age -= 1

    if age < 0 or age > 120:
        return 0

    return age


def calculate_tenure_days(
    start_date: Any,
    end_date: Optional[Any] = None,
    reference_date: Optional[datetime] = None,
) -> int:
    """
    Calculate tenure in days from a start date.

    Args:
        start_date:    Start date (e.g. hire date, join date).
        end_date:      End date (e.g. termination date). If None, uses reference_date. (optional)
        reference_date: Reference date for active records. (default: today)

    Returns:
        Number of days, or 0 if calculation fails.
    """
    if reference_date is None:
        reference_date = datetime.now()

    start = parse_date(start_date)
    if start is None:
        return 0

    if end_date is not None:
        end = parse_date(end_date)
        if end is not None:
            delta = end - start
            return max(0, delta.days)
        return 0

    delta = reference_date - start
    tenure_days = delta.days

    if tenure_days < 0 or tenure_days > 36500:
        return 0

    return tenure_days
