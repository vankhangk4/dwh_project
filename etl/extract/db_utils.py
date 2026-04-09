"""
etl/extract/db_utils.py
Phase 9: Shared database utilities for ETL extract module.

Functions:
    - get_db_connection()              : Create SQL Server connection using env vars.
    - close_connection()               : Safely close database connection.
    - execute_sp()                    : Execute stored procedure with parameters.
    - load_dataframe_to_staging()     : Bulk insert DataFrame into staging table.
    - get_last_watermark()            : Read last successful watermark from ETL_Watermark.
    - update_watermark()              : Update watermark (RUNNING/SUCCESS/FAILED).
    - truncate_staging()              : Truncate/delete staging table for tenant.
    - log_etl_run()                  : Log ETL run to ETL_RunLog table.
    - log_error()                     : Log error record to STG_ErrorLog.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_db_connection(
    autocommit: bool = False,
    timeout: int = 30,
) -> pyodbc.Connection:
    """
    Create a SQL Server connection using environment variables.

    Required env vars:
        CONN_STR  — Full ODBC connection string.
                    Example:
                    "Driver={ODBC Driver 17 for SQL Server};"
                    "Server=localhost,1433;"
                    "Database=DWH_RetailTech;"
                    "UID=sa;Pwd=YourStrong@Passw0rd"

    Args:
        autocommit: If True, connection runs in autocommit mode.
        timeout:     Connection timeout in seconds.

    Returns:
        pyodbc.Connection object.

    Raises:
        ConnectionError: If CONN_STR env var is missing or connection fails.
    """
    conn_str = os.environ.get("CONN_STR")
    if not conn_str:
        logger.error("CONN_STR environment variable is not set.")
        raise ConnectionError(
            "CONN_STR environment variable is not set. "
            "Please set it before running ETL."
        )

    try:
        conn = pyodbc.connect(
            conn_str,
            autocommit=autocommit,
            timeout=timeout,
        )
        logger.info("Database connection established successfully.")
        return conn
    except pyodbc.Error as ex:
        logger.error("Failed to connect to database: %s", ex)
        raise ConnectionError(f"Database connection failed: {ex}") from ex


def close_connection(conn: Optional[pyodbc.Connection]) -> None:
    """
    Safely close a database connection.

    Args:
        conn: pyodbc.Connection object or None.
    """
    if conn is not None:
        try:
            conn.close()
            logger.debug("Database connection closed.")
        except Exception as ex:
            logger.warning("Error closing connection: %s", ex)


# ---------------------------------------------------------------------------
# Stored Procedure execution
# ---------------------------------------------------------------------------

def execute_sp(
    conn: pyodbc.Connection,
    stored_proc_name: str,
    params: Optional[dict[str, Any]] = None,
    fetch: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Execute a stored procedure with optional parameters.

    Args:
        conn:              Active pyodbc connection.
        stored_proc_name: Name of stored procedure (e.g. 'usp_Load_DimProduct').
        params:            Dict of parameter names → values.  (optional)
        fetch:             If True, returns results as DataFrame. (optional)

    Returns:
        DataFrame if fetch=True, else None.

    Raises:
        pyodbc.Error: If the stored procedure execution fails.

    Example:
        execute_sp(conn, "usp_Load_DimProduct")
        execute_sp(conn, "usp_Transform_FactSales",
                   {"@TenantID": "STORE_HN", "@BatchDate": "2024-01-15"})
    """
    logger.info("Executing stored procedure: %s", stored_proc_name)

    try:
        cursor = conn.cursor()

        if params:
            # Build parameter list for pyodbc
            param_values = list(params.values())
            param_placeholders = ", ".join(["?" for _ in params])

            if fetch:
                # Use cursor.execute with list of values
                cursor.execute(
                    f"EXEC {stored_proc_name} {param_placeholders}",
                    param_values,
                )
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                cursor.close()
                logger.info(
                    "Stored procedure %s executed with %d rows returned.",
                    stored_proc_name, len(rows)
                )
                return pd.DataFrame.from_records(rows, columns=columns)
            else:
                # No fetch needed
                cursor.execute(
                    f"EXEC {stored_proc_name} {param_placeholders}",
                    param_values,
                )
                cursor.close()
                logger.info(
                    "Stored procedure %s executed successfully.",
                    stored_proc_name
                )
                return None
        else:
            # No params
            cursor.execute(f"EXEC {stored_proc_name}")
            cursor.close()
            logger.info(
                "Stored procedure %s executed successfully.",
                stored_proc_name
            )
            return None

    except pyodbc.Error as ex:
        logger.error("Stored procedure %s failed: %s", stored_proc_name, ex)
        raise


def execute_sql(
    conn: pyodbc.Connection,
    sql: str,
    params: Optional[list[Any]] = None,
    fetch: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Execute raw SQL with optional parameters.

    Args:
        conn:   Active pyodbc connection.
        sql:    Raw SQL string.
        params: List of parameter values. (optional)
        fetch:  If True, returns results as DataFrame. (optional)

    Returns:
        DataFrame if fetch=True, else None.
    """
    logger.debug("Executing SQL: %s", sql[:200])

    try:
        cursor = conn.cursor()

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if fetch:
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            return pd.DataFrame.from_records(rows, columns=columns)
        else:
            cursor.close()
            return None

    except pyodbc.Error as ex:
        logger.error("SQL execution failed: %s", ex)
        raise


# ---------------------------------------------------------------------------
# Bulk load DataFrame to staging table
# ---------------------------------------------------------------------------

def load_dataframe_to_staging(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    staging_table: str,
    tenant_id: str,
    batch_date: Optional[str] = None,
    source_file: Optional[str] = None,
    commit: bool = True,
) -> int:
    """
    Bulk insert a pandas DataFrame into a staging table.

    Handles:
        - Adds TenantID to every row.
        - Adds STG_LoadDatetime timestamp.
        - Adds STG_SourceFile path.
        - Replaces NaN with None (SQL NULL).

    Args:
        conn:            Active pyodbc connection (NOT autocommit).
        df:              DataFrame to insert.
        staging_table:   Target staging table name (e.g. 'STG_SalesRaw').
        tenant_id:       Tenant identifier (e.g. 'STORE_HN').
        batch_date:      Batch date string 'YYYY-MM-DD'.  (optional)
        source_file:     Source file path for audit.     (optional)
        commit:          If True, commits the transaction. (default True)

    Returns:
        Number of rows inserted.

    Raises:
        pyodbc.Error: If the bulk insert fails.
        ValueError:   If DataFrame is None or empty.
    """
    if df is None or df.empty:
        logger.warning(
            "DataFrame is empty or None. Nothing to load into %s.",
            staging_table
        )
        return 0

    logger.info(
        "Loading %d rows into %s for tenant [%s].",
        len(df), staging_table, tenant_id
    )

    # Make a copy to avoid mutating the original DataFrame
    df_load = df.copy()

    # Normalize column names: strip whitespace
    df_load.columns = df_load.columns.str.strip()

    # Add system audit columns
    df_load["TenantID"] = tenant_id
    df_load["STG_LoadDatetime"] = datetime.now()
    if source_file:
        df_load["STG_SourceFile"] = source_file

    # Replace NaN/NaT with None (SQL NULL)
    df_load = df_load.where(pd.notnull(df_load), None)

    try:
        cursor = conn.cursor()

        # Build INSERT statement
        columns = list(df_load.columns)
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = (
            f"INSERT INTO {staging_table} "
            f"({', '.join(columns)}) VALUES ({placeholders})"
        )

        rows_inserted = 0
        batch_size = int(os.environ.get("ETL_BATCH_SIZE", "1000"))

        for start in range(0, len(df_load), batch_size):
            batch = df_load.iloc[start: start + batch_size]
            for _, row in batch.iterrows():
                values = _prepare_values(row.tolist())
                cursor.execute(insert_sql, values)
                rows_inserted += 1

        cursor.close()

        if commit:
            conn.commit()

        logger.info(
            "Successfully loaded %d rows into %s for tenant [%s].",
            rows_inserted, staging_table, tenant_id
        )
        return rows_inserted

    except pyodbc.Error as ex:
        logger.error(
            "Failed to load DataFrame into %s: %s",
            staging_table, ex
        )
        conn.rollback()
        raise


def _prepare_values(values: list[Any]) -> list[Any]:
    """
    Convert DataFrame row values to Python types compatible with pyodbc.

    Args:
        values: List of values from a DataFrame row.

    Returns:
        List of values with proper type conversion.
    """
    result = []
    for v in values:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            result.append(None)
        elif isinstance(v, pd.Timestamp):
            result.append(v.to_pydatetime())
        elif isinstance(v, datetime):
            result.append(v)
        elif isinstance(v, (int, float)):
            result.append(v)
        else:
            result.append(str(v))
    return result


# ---------------------------------------------------------------------------
# Watermark management
# ---------------------------------------------------------------------------

def get_last_watermark(
    conn: pyodbc.Connection,
    tenant_id: str,
    source_type: str,
) -> datetime:
    """
    Read the last successful watermark (datetime) for a tenant + source.

    Args:
        conn:        Active pyodbc connection.
        tenant_id:   Tenant ID (e.g. 'STORE_HN').
        source_type: Source type (e.g. 'Sales_Excel', 'Inventory_Excel').

    Returns:
        datetime of last successful extraction.
        Defaults to '2020-01-01' if no watermark exists.

    Example:
        wm = get_last_watermark(conn, "STORE_HN", "Sales_Excel")
        # Returns: datetime.datetime(2024, 1, 15, 0, 0)
    """
    source_name = f"{tenant_id}_{source_type}"
    logger.info(
        "Getting last watermark for [%s] (source: %s).",
        source_name, source_type
    )

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT TOP 1 WatermarkValue
            FROM ETL_Watermark
            WHERE SourceName = ?
              AND LastRunStatus = 'SUCCESS'
            ORDER BY LastRunDatetime DESC
            """,
            (source_name,),
        )
        row = cursor.fetchone()
        cursor.close()

        if row is not None and row[0] is not None:
            wm = row[0]
            logger.info(
                "Last watermark for [%s]: %s",
                source_name,
                wm.strftime("%Y-%m-%d %H:%M:%S"),
            )
            return wm

        # Default: start from 2020-01-01 if no watermark found
        default_wm = datetime(2020, 1, 1, 0, 0, 0)
        logger.info(
            "No watermark found for [%s]. Using default: %s",
            source_name,
            default_wm.strftime("%Y-%m-%d"),
        )
        return default_wm

    except pyodbc.Error as ex:
        logger.warning(
            "Error reading watermark for [%s]: %s. Using default 2020-01-01.",
            source_name, ex
        )
        return datetime(2020, 1, 1, 0, 0, 0)


def update_watermark(
    conn: pyodbc.Connection,
    source_name: str,
    tenant_id: str,
    status: str,
    rows_extracted: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    notes: Optional[str] = None,
    source_type: Optional[str] = None,
    commit: bool = True,
) -> None:
    """
    Update or insert a watermark record.

    Args:
        conn:             Active pyodbc connection.
        source_name:      Full source name (e.g. 'STORE_HN_Sales_Excel').
        tenant_id:        Tenant ID.
        status:           'RUNNING' | 'SUCCESS' | 'FAILED'.
        rows_extracted:   Number of rows extracted.   (optional)
        duration_seconds: ETL run duration in seconds. (optional)
        notes:            Free-text notes.             (optional)
        source_type:      Source type for new record. (optional)
        commit:           If True, commits transaction. (default True)

    Raises:
        ValueError: If status is not valid.
        pyodbc.Error: If the update fails.
    """
    valid_statuses = {"RUNNING", "SUCCESS", "FAILED"}
    if status not in valid_statuses:
        raise ValueError(
            f"Status must be one of {valid_statuses}. Got: {status}"
        )

    logger.info(
        "Updating watermark [%s] -> status=%s, rows=%s.",
        source_name, status, rows_extracted
    )

    try:
        cursor = conn.cursor()

        if status == "RUNNING":
            upsert_sql = """
                IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = ?)
                BEGIN
                    UPDATE ETL_Watermark
                    SET LastRunStatus = 'RUNNING',
                        LastRunDatetime = GETDATE(),
                        Notes = ISNULL(?, Notes)
                    WHERE SourceName = ?;
                END
                ELSE
                BEGIN
                    INSERT INTO ETL_Watermark
                        (SourceName, TenantID, SourceType, WatermarkValue,
                         LastRunStatus, LastRunDatetime,
                         RowsExtracted, DurationSeconds, Notes)
                    VALUES (?, ?, ?, '2020-01-01', 'RUNNING',
                            GETDATE(), ?, ?, ?);
                END
            """
            cursor.execute(
                upsert_sql,
                source_name, notes, source_name,
                source_name, tenant_id,
                source_type or source_name,
                rows_extracted, duration_seconds, notes,
            )

        elif status == "SUCCESS":
            upsert_sql = """
                IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = ?)
                BEGIN
                    UPDATE ETL_Watermark
                    SET WatermarkValue = GETDATE(),
                        LastRunStatus = 'SUCCESS',
                        LastRunDatetime = GETDATE(),
                        RowsExtracted = ISNULL(?, RowsExtracted),
                        DurationSeconds = ISNULL(?, DurationSeconds),
                        Notes = ISNULL(?, Notes)
                    WHERE SourceName = ?;
                END
                ELSE
                BEGIN
                    INSERT INTO ETL_Watermark
                        (SourceName, TenantID, SourceType, WatermarkValue,
                         LastRunStatus, LastRunDatetime,
                         RowsExtracted, DurationSeconds, Notes)
                    VALUES (?, ?, ?, GETDATE(), 'SUCCESS',
                            GETDATE(), ?, ?, ?);
                END
            """
            cursor.execute(
                upsert_sql,
                source_name,
                rows_extracted, duration_seconds, notes, source_name,
                source_name, tenant_id,
                source_type or source_name,
                rows_extracted, duration_seconds, notes,
            )

        elif status == "FAILED":
            upsert_sql = """
                IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = ?)
                BEGIN
                    UPDATE ETL_Watermark
                    SET LastRunStatus = 'FAILED',
                        LastRunDatetime = GETDATE(),
                        DurationSeconds = ISNULL(?, DurationSeconds),
                        Notes = ISNULL(?, Notes)
                    WHERE SourceName = ?;
                    -- NOTE: WatermarkValue is NOT updated on FAILURE
                    -- This allows retry from the same point.
                END
                ELSE
                BEGIN
                    INSERT INTO ETL_Watermark
                        (SourceName, TenantID, SourceType, WatermarkValue,
                         LastRunStatus, LastRunDatetime,
                         RowsExtracted, DurationSeconds, Notes)
                    VALUES (?, ?, ?, '2020-01-01', 'FAILED',
                            GETDATE(), ?, ?, ?);
                END
            """
            cursor.execute(
                upsert_sql,
                source_name,
                duration_seconds, notes, source_name,
                source_name, tenant_id,
                source_type or source_name,
                rows_extracted, duration_seconds, notes,
            )

        cursor.close()

        if commit:
            conn.commit()

        logger.info(
            "Watermark [%s] updated to %s successfully.",
            source_name, status
        )

    except pyodbc.Error as ex:
        logger.error(
            "Failed to update watermark [%s]: %s",
            source_name, ex
        )
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Truncate staging table
# ---------------------------------------------------------------------------

def truncate_staging(
    conn: pyodbc.Connection,
    tenant_id: Optional[str] = None,
    commit: bool = True,
) -> int:
    """
    Truncate or delete staging tables for a specific tenant (or all tenants).

    Args:
        conn:       Active pyodbc connection.
        tenant_id:  If provided, deletes rows WHERE TenantID = tenant_id.
                    If None, truncates all staging tables.    (optional)
        commit:     If True, commits transaction.           (default True)

    Returns:
        Number of tables processed.
    """
    staging_tables = [
        "STG_SalesRaw",
        "STG_InventoryRaw",
        "STG_PurchaseRaw",
        "STG_ProductRaw",
        "STG_CustomerRaw",
        "STG_EmployeeRaw",
        "STG_StoreRaw",
        "STG_SupplierRaw",
    ]
    # NOTE: STG_ErrorLog is NEVER truncated — only INSERT + SELECT

    cursor = conn.cursor()
    tables_processed = 0

    for table in staging_tables:
        try:
            if tenant_id:
                # Check if table has TenantID column
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM sys.columns
                    WHERE object_id = OBJECT_ID(?)
                      AND name = 'TenantID'
                    """,
                    (table,),
                )
                has_tenant = cursor.fetchone()[0] > 0

                if has_tenant:
                    delete_sql = f"DELETE FROM {table} WHERE TenantID = ?"
                    cursor.execute(delete_sql, (tenant_id,))
                    logger.info(
                        "Deleted rows from %s WHERE TenantID = %s.",
                        table, tenant_id
                    )
                else:
                    # Shared tables (STG_ProductRaw, STG_SupplierRaw) — always truncate
                    cursor.execute(f"TRUNCATE TABLE {table}")
                    logger.info("Truncated shared table %s.", table)
            else:
                # Truncate all rows
                cursor.execute(f"TRUNCATE TABLE {table}")
                logger.info("Truncated table %s.", table)

            tables_processed += 1

        except pyodbc.Error as ex:
            # Table might not exist yet — skip
            logger.warning(
                "Could not truncate %s (may not exist): %s",
                table, ex
            )

    cursor.close()

    if commit:
        conn.commit()

    logger.info(
        "Staging truncation completed. %d tables processed.",
        tables_processed
    )
    return tables_processed


# ---------------------------------------------------------------------------
# ETL Run Logging
# ---------------------------------------------------------------------------

def log_etl_run(
    conn: pyodbc.Connection,
    tenant_id: str,
    stored_proc_name: str,
    status: str,
    rows_processed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    rows_failed: int = 0,
    error_message: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    pipeline_name: Optional[str] = None,
    commit: bool = True,
) -> None:
    """
    Log an ETL run into ETL_RunLog table.

    Args:
        conn:               Active pyodbc connection.
        tenant_id:          Tenant identifier.
        stored_proc_name:   Name of the stored procedure or pipeline step.
        status:             'SUCCESS' | 'FAILED' | 'RUNNING'.
        rows_processed:     Total rows processed.       (default 0)
        rows_inserted:      Rows inserted.           (default 0)
        rows_updated:       Rows updated.            (default 0)
        rows_skipped:      Rows skipped.           (default 0)
        rows_failed:       Rows with errors.        (default 0)
        error_message:     Error description.       (optional)
        start_time:        ETL start time.         (optional, defaults to now)
        end_time:          ETL end time.           (optional, defaults to now)
        pipeline_name:     Pipeline name for tracking. (optional)
        commit:            Commit transaction.       (default True)
    """
    if start_time is None:
        start_time = datetime.now()
    if end_time is None:
        end_time = datetime.now()

    duration_seconds = int((end_time - start_time).total_seconds())
    run_date = start_time.date()

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ETL_RunLog (
                TenantID, StoredProcedureName, PipelineName, RunDate,
                Status, RowsProcessed, RowsInserted, RowsUpdated,
                RowsSkipped, RowsFailed,
                ErrorMessage,
                StartTime, EndTime, DurationSeconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                stored_proc_name,
                pipeline_name,
                run_date,
                status,
                rows_processed,
                rows_inserted,
                rows_updated,
                rows_skipped,
                rows_failed,
                error_message,
                start_time,
                end_time,
                duration_seconds,
            ),
        )
        cursor.close()

        if commit:
            conn.commit()

        logger.info(
            "ETL run logged: [%s] %s — %s (%d rows processed, %ds)",
            tenant_id, stored_proc_name, status,
            rows_processed, duration_seconds,
        )

    except pyodbc.Error as ex:
        logger.error("Failed to log ETL run: %s", ex)
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Error Logging
# ---------------------------------------------------------------------------

def log_error(
    conn: pyodbc.Connection,
    tenant_id: str,
    source_table: str,
    error_type: str,
    error_message: str,
    raw_data: Optional[str] = None,
    source_key: Optional[str] = None,
    batch_date: Optional[str] = None,
    commit: bool = True,
) -> None:
    """
    Log an error record into STG_ErrorLog table.

    Args:
        conn:           Active pyodbc connection.
        tenant_id:      Tenant identifier.
        source_table:   Source table name (e.g. 'STG_SalesRaw').
        error_type:     Error type (e.g. 'DIMENSION_NOT_FOUND', 'VALIDATION_ERROR').
        error_message:  Human-readable error message.
        raw_data:       Raw data causing the error (JSON string). (optional)
        source_key:     Business key of the failing record.  (optional)
        batch_date:     Batch date string 'YYYY-MM-DD'.      (optional)
        commit:         Commit transaction.                    (default True)
    """
    if batch_date is None:
        batch_date = datetime.now().strftime("%Y-%m-%d")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO STG_ErrorLog (
                TenantID, SourceTable, ErrorType, ErrorMessage,
                SourceKey, RawData, BatchDate,
                IsResolved, ETLRunDate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, GETDATE())
            """,
            (
                tenant_id,
                source_table,
                error_type,
                error_message,
                source_key,
                raw_data,
                batch_date,
            ),
        )
        cursor.close()

        if commit:
            conn.commit()

        logger.warning(
            "Error logged: [%s] %s | %s | %s",
            tenant_id, source_table, error_type, error_message
        )

    except pyodbc.Error as ex:
        logger.error("Failed to log error: %s", ex)
        conn.rollback()
        raise
