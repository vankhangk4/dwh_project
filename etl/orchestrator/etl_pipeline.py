"""
etl/orchestrator/etl_pipeline.py
Phase 11: Core ETL pipeline for a single tenant.

Orchestrates the full ETL process for one tenant:
    1. EXTRACT  : Read raw data from Excel/CSV files
    2. TRANSFORM: Clean, validate, and enrich data
    3. LOAD STAGING: Bulk insert into staging tables
    4. EXECUTE SP : Call stored procedures for dimension/fact loading
    5. REFRESH DM: Refresh data marts
    6. UPDATE WM  : Update watermark on success
    7. ALERT      : Send notification on completion/failure

Pipeline step order:
    [Shared Dimensions]  -> Product -> Supplier
    [Tenant Dimensions]   -> Store -> Employee -> Customer
    [Facts]               -> Sales -> Inventory -> Purchase
    [Data Marts]          -> DM_SalesSummary -> DM_InventoryAlert -> etc.
    [Cleanup]             -> Truncate staging -> Update watermark

Usage:
    run_etl_for_tenant(
        tenant_id="STORE_HN",
        file_paths={
            "sales": "./data/STORE_HN/BaoCaoDoanhThu.xlsx",
            "inventory": "./data/STORE_HN/QuanLyKho.xlsx",
            ...
        },
        conn_str="Driver=...;Server=...;Database=...;Uid=sa;Pwd=..."
    )

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from etl.extract.db_utils import (
    get_db_connection,
    close_connection,
    load_dataframe_to_staging,
    update_watermark,
    truncate_staging,
    log_etl_run,
    log_error,
)
from etl.utils.logging_config import get_logger
from etl.utils.monitoring import (
    alert_pipeline_start,
    alert_pipeline_success,
    alert_pipeline_failure,
    alert_pipeline_warning,
    AlertLevel,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main pipeline function
# ---------------------------------------------------------------------------

def run_etl_for_tenant(
    tenant_id: str,
    file_paths: dict[str, str],
    conn_str: Optional[str] = None,
    conn=None,
    close_conn_on_exit: bool = True,
    truncate_before_load: bool = True,
    run_transform: bool = True,
    run_staging_load: bool = True,
    run_sp_load: bool = True,
    run_datamart: bool = True,
    send_alerts: bool = True,
    stage_only: bool = False,
    skip_sources: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Run the full ETL pipeline for a single tenant.

    This function orchestrates the complete Extract -> Transform -> Load ->
    Execute SP -> Refresh DM pipeline for all configured data sources.

    Args:
        tenant_id:            Tenant identifier (e.g. 'STORE_HN').
        file_paths:           Dict of source type -> file path.
                              Keys: sales, inventory, purchase, product,
                                    customer, employee, store, supplier
        conn_str:             SQL Server connection string.
                              If None, reads from CONN_STR env var. (optional)
        conn:                Existing database connection.
                              If provided, uses it instead of creating new. (optional)
        close_conn_on_exit:   Close the connection when done. (default True)
        truncate_before_load:  Truncate staging tables before loading new data. (default True)
        run_transform:        Run transform step. (default True)
        run_staging_load:     Load data into staging tables. (default True)
        run_sp_load:          Execute stored procedures for dimension/fact loading. (default True)
        run_datamart:         Refresh data marts. (default True)
        send_alerts:          Send email/Slack alerts on completion/failure. (default True)
        stage_only:           Only run Extract + Transform + Load to staging.
                              Skips SP execution and datamart refresh. (default False)
        skip_sources:         List of source types to skip. (optional)

    Returns:
        Dict with pipeline results:
            {
                "tenant_id": "STORE_HN",
                "status": "SUCCESS" | "WARNING" | "FAILED",
                "start_time": datetime,
                "end_time": datetime,
                "duration_seconds": int,
                "steps": {
                    "extract": {...},
                    "transform": {...},
                    "staging_load": {...},
                    "sp_load": {...},
                    "datamart": {...},
                },
                "errors": [...],
                "total_rows_processed": int,
                "total_rows_loaded": int,
            }

    Raises:
        RuntimeError: Only on catastrophic failure (not on individual step failures).

    Example:
        result = run_etl_for_tenant(
            tenant_id="STORE_HN",
            file_paths={
                "sales": "./data/STORE_HN/BaoCaoDoanhThu.xlsx",
                "inventory": "./data/STORE_HN/QuanLyKho.xlsx",
                "purchase": "./data/STORE_HN/PhieuNhapKho.xlsx",
                "customer": "./data/STORE_HN/DanhSachKhachHang.xlsx",
                "employee": "./data/STORE_HN/DanhSachNhanVien.xlsx",
                "product": "./data/DanhMucSanPham.csv",
            }
        )
        print(result["status"])  # "SUCCESS"
    """
    pipeline_name = "run_etl_for_tenant"
    log = get_logger("etl.pipeline", tenant_id=tenant_id, pipeline_name=pipeline_name)

    skip_sources = set(skip_sources or [])

    start_time = datetime.now()
    result: dict[str, Any] = {
        "tenant_id": tenant_id,
        "status": "FAILED",
        "start_time": start_time.isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "steps": {},
        "errors": [],
        "warnings": [],
        "total_rows_extracted": 0,
        "total_rows_loaded": 0,
        "total_rows_processed": 0,
    }

    log.info("=" * 60)
    log.info("[%s] ETL pipeline starting", tenant_id)
    log.info("[%s] File paths: %s", tenant_id, _summarize_file_paths(file_paths))

    conn_provided = conn is not None
    if conn is None:
        if conn_str is None:
            conn_str = os.environ.get("CONN_STR")
        if conn_str is None:
            log.error("[%s] No connection string available", tenant_id)
            raise RuntimeError(
                f"ETL pipeline for tenant {tenant_id} requires a connection string"
            )
        log.info("[%s] Creating new database connection", tenant_id)
        conn = get_db_connection(autocommit=False)

    try:
        if send_alerts:
            try:
                alert_pipeline_start(tenant_id, pipeline_name, {
                    "file_paths": _summarize_file_paths(file_paths),
                    "stage_only": str(stage_only),
                })
            except Exception as ex:
                log.warning("[%s] Start alert failed: %s", tenant_id, ex)

        # ---- Update watermark to RUNNING ----
        _update_watermark(conn, tenant_id, "RUNNING", None, pipeline_name=pipeline_name)

        # ---- STEP 1: Truncate staging tables ----
        if truncate_before_load:
            _step_truncate_staging(conn, tenant_id, log)

        # ---- STEP 2: Extract + Transform + Load for each source ----
        step_names = [
            "product", "supplier",
            "store", "employee", "customer",
            "sales", "inventory", "purchase",
        ]

        extract_results: dict[str, dict] = {}
        transform_results: dict[str, dict] = {}
        staging_results: dict[str, dict] = {}

        for step_name in step_names:
            if step_name in skip_sources:
                log.info("[%s] Skipping step: %s (in skip list)", tenant_id, step_name)
                continue

            file_path = file_paths.get(step_name)
            if file_path is None:
                log.debug("[%s] No file path for source '%s', skipping", tenant_id, step_name)
                continue

            if not os.path.exists(file_path):
                log.warning(
                    "[%s] File not found for source '%s': %s",
                    tenant_id, step_name, file_path
                )
                result["warnings"].append(f"File not found: {file_path}")
                continue

            step_result = _run_pipeline_step(
                conn=conn,
                tenant_id=tenant_id,
                source_type=step_name,
                file_path=file_path,
                log=get_logger(f"etl.step.{step_name}", tenant_id=tenant_id, pipeline_name=step_name),
                run_transform=run_transform,
                run_staging_load=run_staging_load,
                stage_only=stage_only,
            )

            extract_results[step_name] = step_result.get("extract", {})
            transform_results[step_name] = step_result.get("transform", {})
            staging_results[step_name] = step_result.get("staging", {})

            result["steps"][step_name] = step_result
            result["total_rows_extracted"] += step_result.get("rows_extracted", 0)
            result["total_rows_loaded"] += step_result.get("rows_loaded", 0)

            if step_result.get("errors"):
                result["errors"].extend(step_result["errors"])
                for err in step_result["errors"]:
                    log.error("[%s][%s] Step error: %s", tenant_id, step_name, err)

        # ---- STEP 3: Execute stored procedures (dimension + fact loading) ----
        if run_sp_load and not stage_only:
            sp_result = _run_sp_load(
                conn=conn,
                tenant_id=tenant_id,
                log=log,
                steps_completed=list(extract_results.keys()),
            )
            result["steps"]["sp_load"] = sp_result
            result["total_rows_processed"] = sp_result.get("rows_processed", 0)
        else:
            result["steps"]["sp_load"] = {"status": "SKIPPED", "rows_processed": 0}

        # ---- STEP 4: Refresh data marts ----
        if run_datamart and not stage_only:
            dm_result = _run_datamart_refresh(
                conn=conn,
                tenant_id=tenant_id,
                log=log,
            )
            result["steps"]["datamart"] = dm_result
        else:
            result["steps"]["datamart"] = {"status": "SKIPPED"}

        # ---- Determine overall status ----
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        result["end_time"] = end_time.isoformat()
        result["duration_seconds"] = duration

        if result["errors"]:
            error_count = len(result["errors"])
            if result["warnings"]:
                result["status"] = "WARNING"
            else:
                result["status"] = "WARNING" if error_count <= 2 else "FAILED"
            log.warning(
                "[%s] ETL pipeline completed with %d error(s), %d warning(s) in %ds",
                tenant_id, error_count, len(result["warnings"]), duration
            )
        else:
            result["status"] = "SUCCESS"
            log.info(
                "[%s] ETL pipeline completed SUCCESSFULLY in %ds | "
                "Extracted: %d rows | Loaded: %d rows",
                tenant_id, duration,
                result["total_rows_extracted"],
                result["total_rows_loaded"]
            )

        # ---- Update watermark to SUCCESS ----
        _update_watermark(
            conn, tenant_id, "SUCCESS",
            rows_extracted=result["total_rows_extracted"],
            duration_seconds=duration,
            pipeline_name=pipeline_name,
        )

        # ---- Log ETL run ----
        try:
            log_etl_run(
                conn=conn,
                tenant_id=tenant_id,
                stored_proc_name="run_etl_for_tenant",
                status=result["status"],
                rows_processed=result["total_rows_processed"],
                rows_inserted=result["total_rows_loaded"],
                start_time=start_time,
                end_time=end_time,
                pipeline_name=pipeline_name,
                commit=True,
            )
        except Exception as ex:
            log.warning("[%s] Failed to log ETL run: %s", tenant_id, ex)

        # ---- Send completion alerts ----
        if send_alerts:
            try:
                if result["status"] == "SUCCESS":
                    alert_pipeline_success(
                        tenant_id=tenant_id,
                        pipeline_name=pipeline_name,
                        rows_processed=result["total_rows_loaded"],
                        duration_seconds=duration,
                        context={
                            "extracted_rows": str(result["total_rows_extracted"]),
                            "loaded_rows": str(result["total_rows_loaded"]),
                            "sources_processed": str(len(extract_results)),
                        },
                    )
                elif result["status"] == "WARNING":
                    alert_pipeline_warning(
                        message=f"ETL completed with {len(result['errors'])} error(s)",
                        tenant_id=tenant_id,
                        pipeline_name=pipeline_name,
                        context={
                            "error_count": str(len(result["errors"])),
                            "warning_count": str(len(result["warnings"])),
                        },
                    )
                else:
                    alert_pipeline_failure(
                        tenant_id=tenant_id,
                        pipeline_name=pipeline_name,
                        error_detail=f"{len(result['errors'])} errors occurred. "
                                     f"Check ETL logs for details.",
                    )
            except Exception as ex:
                log.warning("[%s] Completion alert failed: %s", tenant_id, ex)

        log.info("[%s] ETL pipeline: %s", tenant_id, result["status"])
        log.info("[%s] Total duration: %ds", tenant_id, duration)
        log.info("=" * 60)

        return result

    except Exception as ex:
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())

        log.error(
            "[%s] ETL pipeline CATASTROPHIC FAILURE: %s",
            tenant_id, ex, exc_info=True
        )

        result["status"] = "FAILED"
        result["end_time"] = end_time.isoformat()
        result["duration_seconds"] = duration
        result["errors"].append(f"Catastrophic failure: {ex}")

        try:
            _update_watermark(
                conn, tenant_id, "FAILED",
                duration_seconds=duration,
                pipeline_name=pipeline_name,
            )
        except Exception:
            pass

        try:
            log_etl_run(
                conn=conn,
                tenant_id=tenant_id,
                stored_proc_name="run_etl_for_tenant",
                status="FAILED",
                error_message=str(ex),
                start_time=start_time,
                end_time=end_time,
                pipeline_name=pipeline_name,
                commit=True,
            )
        except Exception:
            pass

        if send_alerts:
            try:
                alert_pipeline_failure(
                    tenant_id=tenant_id,
                    pipeline_name=pipeline_name,
                    error_detail=str(ex),
                )
            except Exception:
                pass

        return result

    finally:
        if not conn_provided and close_conn_on_exit:
            close_connection(conn)


# ---------------------------------------------------------------------------
# Individual pipeline step
# ---------------------------------------------------------------------------

def _run_pipeline_step(
    conn,
    tenant_id: str,
    source_type: str,
    file_path: str,
    log,
    run_transform: bool = True,
    run_staging_load: bool = True,
    stage_only: bool = False,
) -> dict[str, Any]:
    """
    Run a single ETL step (Extract + Transform + Load to staging) for one source.

    Args:
        conn:           Active database connection.
        tenant_id:      Tenant identifier.
        source_type:    Source type (sales, inventory, product, etc.).
        file_path:      Path to the source file.
        log:            Scoped logger.
        run_transform: Run transform step. (default True)
        run_staging_load: Load to staging table. (default True)
        stage_only:    Only stage (skip SP). (default False)

    Returns:
        Dict with extract/transform/staging results.
    """
    step_result: dict[str, Any] = {
        "source_type": source_type,
        "file_path": file_path,
        "status": "SUCCESS",
        "errors": [],
        "warnings": [],
        "rows_extracted": 0,
        "rows_transformed": 0,
        "rows_loaded": 0,
        "extract": {},
        "transform": {},
        "staging": {},
    }

    staging_table_map = {
        "sales": "STG_SalesRaw",
        "inventory": "STG_InventoryRaw",
        "product": "STG_ProductRaw",
        "customer": "STG_CustomerRaw",
        "employee": "STG_EmployeeRaw",
        "store": "STG_StoreRaw",
        "purchase": "STG_PurchaseRaw",
        "supplier": "STG_SupplierRaw",
    }
    staging_table = staging_table_map.get(source_type, f"STG_{source_type.title()}Raw")

    # ---- EXTRACT ----
    try:
        df_extracted = _extract_source(
            source_type=source_type,
            file_path=file_path,
            tenant_id=tenant_id,
        )
        step_result["rows_extracted"] = len(df_extracted)
        step_result["extract"] = {
            "rows": len(df_extracted),
            "columns": list(df_extracted.columns),
        }
        log.info(
            "[%s][%s] Extracted %d rows from %s",
            tenant_id, source_type, len(df_extracted), file_path
        )
    except Exception as ex:
        log.error(
            "[%s][%s] Extraction failed: %s",
            tenant_id, source_type, ex, exc_info=True
        )
        step_result["errors"].append(f"Extraction failed: {ex}")
        step_result["status"] = "FAILED"
        return step_result

    if df_extracted.empty:
        log.warning(
            "[%s][%s] No data extracted from %s",
            tenant_id, source_type, file_path
        )
        step_result["warnings"].append("No data in source file")
        return step_result

    # ---- TRANSFORM ----
    if run_transform:
        try:
            df_transformed = _transform_source(
                source_type=source_type,
                df=df_extracted,
                tenant_id=tenant_id,
            )
            step_result["rows_transformed"] = len(df_transformed)
            step_result["transform"] = {
                "rows": len(df_transformed),
                "filtered": len(df_extracted) - len(df_transformed),
            }
            log.info(
                "[%s][%s] Transformed %d rows (filtered %d invalid)",
                tenant_id, source_type,
                len(df_transformed),
                len(df_extracted) - len(df_transformed)
            )
        except Exception as ex:
            log.error(
                "[%s][%s] Transform failed: %s",
                tenant_id, source_type, ex, exc_info=True
            )
            step_result["errors"].append(f"Transform failed: {ex}")
            step_result["status"] = "FAILED"
            return step_result
    else:
        df_transformed = df_extracted
        step_result["transform"] = {"rows": len(df_transformed), "skipped": True}

    # ---- LOAD TO STAGING ----
    if run_staging_load and not df_transformed.empty:
        try:
            rows_loaded = _load_to_staging(
                conn=conn,
                df=df_transformed,
                staging_table=staging_table,
                tenant_id=tenant_id,
            )
            step_result["rows_loaded"] = rows_loaded
            step_result["staging"] = {
                "table": staging_table,
                "rows": rows_loaded,
            }
            log.info(
                "[%s][%s] Loaded %d rows to %s",
                tenant_id, source_type, rows_loaded, staging_table
            )
        except Exception as ex:
            log.error(
                "[%s][%s] Staging load failed: %s",
                tenant_id, source_type, ex, exc_info=True
            )
            step_result["errors"].append(f"Staging load failed: {ex}")
            step_result["status"] = "FAILED"
            try:
                log_error(
                    conn=conn,
                    tenant_id=tenant_id,
                    source_table=staging_table,
                    error_type="STAGING_LOAD_ERROR",
                    error_message=str(ex),
                    batch_date=datetime.now().strftime("%Y-%m-%d"),
                )
                conn.commit()
            except Exception:
                pass
            return step_result

    return step_result


# ---------------------------------------------------------------------------
# Extract helpers
# ---------------------------------------------------------------------------

def _extract_source(
    source_type: str,
    file_path: str,
    tenant_id: str,
) -> pd.DataFrame:
    """
    Extract data from source file based on type.

    Uses the appropriate extract function from etl.extract module.
    """
    if source_type == "sales":
        from etl.extract.extract_sales import extract_sales_from_excel
        return extract_sales_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "inventory":
        from etl.extract.extract_inventory import extract_inventory_from_excel
        return extract_inventory_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "product":
        from etl.extract.extract_product import extract_products_from_csv
        return extract_products_from_csv(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "customer":
        from etl.extract.extract_customer import extract_customers_from_excel
        return extract_customers_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "employee":
        from etl.extract.extract_employee import extract_employees_from_excel
        return extract_employees_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "store":
        from etl.extract.extract_store import extract_stores_from_excel
        return extract_stores_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "purchase":
        from etl.extract.extract_purchase import extract_purchases_from_excel
        return extract_purchases_from_excel(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    elif source_type == "supplier":
        from etl.extract.extract_supplier import extract_suppliers_from_csv
        return extract_suppliers_from_csv(
            file_path=file_path,
            tenant_id=tenant_id,
        )

    else:
        raise ValueError(f"Unknown source type: {source_type}")


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def _transform_source(
    source_type: str,
    df: pd.DataFrame,
    tenant_id: str,
) -> pd.DataFrame:
    """
    Transform data based on source type.

    Uses the appropriate transform function from etl.transform module.
    """
    if source_type == "sales":
        from etl.transform.transform_sales import transform_sales
        return transform_sales(df, tenant_id=tenant_id)

    elif source_type == "inventory":
        from etl.transform.transform_inventory import transform_inventory
        return transform_inventory(df, tenant_id=tenant_id)

    elif source_type == "product":
        from etl.transform.transform_product import transform_products
        return transform_products(df)

    elif source_type == "customer":
        from etl.transform.transform_customer import transform_customers
        return transform_customers(df, tenant_id=tenant_id)

    elif source_type == "employee":
        from etl.transform.transform_employee import transform_employees
        return transform_employees(df, tenant_id=tenant_id)

    elif source_type == "store":
        from etl.transform.transform_store import transform_stores
        return transform_stores(df, tenant_id=tenant_id)

    elif source_type == "purchase":
        from etl.transform.transform_purchase import transform_purchases
        return transform_purchases(df, tenant_id=tenant_id)

    elif source_type == "supplier":
        from etl.transform.transform_supplier import transform_suppliers
        return transform_suppliers(df)

    else:
        raise ValueError(f"Unknown source type: {source_type}")


# ---------------------------------------------------------------------------
# Staging load helper
# ---------------------------------------------------------------------------

def _load_to_staging(
    conn,
    df: pd.DataFrame,
    staging_table: str,
    tenant_id: str,
) -> int:
    """
    Load transformed DataFrame into staging table.

    Uses db_utils.load_dataframe_to_staging with transaction management.
    """
    return load_dataframe_to_staging(
        conn=conn,
        df=df,
        staging_table=staging_table,
        tenant_id=tenant_id,
        commit=False,
    )


# ---------------------------------------------------------------------------
# Stored procedure execution
# ---------------------------------------------------------------------------

def _run_sp_load(
    conn,
    tenant_id: str,
    log,
    steps_completed: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Execute stored procedures for dimension loading and fact transformation.

    Calls SPs in correct order:
        1. Shared dimensions (no TenantID): DimProduct, DimSupplier
        2. Tenant-specific dimensions: DimStore, DimEmployee, DimCustomer
        3. Facts: FactSales, FactInventory, FactPurchase

    Args:
        conn:             Active database connection.
        tenant_id:        Tenant identifier.
        log:              Scoped logger.
        steps_completed:  List of source types that were successfully loaded. (optional)

    Returns:
        Dict with SP execution results.
    """
    from etl.extract.db_utils import execute_sp

    sp_map: dict[str, str] = {
        "product": "usp_Load_DimProduct",
        "supplier": "usp_Load_DimSupplier",
        "store": "usp_Load_DimStore",
        "employee": "usp_Load_DimEmployee",
        "customer": "usp_Load_DimCustomer",
        "sales": "usp_Transform_FactSales",
        "inventory": "usp_Transform_FactInventory",
        "purchase": "usp_Transform_FactPurchase",
    }

    sp_order = [
        "product", "supplier",
        "store", "employee", "customer",
        "sales", "inventory", "purchase",
    ]

    results: dict[str, Any] = {
        "status": "SUCCESS",
        "stored_procedures": {},
        "rows_processed": 0,
        "errors": [],
    }

    steps_completed = set(steps_completed or [])

    for source_type in sp_order:
        sp_name = sp_map.get(source_type)
        if sp_name is None:
            continue

        if source_type not in steps_completed:
            log.debug(
                "[%s] Skipping SP %s (source '%s' not loaded)",
                tenant_id, sp_name, source_type
            )
            continue

        try:
            params: dict[str, Any] = {}
            if source_type in ("store", "employee", "customer", "sales", "inventory", "purchase"):
                params["@TenantID"] = tenant_id

            start = time.time()
            df_result = execute_sp(conn, sp_name, params, fetch=True)
            duration = int(time.time() - start)

            rows_processed = len(df_result) if df_result is not None else 0

            results["stored_procedures"][sp_name] = {
                "source_type": source_type,
                "status": "SUCCESS",
                "rows_processed": rows_processed,
                "duration_seconds": duration,
            }

            log.info(
                "[%s] SP %s executed successfully | %d rows | %ds",
                tenant_id, sp_name, rows_processed, duration
            )

            results["rows_processed"] += rows_processed

            conn.commit()

        except Exception as ex:
            log.error(
                "[%s] SP %s failed: %s",
                tenant_id, sp_name, ex, exc_info=True
            )
            results["stored_procedures"][sp_name] = {
                "source_type": source_type,
                "status": "FAILED",
                "error": str(ex),
            }
            results["errors"].append(f"{sp_name}: {ex}")
            results["status"] = "WARNING"

            try:
                conn.rollback()
            except Exception:
                pass

    return results


# ---------------------------------------------------------------------------
# Data mart refresh
# ---------------------------------------------------------------------------

def _run_datamart_refresh(
    conn,
    tenant_id: str,
    log,
) -> dict[str, Any]:
    """
    Refresh all data marts for a tenant.

    SPs called:
        1. usp_Refresh_DM_SalesSummary
        2. usp_Refresh_DM_InventoryAlert
        3. usp_Refresh_DM_CustomerRFM
        4. usp_Refresh_DM_EmployeePerformance
        5. usp_Refresh_DM_PurchaseSummary

    Args:
        conn:       Active database connection.
        tenant_id:  Tenant identifier.
        log:        Scoped logger.

    Returns:
        Dict with datamart refresh results.
    """
    from etl.extract.db_utils import execute_sp

    dm_map = {
        "usp_Refresh_DM_SalesSummary": "sales_summary",
        "usp_Refresh_DM_InventoryAlert": "inventory_alert",
        "usp_Refresh_DM_CustomerRFM": "customer_rfm",
        "usp_Refresh_DM_EmployeePerformance": "employee_performance",
        "usp_Refresh_DM_PurchaseSummary": "purchase_summary",
    }

    results: dict[str, Any] = {
        "status": "SUCCESS",
        "data_marts": {},
        "errors": [],
    }

    for sp_name, dm_key in dm_map.items():
        try:
            start = time.time()
            execute_sp(conn, sp_name, {"@TenantID": tenant_id})
            duration = int(time.time() - start)

            results["data_marts"][dm_key] = {
                "sp": sp_name,
                "status": "SUCCESS",
                "duration_seconds": duration,
            }

            log.info(
                "[%s] DM %s refreshed | %ds",
                tenant_id, dm_key, duration
            )

            conn.commit()

        except Exception as ex:
            log.error(
                "[%s] DM %s refresh failed: %s",
                tenant_id, dm_key, ex, exc_info=True
            )
            results["data_marts"][dm_key] = {
                "sp": sp_name,
                "status": "FAILED",
                "error": str(ex),
            }
            results["errors"].append(f"{dm_key}: {ex}")
            results["status"] = "WARNING"

            try:
                conn.rollback()
            except Exception:
                pass

    return results


# ---------------------------------------------------------------------------
# Truncate staging
# ---------------------------------------------------------------------------

def _step_truncate_staging(
    conn,
    tenant_id: str,
    log,
) -> dict[str, Any]:
    """
    Truncate tenant-specific staging tables before loading new data.

    Does NOT truncate shared dimension tables (Product, Supplier).
    Does NOT truncate error log table.

    Args:
        conn:       Active database connection.
        tenant_id:  Tenant identifier.
        log:        Scoped logger.

    Returns:
        Dict with truncate results.
    """
    result: dict[str, Any] = {"status": "SUCCESS", "tables_processed": 0}

    try:
        tables = truncate_staging(conn, tenant_id=tenant_id, commit=False)
        result["tables_processed"] = tables
        conn.commit()
        log.info(
            "[%s] Staging tables truncated for tenant %s (%d tables)",
            tenant_id, tenant_id, tables
        )
    except Exception as ex:
        log.warning(
            "[%s] Staging truncate partially failed: %s",
            tenant_id, ex
        )
        result["status"] = "WARNING"
        result["error"] = str(ex)
        try:
            conn.rollback()
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Watermark update helper
# ---------------------------------------------------------------------------

def _update_watermark(
    conn,
    tenant_id: str,
    status: str,
    rows_extracted: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    pipeline_name: Optional[str] = None,
) -> None:
    """
    Update ETL_Watermark record for the overall pipeline.

    Uses a generic source name 'ETL_Pipeline' to track the pipeline itself.
    Individual source watermarks are tracked separately per source type.
    """
    try:
        update_watermark(
            conn=conn,
            source_name=f"{tenant_id}_ETL_Pipeline",
            tenant_id=tenant_id,
            status=status,
            rows_extracted=rows_extracted,
            duration_seconds=duration_seconds,
            source_type=pipeline_name or "ETL_Pipeline",
            commit=True,
        )
    except Exception as ex:
        logging.warning(
            "[%s] Watermark update failed: %s",
            tenant_id, ex
        )


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _summarize_file_paths(paths: dict[str, str]) -> str:
    """Create a compact summary of file paths."""
    if not paths:
        return "(no files)"
    parts = [f"{k}={os.path.basename(v)}" for k, v in paths.items()]
    return ", ".join(parts)
