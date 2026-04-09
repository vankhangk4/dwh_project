-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/07_v_DM_PurchaseSummary_ByTenant.sql
-- Description: View tong hop nhap hang cho tenant.
--              Tra ve chi phi, ti le fill rate, trang thai thanh toan.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_PurchaseSummary_ByTenant')
BEGIN
    DROP VIEW v_DM_PurchaseSummary_ByTenant;
END
GO

CREATE VIEW v_DM_PurchaseSummary_ByTenant
AS
SELECT
    s.SummaryKey,
    s.TenantID,
    s.DateKey,
    s.SupplierKey,
    s.StoreKey,
    s.CategoryName,
    s.SupplierCode,
    s.SupplierName,
    s.TotalPurchaseCost,
    s.TotalNetCost,
    s.TotalDiscount,
    s.TotalTax,
    s.TotalOrders,
    s.TotalQty,
    s.TotalReceivedQty,
    s.TotalRejectedQty,
    s.AvgUnitCost,
    s.FillRatePct,
    s.TotalPendingPayment,
    s.TotalPaidPayment,
    s.TotalOverduePayment,
    s.YearKey,
    s.QuarterKey,
    s.MonthKey,
    s.LastRefreshed,

    -- Dimension fields
    d.FullDate,
    d.MonthName,

    st.StoreCode,
    st.StoreName,
    st.City

FROM DM_PurchaseSummary s
INNER JOIN DimDate d ON d.DateKey = s.DateKey
INNER JOIN DimStore st ON st.StoreKey = s.StoreKey
WHERE s.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_PurchaseSummary_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_ETL_RunLog_Recent
-- Description: View log ETL gan nhat cho tenant.
--              Dung de kiem tra trang thai ETL, debug loi.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_ETL_RunLog_Recent')
BEGIN
    DROP VIEW v_ETL_RunLog_Recent;
END
GO

CREATE VIEW v_ETL_RunLog_Recent
AS
SELECT
    l.RunLogID,
    l.TenantID,
    l.StoredProcedureName,
    l.PipelineName,
    l.RunDate,
    l.Status,
    l.ExitCode,
    l.RowsProcessed,
    l.RowsInserted,
    l.RowsUpdated,
    l.RowsSkipped,
    l.RowsFailed,
    l.ErrorCode,
    l.ErrorMessage,
    l.StartTime,
    l.EndTime,
    l.DurationSeconds,
    l.ServerName,
    l.JobName,

    CASE
        WHEN l.Status = 'SUCCESS' THEN N'Success'
        WHEN l.Status = 'FAILED' THEN N'Failed'
        WHEN l.Status = 'RUNNING' THEN N'Running'
        ELSE l.Status
    END AS StatusDescription

FROM ETL_RunLog l
WHERE l.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
   OR l.TenantID = 'SHARED'
ORDER BY l.RunLogID DESC;
GO

PRINT 'Created view: v_ETL_RunLog_Recent';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_STG_ErrorLog_Recent
-- Description: View loi ETL gan nhat cho tenant.
--              Dung de debug du lieu loi, kiem tra chat luong ETL.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_STG_ErrorLog_Recent')
BEGIN
    DROP VIEW v_STG_ErrorLog_Recent;
END
GO

CREATE VIEW v_STG_ErrorLog_Recent
AS
SELECT
    l.ErrorLogID,
    l.TenantID,
    l.SourceTable,
    l.ErrorType,
    l.ErrorCode,
    l.ErrorMessage,
    l.SourceKey,
    l.RawData,
    l.BatchDate,
    l.ETLRunDate,
    l.LoadDatetime,
    l.IsResolved,
    l.ResolvedBy,
    l.ResolvedAt,
    l.ResolutionNotes,

    CASE WHEN l.IsResolved = 1 THEN N'Resolved' ELSE N'Pending' END AS StatusLabel

FROM STG_ErrorLog l
WHERE l.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
ORDER BY l.ETLRunDate DESC, l.ErrorLogID DESC;
GO

PRINT 'Created view: v_STG_ErrorLog_Recent';
GO