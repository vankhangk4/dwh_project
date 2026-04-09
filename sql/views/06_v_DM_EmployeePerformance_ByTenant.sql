-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/06_v_DM_EmployeePerformance_ByTenant.sql
-- Description: View do hieu suat nhan vien cho tenant.
--              Tra ve KPI theo ngay/nguoi.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_EmployeePerformance_ByTenant')
BEGIN
    DROP VIEW v_DM_EmployeePerformance_ByTenant;
END
GO

CREATE VIEW v_DM_EmployeePerformance_ByTenant
AS
SELECT
    e.PerfKey,
    e.TenantID,
    e.DateKey,
    e.EmployeeKey,
    e.EmployeeCode,
    e.FullName,
    e.Position,
    e.Department,
    e.ShiftType,
    e.TotalRevenue,
    e.TotalGrossProfit,
    e.TotalOrders,
    e.TotalQtySold,
    e.TotalReturns,
    e.AvgOrderValue,
    e.ConversionRate,
    e.GrossMarginPct,
    e.TopProduct1Code,
    e.TopProduct1Name,
    e.TopProduct1Qty,
    e.LastRefreshed,

    -- Dimension fields
    d.FullDate,
    d.YearKey,
    d.MonthKey,
    d.MonthName,
    d.DayName,
    d.DayOfWeek,
    d.IsWeekend

FROM DM_EmployeePerformance e
INNER JOIN DimDate d ON d.DateKey = e.DateKey
WHERE e.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_EmployeePerformance_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_DM_EmployeePerformance_Ranking
-- Description: View xep hang nhan vien theo doanh thu trong thang.
--              Dung cho Dashboard Top Sales.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_EmployeePerformance_Ranking')
BEGIN
    DROP VIEW v_DM_EmployeePerformance_Ranking;
END
GO

CREATE VIEW v_DM_EmployeePerformance_Ranking
AS
SELECT
    TenantID,
    EmployeeKey,
    EmployeeCode,
    FullName,
    Position,
    Department,
    ShiftType,
    TotalRevenue,
    TotalGrossProfit,
    TotalOrders,
    TotalQtySold,
    AvgOrderValue,
    GrossMarginPct,
    TopProduct1Name,
    TopProduct1Qty,
    YearKey,
    MonthKey,
    MonthName,
    LastRefreshed,

    ROW_NUMBER() OVER (
        PARTITION BY TenantID, YearKey, MonthKey
        ORDER BY TotalRevenue DESC
    ) AS MonthlyRank,

    ROW_NUMBER() OVER (
        PARTITION BY TenantID, YearKey
        ORDER BY TotalRevenue DESC
    ) AS YearlyRank

FROM DM_EmployeePerformance
WHERE TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_EmployeePerformance_Ranking';
GO