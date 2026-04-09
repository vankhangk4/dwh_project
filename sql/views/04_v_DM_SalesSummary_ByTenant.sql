-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/04_v_DM_SalesSummary_ByTenant.sql
-- Description: View filter DM_SalesSummary theo SESSION_CONTEXT tenant_id.
--              Tra ve tam tong hop doanh thu cho dashboard Superset.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_SalesSummary_ByTenant')
BEGIN
    DROP VIEW v_DM_SalesSummary_ByTenant;
END
GO

CREATE VIEW v_DM_SalesSummary_ByTenant
AS
SELECT
    s.SummaryKey,
    s.TenantID,
    s.DateKey,
    s.StoreKey,
    s.ProductKey,
    s.CategoryName,
    s.BrandName,
    s.TotalRevenue,
    s.TotalGrossProfit,
    s.TotalCost,
    s.TotalDiscount,
    s.TotalOrders,
    s.TotalQty,
    s.TotalReturns,
    s.AvgOrderValue,
    s.AvgQtyPerOrder,
    s.GrossMarginPct,
    s.YearKey,
    s.QuarterKey,
    s.MonthKey,
    s.MonthName,
    s.LastRefreshed,

    -- Dimension fields
    d.FullDate,
    d.DayName,
    d.DayOfWeek,
    d.IsWeekend,
    d.IsHoliday,

    st.StoreCode,
    st.StoreName,
    st.StoreType,
    st.City,
    st.Region

FROM DM_SalesSummary s
INNER JOIN DimDate d ON d.DateKey = s.DateKey
INNER JOIN DimStore st ON st.StoreKey = s.StoreKey
WHERE s.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_SalesSummary_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_DM_InventoryAlert_ByTenant
-- Description: View canh bao ton kho cho tenant.
--              Chi hien thi canh bao (AlertLevel <> 'Normal').
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_InventoryAlert_ByTenant')
BEGIN
    DROP VIEW v_DM_InventoryAlert_ByTenant;
END
GO

CREATE VIEW v_DM_InventoryAlert_ByTenant
AS
SELECT
    a.AlertKey,
    a.TenantID,
    a.DateKey,
    a.ProductKey,
    a.StoreKey,
    a.ProductCode,
    a.ProductName,
    a.CategoryName,
    a.BrandName,
    a.CurrentQty,
    a.OpeningQty,
    a.ReceivedQty,
    a.SoldQty,
    a.ReturnedQty,
    a.AdjustedQty,
    a.ClosingValue,
    a.ReorderLevel,
    a.MaxStockLevel,
    a.DaysOfStock,
    a.AlertLevel,
    a.AlertMessage,
    a.SuggestedOrderQty,
    a.DaysSinceLastSale,
    a.LastRefreshed,

    d.FullDate,
    d.MonthKey,
    d.MonthName,

    st.StoreCode,
    st.StoreName,
    st.City,
    st.Region

FROM DM_InventoryAlert a
INNER JOIN DimDate d ON d.DateKey = a.DateKey
INNER JOIN DimStore st ON st.StoreKey = a.StoreKey
WHERE a.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_InventoryAlert_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_DM_InventoryAlert_Critical
-- Description: View chi hien thi cac canh bao NGHIEM TRONG (Low + Out of Stock).
--              Dung cho Dashboard Canh Bao Ton Kho.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_InventoryAlert_Critical')
BEGIN
    DROP VIEW v_DM_InventoryAlert_Critical;
END
GO

CREATE VIEW v_DM_InventoryAlert_Critical
AS
SELECT
    a.AlertKey,
    a.TenantID,
    a.ProductKey,
    a.StoreKey,
    a.ProductCode,
    a.ProductName,
    a.CategoryName,
    a.CurrentQty,
    a.ReorderLevel,
    a.MaxStockLevel,
    a.AlertLevel,
    a.AlertMessage,
    a.SuggestedOrderQty,
    a.DaysSinceLastSale,
    a.LastRefreshed,

    st.StoreCode,
    st.StoreName,
    st.City,
    st.Region,

    d.FullDate

FROM DM_InventoryAlert a
INNER JOIN DimStore st ON st.StoreKey = a.StoreKey
INNER JOIN DimDate d ON d.DateKey = a.DateKey
WHERE a.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
  AND a.AlertLevel IN (N'Low', N'Out of Stock');
GO

PRINT 'Created view: v_DM_InventoryAlert_Critical';
GO