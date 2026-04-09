-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/02_v_FactInventory_ByTenant.sql
-- Description: View filter FactInventory theo SESSION_CONTEXT tenant_id.
--              Tra ve ton kho hien tai cho tenant.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactInventory_ByTenant')
BEGIN
    DROP VIEW v_FactInventory_ByTenant;
END
GO

CREATE VIEW v_FactInventory_ByTenant
AS
SELECT
    f.FactInventoryKey,
    f.TenantID,
    f.DateKey,
    f.ProductKey,
    f.StoreKey,
    f.OpeningQty,
    f.ReceivedQty,
    f.SoldQty,
    f.ReturnedQty,
    f.AdjustedQty,
    f.ClosingQty,
    f.UnitCostPrice,
    f.OpeningValue,
    f.ReceivedValue,
    f.SoldValue,
    f.ClosingValue,
    f.ReorderLevel,
    f.DaysOfStock,
    f.StockStatus,
    f.MovementType,
    f.LoadDatetime,

    -- Dimension fields
    d.FullDate,
    d.YearKey,
    d.MonthKey,
    d.MonthName,

    p.ProductCode,
    p.ProductName,
    p.Brand,
    p.CategoryName,

    st.StoreCode,
    st.StoreName,
    st.City,
    st.Region

FROM FactInventory f
INNER JOIN DimDate d ON d.DateKey = f.DateKey
INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
WHERE f.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_FactInventory_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_FactInventory_Latest
-- Description: View tra ve ton kho ngan nhat (DateKey = MAX) cho tenant.
--              Dung de hien thi ton kho hien tai tren dashboard.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactInventory_Latest')
BEGIN
    DROP VIEW v_FactInventory_Latest;
END
GO

CREATE VIEW v_FactInventory_Latest
AS
WITH LatestInventory AS (
    SELECT
        TenantID,
        ProductKey,
        StoreKey,
        DateKey,
        ClosingQty,
        ClosingValue,
        DaysOfStock,
        StockStatus,
        ReorderLevel,
        ROW_NUMBER() OVER (
            PARTITION BY TenantID, ProductKey, StoreKey
            ORDER BY DateKey DESC
        ) AS rn
    FROM FactInventory
    WHERE TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
)
SELECT
    li.TenantID,
    li.DateKey,
    li.ProductKey,
    li.StoreKey,
    li.ClosingQty,
    li.ClosingValue,
    li.DaysOfStock,
    li.StockStatus,
    li.ReorderLevel,

    d.FullDate,
    d.YearKey,
    d.MonthKey,
    d.MonthName,

    p.ProductCode,
    p.ProductName,
    p.Brand,
    p.CategoryName,
    p.UnitCostPrice,

    st.StoreCode,
    st.StoreName,
    st.City,
    st.Region

FROM LatestInventory li
INNER JOIN DimDate d ON d.DateKey = li.DateKey
INNER JOIN DimProduct p ON p.ProductKey = li.ProductKey AND p.IsCurrent = 1
INNER JOIN DimStore st ON st.StoreKey = li.StoreKey
WHERE li.rn = 1;
GO

PRINT 'Created view: v_FactInventory_Latest';
GO
