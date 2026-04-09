-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/01_v_FactSales_ByTenant.sql
-- Description: View filter FactSales theo SESSION_CONTEXT tenant_id.
--              Superset ket noi voi SESSION_CONTEXT SET = current tenant,
--              View tu dong loc du lieu theo tenant hien tai.
--
-- CACH SU DUNG:
--   Truoc khi truy van, dat SESSION_CONTEXT:
--     SET SESSION_CONTEXT tenant_id = 'STORE_HN';
--   Sau do truy van view:
--     SELECT * FROM v_FactSales_ByTenant;
--
-- Uoc tinh: 1000-10000 dong/tenant.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactSales_ByTenant')
BEGIN
    DROP VIEW v_FactSales_ByTenant;
END
GO

CREATE VIEW v_FactSales_ByTenant
AS
SELECT
    f.FactSalesKey,
    f.TenantID,
    f.DateKey,
    f.ProductKey,
    f.StoreKey,
    f.CustomerKey,
    f.EmployeeKey,
    f.InvoiceNumber,
    f.InvoiceLine,
    f.Quantity,
    f.UnitPrice,
    f.DiscountAmount,
    f.GrossSalesAmount,
    f.NetSalesAmount,
    f.CostAmount,
    f.GrossProfitAmount,
    f.PaymentMethod,
    f.SalesChannel,
    f.SalesGroup,
    f.ReturnFlag,
    f.ReturnReason,
    f.LoadDatetime,

    -- Join Dimension fields
    d.FullDate,
    d.DayName,
    d.MonthName,
    d.MonthOfYear,
    d.QuarterName,
    d.YearKey,
    d.YearMonth,
    d.IsWeekend,
    d.IsHoliday,
    d.HolidayName,

    p.ProductCode,
    p.ProductName,
    p.Brand,
    p.CategoryName,
    p.SubCategory,

    st.StoreCode,
    st.StoreName,
    st.StoreType,
    st.City,
    st.District,
    st.Region,

    c.CustomerCode   AS CustomerCode,
    c.FullName       AS CustomerName,
    c.CustomerType   AS CustomerTypeName,
    c.LoyaltyTier,

    e.EmployeeCode,
    e.FullName       AS EmployeeName,
    e.Position,
    e.Department,
    e.ShiftType

FROM FactSales f
INNER JOIN DimDate d ON d.DateKey = f.DateKey
INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
LEFT JOIN DimCustomer c ON c.CustomerKey = f.CustomerKey AND c.TenantID = f.TenantID AND c.IsCurrent = 1
LEFT JOIN DimEmployee e ON e.EmployeeKey = f.EmployeeKey AND e.TenantID = f.TenantID AND e.IsActive = 1
WHERE f.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_FactSales_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_FactSales_TenantSummary
-- Description: View tong hop doanh thu theo ngay/tenant — tra ve tong
--              hop cho SESSION_CONTEXT tenant. Dung cho dashboard KPI nhanh.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactSales_TenantSummary')
BEGIN
    DROP VIEW v_FactSales_TenantSummary;
END
GO

CREATE VIEW v_FactSales_TenantSummary
AS
SELECT
    f.TenantID,
    f.DateKey,
    d.FullDate,
    d.YearKey,
    d.QuarterKey,
    d.MonthKey,
    d.MonthName,
    d.DayName,
    d.IsWeekend,
    d.IsHoliday,

    COUNT(DISTINCT f.InvoiceNumber) AS TotalOrders,
    SUM(f.Quantity) AS TotalQtySold,
    SUM(f.GrossSalesAmount) AS TotalGrossSales,
    SUM(f.NetSalesAmount) AS TotalNetSales,
    SUM(f.CostAmount) AS TotalCost,
    SUM(f.GrossProfitAmount) AS TotalGrossProfit,
    SUM(f.DiscountAmount) AS TotalDiscount,

    CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
         THEN CAST(SUM(f.NetSalesAmount) / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(18,2))
         ELSE CAST(0 AS DECIMAL(18,2)) END AS AvgOrderValue,

    CASE WHEN SUM(f.NetSalesAmount) > 0
         THEN CAST(SUM(f.GrossProfitAmount) / SUM(f.NetSalesAmount) * 100 AS DECIMAL(8,4))
         ELSE CAST(0 AS DECIMAL(8,4)) END AS GrossMarginPct,

    SUM(CASE WHEN f.ReturnFlag = 1 THEN f.Quantity ELSE 0 END) AS TotalReturns,
    SUM(CASE WHEN f.ReturnFlag = 1 THEN f.NetSalesAmount ELSE 0 END) AS TotalReturnAmount

FROM FactSales f
INNER JOIN DimDate d ON d.DateKey = f.DateKey
WHERE f.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
GROUP BY
    f.TenantID, f.DateKey, d.FullDate,
    d.YearKey, d.QuarterKey, d.MonthKey, d.MonthName,
    d.DayName, d.IsWeekend, d.IsHoliday;
GO

PRINT 'Created view: v_FactSales_TenantSummary';
GO
