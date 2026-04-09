-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/03_v_FactPurchase_ByTenant.sql
-- Description: View filter FactPurchase theo SESSION_CONTEXT tenant_id.
--              Tra ve du lieu nhap hang cho tenant.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactPurchase_ByTenant')
BEGIN
    DROP VIEW v_FactPurchase_ByTenant;
END
GO

CREATE VIEW v_FactPurchase_ByTenant
AS
SELECT
    f.FactPurchaseKey,
    f.TenantID,
    f.DateKey,
    f.ProductKey,
    f.SupplierKey,
    f.StoreKey,
    f.PurchaseOrderNumber,
    f.PurchaseOrderLine,
    f.GRNNumber,
    f.GRNDate,
    f.Quantity,
    f.UnitCost,
    f.TotalCost,
    f.DiscountAmount,
    f.NetCost,
    f.TaxAmount,
    f.PaymentStatus,
    f.PaymentMethod,
    f.DueDate,
    f.ReceivedQty,
    f.ReceivedDate,
    f.QualityStatus,
    f.Notes,
    f.LoadDatetime,

    -- Dimension fields
    d.FullDate,
    d.YearKey,
    d.MonthKey,
    d.MonthName,
    d.QuarterName,

    p.ProductCode,
    p.ProductName,
    p.Brand,
    p.CategoryName,

    sup.SupplierCode,
    sup.SupplierName,
    sup.ContactName,
    sup.City AS SupplierCity,

    st.StoreCode,
    st.StoreName,
    st.City AS StoreCity

FROM FactPurchase f
INNER JOIN DimDate d ON d.DateKey = f.DateKey
INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
INNER JOIN DimSupplier sup ON sup.SupplierKey = f.SupplierKey
INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
WHERE f.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_FactPurchase_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_FactPurchase_TenantSummary
-- Description: View tong hop nhap hang theo ngay/tenant cho dashboard.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_FactPurchase_TenantSummary')
BEGIN
    DROP VIEW v_FactPurchase_TenantSummary;
END
GO

CREATE VIEW v_FactPurchase_TenantSummary
AS
SELECT
    f.TenantID,
    f.DateKey,
    d.FullDate,
    d.YearKey,
    d.QuarterKey,
    d.MonthKey,
    d.MonthName,

    SUM(f.TotalCost) AS TotalPurchaseCost,
    SUM(f.NetCost) AS TotalNetCost,
    SUM(f.DiscountAmount) AS TotalDiscount,
    SUM(f.TaxAmount) AS TotalTax,
    COUNT(DISTINCT f.PurchaseOrderNumber) AS TotalOrders,
    SUM(f.Quantity) AS TotalQty,
    SUM(f.ReceivedQty) AS TotalReceivedQty,

    CASE WHEN SUM(f.Quantity) > 0
         THEN CAST(SUM(f.ReceivedQty) * 100.0 / SUM(f.Quantity) AS DECIMAL(8,4))
         ELSE CAST(0 AS DECIMAL(8,4)) END AS FillRatePct,

    SUM(CASE WHEN f.PaymentStatus = N'Pending' THEN f.NetCost ELSE 0 END) AS TotalPendingPayment,
    SUM(CASE WHEN f.PaymentStatus = N'Paid' THEN f.NetCost ELSE 0 END) AS TotalPaidPayment,
    SUM(CASE WHEN f.PaymentStatus = N'Overdue' THEN f.NetCost ELSE 0 END) AS TotalOverduePayment

FROM FactPurchase f
INNER JOIN DimDate d ON d.DateKey = f.DateKey
WHERE f.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
GROUP BY
    f.TenantID, f.DateKey, d.FullDate,
    d.YearKey, d.QuarterKey, d.MonthKey, d.MonthName;
GO

PRINT 'Created view: v_FactPurchase_TenantSummary';
GO
