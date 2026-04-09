-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/08_create_indexes.sql
-- Description: Tao cac index bo sung ngoai Phase 1-6 da co.
--              Cac index nay toi uu cac View va Dashboard Superset.
--
-- NOTE:
--   - Tat ca index deu co SET NOCOUNT ON, IF NOT EXISTS.
--   - Index da ton tai se duoc skip.
--   - Chi tao index khi can thiet.
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: Creating Additional Indexes';
PRINT 'Started at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';

---------------------------------------------------------------------------
-- INDEXES: FactSales — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_DateKey_ProductKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_DateKey_ProductKey
        ON FactSales(TenantID, DateKey, ProductKey)
        INCLUDE (Quantity, NetSalesAmount, GrossProfitAmount, CostAmount);

    PRINT 'Created index: IX_FactSales_TenantID_DateKey_ProductKey (covering index for dashboard queries)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactSales_TenantID_DateKey_ProductKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_StoreKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_StoreKey
        ON FactSales(TenantID, StoreKey, DateKey DESC)
        INCLUDE (NetSalesAmount, Quantity);

    PRINT 'Created index: IX_FactSales_TenantID_StoreKey (for store-level dashboard)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactSales_TenantID_StoreKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_CustomerKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_CustomerKey
        ON FactSales(TenantID, CustomerKey, DateKey DESC)
        WHERE CustomerKey > 0;

    PRINT 'Created index: IX_FactSales_TenantID_CustomerKey (for customer RFM analysis)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactSales_TenantID_CustomerKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_EmployeeKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_EmployeeKey
        ON FactSales(TenantID, EmployeeKey, DateKey DESC)
        INCLUDE (NetSalesAmount, Quantity)
        WHERE EmployeeKey > 0;

    PRINT 'Created index: IX_FactSales_TenantID_EmployeeKey (for employee performance)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactSales_TenantID_EmployeeKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_Category'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_Category
        ON FactSales(TenantID, DateKey DESC)
        INCLUDE (ProductKey, NetSalesAmount, GrossProfitAmount, Quantity);

    PRINT 'Created index: IX_FactSales_TenantID_Category (for category analysis)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactSales_TenantID_Category already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: FactInventory — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_TenantID_DateKey_ProductKey'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_TenantID_DateKey_ProductKey
        ON FactInventory(TenantID, DateKey DESC, ProductKey)
        INCLUDE (ClosingQty, ClosingValue, StockStatus, ReorderLevel);

    PRINT 'Created index: IX_FactInventory_TenantID_DateKey_ProductKey (covering index)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactInventory_TenantID_DateKey_ProductKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_TenantID_StockStatus'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_TenantID_StockStatus
        ON FactInventory(TenantID, DateKey DESC)
        INCLUDE (ProductKey, StoreKey, ClosingQty, StockStatus)
        WHERE StockStatus IN (N'Low', N'Out of Stock');

    PRINT 'Created index: IX_FactInventory_TenantID_StockStatus (for alert dashboard)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactInventory_TenantID_StockStatus already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: FactPurchase — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_TenantID_DateKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_TenantID_DateKey
        ON FactPurchase(TenantID, DateKey DESC)
        INCLUDE (TotalCost, NetCost, Quantity, ReceivedQty, PaymentStatus);

    PRINT 'Created index: IX_FactPurchase_TenantID_DateKey (covering index)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactPurchase_TenantID_DateKey already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_TenantID_SupplierKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_TenantID_SupplierKey
        ON FactPurchase(TenantID, SupplierKey, DateKey DESC)
        INCLUDE (TotalCost, NetCost, PaymentStatus);

    PRINT 'Created index: IX_FactPurchase_TenantID_SupplierKey (for supplier analysis)';
END
ELSE
BEGIN
    PRINT 'Index IX_FactPurchase_TenantID_SupplierKey already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: DimCustomer — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimCustomer_TenantID_IsCurrent_Email'
    AND object_id = OBJECT_ID('DimCustomer')
)
BEGIN
    CREATE INDEX IX_DimCustomer_TenantID_IsCurrent_Email
        ON DimCustomer(TenantID, IsCurrent)
        INCLUDE (CustomerCode, FullName, Phone, Email, City, CustomerType, LoyaltyTier)
        WHERE IsCurrent = 1;

    PRINT 'Created index: IX_DimCustomer_TenantID_IsCurrent_Email (for customer lookup)';
END
ELSE
BEGIN
    PRINT 'Index IX_DimCustomer_TenantID_IsCurrent_Email already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: DimEmployee — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimEmployee_TenantID_IsActive'
    AND object_id = OBJECT_ID('DimEmployee')
)
BEGIN
    CREATE INDEX IX_DimEmployee_TenantID_IsActive
        ON DimEmployee(TenantID, IsActive)
        INCLUDE (EmployeeCode, FullName, Position, Department)
        WHERE IsActive = 1;

    PRINT 'Created index: IX_DimEmployee_TenantID_IsActive (for active employee lookup)';
END
ELSE
BEGIN
    PRINT 'Index IX_DimEmployee_TenantID_IsActive already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: DM_SalesSummary — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_SalesSummary_TenantID_MonthKey'
    AND object_id = OBJECT_ID('DM_SalesSummary')
)
BEGIN
    CREATE INDEX IX_DM_SalesSummary_TenantID_MonthKey
        ON DM_SalesSummary(TenantID, YearKey, MonthKey)
        INCLUDE (TotalRevenue, TotalGrossProfit, TotalOrders, TotalQty, GrossMarginPct);

    PRINT 'Created index: IX_DM_SalesSummary_TenantID_MonthKey (for monthly dashboard)';
END
ELSE
BEGIN
    PRINT 'Index IX_DM_SalesSummary_TenantID_MonthKey already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: DM_CustomerRFM — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_CustomerRFM_TenantID_ChurnRisk'
    AND object_id = OBJECT_ID('DM_CustomerRFM')
)
BEGIN
    CREATE INDEX IX_DM_CustomerRFM_TenantID_ChurnRisk
        ON DM_CustomerRFM(TenantID, ChurnRiskScore DESC)
        INCLUDE (CustomerKey, FullName, City, RecencyDays, RFMScore, Segment)
        WHERE ChurnRiskScore > 30;

    PRINT 'Created index: IX_DM_CustomerRFM_TenantID_ChurnRisk (for churn alert)';
END
ELSE
BEGIN
    PRINT 'Index IX_DM_CustomerRFM_TenantID_ChurnRisk already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: ETL_RunLog — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_RunLog_TenantID_StoredProc'
    AND object_id = OBJECT_ID('ETL_RunLog')
)
BEGIN
    CREATE INDEX IX_ETL_RunLog_TenantID_StoredProc
        ON ETL_RunLog(TenantID, StoredProcedureName, RunDate DESC)
        INCLUDE (Status, RowsProcessed, RowsInserted, RowsFailed, DurationSeconds);

    PRINT 'Created index: IX_ETL_RunLog_TenantID_StoredProc (for ETL monitoring)';
END
ELSE
BEGIN
    PRINT 'Index IX_ETL_RunLog_TenantID_StoredProc already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- INDEXES: STG_ErrorLog — bo sung
---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_ErrorLog_TenantID_IsResolved'
    AND object_id = OBJECT_ID('STG_ErrorLog')
)
BEGIN
    CREATE INDEX IX_STG_ErrorLog_TenantID_IsResolved
        ON STG_ErrorLog(TenantID, IsResolved, ETLRunDate DESC)
        INCLUDE (ErrorType, ErrorMessage, SourceTable);

    PRINT 'Created index: IX_STG_ErrorLog_TenantID_IsResolved (for error monitoring)';
END
ELSE
BEGIN
    PRINT 'Index IX_STG_ErrorLog_TenantID_IsResolved already exists — skipping.';
END
GO

---------------------------------------------------------------------------
-- COMPLETED
---------------------------------------------------------------------------
PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: All Additional Indexes Created';
PRINT 'Completed at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';
GO
