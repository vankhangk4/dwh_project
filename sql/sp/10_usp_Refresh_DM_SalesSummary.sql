-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DM_SalesSummary (Tenant-Specific)
-- File: sql/sp/10_usp_Refresh_DM_SalesSummary.sql
-- Description: Refresh tam tong hop doanh thu cho 1 tenant.
--              Tenant-Specific — chi refresh du lieu cua tenant duoc chi dinh.
--
-- Logic:
--   1. DELETE toan bo du lieu cua tenant trong DM_SalesSummary.
--   2. INSERT tong hop tu FactSales + DimProduct + DimStore + DimDate.
--   3. Tinh: Revenue, Profit, Orders, Margin% theo ngay/cua hang/danh muc.
--   4. Ghi log vao ETL_RunLog.
--
-- Dependencies: FactSales, DimProduct, DimStore, DimDate, DM_SalesSummary.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_SalesSummary')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_SalesSummary;
END
GO

CREATE PROCEDURE usp_Refresh_DM_SalesSummary
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Refresh_DM_SalesSummary: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa du lieu cu cua tenant
    DELETE FROM DM_SalesSummary WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert tong hop theo ngay / cua hang / danh muc
    INSERT INTO DM_SalesSummary (
        TenantID, DateKey, StoreKey,
        ProductKey, CategoryName, BrandName,
        TotalRevenue, TotalGrossProfit, TotalCost, TotalDiscount,
        TotalOrders, TotalQty, TotalReturns,
        AvgOrderValue, AvgQtyPerOrder, GrossMarginPct,
        YearKey, QuarterKey, MonthKey, MonthName,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.StoreKey,
        CAST(NULL AS INT) AS ProductKey,
        p.CategoryName,
        p.Brand AS BrandName,
        SUM(f.NetSalesAmount) AS TotalRevenue,
        SUM(f.GrossProfitAmount) AS TotalGrossProfit,
        SUM(f.CostAmount) AS TotalCost,
        SUM(f.DiscountAmount) AS TotalDiscount,
        COUNT(DISTINCT f.InvoiceNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQty,
        SUM(CASE WHEN f.ReturnFlag = 1 THEN f.Quantity ELSE 0 END) AS TotalReturns,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.NetSalesAmount) / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(18,2))
             ELSE CAST(0 AS DECIMAL(18,2)) END AS AvgOrderValue,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.Quantity) * 1.0 / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(10,2))
             ELSE CAST(0 AS DECIMAL(10,2)) END AS AvgQtyPerOrder,

        CASE WHEN SUM(f.NetSalesAmount) > 0
             THEN CAST(SUM(f.GrossProfitAmount) / SUM(f.NetSalesAmount) * 100 AS DECIMAL(8,4))
             ELSE CAST(0 AS DECIMAL(8,4)) END AS GrossMarginPct,

        d.YearKey,
        d.QuarterKey,
        d.MonthKey,
        d.MonthName,

        GETDATE()
    FROM FactSales f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
    INNER JOIN DimDate d ON d.DateKey = f.DateKey
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.StoreKey,
        p.CategoryName, p.Brand,
        d.YearKey, d.QuarterKey, d.MonthKey, d.MonthName;

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_SalesSummary',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Refresh_DM_SalesSummary [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalRevenue DECIMAL(18,2);
    DECLARE @TotalOrders INT;
    SELECT
        @TotalRevenue = SUM(TotalRevenue),
        @TotalOrders = SUM(TotalOrders)
    FROM DM_SalesSummary WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DM_SalesSummary [' + @TenantID + '] — TotalRows: '
        + CAST(@RowsInserted AS VARCHAR(10)) + ', TotalRevenue: '
        + CAST(@TotalRevenue AS VARCHAR(20)) + ', TotalOrders: '
        + CAST(@TotalOrders AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_SalesSummary';
GO