-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DM_PurchaseSummary (Tenant-Specific)
-- File: sql/sp/14_usp_Refresh_DM_PurchaseSummary.sql
-- Description: Refresh tam tong hop nhap hang cho 1 tenant.
--              Tenant-Specific — chi refresh du lieu cua tenant duoc chi dinh.
--
-- Logic:
--   1. DELETE du lieu cu cua tenant.
--   2. Tong hop tu FactPurchase + DimProduct + DimSupplier.
--   3. Tinh: TotalPurchaseCost, FillRate, Payment status.
--   4. Ghi log vao ETL_RunLog.
--
-- Dependencies: FactPurchase, DimProduct, DimSupplier, DM_PurchaseSummary.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_PurchaseSummary')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_PurchaseSummary;
END
GO

CREATE PROCEDURE usp_Refresh_DM_PurchaseSummary
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Refresh_DM_PurchaseSummary: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa du lieu cu cua tenant
    DELETE FROM DM_PurchaseSummary WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert tong hop
    INSERT INTO DM_PurchaseSummary (
        TenantID, DateKey, SupplierKey, StoreKey,
        CategoryName, SupplierCode, SupplierName,
        TotalPurchaseCost, TotalNetCost, TotalDiscount, TotalTax,
        TotalOrders, TotalQty, TotalReceivedQty, TotalRejectedQty,
        AvgUnitCost, FillRatePct,
        TotalPendingPayment, TotalPaidPayment, TotalOverduePayment,
        YearKey, QuarterKey, MonthKey,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.SupplierKey,
        f.StoreKey,
        p.CategoryName,
        sup.SupplierCode,
        sup.SupplierName,
        SUM(f.TotalCost) AS TotalPurchaseCost,
        SUM(f.NetCost) AS TotalNetCost,
        SUM(f.DiscountAmount) AS TotalDiscount,
        SUM(f.TaxAmount) AS TotalTax,
        COUNT(DISTINCT f.PurchaseOrderNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQty,
        SUM(f.ReceivedQty) AS TotalReceivedQty,
        SUM(ABS(f.Quantity - f.ReceivedQty)) AS TotalRejectedQty,

        CASE WHEN SUM(f.Quantity) > 0
             THEN CAST(SUM(f.NetCost) / SUM(f.Quantity) AS DECIMAL(18,2))
             ELSE CAST(0 AS DECIMAL(18,2)) END AS AvgUnitCost,

        CASE WHEN SUM(f.Quantity) > 0
             THEN CAST(SUM(f.ReceivedQty) * 100.0 / SUM(f.Quantity) AS DECIMAL(8,4))
             ELSE CAST(0 AS DECIMAL(8,4)) END AS FillRatePct,

        SUM(CASE WHEN f.PaymentStatus = N'Pending' THEN f.NetCost ELSE 0 END) AS TotalPendingPayment,
        SUM(CASE WHEN f.PaymentStatus = N'Paid' THEN f.NetCost ELSE 0 END) AS TotalPaidPayment,
        SUM(CASE WHEN f.PaymentStatus = N'Overdue' THEN f.NetCost ELSE 0 END) AS TotalOverduePayment,

        CONVERT(INT, LEFT(CONVERT(VARCHAR(8), f.DateKey), 4)) AS YearKey,
        CONVERT(TINYINT, SUBSTRING(CONVERT(VARCHAR(8), f.DateKey), 5, 1)) AS QuarterKey,
        CONVERT(INT, LEFT(CONVERT(VARCHAR(6), f.DateKey), 6)) AS MonthKey,

        GETDATE()
    FROM FactPurchase f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimSupplier sup ON sup.SupplierKey = f.SupplierKey
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.SupplierKey, f.StoreKey,
        p.CategoryName, sup.SupplierCode, sup.SupplierName;

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
        'usp_Refresh_DM_PurchaseSummary',
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

    PRINT 'usp_Refresh_DM_PurchaseSummary [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalCost DECIMAL(18,2);
    DECLARE @Pending DECIMAL(18,2);
    SELECT
        @TotalCost = SUM(TotalPurchaseCost),
        @Pending = SUM(TotalPendingPayment)
    FROM DM_PurchaseSummary WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DM_PurchaseSummary [' + @TenantID + '] — TotalRows: '
        + CAST(@RowsInserted AS VARCHAR(10)) + ', TotalPurchaseCost: '
        + CAST(@TotalCost AS VARCHAR(20)) + ', PendingPayment: '
        + CAST(@Pending AS VARCHAR(20));
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_PurchaseSummary';
GO