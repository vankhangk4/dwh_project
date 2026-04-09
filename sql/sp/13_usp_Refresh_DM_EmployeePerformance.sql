-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DM_EmployeePerformance (Tenant-Specific)
-- File: sql/sp/13_usp_Refresh_DM_EmployeePerformance.sql
-- Description: Refresh do hieu suat nhan vien ban hang cho 1 tenant.
--              Tenant-Specific — chi refresh du lieu cua tenant duoc chi dinh.
--
-- Logic:
--   1. DELETE du lieu cu cua tenant.
--   2. Tong hop tu FactSales + DimEmployee + DimProduct.
--   3. Tinh: Revenue, Profit, Orders, TopProduct.
--   4. Ghi log vao ETL_RunLog.
--
-- Dependencies: FactSales, DimEmployee, DimProduct, DM_EmployeePerformance.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_EmployeePerformance')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_EmployeePerformance;
END
GO

CREATE PROCEDURE usp_Refresh_DM_EmployeePerformance
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Refresh_DM_EmployeePerformance: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa du lieu cu cua tenant
    DELETE FROM DM_EmployeePerformance WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert tong hop theo nhan vien / ngay
    -- Dung ROW_NUMBER de lay TopProduct cho tung nhan vien / ngay
    INSERT INTO DM_EmployeePerformance (
        TenantID, DateKey, EmployeeKey,
        EmployeeCode, FullName, Position, Department, ShiftType,
        TotalRevenue, TotalGrossProfit, TotalOrders, TotalQtySold, TotalReturns,
        AvgOrderValue, ConversionRate, GrossMarginPct,
        TopProduct1Code, TopProduct1Name, TopProduct1Qty,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.EmployeeKey,
        e.EmployeeCode,
        e.FullName,
        e.Position,
        e.Department,
        e.ShiftType,

        SUM(f.NetSalesAmount) AS TotalRevenue,
        SUM(f.GrossProfitAmount) AS TotalGrossProfit,
        COUNT(DISTINCT f.InvoiceNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQtySold,
        SUM(CASE WHEN f.ReturnFlag = 1 THEN f.Quantity ELSE 0 END) AS TotalReturns,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.NetSalesAmount) / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(18,2))
             ELSE CAST(0 AS DECIMAL(18,2)) END AS AvgOrderValue,

        CAST(100.0 AS DECIMAL(8,4)) AS ConversionRate,

        CASE WHEN SUM(f.NetSalesAmount) > 0
             THEN CAST(SUM(f.GrossProfitAmount) / SUM(f.NetSalesAmount) * 100 AS DECIMAL(8,4))
             ELSE CAST(0 AS DECIMAL(8,4)) END AS GrossMarginPct,

        TopP.TopProduct1Code,
        TopP.TopProduct1Name,
        TopP.TopProduct1Qty,

        GETDATE()
    FROM FactSales f
    INNER JOIN DimEmployee e ON e.EmployeeKey = f.EmployeeKey
        AND e.TenantID = f.TenantID AND e.IsActive = 1
    LEFT JOIN (
        SELECT
            f2.TenantID,
            f2.EmployeeKey,
            f2.DateKey,
            p_top.ProductCode AS TopProduct1Code,
            p_top.ProductName AS TopProduct1Name,
            SUM(f2.Quantity) AS TopProduct1Qty,
            ROW_NUMBER() OVER (
                PARTITION BY f2.TenantID, f2.EmployeeKey, f2.DateKey
                ORDER BY SUM(f2.Quantity) DESC
            ) AS rn
        FROM FactSales f2
        INNER JOIN DimProduct p_top ON p_top.ProductKey = f2.ProductKey AND p_top.IsCurrent = 1
        WHERE f2.TenantID = @TenantID AND f2.EmployeeKey > 0 AND f2.ReturnFlag = 0
        GROUP BY f2.TenantID, f2.EmployeeKey, f2.DateKey,
                 p_top.ProductCode, p_top.ProductName
    ) TopP ON TopP.TenantID = f.TenantID
          AND TopP.EmployeeKey = f.EmployeeKey
          AND TopP.DateKey = f.DateKey
          AND TopP.rn = 1
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.EmployeeKey,
        e.EmployeeCode, e.FullName, e.Position, e.Department, e.ShiftType,
        TopP.TopProduct1Code, TopP.TopProduct1Name, TopP.TopProduct1Qty;

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
        'usp_Refresh_DM_EmployeePerformance',
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

    PRINT 'usp_Refresh_DM_EmployeePerformance [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalRevenue DECIMAL(18,2);
    DECLARE @TotalOrders INT;
    SELECT
        @TotalRevenue = SUM(TotalRevenue),
        @TotalOrders = SUM(TotalOrders)
    FROM DM_EmployeePerformance WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DM_EmployeePerformance [' + @TenantID + '] — TotalRows: '
        + CAST(@RowsInserted AS VARCHAR(10)) + ', TotalRevenue: '
        + CAST(@TotalRevenue AS VARCHAR(20)) + ', TotalOrders: '
        + CAST(@TotalOrders AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_EmployeePerformance';
GO