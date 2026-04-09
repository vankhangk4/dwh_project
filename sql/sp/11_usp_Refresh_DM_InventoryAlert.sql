-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DM_InventoryAlert (Tenant-Specific)
-- File: sql/sp/11_usp_Refresh_DM_InventoryAlert.sql
-- Description: Refresh canh bao ton kho cho 1 tenant.
--              Tenant-Specific — chi refresh du lieu cua tenant duoc chi dinh.
--
-- Logic:
--   1. DELETE toan bo canh bao cu cua tenant.
--   2. Lay du lieu tu FactInventory ngan nhat (DateKey = MAX) cho
--      tung san pham / cua hang.
--   3. Xac dinh AlertLevel: OutOfStock / Low / Normal / Overstock.
--   4. Tinh DaysSinceLastSale, SuggestedOrderQty, AlertMessage.
--   5. Ghi log vao ETL_RunLog.
--
-- Dependencies: FactInventory, DimProduct, DimStore, DM_InventoryAlert.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_InventoryAlert')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_InventoryAlert;
END
GO

CREATE PROCEDURE usp_Refresh_DM_InventoryAlert
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Refresh_DM_InventoryAlert: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa canh bao cu cua tenant
    DELETE FROM DM_InventoryAlert WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert canh bao tu FactInventory
    -- Lay du lieu ngan nhat (DateKey = MAX) cho tung san pham / cua hang
    INSERT INTO DM_InventoryAlert (
        TenantID, DateKey, ProductKey, StoreKey,
        ProductCode, ProductName, CategoryName, BrandName,
        CurrentQty, OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
        ClosingValue,
        ReorderLevel, MaxStockLevel,
        DaysOfStock, AlertLevel, AlertMessage, SuggestedOrderQty,
        DaysSinceLastSale,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.ProductKey,
        f.StoreKey,
        p.ProductCode,
        p.ProductName,
        p.CategoryName,
        p.Brand AS BrandName,
        f.ClosingQty AS CurrentQty,
        f.OpeningQty,
        f.ReceivedQty,
        f.SoldQty,
        f.ReturnedQty,
        f.AdjustedQty,
        f.ClosingValue,
        f.ReorderLevel,
        f.ReorderLevel * 5 AS MaxStockLevel,
        f.DaysOfStock,

        CASE
            WHEN f.ClosingQty = 0 THEN N'Out of Stock'
            WHEN f.ClosingQty <= f.ReorderLevel THEN N'Low'
            WHEN f.ClosingQty > f.ReorderLevel * 5 THEN N'Overstock'
            ELSE N'Normal'
        END AS AlertLevel,

        CASE
            WHEN f.ClosingQty = 0
                THEN N'Sản phẩm [' + p.ProductName + N'] đã hết hàng tại cửa hàng. Cần nhập ngay!'
            WHEN f.ClosingQty <= f.ReorderLevel
                THEN N'Sản phẩm [' + p.ProductName + N'] sắp hết ('
                     + CAST(f.ReorderLevel AS NVARCHAR(10)) + N'). Cần nhập thêm.'
            WHEN f.ClosingQty > f.ReorderLevel * 5
                THEN N'Sản phẩm [' + p.ProductName + N'] quá tồn ('
                     + CAST(f.ClosingQty AS NVARCHAR(10)) + N' > '
                     + CAST(f.ReorderLevel * 5 AS NVARCHAR(10)) + N'). Cần giảm nhập.'
            ELSE NULL
        END AS AlertMessage,

        CASE
            WHEN f.ClosingQty <= f.ReorderLevel
                THEN CAST((f.ReorderLevel * 2 - f.ClosingQty) AS INT)
            ELSE CAST(0 AS INT)
        END AS SuggestedOrderQty,

        CASE
            WHEN f.SoldQty > 0 THEN 0
            ELSE DATEDIFF(DAY,
                CONVERT(DATE, CAST((
                    SELECT MAX(fi2.DateKey)
                    FROM FactSales fi2
                    WHERE fi2.TenantID = f.TenantID
                      AND fi2.ProductKey = f.ProductKey
                      AND fi2.StoreKey = f.StoreKey
                      AND fi2.ReturnFlag = 0
                ) AS VARCHAR(8))),
                CONVERT(DATE, CAST(f.DateKey AS VARCHAR(8)))
            )
        END AS DaysSinceLastSale,

        GETDATE()
    FROM FactInventory f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
    WHERE f.TenantID = @TenantID
      AND f.DateKey = (
          SELECT MAX(fi.DateKey)
          FROM FactInventory fi
          WHERE fi.TenantID = f.TenantID
            AND fi.ProductKey = f.ProductKey
            AND fi.StoreKey = f.StoreKey
      );

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
        'usp_Refresh_DM_InventoryAlert',
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

    PRINT 'usp_Refresh_DM_InventoryAlert [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @LowAlert INT;
    DECLARE @OutOfStock INT;
    DECLARE @Overstock INT;
    SELECT
        @LowAlert = SUM(CASE WHEN AlertLevel = N'Low' THEN 1 ELSE 0 END),
        @OutOfStock = SUM(CASE WHEN AlertLevel = N'Out of Stock' THEN 1 ELSE 0 END),
        @Overstock = SUM(CASE WHEN AlertLevel = N'Overstock' THEN 1 ELSE 0 END)
    FROM DM_InventoryAlert WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DM_InventoryAlert [' + @TenantID + '] — Total: '
        + CAST(@RowsInserted AS VARCHAR(10)) + ', Low: '
        + CAST(@LowAlert AS VARCHAR(10)) + ', OutOfStock: '
        + CAST(@OutOfStock AS VARCHAR(10)) + ', Overstock: '
        + CAST(@Overstock AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_InventoryAlert';
GO