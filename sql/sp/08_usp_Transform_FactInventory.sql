-- ============================================================================
-- PHASE 7: SQL Stored Procedures — FactInventory (Tenant-Specific)
-- File: sql/sp/08_usp_Transform_FactInventory.sql
-- Description: Chuyen doi du lieu tu STG_InventoryRaw sang FactInventory.
--              Tenant-Specific — chi xu ly du lieu cua tenant duoc chi dinh.
--              @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. MERGE (upsert) ton kho theo (TenantID, DateKey, ProductKey, StoreKey).
--   2. Tinh ClosingQty = Opening + Received - Sold + Returned + Adjusted.
--   3. Tinh ClosingValue, DaysOfStock, StockStatus.
--   4. Ghi log ket qua vao ETL_RunLog.
--
-- Dependencies: STG_InventoryRaw, DimProduct, DimStore, DimDate.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Transform_FactInventory')
BEGIN
    DROP PROCEDURE usp_Transform_FactInventory;
END
GO

CREATE PROCEDURE usp_Transform_FactInventory
    @TenantID  VARCHAR(20),
    @BatchDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Transform_FactInventory: TenantID is required.';
        RETURN;
    END

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @DateKey INT = CONVERT(INT, FORMAT(@BatchDate, 'yyyyMMdd'));
    DECLARE @RowsMerged INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_InventoryRaw co du lieu
    IF NOT EXISTS (
        SELECT TOP 1 FROM STG_InventoryRaw
        WHERE TenantID = @TenantID AND CAST(NgayChot AS DATE) = @BatchDate
    )
    BEGIN
        PRINT 'usp_Transform_FactInventory [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
            + ']: No data in STG_InventoryRaw for this tenant/date. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: MERGE (INSERT / UPDATE) ton kho
    MERGE FactInventory AS target
    USING (
        SELECT
            @TenantID AS TenantID,
            @DateKey AS DateKey,
            p.ProductKey,
            st.StoreKey,
            ISNULL(s.TonDauNgay, 0) AS OpeningQty,
            ISNULL(s.NhapTrongNgay, 0) AS ReceivedQty,
            ISNULL(s.BanTrongNgay, 0) AS SoldQty,
            ISNULL(s.TraLaiNhap, 0) AS ReturnedQty,
            ISNULL(s.DieuChinh, 0) AS AdjustedQty,
            ISNULL(s.DonGiaVon, p.UnitCostPrice) AS UnitCostPrice,
            ISNULL(s.MucTonToiThieu, 0) AS ReorderLevel,
            ISNULL(s.LoaiChuyen, N'Daily Count') AS MovementType,
            GETDATE() AS LoadDatetime
        FROM STG_InventoryRaw s
        INNER JOIN DimProduct p ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
        INNER JOIN DimStore st ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
        WHERE s.TenantID = @TenantID
          AND CAST(s.NgayChot AS DATE) = @BatchDate
    ) AS source (
        TenantID, DateKey, ProductKey, StoreKey,
        OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
        UnitCostPrice, ReorderLevel, MovementType, LoadDatetime
    )
    ON target.TenantID = source.TenantID
       AND target.DateKey = source.DateKey
       AND target.ProductKey = source.ProductKey
       AND target.StoreKey = source.StoreKey
    WHEN MATCHED THEN
        UPDATE SET
            target.OpeningQty    = source.OpeningQty,
            target.ReceivedQty  = source.ReceivedQty,
            target.SoldQty       = source.SoldQty,
            target.ReturnedQty   = source.ReturnedQty,
            target.AdjustedQty   = source.AdjustedQty,
            target.UnitCostPrice = source.UnitCostPrice,
            target.ReorderLevel  = source.ReorderLevel,
            target.MovementType  = source.MovementType,
            target.LoadDatetime  = source.LoadDatetime
    WHEN NOT MATCHED THEN
        INSERT (
            TenantID, DateKey, ProductKey, StoreKey,
            OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
            UnitCostPrice, ReorderLevel, MovementType, LoadDatetime
        )
        VALUES (
            source.TenantID, source.DateKey, source.ProductKey, source.StoreKey,
            source.OpeningQty, source.ReceivedQty, source.SoldQty,
            source.ReturnedQty, source.AdjustedQty,
            source.UnitCostPrice, source.ReorderLevel, source.MovementType,
            source.LoadDatetime
        );

    SET @RowsMerged = @@ROWCOUNT;

    -- BUOC 2: Cap nhat cac cot tinh toan sau MERGE
    UPDATE fi SET
        fi.ClosingQty    = fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty,
        fi.OpeningValue  = fi.OpeningQty * fi.UnitCostPrice,
        fi.ReceivedValue = fi.ReceivedQty * fi.UnitCostPrice,
        fi.SoldValue     = fi.SoldQty * fi.UnitCostPrice,
        fi.ClosingValue  = (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty) * fi.UnitCostPrice,
        fi.DaysOfStock   = CASE
                               WHEN fi.SoldQty > 0
                               THEN CAST((fi.OpeningQty + fi.ReceivedQty) * 1.0 / fi.SoldQty AS DECIMAL(10,2))
                               ELSE CAST(999.00 AS DECIMAL(10,2))
                           END,
        fi.StockStatus   = CASE
                               WHEN fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty = 0
                               THEN N'Out of Stock'
                               WHEN (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty) <= fi.ReorderLevel
                               THEN N'Low'
                               WHEN (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty)
                                    > (fi.ReorderLevel * 5)
                               THEN N'Overstock'
                               ELSE N'Normal'
                           END
    FROM FactInventory fi
    WHERE fi.TenantID = @TenantID
      AND fi.DateKey = @DateKey;

    SET @RowsUpdated = @@ROWCOUNT;

    -- BUOC 3: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactInventory',
        @BatchDate,
        'SUCCESS',
        @RowsMerged + @RowsUpdated,
        @RowsMerged,
        @RowsUpdated,
        0,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Transform_FactInventory [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Merged=' + CAST(@RowsMerged AS VARCHAR(10))
        + ', Calculated=' + CAST(@RowsUpdated AS VARCHAR(10))
        + '. Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @LowStock INT;
    DECLARE @OutOfStock INT;
    SELECT
        @LowStock = SUM(CASE WHEN StockStatus = N'Low' THEN 1 ELSE 0 END),
        @OutOfStock = SUM(CASE WHEN StockStatus = N'Out of Stock' THEN 1 ELSE 0 END)
    FROM FactInventory
    WHERE TenantID = @TenantID AND DateKey = @DateKey;
    PRINT '[VERIFY] FactInventory [' + @TenantID + '] on ' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ' — Low: ' + CAST(@LowStock AS VARCHAR(10))
        + ', OutOfStock: ' + CAST(@OutOfStock AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactInventory';
GO
