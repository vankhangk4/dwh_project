-- ============================================================================
-- PHASE 7: SQL Stored Procedures — FactPurchase (Tenant-Specific)
-- File: sql/sp/09_usp_Transform_FactPurchase.sql
-- Description: Chuyen doi du lieu tu STG_PurchaseRaw sang FactPurchase.
--              Tenant-Specific — chi xu ly du lieu cua tenant duoc chi dinh.
--              @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. Ghi nhan loi vao STG_ErrorLog neu khong tim thay ProductKey/SupplierKey.
--   2. INSERT vao FactPurchase (neu chua ton tai PurchaseOrderNumber + ProductKey).
--   3. Tinh TotalCost, NetCost.
--   4. Ghi log ket qua vao ETL_RunLog.
--
-- Dependencies: STG_PurchaseRaw, DimProduct, DimSupplier, DimStore.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Transform_FactPurchase')
BEGIN
    DROP PROCEDURE usp_Transform_FactPurchase;
END
GO

CREATE PROCEDURE usp_Transform_FactPurchase
    @TenantID  VARCHAR(20),
    @BatchDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Transform_FactPurchase: TenantID is required.';
        RETURN;
    END

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsSkipped  INT = 0;
    DECLARE @RowsError    INT = 0;
    DECLARE @RowsProcessed INT = 0;
    DECLARE @StartTime    DATETIME2 = GETDATE();

    -- BUOC 1: Ghi nhan loi vao STG_ErrorLog
    -- Khong tim thay ProductKey hoac SupplierKey
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        SourceKey, RawData, BatchDate, ETLRunDate, LoadDatetime
    )
    SELECT
        @TenantID,
        'STG_PurchaseRaw',
        'DIMENSION_NOT_FOUND',
        CASE
            WHEN NOT EXISTS (SELECT 1 FROM DimProduct p WHERE p.ProductCode = s.MaSP AND p.IsCurrent = 1)
                THEN N'Product not found in DimProduct (IsCurrent=1)'
            WHEN NOT EXISTS (SELECT 1 FROM DimSupplier sup WHERE sup.SupplierCode = s.MaNCC)
                THEN N'Supplier not found in DimSupplier'
            ELSE N'Related dimension not found'
        END,
        s.MaSP,
        CONCAT('SoPhieuNhap=', s.SoPhieuNhap, ', MaSP=', s.MaSP, ', MaNCC=', s.MaNCC),
        @BatchDate,
        GETDATE(),
        GETDATE()
    FROM STG_PurchaseRaw s
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayNhap AS DATE) = @BatchDate
      AND (
          NOT EXISTS (SELECT 1 FROM DimProduct p WHERE p.ProductCode = s.MaSP AND p.IsCurrent = 1)
          OR NOT EXISTS (SELECT 1 FROM DimSupplier sup WHERE sup.SupplierCode = s.MaNCC)
      );

    SET @RowsError = @@ROWCOUNT;

    -- BUOC 2: INSERT vao FactPurchase — dong hop le, chua ton tai
    INSERT INTO FactPurchase (
        TenantID, DateKey, ProductKey, SupplierKey, StoreKey,
        PurchaseOrderNumber, PurchaseOrderLine, GRNNumber, GRNDate,
        Quantity, UnitCost, TotalCost, DiscountAmount, NetCost, TaxAmount,
        PaymentStatus, PaymentMethod, DueDate,
        ReceivedQty, ReceivedDate, QualityStatus, Notes,
        LoadDatetime
    )
    SELECT
        @TenantID,
        CONVERT(INT, FORMAT(CAST(s.NgayNhap AS DATE), 'yyyyMMdd')),
        p.ProductKey,
        sup.SupplierKey,
        st.StoreKey,

        UPPER(LTRIM(RTRIM(ISNULL(s.SoPhieuNhap, 'UNKNOWN')))),
        ISNULL(s.SoDong, 1),
        s.SoGRN,
        s.NgayGRN,

        ISNULL(s.SoLuong, 0),
        ISNULL(s.DonGiaNhap, 0),
        ISNULL(s.SoLuong, 0) * ISNULL(s.DonGiaNhap, 0),
        ISNULL(s.ChietKhau, 0),
        (ISNULL(s.SoLuong, 0) * ISNULL(s.DonGiaNhap, 0)) - ISNULL(s.ChietKhau, 0),
        ISNULL(s.ThueGTGT, 0),

        ISNULL(s.TinhTrangThanhToan, N'Pending'),
        ISNULL(s.PhuongThucTT, N'Tiền mặt'),
        s.HanThanhToan,

        ISNULL(s.SoLuongThucNhan, s.SoLuong),
        s.NgayNhanHang,
        ISNULL(s.TinhTrangChatLuong, N'Passed'),
        s.GhiChu,

        GETDATE()
    FROM STG_PurchaseRaw s
    INNER JOIN DimProduct p ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
    INNER JOIN DimSupplier sup ON sup.SupplierCode = s.MaNCC
    INNER JOIN DimStore st ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayNhap AS DATE) = @BatchDate
      AND NOT EXISTS (
          SELECT 1 FROM FactPurchase fp
          WHERE fp.PurchaseOrderNumber = UPPER(LTRIM(RTRIM(ISNULL(s.SoPhieuNhap, 'UNKNOWN'))))
            AND fp.TenantID = @TenantID
            AND fp.ProductKey = p.ProductKey
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Dem so dong skip (da ton tai trong FactPurchase)
    SELECT @RowsSkipped = COUNT(*)
    FROM STG_PurchaseRaw s
    INNER JOIN DimProduct p ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
    INNER JOIN DimSupplier sup ON sup.SupplierCode = s.MaNCC
    INNER JOIN DimStore st ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayNhap AS DATE) = @BatchDate
      AND EXISTS (
          SELECT 1 FROM FactPurchase fp
          WHERE fp.PurchaseOrderNumber = UPPER(LTRIM(RTRIM(ISNULL(s.SoPhieuNhap, 'UNKNOWN'))))
            AND fp.TenantID = @TenantID
            AND fp.ProductKey = p.ProductKey
      );

    SET @RowsProcessed = @RowsInserted + @RowsSkipped + @RowsError;

    -- BUOC 4: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactPurchase',
        @BatchDate,
        CASE WHEN @RowsError = 0 THEN 'SUCCESS' ELSE 'SUCCESS' END,
        @RowsProcessed,
        @RowsInserted,
        0,
        @RowsSkipped,
        @RowsError,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Transform_FactPurchase [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Inserted=' + CAST(@RowsInserted AS VARCHAR(10))
        + ', Skipped=' + CAST(@RowsSkipped AS VARCHAR(10))
        + ', Errors=' + CAST(@RowsError AS VARCHAR(10))
        + ', Processed=' + CAST(@RowsProcessed AS VARCHAR(10))
        + '. Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalRowsInFact INT;
    SELECT @TotalRowsInFact = COUNT(*)
    FROM FactPurchase
    WHERE TenantID = @TenantID
      AND DateKey = CONVERT(INT, FORMAT(@BatchDate, 'yyyyMMdd'));
    PRINT '[VERIFY] FactPurchase [' + @TenantID + '] rows for ' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ': ' + CAST(@TotalRowsInFact AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactPurchase';
GO