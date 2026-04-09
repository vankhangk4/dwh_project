-- ============================================================================
-- PHASE 7: SQL Stored Procedures — FactSales (Tenant-Specific)
-- File: sql/sp/07_usp_Transform_FactSales.sql
-- Description: Chuyen doi du lieu tu STG_SalesRaw sang FactSales.
--              Tenant-Specific — chi xu ly du lieu cua tenant duoc chi dinh.
--              @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. Ghi nhan loi vao STG_ErrorLog neu khong tim thay ProductKey.
--   2. INSERT ban ghi hop le vao FactSales (neu chua ton tai InvoiceNumber + ProductKey).
--   3. Tinh toan: GrossSalesAmount, NetSalesAmount, CostAmount, GrossProfitAmount.
--   4. Ghi log ket qua vao ETL_RunLog.
--
-- Dependencies: STG_SalesRaw, DimProduct, DimStore, DimCustomer, DimEmployee.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Transform_FactSales')
BEGIN
    DROP PROCEDURE usp_Transform_FactSales;
END
GO

CREATE PROCEDURE usp_Transform_FactSales
    @TenantID  VARCHAR(20),
    @BatchDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Transform_FactSales: TenantID is required.';
        RETURN;
    END

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsSkipped  INT = 0;
    DECLARE @RowsError    INT = 0;
    DECLARE @RowsProcessed INT = 0;
    DECLARE @StartTime    DATETIME2 = GETDATE();

    -- BUOC 1: Ghi nhan ban ghi loi vao STG_ErrorLog
    -- Khong tim thay ProductKey trong DimProduct (IsCurrent=1)
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        SourceKey, RawData, BatchDate, ETLRunDate, LoadDatetime
    )
    SELECT
        @TenantID,
        'STG_SalesRaw',
        'DIMENSION_NOT_FOUND',
        N'Product not found in DimProduct (IsCurrent=1)',
        s.MaSP,
        CONCAT('MaHoaDon=', s.MaHoaDon, ', MaSP=', s.MaSP, ', NgayBan=', s.NgayBan),
        @BatchDate,
        GETDATE(),
        GETDATE()
    FROM STG_SalesRaw s
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayBan AS DATE) = @BatchDate
      AND NOT EXISTS (
          SELECT 1 FROM DimProduct p
          WHERE p.ProductCode = s.MaSP AND p.IsCurrent = 1
      );

    SET @RowsError = @@ROWCOUNT;

    -- BUOC 1b: Ghi nhan loi neu khong tim thay StoreKey
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        SourceKey, RawData, BatchDate, ETLRunDate, LoadDatetime
    )
    SELECT
        @TenantID,
        'STG_SalesRaw',
        'STORE_NOT_FOUND',
        N'Store not found in DimStore for this TenantID',
        s.MaCH,
        CONCAT('MaHoaDon=', s.MaHoaDon, ', MaCH=', s.MaCH),
        @BatchDate,
        GETDATE(),
        GETDATE()
    FROM STG_SalesRaw s
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayBan AS DATE) = @BatchDate
      AND NOT EXISTS (
          SELECT 1 FROM DimStore st
          WHERE st.StoreCode = s.MaCH AND st.TenantID = @TenantID
      );

    SET @RowsError = @RowsError + @@ROWCOUNT;

    -- BUOC 2: INSERT vao FactSales — ban ghi hop le
    -- Chi insert dong chua co trong FactSales (khong insert trung InvoiceNumber + ProductKey)
    INSERT INTO FactSales (
        TenantID, DateKey, ProductKey, StoreKey, CustomerKey, EmployeeKey,
        InvoiceNumber, InvoiceLine,
        Quantity, UnitPrice, DiscountAmount,
        GrossSalesAmount, NetSalesAmount, CostAmount, GrossProfitAmount,
        PaymentMethod, SalesChannel, SalesGroup,
        ReturnFlag, ReturnReason,
        LoadDatetime
    )
    SELECT
        @TenantID,
        CONVERT(INT, FORMAT(CAST(s.NgayBan AS DATE), 'yyyyMMdd')),
        p.ProductKey,
        st.StoreKey,
        CASE WHEN c.CustomerKey IS NOT NULL THEN c.CustomerKey ELSE -1 END,
        CASE WHEN e.EmployeeKey IS NOT NULL THEN e.EmployeeKey ELSE -1 END,

        UPPER(LTRIM(RTRIM(ISNULL(s.MaHoaDon, 'UNKNOWN')))),
        ISNULL(s.SoDong, 1),

        ISNULL(s.SoLuong, 0),
        ISNULL(s.DonGiaBan, 0),
        ISNULL(s.ChietKhau, 0),

        ISNULL(s.SoLuong, 0) * ISNULL(s.DonGiaBan, 0),
        (ISNULL(s.SoLuong, 0) * ISNULL(s.DonGiaBan, 0)) - ISNULL(s.ChietKhau, 0),
        ISNULL(s.SoLuong, 0) * p.UnitCostPrice,
        ((ISNULL(s.SoLuong, 0) * ISNULL(s.DonGiaBan, 0)) - ISNULL(s.ChietKhau, 0))
            - (ISNULL(s.SoLuong, 0) * p.UnitCostPrice),

        ISNULL(s.PhuongThucTT, N'Tiền mặt'),
        ISNULL(s.KenhBan, N'InStore'),
        ISNULL(s.NhomBanHang, N'Bán lẻ'),

        ISNULL(s.IsHoanTra, 0),
        s.LyDoHoanTra,

        GETDATE()
    FROM STG_SalesRaw s
    INNER JOIN DimProduct p
        ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
    INNER JOIN DimStore st
        ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
    LEFT JOIN DimCustomer c
        ON c.CustomerCode = s.MaKH
       AND c.TenantID = @TenantID
       AND c.IsCurrent = 1
    LEFT JOIN DimEmployee e
        ON e.EmployeeCode = s.MaNV
       AND e.TenantID = @TenantID
       AND e.IsActive = 1
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayBan AS DATE) = @BatchDate
      AND NOT EXISTS (
          SELECT 1 FROM FactSales f
          WHERE f.InvoiceNumber = UPPER(LTRIM(RTRIM(ISNULL(s.MaHoaDon, 'UNKNOWN'))))
            AND f.TenantID = @TenantID
            AND f.ProductKey = p.ProductKey
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Dem so dong da skip (da ton tai trong FactSales)
    SELECT @RowsSkipped = COUNT(*)
    FROM STG_SalesRaw s
    INNER JOIN DimProduct p ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
    INNER JOIN DimStore st ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayBan AS DATE) = @BatchDate
      AND EXISTS (
          SELECT 1 FROM FactSales f
          WHERE f.InvoiceNumber = UPPER(LTRIM(RTRIM(ISNULL(s.MaHoaDon, 'UNKNOWN'))))
            AND f.TenantID = @TenantID
            AND f.ProductKey = p.ProductKey
      );

    SET @RowsProcessed = @RowsInserted + @RowsSkipped + @RowsError;

    -- BUOC 4: Ghi log ket qua vao ETL_RunLog
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactSales',
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

    PRINT 'usp_Transform_FactSales [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Inserted=' + CAST(@RowsInserted AS VARCHAR(10))
        + ', Skipped=' + CAST(@RowsSkipped AS VARCHAR(10))
        + ', Errors=' + CAST(@RowsError AS VARCHAR(10))
        + ', Processed=' + CAST(@RowsProcessed AS VARCHAR(10))
        + '. Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalRowsInFact INT;
    SELECT @TotalRowsInFact = COUNT(*)
    FROM FactSales
    WHERE TenantID = @TenantID
      AND DateKey = CONVERT(INT, FORMAT(@BatchDate, 'yyyyMMdd'));
    PRINT '[VERIFY] FactSales [' + @TenantID + '] rows for ' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ': ' + CAST(@TotalRowsInFact AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactSales';
GO