-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimProduct (Shared, SCD Type 2)
-- File: sql/sp/02_usp_Load_DimProduct.sql
-- Description: Load / update DimProduct tu STG_ProductRaw.
--              Shared — dung chung cho tat ca tenant.
--              SCD Type 2: Dong cu khi gia thay doi, chen dong moi voi gia moi.
--
-- Dependencies: STG_ProductRaw (da co du lieu), DimProduct (da tao).
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimProduct')
BEGIN
    DROP PROCEDURE usp_Load_DimProduct;
END
GO

CREATE PROCEDURE usp_Load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_ProductRaw co du lieu
    IF NOT EXISTS (SELECT TOP 1 * FROM STG_ProductRaw)
    BEGIN
        PRINT 'usp_Load_DimProduct: STG_ProductRaw is empty. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: Dong cac ban ghi cu khi bat ky thong tin nao thay doi
    -- Chi xu ly dong IsCurrent=1
    UPDATE dp SET
        dp.ExpirationDate = DATEADD(DAY, -1, @BatchDate),
        dp.IsCurrent      = 0,
        dp.LoadDatetime   = GETDATE()
    FROM DimProduct dp
    INNER JOIN STG_ProductRaw s ON s.MaSP = dp.ProductCode
    WHERE dp.IsCurrent = 1
      AND (
          dp.UnitCostPrice <> ISNULL(s.GiaVon, 0)
          OR dp.UnitListPrice <> ISNULL(s.GiaNiemYet, 0)
          OR dp.ProductName <> s.TenSP
          OR ISNULL(dp.Brand, N'') <> ISNULL(s.ThuongHieu, N'')
          OR ISNULL(dp.CategoryName, N'') <> ISNULL(s.DanhMuc, N'')
          OR ISNULL(dp.SubCategory, N'') <> ISNULL(s.PhanLoai, N'')
          OR ISNULL(dp.SKU, N'') <> ISNULL(s.SKU, N'')
          OR ISNULL(dp.Barcode, N'') <> ISNULL(s.Barcode, N'')
      );

    SET @RowsClosed = @@ROWCOUNT;

    -- BUOC 2: Chen ban ghi moi cho san pham CHUA TON TAI trong DimProduct
    -- (MaSP chua tung ton tai trong DimProduct)
    INSERT INTO DimProduct (
        ProductCode, ProductName, Brand, CategoryName, SubCategory,
        UnitCostPrice, UnitListPrice, SKU, Barcode,
        SupplierKey, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent,
        LoadDatetime
    )
    SELECT
        s.MaSP,
        s.TenSP,
        ISNULL(s.ThuongHieu, N'Khác'),
        ISNULL(s.DanhMuc, N'Khác'),
        s.PhanLoai,
        ISNULL(s.GiaVon, 0),
        ISNULL(s.GiaNiemYet, 0),
        s.SKU,
        s.Barcode,
        NULL,
        1,
        @BatchDate,
        NULL,
        1,
        GETDATE()
    FROM STG_ProductRaw s
    WHERE NOT EXISTS (
        SELECT 1 FROM DimProduct dp
        WHERE dp.ProductCode = s.MaSP AND dp.IsCurrent = 1
    );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Chen ban ghi moi cho san pham DA TON TAI nhung THONG TIN THAY DOI
    -- Dong cu da bi dong o buoc 1 (ExpirationDate = hom qua), gio chen dong moi
    INSERT INTO DimProduct (
        ProductCode, ProductName, Brand, CategoryName, SubCategory,
        UnitCostPrice, UnitListPrice, SKU, Barcode,
        SupplierKey, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent,
        LoadDatetime
    )
    SELECT
        s.MaSP,
        s.TenSP,
        ISNULL(s.ThuongHieu, N'Khác'),
        ISNULL(s.DanhMuc, N'Khác'),
        s.PhanLoai,
        ISNULL(s.GiaVon, 0),
        ISNULL(s.GiaNiemYet, 0),
        s.SKU,
        s.Barcode,
        NULL,
        1,
        @BatchDate,
        NULL,
        1,
        GETDATE()
    FROM STG_ProductRaw s
    WHERE EXISTS (
        SELECT 1 FROM DimProduct dp_closed
        WHERE dp_closed.ProductCode = s.MaSP
          AND dp_closed.IsCurrent = 0
          AND dp_closed.ExpirationDate = DATEADD(DAY, -1, @BatchDate)
    )
    AND NOT EXISTS (
        SELECT 1 FROM DimProduct dp_current
        WHERE dp_current.ProductCode = s.MaSP AND dp_current.IsCurrent = 1
    );

    SET @RowsUpdated = @@ROWCOUNT;
    SET @RowsInserted = @RowsInserted + @@ROWCOUNT;

    -- BUOC 4: Ghi log ket qua
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
    DECLARE @TotalRows INT = @RowsClosed + @RowsInserted;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    SELECT
        'SHARED',
        'usp_Load_DimProduct',
        CAST(GETDATE() AS DATE),
        CASE WHEN @TotalRows > 0 THEN 'SUCCESS' ELSE 'SUCCESS' END,
        @TotalRows,
        @RowsInserted,
        @RowsClosed,
        0,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration;

    PRINT 'usp_Load_DimProduct: Closed ' + CAST(@RowsClosed AS VARCHAR(10))
        + ' row(s), Inserted ' + CAST(@RowsInserted AS VARCHAR(10)) + ' new row(s).'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalCurrent INT;
    DECLARE @TotalExpired INT;
    SELECT
        @TotalCurrent = SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END),
        @TotalExpired = SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END)
    FROM DimProduct;
    PRINT '[VERIFY] DimProduct — Current: ' + CAST(@TotalCurrent AS VARCHAR(10))
        + ', Expired: ' + CAST(@TotalExpired AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimProduct';
GO