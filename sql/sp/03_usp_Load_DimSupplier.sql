-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimSupplier (Shared)
-- File: sql/sp/03_usp_Load_DimSupplier.sql
-- Description: Load / update DimSupplier tu STG_SupplierRaw.
--              Shared — dung chung cho tat ca tenant.
--              Khong ap dung SCD Type 2 — chi insert/update don gian.
--
-- Dependencies: STG_SupplierRaw, DimSupplier.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimSupplier')
BEGIN
    DROP PROCEDURE usp_Load_DimSupplier;
END
GO

CREATE PROCEDURE usp_Load_DimSupplier
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_SupplierRaw co du lieu
    IF NOT EXISTS (SELECT TOP 1 * FROM STG_SupplierRaw)
    BEGIN
        PRINT 'usp_Load_DimSupplier: STG_SupplierRaw is empty. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: Chen nha cung cap moi (chua ton tai theo SupplierCode)
    INSERT INTO DimSupplier (
        SupplierCode, SupplierName, ContactName, ContactTitle,
        Phone, Email, Address, City, Country,
        TaxCode, PaymentTerms, IsActive,
        CreatedAt, LoadDatetime
    )
    SELECT
        s.MaNCC,
        s.TenNCC,
        s.NguoiLienHe,
        s.ChucVu,
        s.DienThoai,
        s.Email,
        s.DiaChi,
        s.ThanhPho,
        ISNULL(s.QuocGia, N'Việt Nam'),
        s.MaSoThue,
        s.DieuKhoanTT,
        1,
        GETDATE(),
        GETDATE()
    FROM STG_SupplierRaw s
    WHERE NOT EXISTS (
        SELECT 1 FROM DimSupplier d
        WHERE d.SupplierCode = s.MaNCC
    );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 2: Cap nhat thong tin nha cung cap da ton tai (thong tin thay doi)
    UPDATE d SET
        d.SupplierName  = s.TenNCC,
        d.ContactName   = s.NguoiLienHe,
        d.ContactTitle   = s.ChucVu,
        d.Phone         = s.DienThoai,
        d.Email         = s.Email,
        d.Address       = s.DiaChi,
        d.City          = s.ThanhPho,
        d.TaxCode       = s.MaSoThue,
        d.PaymentTerms  = s.DieuKhoanTT,
        d.LoadDatetime  = GETDATE()
    FROM DimSupplier d
    INNER JOIN STG_SupplierRaw s ON s.MaNCC = d.SupplierCode;

    SET @RowsUpdated = @@ROWCOUNT;

    -- BUOC 3: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
    DECLARE @TotalRows INT = @RowsInserted + @RowsUpdated;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        'SHARED',
        'usp_Load_DimSupplier',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @TotalRows,
        @RowsInserted,
        @RowsUpdated,
        0,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Load_DimSupplier: Inserted ' + CAST(@RowsInserted AS VARCHAR(10))
        + ' new supplier(s), Updated ' + CAST(@RowsUpdated AS VARCHAR(10))
        + ' existing supplier(s). Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalActive INT;
    SELECT @TotalActive = COUNT(*) FROM DimSupplier WHERE IsActive = 1;
    PRINT '[VERIFY] DimSupplier active suppliers: ' + CAST(@TotalActive AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimSupplier';
GO
