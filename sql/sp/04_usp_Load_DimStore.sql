-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimStore (Tenant-Specific)
-- File: sql/sp/04_usp_Load_DimStore.sql
-- Description: Load / update DimStore tu STG_StoreRaw.
--              Tenant-Specific — chi xu ly cua hang cua tenant duoc chi dinh.
--              Khong ap dung SCD Type 2 — chi insert/update don gian.
--
-- Dependencies: STG_StoreRaw, DimStore, DimSupplier (FK).
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimStore')
BEGIN
    DROP PROCEDURE usp_Load_DimStore;
END
GO

CREATE PROCEDURE usp_Load_DimStore
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Load_DimStore: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_StoreRaw co du lieu cho tenant nay
    IF NOT EXISTS (SELECT TOP 1 * FROM STG_StoreRaw WHERE TenantID = @TenantID)
    BEGIN
        PRINT 'usp_Load_DimStore [' + @TenantID + ']: STG_StoreRaw is empty for this tenant. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: Chen cua hang moi (chua ton tai trong tenant do)
    INSERT INTO DimStore (
        TenantID, StoreCode, StoreName, StoreType,
        Address, Ward, District, City, Region,
        Phone, Email, ManagerName,
        OpenDate, CloseDate, IsActive,
        LoadDatetime
    )
    SELECT
        @TenantID,
        s.MaCH,
        s.TenCH,
        ISNULL(s.LoaiCH, N'Cửa hàng truyền thống'),
        s.DiaChi,
        s.Phuong,
        s.Quan,
        s.ThanhPho,
        s.Vung,
        s.DienThoai,
        s.Email,
        s.NguoiQuanLy,
        s.NgayKhaiTruong,
        s.NgayDongCua,
        CASE WHEN s.NgayDongCua IS NOT NULL THEN 0 ELSE 1 END,
        GETDATE()
    FROM STG_StoreRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimStore d
          WHERE d.TenantID = @TenantID
            AND d.StoreCode = s.MaCH
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 2: Cap nhat thong tin cua hang da ton tai (neu thay doi)
    UPDATE d SET
        d.StoreName   = s.TenCH,
        d.StoreType   = ISNULL(s.LoaiCH, d.StoreType),
        d.Address     = s.DiaChi,
        d.Ward        = s.Phuong,
        d.District    = s.Quan,
        d.City        = s.ThanhPho,
        d.Region      = s.Vung,
        d.Phone       = s.DienThoai,
        d.Email       = s.Email,
        d.ManagerName = s.NguoiQuanLy,
        d.OpenDate    = s.NgayKhaiTruong,
        d.CloseDate   = s.NgayDongCua,
        d.IsActive    = CASE WHEN s.NgayDongCua IS NOT NULL THEN 0 ELSE 1 END,
        d.LoadDatetime = GETDATE()
    FROM DimStore d
    INNER JOIN STG_StoreRaw s ON s.MaCH = d.StoreCode
    WHERE d.TenantID = @TenantID
      AND s.TenantID = @TenantID;

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
        @TenantID,
        'usp_Load_DimStore',
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

    PRINT 'usp_Load_DimStore [' + @TenantID + ']: Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new store(s), Updated '
        + CAST(@RowsUpdated AS VARCHAR(10)) + ' existing store(s).'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalStores INT;
    DECLARE @ActiveStores INT;
    SELECT
        @TotalStores = COUNT(*),
        @ActiveStores = SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END)
    FROM DimStore WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DimStore [' + @TenantID + '] — Total: '
        + CAST(@TotalStores AS VARCHAR(10)) + ', Active: '
        + CAST(@ActiveStores AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimStore';
GO
