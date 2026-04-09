-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimCustomer (Tenant-Specific, SCD Type 2)
-- File: sql/sp/05_usp_Load_DimCustomer.sql
-- Description: Load / update DimCustomer tu STG_CustomerRaw.
--              Tenant-Specific — chi xu ly khach hang cua tenant duoc chi dinh.
--              SCD Type 2: Theo doi thay doi FullName, CustomerType, City,
--              LoyaltyTier, Phone. Dong cu khi thay doi, chen dong moi.
--
-- Dependencies: STG_CustomerRaw, DimCustomer.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimCustomer')
BEGIN
    DROP PROCEDURE usp_Load_DimCustomer;
END
GO

CREATE PROCEDURE usp_Load_DimCustomer
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Load_DimCustomer: TenantID is required.';
        RETURN;
    END

    DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_CustomerRaw co du lieu cho tenant nay
    IF NOT EXISTS (SELECT TOP 1 * FROM STG_CustomerRaw WHERE TenantID = @TenantID)
    BEGIN
        PRINT 'usp_Load_DimCustomer [' + @TenantID + ']: STG_CustomerRaw is empty for this tenant. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: Dong cac ban ghi cu khi bat ky thong tin nao thay doi
    -- Chi xu ly dong IsCurrent=1, TenantID = @TenantID
    UPDATE dc SET
        dc.ExpirationDate = DATEADD(DAY, -1, @BatchDate),
        dc.IsCurrent      = 0,
        dc.LoadDatetime   = GETDATE()
    FROM DimCustomer dc
    INNER JOIN STG_CustomerRaw s
        ON s.MaKH = dc.CustomerCode
        AND dc.TenantID = @TenantID
        AND s.TenantID = @TenantID
    WHERE dc.IsCurrent = 1
      AND dc.TenantID = @TenantID
      AND (
          dc.FullName    <> s.HoTen
          OR dc.CustomerType <> ISNULL(s.LoaiKH, dc.CustomerType)
          OR dc.City      <> ISNULL(s.ThanhPho, dc.City)
          OR dc.LoyaltyTier <> ISNULL(s.HangTV, dc.LoyaltyTier)
          OR dc.Phone     <> ISNULL(s.DienThoai, dc.Phone)
          OR dc.Address   <> ISNULL(s.DiaChi, dc.Address)
          OR dc.Email     <> ISNULL(s.Email, dc.Email)
      );

    SET @RowsClosed = @@ROWCOUNT;

    -- BUOC 2: Chen ban ghi moi cho khach hang CHUA TON TAI trong tenant do
    -- (MaKH chua tung ton tai trong DimCustomer cua tenant nay)
    INSERT INTO DimCustomer (
        TenantID, CustomerCode, FullName, Gender, DateOfBirth,
        Phone, Email, Address, City,
        CustomerType, LoyaltyTier, LoyaltyPoint,
        MemberSince, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent,
        LoadDatetime
    )
    SELECT
        @TenantID,
        s.MaKH,
        s.HoTen,
        s.GioiTinh,
        s.NgaySinh,
        s.DienThoai,
        s.Email,
        s.DiaChi,
        s.ThanhPho,
        ISNULL(s.LoaiKH, N'Khách lẻ'),
        ISNULL(s.HangTV, N'Bronze'),
        ISNULL(s.DiemTichLuy, 0),
        s.NgayDangKy,
        1,
        @BatchDate,
        NULL,
        1,
        GETDATE()
    FROM STG_CustomerRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimCustomer dc
          WHERE dc.CustomerCode = s.MaKH
            AND dc.TenantID = @TenantID
            AND dc.IsCurrent = 1
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Chen ban ghi moi cho khach hang DA TON TAI nhung THONG TIN THAY DOI
    -- Dong cu da bi dong o buoc 1 (ExpirationDate = hom qua),
    -- gio chen dong moi voi thong tin cap nhat
    INSERT INTO DimCustomer (
        TenantID, CustomerCode, FullName, Gender, DateOfBirth,
        Phone, Email, Address, City,
        CustomerType, LoyaltyTier, LoyaltyPoint,
        MemberSince, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent,
        LoadDatetime
    )
    SELECT
        @TenantID,
        s.MaKH,
        s.HoTen,
        s.GioiTinh,
        s.NgaySinh,
        s.DienThoai,
        s.Email,
        s.DiaChi,
        s.ThanhPho,
        ISNULL(s.LoaiKH, N'Khách lẻ'),
        ISNULL(s.HangTV, N'Bronze'),
        ISNULL(s.DiemTichLuy, 0),
        s.NgayDangKy,
        1,
        @BatchDate,
        NULL,
        1,
        GETDATE()
    FROM STG_CustomerRaw s
    WHERE s.TenantID = @TenantID
      AND EXISTS (
          SELECT 1 FROM DimCustomer dc_closed
          WHERE dc_closed.CustomerCode = s.MaKH
            AND dc_closed.TenantID = @TenantID
            AND dc_closed.IsCurrent = 0
            AND dc_closed.ExpirationDate = DATEADD(DAY, -1, @BatchDate)
      )
      AND NOT EXISTS (
          SELECT 1 FROM DimCustomer dc_current
          WHERE dc_current.CustomerCode = s.MaKH
            AND dc_current.TenantID = @TenantID
            AND dc_current.IsCurrent = 1
      );

    SET @RowsInserted = @RowsInserted + @@ROWCOUNT;

    -- BUOC 4: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
    DECLARE @TotalRows INT = @RowsClosed + @RowsInserted;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Load_DimCustomer',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @TotalRows,
        @RowsInserted,
        @RowsClosed,
        0,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Load_DimCustomer [' + @TenantID + ']: Closed '
        + CAST(@RowsClosed AS VARCHAR(10)) + ' row(s), Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new row(s).'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalCurrent INT;
    DECLARE @TotalExpired INT;
    SELECT
        @TotalCurrent = SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END),
        @TotalExpired = SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END)
    FROM DimCustomer WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DimCustomer [' + @TenantID + '] — Current: '
        + CAST(@TotalCurrent AS VARCHAR(10)) + ', Expired: '
        + CAST(@TotalExpired AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimCustomer';
GO
