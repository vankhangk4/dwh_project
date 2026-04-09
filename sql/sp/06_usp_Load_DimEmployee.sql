-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimEmployee (Tenant-Specific)
-- File: sql/sp/06_usp_Load_DimEmployee.sql
-- Description: Load / update DimEmployee tu STG_EmployeeRaw.
--              Tenant-Specific — chi xu ly nhan vien cua tenant duoc chi dinh.
--              Khong ap dung SCD Type 2 — chi insert/update don gian.
--              Nhan vien nghi viec (TerminationDate IS NOT NULL) -> IsActive = 0.
--
-- Dependencies: STG_EmployeeRaw, DimEmployee.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimEmployee')
BEGIN
    DROP PROCEDURE usp_Load_DimEmployee;
END
GO

CREATE PROCEDURE usp_Load_DimEmployee
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Load_DimEmployee: TenantID is required.';
        RETURN;
    END

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @RowsTerminated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Kiem tra STG_EmployeeRaw co du lieu cho tenant nay
    IF NOT EXISTS (SELECT TOP 1 * FROM STG_EmployeeRaw WHERE TenantID = @TenantID)
    BEGIN
        PRINT 'usp_Load_DimEmployee [' + @TenantID + ']: STG_EmployeeRaw is empty for this tenant. Nothing to load.';
        RETURN;
    END

    -- BUOC 1: Chen nhan vien moi (chua ton tai trong tenant do)
    INSERT INTO DimEmployee (
        TenantID, EmployeeCode, FullName, Gender, DateOfBirth,
        Phone, Email, Position, Department,
        HireDate, TerminationDate, ShiftType, IsActive,
        LoadDatetime
    )
    SELECT
        @TenantID,
        s.MaNV,
        s.HoTen,
        s.GioiTinh,
        s.NgaySinh,
        s.DienThoai,
        s.Email,
        s.ChucVu,
        s.PhongBan,
        s.NgayVaoLam,
        s.NgayNghiViec,
        ISNULL(s.CaLamViec, N'Sáng'),
        CASE WHEN s.NgayNghiViec IS NOT NULL THEN 0 ELSE 1 END,
        GETDATE()
    FROM STG_EmployeeRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimEmployee e
          WHERE e.TenantID = @TenantID
            AND e.EmployeeCode = s.MaNV
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 2: Cap nhat thong tin nhan vien da ton tai (neu thay doi)
    UPDATE e SET
        e.FullName        = s.HoTen,
        e.Gender          = s.GioiTinh,
        e.DateOfBirth     = s.NgaySinh,
        e.Phone           = s.DienThoai,
        e.Email           = s.Email,
        e.Position        = s.ChucVu,
        e.Department      = s.PhongBan,
        e.ShiftType       = ISNULL(s.CaLamViec, e.ShiftType),
        e.HireDate        = s.NgayVaoLam,
        e.TerminationDate = s.NgayNghiViec,
        e.IsActive        = CASE WHEN s.NgayNghiViec IS NOT NULL THEN 0 ELSE 1 END,
        e.LoadDatetime    = GETDATE()
    FROM DimEmployee e
    INNER JOIN STG_EmployeeRaw s ON s.MaNV = e.EmployeeCode
    WHERE e.TenantID = @TenantID
      AND s.TenantID = @TenantID;

    SET @RowsUpdated = @@ROWCOUNT;

    -- BUOC 3: Dem so nhan vien da nghi viec trong tenant nay
    SELECT @RowsTerminated = COUNT(*)
    FROM DimEmployee e
    WHERE e.TenantID = @TenantID
      AND e.TerminationDate IS NOT NULL
      AND e.IsActive = 0;

    -- BUOC 4: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
    DECLARE @TotalRows INT = @RowsInserted + @RowsUpdated;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
    )
    VALUES (
        @TenantID,
        'usp_Load_DimEmployee',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @TotalRows,
        @RowsInserted,
        @RowsUpdated,
        @RowsTerminated,
        0,
        NULL,
        @StartTime,
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Load_DimEmployee [' + @TenantID + ']: Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new employee(s), Updated '
        + CAST(@RowsUpdated AS VARCHAR(10)) + ' existing employee(s), '
        + CAST(@RowsTerminated AS VARCHAR(10)) + ' terminated employee(s).'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @TotalEmployees INT;
    DECLARE @ActiveEmployees INT;
    SELECT
        @TotalEmployees = COUNT(*),
        @ActiveEmployees = SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END)
    FROM DimEmployee WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DimEmployee [' + @TenantID + '] — Total: '
        + CAST(@TotalEmployees AS VARCHAR(10)) + ', Active: '
        + CAST(@ActiveEmployees AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimEmployee';
GO