-- ============================================================================
-- PHASE 3: Tenant-Specific Dimensions Schema
-- File: sql/schema/03_create_dimensions_tenant.sql
-- Description: Tao cac bang Dimension co TenantID.
--              - DimStore    : Chiều cửa hàng (co TenantID)
--              - DimCustomer : Chiều khách hàng (co TenantID, SCD Type 2)
--              - DimEmployee : Chiều nhân viên (co TenantID)
--
-- NOTE: Cac bang nay CO TenantID vi thuoc ve tung cua hang/chi nhanh.
--       Phu thuoc: Chay SAU Phase 1 & Phase 2
--                  (DimCustomer can DimStore da ton tai de seed)
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG 1: DimStore
-- Chiều cửa hàng — CO TenantID.
-- Moi tenant co mot hoac nhieu cua hang (thuong thi 1).
-- TenantID la bat buoc — dam bao rang cua hang chi thuoc ve 1 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimStore')
BEGIN
    CREATE TABLE DimStore (
        StoreKey        INT IDENTITY(1,1) NOT NULL,
        TenantID        VARCHAR(20)       NOT NULL,
        StoreCode       VARCHAR(50)       NOT NULL,
        StoreName       NVARCHAR(200)     NOT NULL,
        StoreType       NVARCHAR(50)      NULL,       -- 'Cửa hàng truyền thống', 'Siêu thị', 'Kiosk'
        Address         NVARCHAR(500)     NULL,
        Ward            NVARCHAR(100)     NULL,
        District        NVARCHAR(100)     NULL,
        City            NVARCHAR(100)     NOT NULL,
        Region          NVARCHAR(50)      NULL,       -- 'Miền Bắc', 'Miền Trung', 'Miền Nam'
        Phone           VARCHAR(30)         NULL,
        Email           VARCHAR(100)        NULL,
        ManagerName     NVARCHAR(200)     NULL,
        OpenDate        DATE              NULL,
        CloseDate       DATE              NULL,
        IsActive        BIT               NOT NULL DEFAULT 1,
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimStore PRIMARY KEY CLUSTERED (StoreKey),
        CONSTRAINT UQ_DimStore_TenantCode UNIQUE (TenantID, StoreCode)
    );

    PRINT 'Created table: DimStore';
END
ELSE
BEGIN
    PRINT 'Table DimStore already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimStore_TenantID'
    AND object_id = OBJECT_ID('DimStore')
)
BEGIN
    CREATE INDEX IX_DimStore_TenantID ON DimStore(TenantID);
    PRINT 'Created index: IX_DimStore_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimStore_IsActive'
    AND object_id = OBJECT_ID('DimStore')
)
BEGIN
    CREATE INDEX IX_DimStore_IsActive ON DimStore(IsActive);
    PRINT 'Created index: IX_DimStore_IsActive';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimStore_City'
    AND object_id = OBJECT_ID('DimStore')
)
BEGIN
    CREATE INDEX IX_DimStore_City ON DimStore(City);
    PRINT 'Created index: IX_DimStore_City';
END
GO

-- ============================================================================
-- BANG 2: DimCustomer
-- Chiều khách hàng — CO TenantID, SCD Type 2.
-- TenantID bat buoc — dam bao rang khach hang chi thuoc ve 1 tenant.
-- SCD Type 2: Theo doi thay doi thong tin (ho ten, dia chi, hang thanh vien).
-- Luu y: Khong co Unique constraint (TenantID, CustomerCode) vi SCD tao nhieu dong.
--         dung filtered index thay vi: UNIQUE (CustomerCode) WHERE TenantID = 'X'
--         Hoac kiem tra trong SP: NOT EXISTS.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimCustomer')
BEGIN
    CREATE TABLE DimCustomer (
        CustomerKey     INT IDENTITY(1,1) NOT NULL,
        TenantID        VARCHAR(20)       NOT NULL,
        CustomerCode    VARCHAR(50)       NOT NULL,
        FullName        NVARCHAR(200)     NOT NULL,
        Gender          NVARCHAR(10)      NULL,       -- 'Nam', 'Nữ', 'Khác'
        DateOfBirth     DATE              NULL,
        Phone           VARCHAR(20)        NULL,
        Email           VARCHAR(100)        NULL,
        Address         NVARCHAR(500)     NULL,
        City            NVARCHAR(100)     NULL,
        CustomerType    NVARCHAR(50)      NULL,       -- 'Khách lẻ', 'Khách VIP', 'Đại lý'
        LoyaltyTier     NVARCHAR(50)      NULL,       -- 'Bronze', 'Silver', 'Gold', 'Platinum'
        LoyaltyPoint    INT               NOT NULL DEFAULT 0,
        MemberSince     DATE              NULL,
        IsActive        BIT               NOT NULL DEFAULT 1,
        EffectiveDate   DATE              NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        ExpirationDate  DATE              NULL,
        IsCurrent       BIT               NOT NULL DEFAULT 1,
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimCustomer PRIMARY KEY CLUSTERED (CustomerKey)
    );

    PRINT 'Created table: DimCustomer';
END
ELSE
BEGIN
    PRINT 'Table DimCustomer already exists — skipping CREATE.';
END
GO

-- Unique constraint: 1 CustomerCode chi co 1 dong IsCurrent=1 trong 1 tenant
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQIX_DimCustomer_TenantCode_Current'
    AND object_id = OBJECT_ID('DimCustomer')
)
BEGIN
    CREATE UNIQUE INDEX UQIX_DimCustomer_TenantCode_Current
        ON DimCustomer(TenantID, CustomerCode)
        WHERE IsCurrent = 1;
    PRINT 'Created filtered index: UQIX_DimCustomer_TenantCode_Current';
END
ELSE
BEGIN
    PRINT 'Filtered index UQIX_DimCustomer_TenantCode_Current already exists — skipping.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimCustomer_TenantID_IsCurrent'
    AND object_id = OBJECT_ID('DimCustomer')
)
BEGIN
    CREATE INDEX IX_DimCustomer_TenantID_IsCurrent
        ON DimCustomer(TenantID, IsCurrent)
        WHERE IsCurrent = 1;
    PRINT 'Created filtered index: IX_DimCustomer_TenantID_IsCurrent';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimCustomer_City'
    AND object_id = OBJECT_ID('DimCustomer')
)
BEGIN
    CREATE INDEX IX_DimCustomer_City ON DimCustomer(City);
    PRINT 'Created index: IX_DimCustomer_City';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimCustomer_LoyaltyTier'
    AND object_id = OBJECT_ID('DimCustomer')
)
BEGIN
    CREATE INDEX IX_DimCustomer_LoyaltyTier ON DimCustomer(LoyaltyTier);
    PRINT 'Created index: IX_DimCustomer_LoyaltyTier';
END
GO

-- ============================================================================
-- BANG 3: DimEmployee
-- Chiều nhân viên — CO TenantID.
-- TenantID bat buoc — dam bao rang nhan vien chi thuoc ve 1 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimEmployee')
BEGIN
    CREATE TABLE DimEmployee (
        EmployeeKey     INT IDENTITY(1,1) NOT NULL,
        TenantID        VARCHAR(20)       NOT NULL,
        EmployeeCode    VARCHAR(50)       NOT NULL,
        FullName        NVARCHAR(200)     NOT NULL,
        Gender          NVARCHAR(10)      NULL,
        DateOfBirth     DATE              NULL,
        Phone           VARCHAR(20)        NULL,
        Email           VARCHAR(100)        NULL,
        Position        NVARCHAR(100)     NULL,       -- 'Nhân viên bán hàng', 'Quản lý', 'Kế toán'
        Department      NVARCHAR(100)     NULL,       -- 'Kinh doanh', 'Kho vận', 'Kế toán'
        HireDate        DATE              NULL,
        TerminationDate DATE              NULL,
        ShiftType       NVARCHAR(20)      NULL,       -- 'Sáng', 'Chiều', 'Tối', 'Ca đêm'
        IsActive        BIT               NOT NULL DEFAULT 1,
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimEmployee PRIMARY KEY CLUSTERED (EmployeeKey),
        CONSTRAINT UQ_DimEmployee_TenantCode UNIQUE (TenantID, EmployeeCode)
    );

    PRINT 'Created table: DimEmployee';
END
ELSE
BEGIN
    PRINT 'Table DimEmployee already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimEmployee_TenantID'
    AND object_id = OBJECT_ID('DimEmployee')
)
BEGIN
    CREATE INDEX IX_DimEmployee_TenantID ON DimEmployee(TenantID);
    PRINT 'Created index: IX_DimEmployee_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimEmployee_IsActive'
    AND object_id = OBJECT_ID('DimEmployee')
)
BEGIN
    CREATE INDEX IX_DimEmployee_IsActive ON DimEmployee(IsActive);
    PRINT 'Created index: IX_DimEmployee_IsActive';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimEmployee_Position'
    AND object_id = OBJECT_ID('DimEmployee')
)
BEGIN
    CREATE INDEX IX_DimEmployee_Position ON DimEmployee(Position);
    PRINT 'Created index: IX_DimEmployee_Position';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DimEmployee_Department'
    AND object_id = OBJECT_ID('DimEmployee')
)
BEGIN
    CREATE INDEX IX_DimEmployee_Department ON DimEmployee(Department);
    PRINT 'Created index: IX_DimEmployee_Department';
END
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimStore
-- Load cua hang tu STG_StoreRaw.
-- TenantID la tham so dau vao — chi xu ly cua hang cua tenant do.
-- Buoc 1: INSERT cua hang moi.
-- Buoc 2: UPDATE thong tin cua hang da ton tai (nếu thay doi).
-- ============================================================================
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

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;

    -- BUOC 1: Chen cua hang moi (chua ton tai trong tenant do)
    INSERT INTO DimStore (
        TenantID, StoreCode, StoreName, StoreType,
        Address, Ward, District, City, Region,
        Phone, Email, ManagerName,
        OpenDate, CloseDate, IsActive
    )
    SELECT
        @TenantID,
        s.MaCH,
        s.TenCH,
        s.LoaiCH,
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
        1
    FROM STG_StoreRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimStore d
          WHERE d.TenantID = @TenantID
            AND d.StoreCode = s.MaCH
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 2: Cap nhat cua hang da ton tai (thong tin thay doi)
    UPDATE d SET
        d.StoreName   = s.TenCH,
        d.StoreType   = s.LoaiCH,
        d.Address     = s.DiaChi,
        d.Ward        = s.Phuong,
        d.District    = s.Quan,
        d.City        = s.ThanhPho,
        d.Region      = s.Vung,
        d.Phone       = s.DienThoai,
        d.Email       = s.Email,
        d.ManagerName = s.NguoiQuanLy,
        d.CloseDate   = s.NgayDongCua,
        d.IsActive    = CASE WHEN s.NgayDongCua IS NOT NULL THEN 0 ELSE 1 END
    FROM DimStore d
    INNER JOIN STG_StoreRaw s ON s.MaCH = d.StoreCode
    WHERE d.TenantID = @TenantID
      AND s.TenantID = @TenantID;

    SET @RowsUpdated = @@ROWCOUNT;

    PRINT 'usp_Load_DimStore [' + @TenantID + ']: Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new store(s), Updated '
        + CAST(@RowsUpdated AS VARCHAR(10)) + ' existing store(s).';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimStore';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimCustomer
-- Load khach hang tu STG_CustomerRaw.
-- TenantID la tham so dau vao — chi xu ly khach hang cua tenant do.
-- SCD Type 2: Theo doi thay doi FullName, CustomerType, City.
-- Buoc 1: Dong (close) cac ban ghi cu khi thong tin thay doi.
-- Buoc 2: Chen ban ghi moi cho khach hang CHUA TON TAI.
-- Buoc 3: Chen ban ghi moi cho khach hang DA TON TAI nhung thong tin THAY DOI.
-- ============================================================================
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

    DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsInserted INT = 0;

    -- BUOC 1: Dong cac ban ghi cu khi bat ky thong tin nao thay doi
    -- Chi xu ly dong IsCurrent=1, TenantID = @TenantID
    UPDATE dc SET
        dc.ExpirationDate = DATEADD(DAY, -1, @BatchDate),
        dc.IsCurrent      = 0
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
      );

    SET @RowsClosed = @@ROWCOUNT;

    -- BUOC 2: Chen ban ghi moi cho khach hang CHUA TON TAI trong tenant do
    INSERT INTO DimCustomer (
        TenantID, CustomerCode, FullName, Gender, DateOfBirth,
        Phone, Email, Address, City,
        CustomerType, LoyaltyTier, LoyaltyPoint,
        MemberSince, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
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
        1
    FROM STG_CustomerRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimCustomer dc
          WHERE dc.CustomerCode = s.MaKH
            AND dc.TenantID = @TenantID
            AND dc.IsCurrent = 1
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Chen ban ghi moi cho khach hang DA TON TAI nhung thong tin THAY DOI
    -- Dong cu da bi dong o buoc 1, gio chen dong moi voi thong tin moi
    INSERT INTO DimCustomer (
        TenantID, CustomerCode, FullName, Gender, DateOfBirth,
        Phone, Email, Address, City,
        CustomerType, LoyaltyTier, LoyaltyPoint,
        MemberSince, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
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
        1
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

    PRINT 'usp_Load_DimCustomer [' + @TenantID + ']: Closed '
        + CAST(@RowsClosed AS VARCHAR(10)) + ' row(s), Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new row(s).';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimCustomer';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimEmployee
-- Load nhan vien tu STG_EmployeeRaw.
-- TenantID la tham so dau vao — chi xu ly nhan vien cua tenant do.
-- Khong ap dung SCD Type 2 (nhan vien thuong khong nhieu thay doi, chi can update).
-- Buoc 1: INSERT nhan vien moi.
-- Buoc 2: UPDATE nhan vien da ton tai (neu thong tin thay doi).
-- Buoc 3: Dong nhan vien nghi viec (TerminationDate da duoc dien).
-- ============================================================================
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

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @RowsTerminated INT = 0;

    -- BUOC 1: Chen nhan vien moi (chua ton tai trong tenant do)
    INSERT INTO DimEmployee (
        TenantID, EmployeeCode, FullName, Gender, DateOfBirth,
        Phone, Email, Position, Department,
        HireDate, TerminationDate, ShiftType, IsActive
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
        s.CaLamViec,
        CASE WHEN s.NgayNghiViec IS NOT NULL THEN 0 ELSE 1 END
    FROM STG_EmployeeRaw s
    WHERE s.TenantID = @TenantID
      AND NOT EXISTS (
          SELECT 1 FROM DimEmployee e
          WHERE e.TenantID = @TenantID
            AND e.EmployeeCode = s.MaNV
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 2: Cap nhat nhan vien da ton tai (thong tin thay doi)
    UPDATE e SET
        e.FullName       = s.HoTen,
        e.Gender         = s.GioiTinh,
        e.DateOfBirth    = s.NgaySinh,
        e.Phone          = s.DienThoai,
        e.Email          = s.Email,
        e.Position       = s.ChucVu,
        e.Department     = s.PhongBan,
        e.ShiftType      = s.CaLamViec,
        e.TerminationDate = s.NgayNghiViec,
        e.IsActive       = CASE WHEN s.NgayNghiViec IS NOT NULL THEN 0 ELSE 1 END
    FROM DimEmployee e
    INNER JOIN STG_EmployeeRaw s ON s.MaNV = e.EmployeeCode
    WHERE e.TenantID = @TenantID
      AND s.TenantID = @TenantID;

    SET @RowsUpdated = @@ROWCOUNT;

    -- BUOC 3: Dong nhan vien da co TerminationDate (nghi viec)
    -- Nhung nhan vien nay da duoc update o buoc 2, chi log them
    SELECT @RowsTerminated = COUNT(*)
    FROM DimEmployee e
    WHERE e.TenantID = @TenantID
      AND e.TerminationDate IS NOT NULL
      AND e.IsActive = 0;

    PRINT 'usp_Load_DimEmployee [' + @TenantID + ']: Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new employee(s), Updated '
        + CAST(@RowsUpdated AS VARCHAR(10)) + ' existing employee(s), '
        + CAST(@RowsTerminated AS VARCHAR(10)) + ' terminated employee(s).';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimEmployee';
GO

-- ============================================================================
-- SEED DATA: Insert du lieu mau cho DimStore
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimStore)
BEGIN
    INSERT INTO DimStore (
        TenantID, StoreCode, StoreName, StoreType,
        Address, Ward, District, City, Region,
        Phone, Email, ManagerName,
        OpenDate, CloseDate, IsActive
    )
    VALUES
        -- STORE_HN
        ('STORE_HN', 'STORE_HN_01', N'Cửa hàng Trần Duy Hưng', N'Cửa hàng truyền thống',
         N'45 Trần Duy Hưng, Cầu Giấy', N'Trung Hoà', N'Cầu Giấy', N'Hà Nội', N'Miền Bắc',
         '024-37891234', 'hanoi@dwh.local', N'Nguyễn Văn Minh',
         '2019-03-15', NULL, 1),
        ('STORE_HN', 'STORE_HN_02', N'Cửa hàng Ngọc Khánh', N'Cửa hàng truyền thống',
         N'78 Ngọc Khánh, Ba Đình', N'Ngọc Khánh', N'Ba Đình', N'Hà Nội', N'Miền Bắc',
         '024-37654321', 'hanoi2@dwh.local', N'Trần Thị Lan',
         '2020-07-01', NULL, 1),
        -- STORE_HCM
        ('STORE_HCM', 'STORE_HCM_01', N'Cửa hàng Lê Lợi', N'Cửa hàng truyền thống',
         N'123 Lê Lợi, Q1', N'Bến Nghé', N'Quận 1', N'Hồ Chí Minh', N'Miền Nam',
         '028-38221234', 'hcm@dwh.local', N'Lê Thị Hương',
         '2018-01-10', NULL, 1),
        ('STORE_HCM', 'STORE_HCM_02', N'Cửa hàng Quang Trung', N'Kiosk',
         N'456 Quang Trung, Q10', N'Phường 11', N'Quận 10', N'Hồ Chí Minh', N'Miền Nam',
         '028-38671234', 'hcm2@dwh.local', N'Phạm Văn Đức',
         '2021-04-20', NULL, 1);

    PRINT 'Inserted 4 seed stores into DimStore.';
END
ELSE
BEGIN
    PRINT 'DimStore already has data — skipping seed.';
END
GO

-- ============================================================================
-- SEED DATA: Insert du lieu mau cho DimCustomer (SCD Type 2, IsCurrent=1)
-- Chia deu cho 2 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimCustomer)
BEGIN
    INSERT INTO DimCustomer (
        TenantID, CustomerCode, FullName, Gender, DateOfBirth,
        Phone, Email, Address, City,
        CustomerType, LoyaltyTier, LoyaltyPoint,
        MemberSince, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
    )
    VALUES
        -- STORE_HN (10 khach hang)
        ('STORE_HN', 'KH_HN_001', N'Nguyễn Hoàng Nam', N'Nam', '1988-05-15',
         '0912345001', 'nam.nh@gmail.com', N'12 Đại Cồ Việt, Hai Bà Trưng', N'Hà Nội',
         N'Khách VIP', N'Platinum', 8500, '2019-06-01', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_002', N'Trần Thị Mai', N'Nữ', '1992-08-22',
         '0912345002', 'mai.tt@gmail.com', N'34 Hoàng Quốc Việt, Cầu Giấy', N'Hà Nội',
         N'Khách VIP', N'Gold', 4200, '2020-01-15', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_003', N'Lê Đức Anh', N'Nam', '1995-03-10',
         '0912345003', 'anh.ld@gmail.com', N'56 Trần Duy Hưng, Cầu Giấy', N'Hà Nội',
         N'Khách lẻ', N'Bronze', 800, '2021-03-20', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_004', N'Phạm Thị Thuỷ', N'Nữ', '1985-11-30',
         '0912345004', 'thuy.pt@gmail.com', N'78 Láng Hạ, Đống Đa', N'Hà Nội',
         N'Khách VIP', N'Silver', 3100, '2020-08-10', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_005', N'Hoàng Văn Cường', N'Nam', '1990-07-08',
         '0912345005', 'cuong.hv@gmail.com', N'90 Giải Phóng, Hai Bà Trưng', N'Hà Nội',
         N'Khách lẻ', N'Bronze', 450, '2022-02-14', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_006', N'Đỗ Thị Hà', N'Nữ', '1998-01-25',
         '0912345006', 'ha.dt@gmail.com', N'11 Cầu Giấy, Cầu Giấy', N'Hà Nội',
         N'Khách lẻ', N'Bronze', 320, '2022-05-30', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_007', N'Bùi Minh Tuấn', N'Nam', '1982-12-03',
         '0912345007', 'tuan.bm@gmail.com', N'22 Phạm Văn Đồng, Cổ Nhuế', N'Hà Nội',
         N'Đại lý', N'Gold', 5600, '2019-11-01', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_008', N'Vũ Thị Phương', N'Nữ', '1993-09-18',
         '0912345008', 'phuong.vt@gmail.com', N'33 Tôn Thất Tùng, Đống Đa', N'Hà Nội',
         N'Khách lẻ', N'Silver', 1800, '2021-07-22', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_009', N'Đinh Văn Nam', N'Nam', '1997-04-12',
         '0912345009', 'nam.dv@gmail.com', N'44 Nguyễn Trãi, Thanh Xuân', N'Hà Nội',
         N'Khách lẻ', N'Bronze', 600, '2022-09-05', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HN', 'KH_HN_010', N'Ngô Thị Lan', N'Nữ', '1989-06-28',
         '0912345010', 'lan.nt@gmail.com', N'55 Xuân Thủy, Cầu Giấy', N'Hà Nội',
         N'Khách VIP', N'Gold', 3900, '2020-04-18', 1, CAST(GETDATE() AS DATE), NULL, 1),
        -- STORE_HCM (10 khach hang)
        ('STORE_HCM', 'KH_HCM_001', N'Lê Hoàng Minh', N'Nam', '1987-02-14',
         '0909001001', 'minh.lh@gmail.com', N'100 Nguyễn Huệ, Q1', N'Hồ Chí Minh',
         N'Khách VIP', N'Platinum', 9200, '2018-05-20', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_002', N'Trần Ngọc Linh', N'Nữ', '1994-10-05',
         '0909001002', 'linh.tn@gmail.com', N'200 Đồng Khởi, Q1', N'Hồ Chí Minh',
         N'Khách VIP', N'Gold', 4800, '2019-12-01', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_003', N'Phạm Đức Thành', N'Nam', '1991-07-19',
         '0909001003', 'thanh.pd@gmail.com', N'300 Lê Lợi, Q1', N'Hồ Chí Minh',
         N'Khách lẻ', N'Bronze', 950, '2021-06-15', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_004', N'Huỳnh Thị Mai', N'Nữ', '1986-03-25',
         '0909001004', 'mai.ht@gmail.com', N'400 Pasteur, Q3', N'Hồ Chí Minh',
         N'Khách VIP', N'Silver', 3500, '2020-02-28', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_005', N'Nguyễn Văn Hùng', N'Nam', '1996-11-11',
         '0909001005', 'hung.nv@gmail.com', N'500 Võ Văn Tần, Q3', N'Hồ Chí Minh',
         N'Khách lẻ', N'Bronze', 550, '2022-03-10', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_006', N'Lý Thị Hồng', N'Nữ', '1999-08-07',
         '0909001006', 'hong.lt@gmail.com', N'600 Phạm Ngũ Lão, Q5', N'Hồ Chí Minh',
         N'Khách lẻ', N'Bronze', 280, '2022-11-22', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_007', N'Võ Đình Khánh', N'Nam', '1983-01-30',
         '0909001007', 'khanh.vd@gmail.com', N'700 Nguyễn Thị Minh Khai, Q3', N'Hồ Chí Minh',
         N'Đại lý', N'Gold', 6100, '2018-09-15', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_008', N'Đặng Thị Yến', N'Nữ', '1992-05-20',
         '0909001008', 'yen.dt@gmail.com', N'800 Trần Hưng Đạo, Q5', N'Hồ Chí Minh',
         N'Khách VIP', N'Silver', 2700, '2021-01-08', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_009', N'Châu Văn Tâm', N'Nam', '1995-12-14',
         '0909001009', 'tam.cv@gmail.com', N'900 Quang Trung, Q10', N'Hồ Chí Minh',
         N'Khách lẻ', N'Bronze', 720, '2022-07-03', 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('STORE_HCM', 'KH_HCM_010', N'Trịnh Ngọc Bảo', N'Nữ', '1988-09-03',
         '0909001010', 'bao.tn@gmail.com', N'1000 Cách Mạng Tháng 8, Q3', N'Hồ Chí Minh',
         N'Khách VIP', N'Gold', 4400, '2019-08-25', 1, CAST(GETDATE() AS DATE), NULL, 1);

    PRINT 'Inserted 20 seed customers into DimCustomer.';
END
ELSE
BEGIN
    PRINT 'DimCustomer already has data — skipping seed.';
END
GO

-- ============================================================================
-- SEED DATA: Insert du lieu mau cho DimEmployee
-- Chia deu cho 2 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimEmployee)
BEGIN
    INSERT INTO DimEmployee (
        TenantID, EmployeeCode, FullName, Gender, DateOfBirth,
        Phone, Email, Position, Department,
        HireDate, TerminationDate, ShiftType, IsActive
    )
    VALUES
        -- STORE_HN (6 nhan vien)
        ('STORE_HN', 'NV_HN_001', N'Nguyễn Thị Thu Hà', N'Nữ', '1990-05-15',
         '0961001001', 'ha.nt@dwh.local', N'Quản lý cửa hàng', N'Kinh doanh',
         '2019-03-15', NULL, N'Sáng', 1),
        ('STORE_HN', 'NV_HN_002', N'Trần Văn Đạt', N'Nam', '1995-08-22',
         '0961001002', 'dat.tv@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2020-01-10', NULL, N'Sáng', 1),
        ('STORE_HN', 'NV_HN_003', N'Lê Thị Mai', N'Nữ', '1993-11-30',
         '0961001003', 'mai.lt@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2020-06-01', NULL, N'Chiều', 1),
        ('STORE_HN', 'NV_HN_004', N'Hoàng Văn Nam', N'Nam', '1992-03-18',
         '0961001004', 'nam.hv@dwh.local', N'Nhân viên kho', N'Kho vận',
         '2020-09-15', NULL, N'Sáng', 1),
        ('STORE_HN', 'NV_HN_005', N'Phạm Thị Hoa', N'Nữ', '1997-07-08',
         '0961001005', 'hoa.pt@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2021-04-20', NULL, N'Tối', 1),
        ('STORE_HN', 'NV_HN_006', N'Đỗ Văn Cường', N'Nam', '1991-12-25',
         '0961001006', 'cuong.dv@dwh.local', N'Kế toán', N'Kế toán',
         '2021-01-05', NULL, N'Sáng', 1),
        -- STORE_HCM (6 nhan vien)
        ('STORE_HCM', 'NV_HCM_001', N'Lê Thị Kim Oanh', N'Nữ', '1988-06-10',
         '0962001001', 'oanh.lt@dwh.local', N'Quản lý cửa hàng', N'Kinh doanh',
         '2018-01-10', NULL, N'Sáng', 1),
        ('STORE_HCM', 'NV_HCM_002', N'Nguyễn Hoàng Dương', N'Nam', '1994-02-14',
         '0962001002', 'duong.nh@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2019-05-20', NULL, N'Sáng', 1),
        ('STORE_HCM', 'NV_HCM_003', N'Trịnh Minh Châu', N'Nữ', '1996-09-28',
         '0962001003', 'chau.tm@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2020-03-15', NULL, N'Chiều', 1),
        ('STORE_HCM', 'NV_HCM_004', N'Bùi Đình Tuấn', N'Nam', '1992-04-05',
         '0962001004', 'tuan.bd@dwh.local', N'Nhân viên kho', N'Kho vận',
         '2020-08-01', NULL, N'Sáng', 1),
        ('STORE_HCM', 'NV_HCM_005', N'Vũ Thị Ngọc', N'Nữ', '1998-11-20',
         '0962001005', 'ngoc.vt@dwh.local', N'Nhân viên bán hàng', N'Kinh doanh',
         '2021-06-10', NULL, N'Tối', 1),
        ('STORE_HCM', 'NV_HCM_006', N'Huỳnh Văn Lộc', N'Nam', '1990-01-03',
         '0962001006', 'loc.hv@dwh.local', N'Kế toán', N'Kế toán',
         '2021-02-15', NULL, N'Sáng', 1);

    PRINT 'Inserted 12 seed employees into DimEmployee.';
END
ELSE
BEGIN
    PRINT 'DimEmployee already has data — skipping seed.';
END
GO

-- ============================================================================
-- XAC MINH: Doc lai du lieu
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: DimStore ===';
SELECT COUNT(*) AS TotalRows,
       SUM(CASE WHEN TenantID = 'STORE_HN' THEN 1 ELSE 0 END) AS HN_Stores,
       SUM(CASE WHEN TenantID = 'STORE_HCM' THEN 1 ELSE 0 END) AS HCM_Stores,
       SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveStores
FROM DimStore;
SELECT StoreKey, TenantID, StoreCode, StoreName, City, Region, ManagerName, IsActive
FROM DimStore ORDER BY TenantID, StoreCode;

PRINT '';
PRINT '=== VERIFICATION: DimCustomer ===';
SELECT COUNT(*) AS TotalRows,
       SUM(CASE WHEN TenantID = 'STORE_HN' THEN 1 ELSE 0 END) AS HN_Customers,
       SUM(CASE WHEN TenantID = 'STORE_HCM' THEN 1 ELSE 0 END) AS HCM_Customers,
       SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows
FROM DimCustomer;
SELECT TOP 10 CustomerKey, TenantID, CustomerCode, FullName, City,
       CustomerType, LoyaltyTier, LoyaltyPoint, IsCurrent, MemberSince
FROM DimCustomer WHERE IsCurrent = 1
ORDER BY TenantID, CustomerCode;

PRINT '';
PRINT '=== VERIFICATION: DimEmployee ===';
SELECT COUNT(*) AS TotalRows,
       SUM(CASE WHEN TenantID = 'STORE_HN' THEN 1 ELSE 0 END) AS HN_Employees,
       SUM(CASE WHEN TenantID = 'STORE_HCM' THEN 1 ELSE 0 END) AS HCM_Employees,
       SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveEmployees
FROM DimEmployee;
SELECT EmployeeKey, TenantID, EmployeeCode, FullName, Position, Department,
       ShiftType, IsActive, HireDate
FROM DimEmployee ORDER BY TenantID, EmployeeCode;

PRINT '';
PRINT '=== VERIFICATION: Stored Procedures ===';
SELECT name AS ProcedureName, create_date, modify_date
FROM sys.procedures
WHERE name IN ('usp_Load_DimStore', 'usp_Load_DimCustomer', 'usp_Load_DimEmployee')
ORDER BY name;

PRINT '';
PRINT '=== PHASE 3 COMPLETED SUCCESSFULLY ===';
GO
