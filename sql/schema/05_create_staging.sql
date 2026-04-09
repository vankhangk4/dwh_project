-- ============================================================================
-- PHASE 5: Staging Layer & ETL Support Tables
-- File: sql/schema/05_create_staging.sql
-- Description: Tao cac bang Staging (STG_*) — vung dem tam thoi truoc khi nap
--               vao Data Warehouse. Dong thoi tao cac bang ho tro ETL:
--               ETL_Watermark, ETL_RunLog, STG_ErrorLog.
--
-- NOTE:
--   - Tat ca bang STG_* CO TenantID (tru STG_ProductRaw, STG_SupplierRaw la Shared).
--   - Cac bang Staging duoc TRUNCATE + RELOAD moi chu ky ETL.
--   - Khong co Primary Key / Unique constraint tren Staging (tranh loi khi reload).
--   - Phu thuoc: Chay SAU Phase 1, 2, 3, 4
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG STAGING 1: STG_SalesRaw
-- Vung dem tam cho du lieu ban hang tu Excel/CSV.
-- TENANT-SPECIFIC: Co TenantID.
-- Grain: 1 dong = 1 dong chi tiet hoa don (1 san pham trong 1 hoa don).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_SalesRaw')
BEGIN
    CREATE TABLE STG_SalesRaw (
        -- Khoa tam (auto)
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        -- Tenant
        TenantID        VARCHAR(20)       NOT NULL,

        -- Khoa kinh doanh tu nguon
        MaHoaDon        VARCHAR(50)        NULL,
        SoDong          INT                NULL DEFAULT 1,
        NgayBan         DATE              NULL,

        -- Thong tin mat hang
        MaSP            VARCHAR(50)        NULL,
        SoLuong         DECIMAL(18,4)      NULL,
        DonGiaBan        DECIMAL(18,2)    NULL,

        -- Thong tin khach hang
        MaKH            VARCHAR(50)        NULL,
        MaNV            VARCHAR(50)        NULL,
        MaCH            VARCHAR(50)        NULL,

        -- Thong tin thanh toan
        PhuongThucTT    NVARCHAR(50)      NULL,
        ChietKhau       DECIMAL(18,2)     NULL DEFAULT 0,

        -- Kenh ban / Nhom ban hang
        KenhBan         NVARCHAR(50)       NULL DEFAULT N'InStore',
        NhomBanHang     NVARCHAR(50)       NULL DEFAULT N'Bán lẻ',

        -- Tra hang
        IsHoanTra       BIT                NULL DEFAULT 0,
        LyDoHoanTra     NVARCHAR(200)     NULL,

        -- Du lieu goc (can cuu khi co loi)
        RawData         NVARCHAR(MAX)     NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_SalesRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_SalesRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_SalesRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_SalesRaw_TenantID'
    AND object_id = OBJECT_ID('STG_SalesRaw')
)
BEGIN
    CREATE INDEX IX_STG_SalesRaw_TenantID ON STG_SalesRaw(TenantID);
    PRINT 'Created index: IX_STG_SalesRaw_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_SalesRaw_NgayBan_TenantID'
    AND object_id = OBJECT_ID('STG_SalesRaw')
)
BEGIN
    CREATE INDEX IX_STG_SalesRaw_NgayBan_TenantID ON STG_SalesRaw(NgayBan, TenantID);
    PRINT 'Created index: IX_STG_SalesRaw_NgayBan_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 2: STG_InventoryRaw
-- Vung dem tam cho du lieu ton kho tu Excel.
-- TENANT-SPECIFIC: Co TenantID.
-- Grain: 1 dong = 1 san pham / 1 cua hang / 1 ngay.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_InventoryRaw')
BEGIN
    CREATE TABLE STG_InventoryRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        TenantID        VARCHAR(20)       NOT NULL,

        MaCH            VARCHAR(50)        NULL,
        MaSP            VARCHAR(50)        NULL,
        NgayChot        DATE              NULL,

        -- So luong
        TonDauNgay      INT                NULL DEFAULT 0,
        NhapTrongNgay   INT                NULL DEFAULT 0,
        BanTrongNgay    INT                NULL DEFAULT 0,
        TraLaiNhap      INT                NULL DEFAULT 0,
        DieuChinh       INT                NULL DEFAULT 0,

        -- Gia tri
        DonGiaVon       DECIMAL(18,2)     NULL,
        MucTonToiThieu  INT                NULL DEFAULT 0,

        -- Loai chuyen
        LoaiChuyen       NVARCHAR(50)      NULL DEFAULT N'Daily Count',

        -- Du lieu goc
        RawData         NVARCHAR(MAX)     NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_InventoryRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_InventoryRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_InventoryRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_InventoryRaw_TenantID'
    AND object_id = OBJECT_ID('STG_InventoryRaw')
)
BEGIN
    CREATE INDEX IX_STG_InventoryRaw_TenantID ON STG_InventoryRaw(TenantID);
    PRINT 'Created index: IX_STG_InventoryRaw_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_InventoryRaw_NgayChot_TenantID'
    AND object_id = OBJECT_ID('STG_InventoryRaw')
)
BEGIN
    CREATE INDEX IX_STG_InventoryRaw_NgayChot_TenantID ON STG_InventoryRaw(NgayChot, TenantID);
    PRINT 'Created index: IX_STG_InventoryRaw_NgayChot_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 3: STG_PurchaseRaw
-- Vung dem tam cho du lieu nhap hang tu Excel.
-- TENANT-SPECIFIC: Co TenantID.
-- Grain: 1 dong = 1 dong trong phieu nhap.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_PurchaseRaw')
BEGIN
    CREATE TABLE STG_PurchaseRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        TenantID        VARCHAR(20)       NOT NULL,

        MaCH            VARCHAR(50)        NULL,
        MaNCC           VARCHAR(50)        NULL,
        MaSP            VARCHAR(50)        NULL,
        SoPhieuNhap      VARCHAR(50)        NULL,
        SoDong          INT                NULL DEFAULT 1,
        NgayNhap         DATE              NULL,

        -- So luong & gia
        SoLuong         DECIMAL(18,4)      NULL,
        DonGiaNhap       DECIMAL(18,2)    NULL,
        ChietKhau       DECIMAL(18,2)     NULL DEFAULT 0,
        ThueGTGT        DECIMAL(18,2)     NULL DEFAULT 0,

        -- Nhan hang
        SoGRN           VARCHAR(50)        NULL,
        NgayGRN         DATE              NULL,
        SoLuongThucNhan  DECIMAL(18,4)     NULL,
        NgayNhanHang     DATE              NULL,
        TinhTrangChatLuong NVARCHAR(50)    NULL DEFAULT N'Passed',

        -- Thanh toan
        TinhTrangThanhToan NVARCHAR(50)    NULL DEFAULT N'Pending',
        PhuongThucTT     NVARCHAR(50)       NULL DEFAULT N'Tiền mặt',
        HanThanhToan     DATE              NULL,

        -- Ghi chu
        GhiChu          NVARCHAR(500)     NULL,

        -- Du lieu goc
        RawData         NVARCHAR(MAX)     NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_PurchaseRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_PurchaseRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_PurchaseRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_PurchaseRaw_TenantID'
    AND object_id = OBJECT_ID('STG_PurchaseRaw')
)
BEGIN
    CREATE INDEX IX_STG_PurchaseRaw_TenantID ON STG_PurchaseRaw(TenantID);
    PRINT 'Created index: IX_STG_PurchaseRaw_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 4: STG_ProductRaw
-- Vung dem tam cho du lieu danh muc san pham tu CSV.
-- SHARED: KHONG co TenantID (danh muc san pham dung chung).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_ProductRaw')
BEGIN
    CREATE TABLE STG_ProductRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        MaSP            VARCHAR(50)        NOT NULL,
        TenSP           NVARCHAR(200)     NOT NULL,
        ThuongHieu      NVARCHAR(100)     NULL,
        DanhMuc         NVARCHAR(100)     NOT NULL,
        PhanLoai        NVARCHAR(100)     NULL,

        -- Gia
        GiaVon          DECIMAL(18,2)     NULL,
        GiaNiemYet      DECIMAL(18,2)     NULL,

        -- San pham
        SKU             VARCHAR(50)        NULL,
        Barcode         VARCHAR(50)        NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_ProductRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_ProductRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_ProductRaw already exists — skipping CREATE.';
END
GO

-- ============================================================================
-- BANG STAGING 5: STG_CustomerRaw
-- Vung dem tam cho du lieu khach hang tu Excel.
-- TENANT-SPECIFIC: Co TenantID.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_CustomerRaw')
BEGIN
    CREATE TABLE STG_CustomerRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        TenantID        VARCHAR(20)       NOT NULL,

        MaKH            VARCHAR(50)        NOT NULL,
        HoTen           NVARCHAR(200)     NOT NULL,
        GioiTinh        NVARCHAR(10)      NULL,
        NgaySinh        DATE              NULL,

        -- Lien lac
        DienThoai       VARCHAR(20)        NULL,
        Email           VARCHAR(100)        NULL,
        DiaChi          NVARCHAR(500)     NULL,
        ThanhPho        NVARCHAR(100)     NULL,

        -- Thong tin thanh vien
        LoaiKH          NVARCHAR(50)       NULL DEFAULT N'Khách lẻ',
        HangTV          NVARCHAR(50)       NULL DEFAULT N'Bronze',
        DiemTichLuy     INT                NULL DEFAULT 0,
        NgayDangKy      DATE              NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_CustomerRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_CustomerRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_CustomerRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_CustomerRaw_TenantID'
    AND object_id = OBJECT_ID('STG_CustomerRaw')
)
BEGIN
    CREATE INDEX IX_STG_CustomerRaw_TenantID ON STG_CustomerRaw(TenantID);
    PRINT 'Created index: IX_STG_CustomerRaw_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 6: STG_EmployeeRaw
-- Vung dem tam cho du lieu nhan vien tu Excel.
-- TENANT-SPECIFIC: Co TenantID.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_EmployeeRaw')
BEGIN
    CREATE TABLE STG_EmployeeRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        TenantID        VARCHAR(20)       NOT NULL,

        MaNV            VARCHAR(50)        NOT NULL,
        HoTen           NVARCHAR(200)     NOT NULL,
        GioiTinh        NVARCHAR(10)      NULL,
        NgaySinh        DATE              NULL,

        -- Lien lac
        DienThoai       VARCHAR(20)        NULL,
        Email           VARCHAR(100)        NULL,

        -- Cong viec
        ChucVu          NVARCHAR(100)     NULL,
        PhongBan        NVARCHAR(100)     NULL,
        CaLamViec       NVARCHAR(20)      NULL DEFAULT N'Sáng',

        -- Hanh chinh
        NgayVaoLam      DATE              NULL,
        NgayNghiViec    DATE              NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_EmployeeRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_EmployeeRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_EmployeeRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_EmployeeRaw_TenantID'
    AND object_id = OBJECT_ID('STG_EmployeeRaw')
)
BEGIN
    CREATE INDEX IX_STG_EmployeeRaw_TenantID ON STG_EmployeeRaw(TenantID);
    PRINT 'Created index: IX_STG_EmployeeRaw_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 7: STG_StoreRaw
-- Vung dem tam cho du lieu cua hang tu Excel.
-- TENANT-SPECIFIC: Co TenantID.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_StoreRaw')
BEGIN
    CREATE TABLE STG_StoreRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        TenantID        VARCHAR(20)       NOT NULL,

        MaCH            VARCHAR(50)        NOT NULL,
        TenCH           NVARCHAR(200)     NOT NULL,
        LoaiCH          NVARCHAR(50)       NULL DEFAULT N'Cửa hàng truyền thống',

        -- Dia chi
        DiaChi          NVARCHAR(500)     NULL,
        Phuong          NVARCHAR(100)     NULL,
        Quan            NVARCHAR(100)     NULL,
        ThanhPho        NVARCHAR(100)     NOT NULL,
        Vung            NVARCHAR(50)       NULL,

        -- Lien lac
        DienThoai       VARCHAR(30)        NULL,
        Email           VARCHAR(100)        NULL,

        -- Quan ly
        NguoiQuanLy     NVARCHAR(200)     NULL,
        NgayKhaiTruong  DATE              NULL,
        NgayDongCua     DATE              NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_StoreRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_StoreRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_StoreRaw already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_StoreRaw_TenantID'
    AND object_id = OBJECT_ID('STG_StoreRaw')
)
BEGIN
    CREATE INDEX IX_STG_StoreRaw_TenantID ON STG_StoreRaw(TenantID);
    PRINT 'Created index: IX_STG_StoreRaw_TenantID';
END
GO

-- ============================================================================
-- BANG STAGING 8: STG_SupplierRaw
-- Vung dem tam cho du lieu nha cung cap tu CSV.
-- SHARED: KHONG co TenantID.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_SupplierRaw')
BEGIN
    CREATE TABLE STG_SupplierRaw (
        STGRowID        BIGINT IDENTITY(1,1) NOT NULL,

        MaNCC           VARCHAR(50)        NOT NULL,
        TenNCC          NVARCHAR(200)     NOT NULL,
        NguoiLienHe     NVARCHAR(100)     NULL,
        ChucVu          NVARCHAR(100)     NULL,
        DienThoai       VARCHAR(30)        NULL,
        Email           VARCHAR(100)        NULL,
        DiaChi          NVARCHAR(500)     NULL,
        ThanhPho        NVARCHAR(100)     NULL,
        QuocGia         NVARCHAR(50)       NULL DEFAULT N'Việt Nam',
        MaSoThue        VARCHAR(50)        NULL,
        DieuKhoanTT     NVARCHAR(100)     NULL,

        -- Ghi nhan
        STG_LoadDatetime DATETIME2         NOT NULL DEFAULT GETDATE(),
        STG_SourceFile   VARCHAR(500)       NULL,

        CONSTRAINT PK_STG_SupplierRaw PRIMARY KEY CLUSTERED (STGRowID)
    );

    PRINT 'Created table: STG_SupplierRaw';
END
ELSE
BEGIN
    PRINT 'Table STG_SupplierRaw already exists — skipping CREATE.';
END
GO

-- ============================================================================
-- BANG ETL_Watermark
-- Luu moc thoi gian cho phep incremental extraction.
-- Moi nguon du lieu cua moi tenant co 1 watermark rieng.
-- SourceName = '{TenantID}_{SourceType}' (VD: 'STORE_HN_Sales_Excel').
-- LastRunStatus: 'SUCCESS' | 'FAILED' | 'RUNNING'.
-- WatermarkValue: Ngay gio cuoi cung ETL thanh cong.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ETL_Watermark')
BEGIN
    CREATE TABLE ETL_Watermark (
        WatermarkID      INT IDENTITY(1,1) NOT NULL,
        SourceName       VARCHAR(100)    NOT NULL,
        TenantID         VARCHAR(20)     NOT NULL,
        SourceType       VARCHAR(50)     NOT NULL,
        WatermarkValue   DATETIME2      NOT NULL DEFAULT CAST('2020-01-01' AS DATETIME2),
        LastRunStatus    VARCHAR(20)     NOT NULL DEFAULT 'SUCCESS',
        LastRunDatetime  DATETIME2       NOT NULL DEFAULT GETDATE(),
        RowsExtracted    INT             NULL,
        DurationSeconds  INT             NULL,
        Notes            NVARCHAR(500)    NULL,

        CONSTRAINT PK_ETL_Watermark PRIMARY KEY CLUSTERED (WatermarkID),
        CONSTRAINT UQ_ETL_Watermark_SourceName UNIQUE (SourceName)
    );

    PRINT 'Created table: ETL_Watermark';
END
ELSE
BEGIN
    PRINT 'Table ETL_Watermark already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_Watermark_TenantID'
    AND object_id = OBJECT_ID('ETL_Watermark')
)
BEGIN
    CREATE INDEX IX_ETL_Watermark_TenantID ON ETL_Watermark(TenantID);
    PRINT 'Created index: IX_ETL_Watermark_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_Watermark_LastRunStatus'
    AND object_id = OBJECT_ID('ETL_Watermark')
)
BEGIN
    CREATE INDEX IX_ETL_Watermark_LastRunStatus ON ETL_Watermark(LastRunStatus);
    PRINT 'Created index: IX_ETL_Watermark_LastRunStatus';
END
GO

-- ============================================================================
-- BANG ETL_RunLog
-- Log chi tiet moi lan chay ETL.
-- Ghi nhan: so ban ghi xu ly, loi, thoi gian, trang thai.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ETL_RunLog')
BEGIN
    CREATE TABLE ETL_RunLog (
        RunLogID         BIGINT IDENTITY(1,1) NOT NULL,

        -- Thong tin ETL
        TenantID         VARCHAR(20)     NOT NULL,
        StoredProcedureName VARCHAR(100)  NOT NULL,
        PipelineName     VARCHAR(100)     NULL,
        RunDate          DATE             NOT NULL,
        RunNumber        INT              NULL,

        -- Trang thai
        Status           VARCHAR(20)      NOT NULL,  -- 'SUCCESS', 'FAILED', 'RUNNING'
        ExitCode         INT              NULL,

        -- Do luong
        RowsProcessed    INT              NOT NULL DEFAULT 0,
        RowsInserted     INT              NOT NULL DEFAULT 0,
        RowsUpdated      INT              NOT NULL DEFAULT 0,
        RowsSkipped      INT              NOT NULL DEFAULT 0,
        RowsFailed       INT              NOT NULL DEFAULT 0,

        -- Loi
        ErrorCode        INT              NULL,
        ErrorMessage     NVARCHAR(MAX)    NULL,

        -- Thoi gian
        StartTime        DATETIME2        NOT NULL DEFAULT GETDATE(),
        EndTime          DATETIME2        NULL,
        DurationSeconds   INT              NULL,

        -- May chu
        ServerName       VARCHAR(100)     NULL,
        JobName          VARCHAR(100)     NULL,

        CONSTRAINT PK_ETL_RunLog PRIMARY KEY CLUSTERED (RunLogID)
    );

    PRINT 'Created table: ETL_RunLog';
END
ELSE
BEGIN
    PRINT 'Table ETL_RunLog already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_RunLog_TenantID_RunDate'
    AND object_id = OBJECT_ID('ETL_RunLog')
)
BEGIN
    CREATE INDEX IX_ETL_RunLog_TenantID_RunDate ON ETL_RunLog(TenantID, RunDate DESC);
    PRINT 'Created index: IX_ETL_RunLog_TenantID_RunDate';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_RunLog_Status'
    AND object_id = OBJECT_ID('ETL_RunLog')
)
BEGIN
    CREATE INDEX IX_ETL_RunLog_Status ON ETL_RunLog(Status);
    PRINT 'Created index: IX_ETL_RunLog_Status';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ETL_RunLog_RunDate'
    AND object_id = OBJECT_ID('ETL_RunLog')
)
BEGIN
    CREATE INDEX IX_ETL_RunLog_RunDate ON ETL_RunLog(RunDate DESC);
    PRINT 'Created index: IX_ETL_RunLog_RunDate';
END
GO

-- ============================================================================
-- BANG STG_ErrorLog
-- Log chi tiet cac ban ghi bi loi trong qua trinh ETL.
-- Moi ban ghi loi deu co TenantID de loc theo tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'STG_ErrorLog')
BEGIN
    CREATE TABLE STG_ErrorLog (
        ErrorLogID       BIGINT IDENTITY(1,1) NOT NULL,

        TenantID         VARCHAR(20)      NOT NULL,
        SourceTable      VARCHAR(100)     NOT NULL,
        ErrorType        VARCHAR(50)      NOT NULL,
        ErrorCode        INT              NULL,
        ErrorMessage     NVARCHAR(500)    NULL,

        -- Thong tin ban ghi loi
        SourceKey        VARCHAR(100)     NULL,
        RawData          NVARCHAR(MAX)    NULL,

        -- Thong tin batch
        BatchDate        DATE             NOT NULL,
        ETLRunDate       DATETIME2        NOT NULL DEFAULT GETDATE(),
        LoadDatetime     DATETIME2        NOT NULL DEFAULT GETDATE(),

        -- Xu ly loi
        IsResolved       BIT              NOT NULL DEFAULT 0,
        ResolvedBy       VARCHAR(100)     NULL,
        ResolvedAt       DATETIME2        NULL,
        ResolutionNotes  NVARCHAR(500)     NULL,

        CONSTRAINT PK_STG_ErrorLog PRIMARY KEY CLUSTERED (ErrorLogID)
    );

    PRINT 'Created table: STG_ErrorLog';
END
ELSE
BEGIN
    PRINT 'Table STG_ErrorLog already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_ErrorLog_TenantID'
    AND object_id = OBJECT_ID('STG_ErrorLog')
)
BEGIN
    CREATE INDEX IX_STG_ErrorLog_TenantID ON STG_ErrorLog(TenantID);
    PRINT 'Created index: IX_STG_ErrorLog_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_ErrorLog_SourceTable_ErrorType'
    AND object_id = OBJECT_ID('STG_ErrorLog')
)
BEGIN
    CREATE INDEX IX_STG_ErrorLog_SourceTable_ErrorType
        ON STG_ErrorLog(SourceTable, ErrorType);
    PRINT 'Created index: IX_STG_ErrorLog_SourceTable_ErrorType';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_ErrorLog_BatchDate_TenantID'
    AND object_id = OBJECT_ID('STG_ErrorLog')
)
BEGIN
    CREATE INDEX IX_STG_ErrorLog_BatchDate_TenantID ON STG_ErrorLog(BatchDate DESC, TenantID);
    PRINT 'Created index: IX_STG_ErrorLog_BatchDate_TenantID';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_STG_ErrorLog_IsResolved'
    AND object_id = OBJECT_ID('STG_ErrorLog')
)
BEGIN
    CREATE INDEX IX_STG_ErrorLog_IsResolved ON STG_ErrorLog(IsResolved);
    PRINT 'Created index: IX_STG_ErrorLog_IsResolved';
END
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Truncate_Staging_Tables
-- Xoa toan bo du lieu trong tat ca bang Staging.
-- Chay TRUNCATE thay vi DELETE de reset identity va giai phong log.
-- Can dung trong transaction de dam bao tinh nguyen cua ETL.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Truncate_StagingTables')
BEGIN
    DROP PROCEDURE usp_Truncate_StagingTables;
END
GO

CREATE PROCEDURE usp_Truncate_StagingTables
    @TenantID VARCHAR(20) = NULL  -- NULL = truncate ALL staging tables; tenant-specific = truncate chi tenant do
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowsDeleted INT = 0;
    DECLARE @TableName VARCHAR(100);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @Cur CURSOR;

    SET @Cur = CURSOR FOR
        SELECT name FROM sys.tables
        WHERE name LIKE 'STG_%'
          AND name NOT IN ('STG_ErrorLog')  -- ErrorLog KHONG truncate, chi INSERT
        ORDER BY name;

    OPEN @Cur;
    FETCH NEXT FROM @Cur INTO @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Chi truncate bang tenant-specific neu co TenantID
        IF @TenantID IS NULL
        BEGIN
            SET @SQL = N'TRUNCATE TABLE ' + QUOTENAME(@TableName);
            EXEC sp_executesql @SQL;
            PRINT 'Truncated: ' + @TableName;
        END
        ELSE
        BEGIN
            -- Xoa theo TenantID
            SET @SQL = N'DELETE FROM ' + QUOTENAME(@TableName) + N' WHERE TenantID = @pTenantID';
            EXEC sp_executesql @SQL, N'@pTenantID VARCHAR(20)', @pTenantID = @TenantID;
            PRINT 'Deleted from ' + @TableName + ' where TenantID=' + @TenantID;
        END

        FETCH NEXT FROM @Cur INTO @TableName;
    END;

    CLOSE @Cur;
    DEALLOCATE @Cur;

    -- ErrorLog: KHONG bao gio truncate — chi INSERT + SELECT
    -- Neu can xoa ErrorLog, goi usp_ClearErrorLog

    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());
    PRINT 'usp_Truncate_StagingTables: Completed in ' + CAST(@Duration AS VARCHAR(10)) + ' seconds.';
END;
GO

PRINT 'Created stored procedure: usp_Truncate_StagingTables';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Update_Watermark
-- Cap nhat trang thai va gia tri watermark sau khi ETL chay.
-- Status = 'RUNNING': Dat truoc khi ETL bat dau.
-- Status = 'SUCCESS': Cap nhat WatermarkValue = GETDATE() sau khi ETL thanh cong.
-- Status = 'FAILED':  Giu nguyen WatermarkValue de retry tu diem cu.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Update_Watermark')
BEGIN
    DROP PROCEDURE usp_Update_Watermark;
END
GO

CREATE PROCEDURE usp_Update_Watermark
    @SourceName   VARCHAR(100),
    @TenantID     VARCHAR(20),
    @SourceType   VARCHAR(50) = NULL,
    @Status       VARCHAR(20),   -- 'RUNNING', 'SUCCESS', 'FAILED'
    @RowsExtracted INT = NULL,
    @DurationSeconds INT = NULL,
    @Notes        NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentStatus VARCHAR(20);
    DECLARE @CurrentWatermark DATETIME2;

    -- Lay trang thai hien tai
    SELECT
        @CurrentStatus = LastRunStatus,
        @CurrentWatermark = WatermarkValue
    FROM ETL_Watermark
    WHERE SourceName = @SourceName;

    IF @Status = 'RUNNING'
    BEGIN
        -- Upsert: UPDATE hoac INSERT
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                LastRunStatus   = 'RUNNING',
                LastRunDatetime = GETDATE(),
                Notes = ISNULL(@Notes, Notes)
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName, @TenantID, ISNULL(@SourceType, @SourceName),
                CAST('2020-01-01' AS DATETIME2), 'RUNNING',
                GETDATE(), @RowsExtracted, @DurationSeconds, @Notes
            );
        END

        PRINT 'Watermark [' + @SourceName + ']: Set to RUNNING.';
    END

    IF @Status = 'SUCCESS'
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                WatermarkValue   = GETDATE(),
                LastRunStatus    = 'SUCCESS',
                LastRunDatetime  = GETDATE(),
                RowsExtracted    = ISNULL(@RowsExtracted, RowsExtracted),
                DurationSeconds  = ISNULL(@DurationSeconds, DurationSeconds),
                Notes            = ISNULL(@Notes, Notes)
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName, @TenantID, ISNULL(@SourceType, @SourceName),
                GETDATE(), 'SUCCESS',
                GETDATE(), @RowsExtracted, @DurationSeconds, @Notes
            );
        END

        PRINT 'Watermark [' + @SourceName + ']: SUCCESS. Watermark updated to '
            + CONVERT(VARCHAR(30), GETDATE(), 120) + '.';
    END

    IF @Status = 'FAILED'
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                LastRunStatus   = 'FAILED',
                LastRunDatetime = GETDATE(),
                DurationSeconds = ISNULL(@DurationSeconds, DurationSeconds),
                Notes           = ISNULL(@Notes, Notes)
                -- KHONG update WatermarkValue khi FAILED
                -- De retry tu diem cu
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName, @TenantID, ISNULL(@SourceType, @SourceName),
                CAST('2020-01-01' AS DATETIME2), 'FAILED',
                GETDATE(), @RowsExtracted, @DurationSeconds, @Notes
            );
        END

        PRINT 'Watermark [' + @SourceName + ']: FAILED. Watermark KEPT at '
            + CONVERT(VARCHAR(30), @CurrentWatermark, 120) + ' for retry.';
    END
END;
GO

PRINT 'Created stored procedure: usp_Update_Watermark';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Get_Last_Watermark
-- Doc gia tri watermark cuoi cung thanh cong cua mot nguon du lieu.
-- Tra ve NULL neu chua co watermark nao (se dung mac dinh '2020-01-01').
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Get_Last_Watermark')
BEGIN
    DROP PROCEDURE usp_Get_Last_Watermark;
END
GO

CREATE PROCEDURE usp_Get_Last_Watermark
    @SourceName VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Watermark DATETIME2;

    SELECT TOP 1
        @Watermark = WatermarkValue
    FROM ETL_Watermark
    WHERE SourceName = @SourceName
      AND LastRunStatus = 'SUCCESS'
    ORDER BY LastRunDatetime DESC;

    IF @Watermark IS NULL
    BEGIN
        SET @Watermark = CAST('2020-01-01' AS DATETIME2);
        PRINT 'No successful watermark found for [' + @SourceName + ']. Using default: 2020-01-01.';
    END
    ELSE
    BEGIN
        PRINT 'Watermark for [' + @SourceName + ']: '
            + CONVERT(VARCHAR(30), @Watermark, 120) + '.';
    END

    -- Tra ve ket qua
    SELECT
        @SourceName AS SourceName,
        @Watermark AS LastSuccessfulWatermark,
        CASE WHEN @Watermark = CAST('2020-01-01' AS DATETIME2)
             THEN 'DEFAULT' ELSE 'FOUND' END AS WatermarkSource;
END;
GO

PRINT 'Created stored procedure: usp_Get_Last_Watermark';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Get_All_Active_Watermarks
-- Doc tat ca watermark cua tat ca tenant.
-- Dung de chay ETL cho nhieu tenant cung luc.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Get_All_Active_Watermarks')
BEGIN
    DROP PROCEDURE usp_Get_All_Active_Watermarks;
END
GO

CREATE PROCEDURE usp_Get_All_Active_Watermarks
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        w.SourceName,
        w.TenantID,
        w.SourceType,
        w.WatermarkValue,
        w.LastRunStatus,
        w.LastRunDatetime,
        t.TenantName,
        t.FilePath
    FROM ETL_Watermark w
    INNER JOIN Tenants t ON t.TenantID = w.TenantID
    WHERE t.IsActive = 1
    ORDER BY w.TenantID, w.SourceName;
END;
GO

PRINT 'Created stored procedure: usp_Get_All_Active_Watermarks';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_ClearErrorLog
-- Xoa cac ban ghi loi cu (da resolved hoac cu hon N ngay).
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_ClearErrorLog')
BEGIN
    DROP PROCEDURE usp_ClearErrorLog;
END
GO

CREATE PROCEDURE usp_ClearErrorLog
    @DaysOld       INT = 90,      -- Xoa loi cu hon N ngay
    @IsResolvedOnly BIT = 0,       -- 1 = chi xoa da resolved, 0 = xoa tat ca
    @TenantID      VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;

    IF @TenantID IS NULL
    BEGIN
        DELETE FROM STG_ErrorLog
        WHERE (@IsResolvedOnly = 1 AND IsResolved = 1)
           OR ETLRunDate < DATEADD(DAY, -@DaysOld, CAST(GETDATE() AS DATE));

        SET @RowsDeleted = @@ROWCOUNT;
    END
    ELSE
    BEGIN
        DELETE FROM STG_ErrorLog
        WHERE TenantID = @TenantID
          AND ((@IsResolvedOnly = 1 AND IsResolved = 1)
               OR ETLRunDate < DATEADD(DAY, -@DaysOld, CAST(GETDATE() AS DATE)));

        SET @RowsDeleted = @@ROWCOUNT;
    END

    PRINT 'usp_ClearErrorLog: Deleted ' + CAST(@RowsDeleted AS VARCHAR(10))
        + ' error log rows (older than ' + CAST(@DaysOld AS VARCHAR(10)) + ' days).';
END;
GO

PRINT 'Created stored procedure: usp_ClearErrorLog';
GO

-- ============================================================================
-- SEED DATA: Insert watermark khoi tao cho 2 tenant, 3 nguon
-- ============================================================================
IF NOT EXISTS (SELECT * FROM ETL_Watermark)
BEGIN
    INSERT INTO ETL_Watermark (SourceName, TenantID, SourceType, WatermarkValue, LastRunStatus, LastRunDatetime, Notes)
    VALUES
        ('STORE_HN_Sales_Excel',     'STORE_HN',  'Sales',      CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark'),
        ('STORE_HN_Inventory_Excel', 'STORE_HN',  'Inventory',  CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark'),
        ('STORE_HN_Purchase_Excel',   'STORE_HN',  'Purchase',  CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark'),
        ('STORE_HCM_Sales_Excel',    'STORE_HCM', 'Sales',      CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark'),
        ('STORE_HCM_Inventory_Excel','STORE_HCM', 'Inventory',  CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark'),
        ('STORE_HCM_Purchase_Excel', 'STORE_HCM', 'Purchase',  CAST('2020-01-01' AS DATETIME2), 'SUCCESS', GETDATE(), N'Initial watermark');

    PRINT 'Inserted 6 seed watermarks into ETL_Watermark.';
END
ELSE
BEGIN
    PRINT 'ETL_Watermark already has data — skipping seed.';
END
GO

-- ============================================================================
-- SEED DATA: Insert 1 ban ghi loi mau vao STG_ErrorLog (de test)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM STG_ErrorLog)
BEGIN
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        SourceKey, RawData, BatchDate,
        IsResolved, ETLRunDate
    )
    VALUES (
        'STORE_HN',
        'STG_SalesRaw',
        'DIMENSION_NOT_FOUND',
        'Product not found in DimProduct (IsCurrent=1)',
        'LAP999',
        '{"MaHoaDon":"HD001","MaSP":"LAP999","SoLuong":1}',
        CAST(GETDATE() AS DATE),
        0,
        GETDATE()
    );

    PRINT 'Inserted 1 sample error log into STG_ErrorLog.';
END
ELSE
BEGIN
    PRINT 'STG_ErrorLog already has data — skipping seed.';
END
GO

-- ============================================================================
-- XAC MINH: Doc lai cau truc
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: Staging Tables ===';
SELECT
    t.name AS TableName,
    p.rows AS ApproxRows,
    SUM(CASE WHEN c.is_nullable = 0 THEN 1 ELSE 0 END) AS NotNullCols,
    SUM(CASE WHEN c.is_nullable = 1 THEN 1 ELSE 0 END) AS NullableCols,
    COUNT(*) AS TotalCols
FROM sys.tables t
INNER JOIN sys.columns c ON c.object_id = t.object_id
INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE t.name LIKE 'STG_%' OR t.name IN ('ETL_Watermark', 'ETL_RunLog', 'STG_ErrorLog')
GROUP BY t.name, p.rows
ORDER BY t.name;

PRINT '';
PRINT '=== VERIFICATION: STG_ Tables — TenantID columns ===';
SELECT t.name AS TableName,
       CASE WHEN EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = t.object_id AND c.name = 'TenantID')
            THEN 'HAS TenantID' ELSE 'NO TenantID (SHARED)' END AS TenantIDFlag
FROM sys.tables t
WHERE t.name LIKE 'STG_%'
ORDER BY t.name;

PRINT '';
PRINT '=== VERIFICATION: ETL_Watermark — Sample ===';
SELECT TOP 10 WatermarkID, SourceName, TenantID, SourceType,
       CONVERT(VARCHAR(30), WatermarkValue, 120) AS WatermarkValue,
       LastRunStatus,
       CONVERT(VARCHAR(30), LastRunDatetime, 120) AS LastRunDatetime
FROM ETL_Watermark ORDER BY TenantID, SourceName;

PRINT '';
PRINT '=== VERIFICATION: STG_ErrorLog — Sample ===';
SELECT TOP 5 ErrorLogID, TenantID, SourceTable, ErrorType, ErrorMessage,
       CASE WHEN IsResolved = 1 THEN 'Resolved' ELSE 'Pending' END AS Status,
       CONVERT(VARCHAR(30), ETLRunDate, 120) AS ETLRunDate
FROM STG_ErrorLog ORDER BY ErrorLogID DESC;

PRINT '';
PRINT '=== VERIFICATION: Stored Procedures ===';
SELECT name AS ProcedureName, create_date
FROM sys.procedures
WHERE name IN (
    'usp_Truncate_StagingTables', 'usp_Update_Watermark',
    'usp_Get_Last_Watermark', 'usp_Get_All_Active_Watermarks',
    'usp_ClearErrorLog'
)
ORDER BY name;

PRINT '';
PRINT '=== PHASE 5 COMPLETED SUCCESSFULLY ===';
GO
