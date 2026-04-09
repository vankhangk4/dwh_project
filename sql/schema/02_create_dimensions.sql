-- ============================================================================
-- PHASE 2: Shared Dimensions Schema
-- File: sql/schema/02_create_dimensions.sql
-- Description: Tao cac bang Dimension dung chung cho tat ca tenant.
--              - DimDate    : Chiều thời gian (Shared, pre-populated 2015-2030)
--              - DimProduct : Chiều sản phẩm (Shared, SCD Type 2)
--              - DimSupplier: Chiều nhà cung cấp (Shared)
--
-- NOTE: Cac bang nay KHONG co TenantID vi duoc dung chung boi tat ca tenant.
--       Phu thuoc: Chay SAU Phase 1 (01_create_tenants.sql)
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG 1: DimDate
-- Chiều thời gian — Shared, pre-populated 2015-01-01 den 2030-12-31.
-- Khong co TenantID.
-- DateKey dinh dang INT yyyyMMdd (VD: 20240115).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimDate')
BEGIN
    CREATE TABLE DimDate (
        DateKey           INT           NOT NULL,
        FullDate          DATE          NOT NULL,
        DayName           NVARCHAR(20)  NOT NULL,
        DayOfWeek         TINYINT       NOT NULL,  -- 1=Monday, 7=Sunday
        DayOfMonth        TINYINT       NOT NULL,
        DayOfYear         SMALLINT      NOT NULL,
        WeekOfYear        TINYINT       NOT NULL,
        MonthKey          INT           NOT NULL,  -- yyyyMM
        MonthName         NVARCHAR(20)  NOT NULL,
        MonthOfYear       TINYINT       NOT NULL,
        QuarterKey        TINYINT       NOT NULL,  -- 1,2,3,4
        QuarterName       NVARCHAR(10)  NOT NULL,
        YearKey           INT           NOT NULL,
        YearMonth         NVARCHAR(10)  NOT NULL,  -- yyyy-MM
        IsWeekend         BIT           NOT NULL DEFAULT 0,
        IsHoliday         BIT           NOT NULL DEFAULT 0,
        HolidayName       NVARCHAR(100) NULL,
        FiscalYear        INT           NOT NULL,
        FiscalQuarter     TINYINT       NOT NULL,
        CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED (DateKey)
    );

    PRINT 'Created table: DimDate';
END
ELSE
BEGIN
    PRINT 'Table DimDate already exists — skipping CREATE.';
END
GO

-- Index cho DimDate
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimDate_FullDate' AND object_id = OBJECT_ID('DimDate'))
BEGIN
    CREATE INDEX IX_DimDate_FullDate ON DimDate(FullDate);
    PRINT 'Created index: IX_DimDate_FullDate';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimDate_YearMonth' AND object_id = OBJECT_ID('DimDate'))
BEGIN
    CREATE INDEX IX_DimDate_YearMonth ON DimDate(YearMonth);
    PRINT 'Created index: IX_DimDate_YearMonth';
END
GO

-- ============================================================================
-- POPULATE DimDate: 2015-01-01 → 2030-12-31 (5844 ngay)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimDate)
BEGIN
    DECLARE @start_date DATE = '2015-01-01';
    DECLARE @end_date   DATE = '2030-12-31';
    DECLARE @current_date DATE = @start_date;

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO DimDate (
            DateKey,
            FullDate,
            DayName,
            DayOfWeek,
            DayOfMonth,
            DayOfYear,
            WeekOfYear,
            MonthKey,
            MonthName,
            MonthOfYear,
            QuarterKey,
            QuarterName,
            YearKey,
            YearMonth,
            IsWeekend,
            IsHoliday,
            HolidayName,
            FiscalYear,
            FiscalQuarter
        )
        SELECT
            CAST(FORMAT(@current_date, 'yyyyMMdd') AS INT) AS DateKey,
            @current_date AS FullDate,
            CAST(DATENAME(WEEKDAY, @current_date) AS NVARCHAR(20)) AS DayName,
            CAST(DATEPART(WEEKDAY, @current_date) AS TINYINT) AS DayOfWeek,
            CAST(DATEPART(DAY, @current_date) AS TINYINT) AS DayOfMonth,
            CAST(DATEPART(DAYOFYEAR, @current_date) AS SMALLINT) AS DayOfYear,
            CAST(DATEPART(WEEK, @current_date) AS TINYINT) AS WeekOfYear,
            CAST(FORMAT(@current_date, 'yyyyMM') AS INT) AS MonthKey,
            CAST(FORMAT(@current_date, 'MMMM') AS NVARCHAR(20)) AS MonthName,
            CAST(MONTH(@current_date) AS TINYINT) AS MonthOfYear,
            DATEPART(QUARTER, @current_date) AS QuarterKey,
            'Q' + CAST(DATEPART(QUARTER, @current_date) AS NVARCHAR(10)) AS QuarterName,
            YEAR(@current_date) AS YearKey,
            FORMAT(@current_date, 'yyyy-MM') AS YearMonth,
            CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
            0 AS IsHoliday,
            NULL AS HolidayName,
            YEAR(DATEADD(MONTH, 3, @current_date)) AS FiscalYear,
            DATEPART(QUARTER, DATEADD(MONTH, 3, @current_date)) AS FiscalQuarter;

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    PRINT 'Populated DimDate: 5844 rows (2015-01-01 → 2030-12-31)';
END
ELSE
BEGIN
    DECLARE @row_count INT;
    SELECT @row_count = COUNT(*) FROM DimDate;
    PRINT 'DimDate already populated: ' + CAST(@row_count AS VARCHAR(10)) + ' rows — skipping.';
END
GO

-- Danh sach ngay le Viet Nam (mau) — update IsHoliday = 1
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'DimDate')
BEGIN
    UPDATE d SET d.IsHoliday = 1, d.HolidayName = h.HolidayName
    FROM DimDate d
    INNER JOIN (
        SELECT FullDate, HolidayName FROM (VALUES
            ('2015-01-01', N'Tết Dương lịch'),
            ('2015-02-14', N'Tết Nguyên đán 2015'),
            ('2015-02-15', N'Tết Nguyên đán 2015'),
            ('2015-02-16', N'Tết Nguyên đán 2015'),
            ('2015-04-30', N'Ngày Giải phóng miền Nam'),
            ('2015-05-01', N'Ngày Quốc tế Lao động'),
            ('2015-09-02', N'Ngày Quốc khánh'),
            ('2016-01-01', N'Tết Dương lịch'),
            ('2016-02-08', N'Tết Nguyên đán 2016'),
            ('2016-04-30', N'Ngày Giải phóng miền Nam'),
            ('2016-05-01', N'Ngày Quốc tế Lao động'),
            ('2016-09-02', N'Ngày Quốc khánh'),
            ('2017-01-01', N'Tết Dương lịch'),
            ('2017-01-28', N'Tết Nguyên đán 2017'),
            ('2017-04-30', N'Ngày Giải phóng miền Nam'),
            ('2017-05-01', N'Ngày Quốc tế Lao động'),
            ('2017-09-02', N'Ngày Quốc khánh'),
            ('2018-01-01', N'Tết Dương lịch'),
            ('2018-02-16', N'Tết Nguyên đán 2018'),
            ('2018-04-30', N'Ngày Giải phóng miền Nam'),
            ('2018-05-01', N'Ngày Quốc tế Lao động'),
            ('2018-09-02', N'Ngày Quốc khánh'),
            ('2019-01-01', N'Tết Dương lịch'),
            ('2019-02-05', N'Tết Nguyên đán 2019'),
            ('2019-04-30', N'Ngày Giải phóng miền Nam'),
            ('2019-05-01', N'Ngày Quốc tế Lao động'),
            ('2019-09-02', N'Ngày Quốc khánh'),
            ('2020-01-01', N'Tết Dương lịch'),
            ('2020-01-25', N'Tết Nguyên đán 2020'),
            ('2020-04-30', N'Ngày Giải phóng miền Nam'),
            ('2020-05-01', N'Ngày Quốc tế Lao động'),
            ('2020-09-02', N'Ngày Quốc khánh'),
            ('2021-01-01', N'Tết Dương lịch'),
            ('2021-02-12', N'Tết Nguyên đán 2021'),
            ('2021-04-30', N'Ngày Giải phóng miền Nam'),
            ('2021-05-01', N'Ngày Quốc tế Lao động'),
            ('2021-09-02', N'Ngày Quốc khánh'),
            ('2022-01-01', N'Tết Dương lịch'),
            ('2022-02-01', N'Tết Nguyên đán 2022'),
            ('2022-04-30', N'Ngày Giải phóng miền Nam'),
            ('2022-05-01', N'Ngày Quốc tế Lao động'),
            ('2022-09-02', N'Ngày Quốc khánh'),
            ('2023-01-01', N'Tết Dương lịch'),
            ('2023-01-22', N'Tết Nguyên đán 2023'),
            ('2023-04-30', N'Ngày Giải phóng miền Nam'),
            ('2023-05-01', N'Ngày Quốc tế Lao động'),
            ('2023-09-02', N'Ngày Quốc khánh'),
            ('2024-01-01', N'Tết Dương lịch'),
            ('2024-02-10', N'Tết Nguyên đán 2024'),
            ('2024-04-30', N'Ngày Giải phóng miền Nam'),
            ('2024-05-01', N'Ngày Quốc tế Lao động'),
            ('2024-09-02', N'Ngày Quốc khánh'),
            ('2025-01-01', N'Tết Dương lịch'),
            ('2025-01-29', N'Tết Nguyên đán 2025'),
            ('2025-04-30', N'Ngày Giải phóng miền Nam'),
            ('2025-05-01', N'Ngày Quốc tế Lao động'),
            ('2025-09-02', N'Ngày Quốc khánh'),
            ('2026-01-01', N'Tết Dương lịch'),
            ('2026-02-17', N'Tết Nguyên đán 2026'),
            ('2026-04-30', N'Ngày Giải phóng miền Nam'),
            ('2026-05-01', N'Ngày Quốc tế Lao động'),
            ('2026-09-02', N'Ngày Quốc khánh'),
            ('2027-01-01', N'Tết Dương lịch'),
            ('2027-02-06', N'Tết Nguyên đán 2027'),
            ('2027-04-30', N'Ngày Giải phóng miền Nam'),
            ('2027-05-01', N'Ngày Quốc tế Lao động'),
            ('2027-09-02', N'Ngày Quốc khánh'),
            ('2028-01-01', N'Tết Dương lịch'),
            ('2028-01-26', N'Tết Nguyên đán 2028'),
            ('2028-04-30', N'Ngày Giải phóng miền Nam'),
            ('2028-05-01', N'Ngày Quốc tế Lao động'),
            ('2028-09-02', N'Ngày Quốc khánh'),
            ('2029-01-01', N'Tết Dương lịch'),
            ('2029-02-13', N'Tết Nguyên đán 2029'),
            ('2029-04-30', N'Ngày Giải phóng miền Nam'),
            ('2029-05-01', N'Ngày Quốc tế Lao động'),
            ('2029-09-02', N'Ngày Quốc khánh'),
            ('2030-01-01', N'Tết Dương lịch'),
            ('2030-02-03', N'Tết Nguyên đán 2030'),
            ('2030-04-30', N'Ngày Giải phóng miền Nam'),
            ('2030-05-01', N'Ngày Quốc tế Lao động'),
            ('2030-09-02', N'Ngày Quốc khánh')
        ) AS h(FullDate, HolidayName)
    ) h ON d.FullDate = h.FullDate;

    PRINT 'Updated Vietnam holidays in DimDate.';
END
GO

-- ============================================================================
-- BANG 2: DimProduct
-- Chiều sản phẩm — Shared, SCD Type 2.
-- KHONG co TenantID vi san pham duoc dung chung cho tat ca chi nhanh.
-- SCD Type 2: Khi gia thay doi, dong cu se bi dong (IsCurrent=0, ExpirationDate=hom qua)
--             va dong moi duoc tao (IsCurrent=1, EffectiveDate=hôm nay).
-- IsCurrent = 1: chi dong hien tai con hieu luc.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimProduct')
BEGIN
    CREATE TABLE DimProduct (
        ProductKey       INT IDENTITY(1,1) NOT NULL,
        ProductCode       VARCHAR(50)       NOT NULL,
        ProductName       NVARCHAR(200)     NOT NULL,
        Brand             NVARCHAR(100)     NULL,
        CategoryName      NVARCHAR(100)    NOT NULL,
        SubCategory       NVARCHAR(100)    NULL,
        UnitCostPrice     DECIMAL(18,2)    NOT NULL DEFAULT 0,
        UnitListPrice     DECIMAL(18,2)    NOT NULL DEFAULT 0,
        UnitWeight        DECIMAL(10,3)    NULL,
        WeightUnit        NVARCHAR(20)     NULL,
        SKU               VARCHAR(50)       NULL,
        Barcode           VARCHAR(50)       NULL,
        SupplierKey       INT               NULL,
        IsActive          BIT               NOT NULL DEFAULT 1,
        EffectiveDate     DATE              NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        ExpirationDate    DATE              NULL,
        IsCurrent         BIT               NOT NULL DEFAULT 1,
        LoadDatetime      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimProduct PRIMARY KEY CLUSTERED (ProductKey),
        CONSTRAINT UQ_DimProduct_Code_Current
            UNIQUE (ProductCode, IsCurrent)
            WHERE IsCurrent = 1
    );

    PRINT 'Created table: DimProduct';
END
ELSE
BEGIN
    PRINT 'Table DimProduct already exists — skipping CREATE.';
END
GO

-- Indexes cho DimProduct
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimProduct_ProductCode_IsCurrent' AND object_id = OBJECT_ID('DimProduct'))
BEGIN
    CREATE INDEX IX_DimProduct_ProductCode_IsCurrent
        ON DimProduct(ProductCode, IsCurrent)
        WHERE IsCurrent = 1;
    PRINT 'Created index: IX_DimProduct_ProductCode_IsCurrent';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimProduct_CategoryName' AND object_id = OBJECT_ID('DimProduct'))
BEGIN
    CREATE INDEX IX_DimProduct_CategoryName ON DimProduct(CategoryName);
    PRINT 'Created index: IX_DimProduct_CategoryName';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimProduct_Brand' AND object_id = OBJECT_ID('DimProduct'))
BEGIN
    CREATE INDEX IX_DimProduct_Brand ON DimProduct(Brand);
    PRINT 'Created index: IX_DimProduct_Brand';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimProduct_IsCurrent' AND object_id = OBJECT_ID('DimProduct'))
BEGIN
    CREATE INDEX IX_DimProduct_IsCurrent ON DimProduct(IsCurrent)
        WHERE IsCurrent = 1;
    PRINT 'Created index: IX_DimProduct_IsCurrent';
END
GO

-- ============================================================================
-- BANG 3: DimSupplier
-- Chiều nhà cung cấp — Shared, khong co TenantID.
-- Nha cung cap la doi tac cap toan chuoi, dung chung cho moi chi nhanh.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimSupplier')
BEGIN
    CREATE TABLE DimSupplier (
        SupplierKey    INT IDENTITY(1,1) NOT NULL,
        SupplierCode   VARCHAR(50)       NOT NULL,
        SupplierName   NVARCHAR(200)     NOT NULL,
        ContactName    NVARCHAR(100)     NULL,
        ContactTitle   NVARCHAR(100)     NULL,
        Phone          VARCHAR(30)        NULL,
        Email          VARCHAR(100)      NULL,
        Address        NVARCHAR(500)     NULL,
        City           NVARCHAR(100)     NULL,
        Country        NVARCHAR(50)      NOT NULL DEFAULT N'Việt Nam',
        TaxCode        VARCHAR(50)        NULL,
        PaymentTerms   NVARCHAR(100)     NULL,
        IsActive       BIT               NOT NULL DEFAULT 1,
        CreatedAt      DATETIME2         NOT NULL DEFAULT GETDATE(),
        LoadDatetime   DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimSupplier PRIMARY KEY CLUSTERED (SupplierKey),
        CONSTRAINT UQ_DimSupplier_SupplierCode UNIQUE (SupplierCode)
    );

    PRINT 'Created table: DimSupplier';
END
ELSE
BEGIN
    PRINT 'Table DimSupplier already exists — skipping CREATE.';
END
GO

-- Indexes cho DimSupplier
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimSupplier_City' AND object_id = OBJECT_ID('DimSupplier'))
BEGIN
    CREATE INDEX IX_DimSupplier_City ON DimSupplier(City);
    PRINT 'Created index: IX_DimSupplier_City';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimSupplier_IsActive' AND object_id = OBJECT_ID('DimSupplier'))
BEGIN
    CREATE INDEX IX_DimSupplier_IsActive ON DimSupplier(IsActive);
    PRINT 'Created index: IX_DimSupplier_IsActive';
END
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimDate
-- Populate lai DimDate (chi chay mot lan, hoac chay lai neu can).
-- Khoa chinh: yyyyMMdd.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimDate')
    DROP PROCEDURE usp_Load_DimDate;
GO

CREATE PROCEDURE usp_Load_DimDate
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_date DATE = '2015-01-01';
    DECLARE @end_date   DATE = '2030-12-31';
    DECLARE @current_date DATE;

    -- Xoa du lieu cu (nếu can populate lai)
    -- DELETE FROM DimDate WHERE 1=1;  -- Uncomment neu muon reload

    -- Kiem tra da co du lieu chua
    IF EXISTS (SELECT TOP 1 * FROM DimDate)
    BEGIN
        PRINT 'DimDate da co du lieu — khong reload.';
        RETURN;
    END

    SET @current_date = @start_date;

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO DimDate (
            DateKey, FullDate, DayName, DayOfWeek, DayOfMonth, DayOfYear,
            WeekOfYear, MonthKey, MonthName, MonthOfYear, QuarterKey,
            QuarterName, YearKey, YearMonth, IsWeekend, IsHoliday,
            HolidayName, FiscalYear, FiscalQuarter
        )
        SELECT
            CAST(FORMAT(@current_date, 'yyyyMMdd') AS INT),
            @current_date,
            CAST(DATENAME(WEEKDAY, @current_date) AS NVARCHAR(20)),
            CAST(DATEPART(WEEKDAY, @current_date) AS TINYINT),
            CAST(DATEPART(DAY, @current_date) AS TINYINT),
            CAST(DATEPART(DAYOFYEAR, @current_date) AS SMALLINT),
            CAST(DATEPART(WEEK, @current_date) AS TINYINT),
            CAST(FORMAT(@current_date, 'yyyyMM') AS INT),
            CAST(FORMAT(@current_date, 'MMMM') AS NVARCHAR(20)),
            CAST(MONTH(@current_date) AS TINYINT),
            DATEPART(QUARTER, @current_date),
            'Q' + CAST(DATEPART(QUARTER, @current_date) AS NVARCHAR(10)),
            YEAR(@current_date),
            FORMAT(@current_date, 'yyyy-MM'),
            CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END,
            0, NULL,
            YEAR(DATEADD(MONTH, 3, @current_date)),
            DATEPART(QUARTER, DATEADD(MONTH, 3, @current_date));

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    PRINT 'usp_Load_DimDate: Populated ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows.';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimDate';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimProduct
-- SCD Type 2: Dong cu bi dong khi gia thay doi; dong moi duoc chen.
-- Chi xu ly san pham hien tai (IsCurrent=1) va tao dong moi khi thay doi.
-- Input: Khong can — doc tu STG_ProductRaw.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimProduct')
    DROP PROCEDURE usp_Load_DimProduct;
GO

CREATE PROCEDURE usp_Load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsInserted INT = 0;

    -- ================================================================
    -- BUOC 1: Dong (close) cac ban ghi cu khi gia nhap hoac gia ban thay doi
    -- ================================================================
    UPDATE dp SET
        dp.ExpirationDate = DATEADD(DAY, -1, @BatchDate),
        dp.IsCurrent      = 0
    FROM DimProduct dp
    INNER JOIN STG_ProductRaw s ON s.MaSP = dp.ProductCode
    WHERE dp.IsCurrent = 1
      AND (
          dp.UnitCostPrice <> s.GiaVon
          OR dp.UnitListPrice <> s.GiaNiemYet
          OR dp.ProductName <> s.TenSP
          OR dp.Brand <> s.ThuongHieu
          OR dp.CategoryName <> s.DanhMuc
      );

    SET @RowsClosed = @@ROWCOUNT;

    -- ================================================================
    -- BUOC 2: Chen ban ghi moi cho san pham moi (chua ton tai)
    -- ================================================================
    INSERT INTO DimProduct (
        ProductCode, ProductName, Brand, CategoryName, SubCategory,
        UnitCostPrice, UnitListPrice, SKU, Barcode,
        SupplierKey, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
    )
    SELECT
        s.MaSP,
        s.TenSP,
        s.ThuongHieu,
        s.DanhMuc,
        s.PhanLoai,
        ISNULL(s.GiaVon, 0),
        ISNULL(s.GiaNiemYet, 0),
        s.SKU,
        s.Barcode,
        NULL,
        1,
        @BatchDate,
        NULL,
        1
    FROM STG_ProductRaw s
    WHERE NOT EXISTS (
        SELECT 1 FROM DimProduct dp
        WHERE dp.ProductCode = s.MaSP AND dp.IsCurrent = 1
    );

    SET @RowsInserted = @@ROWCOUNT;

    -- ================================================================
    -- BUOC 3: Chen ban ghi moi cho san pham THAY DOI GIA
    -- (da dong o buoc 1, gio chen dong moi voi gia moi)
    -- ================================================================
    INSERT INTO DimProduct (
        ProductCode, ProductName, Brand, CategoryName, SubCategory,
        UnitCostPrice, UnitListPrice, SKU, Barcode,
        SupplierKey, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
    )
    SELECT
        s.MaSP,
        s.TenSP,
        s.ThuongHieu,
        s.DanhMuc,
        s.PhanLoai,
        ISNULL(s.GiaVon, 0),
        ISNULL(s.GiaNiemYet, 0),
        s.SKU,
        s.Barcode,
        NULL,
        1,
        @BatchDate,
        NULL,
        1
    FROM STG_ProductRaw s
    INNER JOIN DimProduct dp_old ON dp_old.ProductCode = s.MaSP AND dp_old.IsCurrent = 0
    WHERE NOT EXISTS (
        SELECT 1 FROM DimProduct dp_new
        WHERE dp_new.ProductCode = s.MaSP AND dp_new.IsCurrent = 1
    );

    SET @RowsInserted = @RowsInserted + @@ROWCOUNT;

    PRINT 'usp_Load_DimProduct: Closed ' + CAST(@RowsClosed AS VARCHAR(10))
        + ' rows, Inserted ' + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimProduct';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Load_DimSupplier
-- Load nha cung cap tu STG_SupplierRaw.
-- Shared (khong co TenantID).
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimSupplier')
    DROP PROCEDURE usp_Load_DimSupplier;
GO

CREATE PROCEDURE usp_Load_DimSupplier
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsInserted INT = 0;

    -- Chen nha cung cap moi (chua ton tai)
    INSERT INTO DimSupplier (
        SupplierCode, SupplierName, ContactName, ContactTitle,
        Phone, Email, Address, City, Country,
        TaxCode, PaymentTerms, IsActive
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
        1
    FROM STG_SupplierRaw s
    WHERE NOT EXISTS (
        SELECT 1 FROM DimSupplier d
        WHERE d.SupplierCode = s.MaNCC
    );

    SET @RowsInserted = @@ROWCOUNT;

    -- Cap nhat nha cung cap da ton tai (thong tin thay doi)
    UPDATE d SET
        d.SupplierName  = s.TenNCC,
        d.ContactName   = s.NguoiLienHe,
        d.ContactTitle  = s.ChucVu,
        d.Phone         = s.DienThoai,
        d.Email         = s.Email,
        d.Address       = s.DiaChi,
        d.City          = s.ThanhPho,
        d.TaxCode       = s.MaSoThue,
        d.PaymentTerms  = s.DieuKhoanTT
    FROM DimSupplier d
    INNER JOIN STG_SupplierRaw s ON s.MaNCC = d.SupplierCode
    WHERE NOT EXISTS (
        SELECT 1 FROM DimSupplier d2
        WHERE d2.SupplierCode = s.MaNCC AND d2.SupplierKey <> d.SupplierKey
    );

    PRINT 'usp_Load_DimSupplier: Inserted ' + CAST(@RowsInserted AS VARCHAR(10)) + ' new suppliers.';
END;
GO

PRINT 'Created stored procedure: usp_Load_DimSupplier';
GO

-- ============================================================================
-- SEED DATA: Insert du lieu mau cho DimSupplier
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimSupplier)
BEGIN
    INSERT INTO DimSupplier (
        SupplierCode, SupplierName, ContactName, ContactTitle,
        Phone, Email, Address, City, Country, TaxCode, PaymentTerms, IsActive
    )
    VALUES
        ('SUP001', N'TechWorld Distribution', N'Nguyễn Văn An', N'Giám đốc kinh doanh',
         '0901234567', 'an@techworld.vn', N'123 Nguyễn Trãi, Q1', N'Hồ Chí Minh', N'Việt Nam',
         '0123456789', N'NET 30', 1),
        ('SUP002', N'Samsung Electronics VN', N'Trần Thị Bình', N'Trưởng phòng phân phối',
         '0902345678', 'binh@samsung.vn', N'456 Lê Lợi, Q3', N'Hồ Chí Minh', N'Việt Nam',
         '9876543210', N'NET 45', 1),
        ('SUP003', N'Apple Authorized Distributor', N'Lê Văn Cường', N'Quản lý tài khoản',
         '0903456789', 'cuong@apple-dist.vn', N'789 Trần Hưng Đạo, Q5', N'Hồ Chí Minh', N'Việt Nam',
         '4567891230', N'NET 30', 1),
        ('SUP004', N'DELL Việt Nam', N'Phạm Thị Dung', N'Giám đốc khu vực',
         '0904567890', 'dung@dell.vn', N'321 Phạm Hùng, Q10', N'Hồ Chí Minh', N'Việt Nam',
         '7890123456', N'NET 60', 1),
        ('SUP005', N'ASUS Regional HQ', N'Hoàng Văn Em', N'Trưởng bộ phận bán hàng',
         '0905678901', 'em@asus.com', N'555 Điện Biên Phủ, Q3', N'Hồ Chí Minh', N'Việt Nam',
         '3216549870', N'NET 30', 1);

    PRINT 'Inserted 5 seed suppliers into DimSupplier.';
END
ELSE
BEGIN
    PRINT 'DimSupplier already has data — skipping seed.';
END
GO

-- ============================================================================
-- SEED DATA: Insert du lieu mau cho DimProduct (SCD Type 2)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM DimProduct)
BEGIN
    INSERT INTO DimProduct (
        ProductCode, ProductName, Brand, CategoryName, SubCategory,
        UnitCostPrice, UnitListPrice, SKU, Barcode,
        SupplierKey, IsActive,
        EffectiveDate, ExpirationDate, IsCurrent
    )
    VALUES
        -- Laptop
        ('LAP001', N'MacBook Air M2 13 inch', N'Apple', N'Laptop', N'MacBook',
         19500000, 23990000, 'MBA-M2-13', '194253469281', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('LAP002', N'MacBook Pro 14 inch M3', N'Apple', N'Laptop', N'MacBook',
         38000000, 47990000, 'MBP-14-M3', '194253469298', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('LAP003', N'Dell XPS 15 2024', N'DELL', N'Laptop', N'Ultrabook',
         25000000, 32990000, 'XPS15-2024', '884116123456', 4, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('LAP004', N'ASUS ROG Zephyrus G14', N'ASUS', N'Laptop', N'Gaming',
         22000000, 28990000, 'ROG-G14', '192200876543', 5, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('LAP005', N'Lenovo ThinkPad X1 Carbon', N'Lenovo', N'Laptop', N'Business',
         28000000, 34990000, 'X1C-2024', '195200765432', NULL, 1, CAST(GETDATE() AS DATE), NULL, 1),
        -- Dien thoai
        ('DT001', N'iPhone 15 Pro 256GB', N'Apple', N'Điện thoại', N'iPhone',
         18500000, 27990000, 'IP15PRO-256', '194253469312', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('DT002', N'Samsung Galaxy S24 Ultra', N'Samsung', N'Điện thoại', N'Galaxy',
         17000000, 25990000, 'S24U-256', '8806095456789', 2, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('DT003', N'Xiaomi 14 Pro', N'Xiaomi', N'Điện thoại', N'Mi',
         9000000, 15990000, 'XM14PRO', '6941815761234', NULL, 1, CAST(GETDATE() AS DATE), NULL, 1),
        -- Tablet
        ('TAB001', N'iPad Pro 12.9 M4', N'Apple', N'Tablet', N'iPad',
         22000000, 29990000, 'IPAD-PRO-13-M4', '194253469343', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('TAB002', N'Samsung Galaxy Tab S9 Ultra', N'Samsung', N'Tablet', N'Galaxy Tab',
         16000000, 22990000, 'TAB-S9U', '8806095457890', 2, 1, CAST(GETDATE() AS DATE), NULL, 1),
        -- Phu kien
        ('PK001', N'AirPods Pro 2', N'Apple', N'Phụ kiện', N'Tai nghe',
         3500000, 5990000, 'APP2-USB-C', '194253469434', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('PK002', N'Samsung Galaxy Buds2 Pro', N'Samsung', N'Phụ kiện', N'Tai nghe',
         2500000, 4990000, 'BUDS2-PRO', '8806095458901', 2, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('PK003', N'Sạc dự phòng Anker 20000mAh', N'Anker', N'Phụ kiện', N'Sạc',
         450000, 890000, 'AKR-PB20K', '848483028765', NULL, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('PK004', N'Apple Watch Series 9 45mm', N'Apple', N'Phụ kiện', N'Smartwatch',
         7500000, 11990000, 'AW-S9-45', '194253469512', 3, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('PK005', N'Cáp Lightning Anker', N'Anker', N'Phụ kiện', N'Cáp sạc',
         150000, 350000, 'AKR-LTN-1M', '848483028999', NULL, 1, CAST(GETDATE() AS DATE), NULL, 1),
        -- Man hinh
        ('MH001', N'Monitor Dell UltraSharp 27"', N'DELL', N'Màn hình', N'4K',
         8000000, 14990000, 'U2723QE', '884116123789', 4, 1, CAST(GETDATE() AS DATE), NULL, 1),
        ('MH002', N'Monitor LG 32" 4K IPS', N'LG', N'Màn hình', N'4K',
         7000000, 12990000, '32UN880-B', '880609123456', NULL, 1, CAST(GETDATE() AS DATE), NULL, 1);

    PRINT 'Inserted 18 seed products into DimProduct.';
END
ELSE
BEGIN
    PRINT 'DimProduct already has data — skipping seed.';
END
GO

-- ============================================================================
-- XAC MINH: Doc lai du lieu
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: DimDate ===';
DECLARE @date_count INT;
SELECT @date_count = COUNT(*) FROM DimDate;
PRINT 'DimDate row count: ' + CAST(@date_count AS VARCHAR(10));
IF @date_count = 5844
    PRINT '[PASS] DimDate: Co dung 5844 ngay (2015-01-01 → 2030-12-31).';
ELSE
    PRINT '[WARN] DimDate: ' + CAST(@date_count AS VARCHAR(10)) + ' rows.';

-- Hien thi mau
SELECT TOP 10 DateKey, FullDate, DayName, DayOfWeek, MonthName, QuarterName,
       YearKey, IsWeekend, IsHoliday, HolidayName
FROM DimDate ORDER BY DateKey;

PRINT '';
PRINT '=== VERIFICATION: DimProduct ===';
SELECT COUNT(*) AS TotalRows,
       SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
       SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS ExpiredRows
FROM DimProduct;

SELECT TOP 10 ProductKey, ProductCode, ProductName, Brand, CategoryName,
       UnitCostPrice, UnitListPrice, IsCurrent, EffectiveDate
FROM DimProduct WHERE IsCurrent = 1 ORDER BY ProductKey;

PRINT '';
PRINT '=== VERIFICATION: DimSupplier ===';
SELECT SupplierKey, SupplierCode, SupplierName, City, Country, IsActive
FROM DimSupplier ORDER BY SupplierKey;

PRINT '';
PRINT '=== PHASE 2 COMPLETED SUCCESSFULLY ===';
GO
