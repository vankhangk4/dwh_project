-- ============================================================================
-- PHASE 4: Fact Tables Schema
-- File: sql/schema/04_create_facts.sql
-- Description: Tao cac bang Fact cho Data Warehouse.
--              - FactSales     : Su kien ban hang (grain: 1 dong = 1 san pham / 1 hoa don)
--              - FactInventory : Su kien ton kho (grain: 1 san pham / 1 cua hang / 1 ngay)
--              - FactPurchase  : Su kien nhap hang (grain: 1 dong nhap hang)
--
-- NOTE: Tat ca bang Fact DEU CO TenantID vi du lieu ban hang/ton kho/nhap hang
--       thuoc ve tung cua hang cua tung tenant.
--       Phu thuoc: Chay SAU Phase 1, 2, 3
--                  (Fact can DimDate, DimProduct, DimStore, DimCustomer, DimEmployee)
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG 1: FactSales
-- Su kien ban hang.
-- Grain: 1 dong = 1 san pham trong 1 hoa don (1 line item).
-- TenantID bat buoc — dam bao rang ban hang chi thuoc ve 1 tenant.
-- DateKey la khoa ngoai den DimDate (Shared, khong co TenantID).
-- ProductKey la khoa ngoai den DimProduct (Shared, khong co TenantID).
-- StoreKey la khoa ngoai den DimStore (CO TenantID).
-- CustomerKey la khoa ngoai den DimCustomer (CO TenantID, NULL cho khach vang lai).
-- EmployeeKey la khoa ngoai den DimEmployee (CO TenantID, NULL cho nhan vien khong xac dinh).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactSales')
BEGIN
    CREATE TABLE FactSales (
        -- Khoa
        FactSalesKey    BIGINT IDENTITY(1,1) NOT NULL,

        -- Khoa ngoai — Dimension (tat ca deu co IsCurrent = 1 trong dim tuong ung)
        TenantID        VARCHAR(20)       NOT NULL,  -- Tenant cua cua hang ban
        DateKey         INT               NOT NULL,  -- Den DimDate (Shared)
        ProductKey      INT               NOT NULL,  -- Den DimProduct (Shared)
        StoreKey        INT               NOT NULL,  -- Den DimStore (CO TenantID)
        CustomerKey     INT                    NULL,  -- Den DimCustomer (CO TenantID), NULL = khach vang lai
        EmployeeKey     INT                    NULL,  -- Den DimEmployee (CO TenantID), NULL = khong xac dinh

        -- Khoa kinh doanh
        InvoiceNumber   VARCHAR(50)       NOT NULL,
        InvoiceLine     INT               NOT NULL DEFAULT 1,

        -- Do luong (Measures)
        Quantity        INT               NOT NULL DEFAULT 0,
        UnitPrice       DECIMAL(18,2)    NOT NULL DEFAULT 0,        -- Don gia tai thoi diem ban
        DiscountAmount  DECIMAL(18,2)    NOT NULL DEFAULT 0,       -- So tien chiet khau

        -- Tinh toan
        GrossSalesAmount DECIMAL(18,2)   NOT NULL DEFAULT 0,        -- Quantity * UnitPrice
        NetSalesAmount   DECIMAL(18,2)   NOT NULL DEFAULT 0,        -- GrossSalesAmount - DiscountAmount
        CostAmount      DECIMAL(18,2)    NOT NULL DEFAULT 0,        -- Quantity * UnitCostPrice (tu DimProduct)
        GrossProfitAmount DECIMAL(18,2)  NOT NULL DEFAULT 0,        -- NetSalesAmount - CostAmount

        -- Thuoc tinh ban hang
        PaymentMethod   NVARCHAR(50)      NULL,                     -- 'Tiền mặt', 'Chuyển khoản', 'Quẹt thẻ', 'Ví điện tử'
        SalesChannel    NVARCHAR(50)      NULL DEFAULT N'InStore', -- 'InStore', 'Online', 'Hotline', 'Marketplace'
        SalesGroup      NVARCHAR(50)      NULL,                     -- 'Bán lẻ', 'Bán sỉ', 'Khuyến mãi'

        -- Du lieu tra hang
        ReturnFlag      BIT               NOT NULL DEFAULT 0,        -- 1 = hoan tra, 0 = ban binh thuong
        ReturnReason    NVARCHAR(200)      NULL,

        -- Ghi nhan
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),

        -- Khoa chinh
        CONSTRAINT PK_FactSales PRIMARY KEY CLUSTERED (FactSalesKey),
        CONSTRAINT UQ_FactSales_InvoiceLine UNIQUE (TenantID, InvoiceNumber, InvoiceLine)
    );

    PRINT 'Created table: FactSales';
END
ELSE
BEGIN
    PRINT 'Table FactSales already exists — skipping CREATE.';
END
GO

-- Indexes cho FactSales
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_TenantID_DateKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_TenantID_DateKey ON FactSales(TenantID, DateKey);
    PRINT 'Created index: IX_FactSales_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_InvoiceNumber'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_InvoiceNumber ON FactSales(InvoiceNumber);
    PRINT 'Created index: IX_FactSales_InvoiceNumber';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_ProductKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_ProductKey ON FactSales(ProductKey);
    PRINT 'Created index: IX_FactSales_ProductKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_StoreKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_StoreKey ON FactSales(StoreKey);
    PRINT 'Created index: IX_FactSales_StoreKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_EmployeeKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_EmployeeKey ON FactSales(EmployeeKey);
    PRINT 'Created index: IX_FactSales_EmployeeKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_CustomerKey'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_CustomerKey ON FactSales(CustomerKey);
    PRINT 'Created index: IX_FactSales_CustomerKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_ReturnFlag'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_ReturnFlag ON FactSales(ReturnFlag);
    PRINT 'Created index: IX_FactSales_ReturnFlag';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactSales_SalesChannel'
    AND object_id = OBJECT_ID('FactSales')
)
BEGIN
    CREATE INDEX IX_FactSales_SalesChannel ON FactSales(SalesChannel);
    PRINT 'Created index: IX_FactSales_SalesChannel';
END
GO

-- ============================================================================
-- BANG 2: FactInventory
-- Su kien ton kho.
-- Grain: 1 dong = ton kho cua 1 san pham tai 1 cua hang trong 1 ngay.
-- TenantID bat buoc — dam bao rang du lieu ton kho chi thuoc ve 1 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactInventory')
BEGIN
    CREATE TABLE FactInventory (
        -- Khoa
        FactInventoryKey BIGINT IDENTITY(1,1) NOT NULL,

        -- Khoa ngoai
        TenantID        VARCHAR(20)       NOT NULL,
        DateKey         INT               NOT NULL,   -- Den DimDate (Shared)
        ProductKey      INT               NOT NULL,   -- Den DimProduct (Shared)
        StoreKey        INT               NOT NULL,   -- Den DimStore (CO TenantID)

        -- Do luong ton kho
        OpeningQty      INT               NOT NULL DEFAULT 0,    -- Ton dau ngay
        ReceivedQty     INT               NOT NULL DEFAULT 0,    -- Nhap trong ngay
        SoldQty         INT               NOT NULL DEFAULT 0,    -- Ban trong ngay
        ReturnedQty     INT               NOT NULL DEFAULT 0,   -- Tra lai trong ngay
        AdjustedQty     INT               NOT NULL DEFAULT 0,   -- Dieu chinh (+/-)
        ClosingQty      INT               NOT NULL DEFAULT 0,   -- Ton cuoi ngay = Opening + Received - Sold + Returned + Adjusted

        -- Tinh toan gia tri
        UnitCostPrice   DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- Don gia von (tu DimProduct)
        OpeningValue    DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- OpeningQty * UnitCostPrice
        ReceivedValue   DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- ReceivedQty * UnitCostPrice
        SoldValue       DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- SoldQty * UnitCostPrice
        ClosingValue    DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- ClosingQty * UnitCostPrice

        -- Nguong va canh bao
        ReorderLevel    INT               NOT NULL DEFAULT 0,   -- Muc toi thieu can nhap
        DaysOfStock     DECIMAL(10,2)    NOT NULL DEFAULT 0,   -- So ngay duoc ton kho (ClosingQty / BanTB_Ngay)
        StockStatus     NVARCHAR(20)      NULL,                 -- 'Normal', 'Low', 'Out of Stock', 'Overstock'

        -- Khoa kinh doanh
        MovementType    NVARCHAR(50)      NULL,                 -- 'Daily Count', 'Purchase Receipt', 'Sales Issue', 'Return', 'Adjustment'

        -- Ghi nhan
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),

        -- Khoa chinh
        CONSTRAINT PK_FactInventory PRIMARY KEY CLUSTERED (FactInventoryKey),
        CONSTRAINT UQ_FactInventory_DateProductStore UNIQUE (TenantID, DateKey, ProductKey, StoreKey)
    );

    PRINT 'Created table: FactInventory';
END
ELSE
BEGIN
    PRINT 'Table FactInventory already exists — skipping CREATE.';
END
GO

-- Indexes cho FactInventory
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_TenantID_DateKey'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_TenantID_DateKey ON FactInventory(TenantID, DateKey);
    PRINT 'Created index: IX_FactInventory_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_ProductKey'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_ProductKey ON FactInventory(ProductKey);
    PRINT 'Created index: IX_FactInventory_ProductKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_StoreKey'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_StoreKey ON FactInventory(StoreKey);
    PRINT 'Created index: IX_FactInventory_StoreKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_ClosingQty'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_ClosingQty ON FactInventory(ClosingQty);
    PRINT 'Created index: IX_FactInventory_ClosingQty';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_ReorderLevel'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_ReorderLevel ON FactInventory(ReorderLevel);
    PRINT 'Created index: IX_FactInventory_ReorderLevel';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactInventory_StockStatus'
    AND object_id = OBJECT_ID('FactInventory')
)
BEGIN
    CREATE INDEX IX_FactInventory_StockStatus ON FactInventory(StockStatus);
    PRINT 'Created index: IX_FactInventory_StockStatus';
END
GO

-- ============================================================================
-- BANG 3: FactPurchase
-- Su kien nhap hang tu nha cung cap.
-- Grain: 1 dong = 1 dong trong phieu nhap kho (1 line item).
-- TenantID bat buoc — dam bao rang du lieu nhap hang chi thuoc ve 1 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactPurchase')
BEGIN
    CREATE TABLE FactPurchase (
        -- Khoa
        FactPurchaseKey  BIGINT IDENTITY(1,1) NOT NULL,

        -- Khoa ngoai
        TenantID        VARCHAR(20)       NOT NULL,
        DateKey         INT               NOT NULL,   -- Den DimDate (Shared)
        ProductKey      INT               NOT NULL,   -- Den DimProduct (Shared)
        SupplierKey     INT               NOT NULL,   -- Den DimSupplier (Shared)
        StoreKey        INT               NOT NULL,   -- Den DimStore (CO TenantID)

        -- Khoa kinh doanh
        PurchaseOrderNumber VARCHAR(50)   NOT NULL,
        PurchaseOrderLine   INT          NOT NULL DEFAULT 1,
        GRNNumber           VARCHAR(50)   NULL,        -- Goods Receipt Note Number
        GRNDate             DATE          NULL,

        -- Do luong
        Quantity            INT          NOT NULL DEFAULT 0,
        UnitCost            DECIMAL(18,2) NOT NULL DEFAULT 0,  -- Don gia nhap
        TotalCost           DECIMAL(18,2) NOT NULL DEFAULT 0,  -- Quantity * UnitCost
        DiscountAmount      DECIMAL(18,2) NOT NULL DEFAULT 0,
        NetCost             DECIMAL(18,2) NOT NULL DEFAULT 0,  -- TotalCost - DiscountAmount
        TaxAmount           DECIMAL(18,2) NOT NULL DEFAULT 0,

        -- Thanh toan
        PaymentStatus   NVARCHAR(50)      NULL,                     -- 'Pending', 'Partial', 'Paid', 'Overdue'
        PaymentMethod   NVARCHAR(50)      NULL,                     -- 'Cash', 'Bank Transfer', 'Credit Note'
        DueDate         DATE              NULL,

        -- Nhan
        ReceivedQty     INT               NOT NULL DEFAULT 0,   -- So luong thuc nhan
        ReceivedDate    DATE              NULL,
        QualityStatus   NVARCHAR(50)      NULL,                  -- 'Passed', 'Rejected', 'Partial'
        Notes           NVARCHAR(500)     NULL,

        -- Ghi nhan
        LoadDatetime    DATETIME2         NOT NULL DEFAULT GETDATE(),

        -- Khoa chinh
        CONSTRAINT PK_FactPurchase PRIMARY KEY CLUSTERED (FactPurchaseKey),
        CONSTRAINT UQ_FactPurchase_OrderLine UNIQUE (TenantID, PurchaseOrderNumber, PurchaseOrderLine)
    );

    PRINT 'Created table: FactPurchase';
END
ELSE
BEGIN
    PRINT 'Table FactPurchase already exists — skipping CREATE.';
END
GO

-- Indexes cho FactPurchase
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_TenantID_DateKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_TenantID_DateKey ON FactPurchase(TenantID, DateKey);
    PRINT 'Created index: IX_FactPurchase_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_ProductKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_ProductKey ON FactPurchase(ProductKey);
    PRINT 'Created index: IX_FactPurchase_ProductKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_SupplierKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_SupplierKey ON FactPurchase(SupplierKey);
    PRINT 'Created index: IX_FactPurchase_SupplierKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_StoreKey'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_StoreKey ON FactPurchase(StoreKey);
    PRINT 'Created index: IX_FactPurchase_StoreKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_PurchaseOrderNumber'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_PurchaseOrderNumber ON FactPurchase(PurchaseOrderNumber);
    PRINT 'Created index: IX_FactPurchase_PurchaseOrderNumber';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactPurchase_PaymentStatus'
    AND object_id = OBJECT_ID('FactPurchase')
)
BEGIN
    CREATE INDEX IX_FactPurchase_PaymentStatus ON FactPurchase(PaymentStatus);
    PRINT 'Created index: IX_FactPurchase_PaymentStatus';
END
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Transform_FactSales
-- Chuyen doi du lieu tu STG_SalesRaw sang FactSales.
-- @TenantID: Bat buoc — chi xu ly du lieu cua tenant duoc chi dinh.
-- @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. Ghi nhan ban ghi loi vao STG_ErrorLog neu khong tim thay Dimension.
--   2. INSERT ban ghi moi vao FactSales (neu chua ton tai InvoiceNumber + ProductKey).
--   3. Tinh toan: GrossSalesAmount, NetSalesAmount, CostAmount, GrossProfitAmount.
--   4. Ghi log ket qua vao ETL_RunLog.
-- ============================================================================
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

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsSkipped  INT = 0;
    DECLARE @RowsError    INT = 0;
    DECLARE @StartTime    DATETIME2 = GETDATE();

    -- BUOC 1: Ghi nhan ban ghi loi vao STG_ErrorLog
    -- Khong tim thay ProductKey trong DimProduct (IsCurrent=1)
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        RawData, BatchDate, LoadDatetime
    )
    SELECT
        @TenantID,
        'STG_SalesRaw',
        'DIMENSION_NOT_FOUND',
        'Product not found in DimProduct (IsCurrent=1)',
        CONCAT('MaSP=', s.MaSP, ', MaHoaDon=', s.MaHoaDon),
        @BatchDate,
        GETDATE()
    FROM STG_SalesRaw s
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayBan AS DATE) = @BatchDate
      AND NOT EXISTS (
          SELECT 1 FROM DimProduct p
          WHERE p.ProductCode = s.MaSP AND p.IsCurrent = 1
      );

    SET @RowsError = @@ROWCOUNT;

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
        ON st.StoreCode = s.MaCH
       AND st.TenantID = @TenantID
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

    -- BUOC 3: Dem so dong da skip (da ton tai)
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

    -- BUOC 4: Ghi log ket qua
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactSales',
        @BatchDate,
        'SUCCESS',
        @RowsInserted + @RowsSkipped + @RowsError,
        @RowsInserted,
        0,
        @RowsSkipped,
        @RowsError,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Transform_FactSales [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Inserted=' + CAST(@RowsInserted AS VARCHAR(10))
        + ', Skipped=' + CAST(@RowsSkipped AS VARCHAR(10))
        + ', Errors=' + CAST(@RowsError AS VARCHAR(10)) + '.';
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactSales';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Transform_FactInventory
-- Chuyen doi du lieu tu STG_InventoryRaw sang FactInventory.
-- @TenantID: Bat buoc — chi xu ly du lieu cua tenant duoc chi dinh.
-- @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. INSERT / UPDATE (upsert) ton kho theo (TenantID, DateKey, ProductKey, StoreKey).
--   2. Tinh ClosingQty = Opening + Received - Sold + Returned + Adjusted.
--   3. Tinh DaysOfStock = ClosingQty / AvgDailySales.
--   4. Xac dinh StockStatus: Normal / Low / Out of Stock / Overstock.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Transform_FactInventory')
BEGIN
    DROP PROCEDURE usp_Transform_FactInventory;
END
GO

CREATE PROCEDURE usp_Transform_FactInventory
    @TenantID  VARCHAR(20),
    @BatchDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @DateKey INT = CONVERT(INT, FORMAT(@BatchDate, 'yyyyMMdd'));
    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated  INT = 0;
    DECLARE @StartTime    DATETIME2 = GETDATE();

    -- INSERT vao FactInventory — dong moi hoac cap nhat dong da ton tai
    MERGE FactInventory AS target
    USING (
        SELECT
            @TenantID AS TenantID,
            @DateKey AS DateKey,
            p.ProductKey,
            st.StoreKey,
            ISNULL(s.TonDauNgay, 0) AS OpeningQty,
            ISNULL(s.NhapTrongNgay, 0) AS ReceivedQty,
            ISNULL(s.BanTrongNgay, 0) AS SoldQty,
            ISNULL(s.TraLaiNhap, 0) AS ReturnedQty,
            ISNULL(s.DieuChinh, 0) AS AdjustedQty,
            ISNULL(s.DonGiaVon, p.UnitCostPrice) AS UnitCostPrice,
            ISNULL(s.MucTonToiThieu, 0) AS ReorderLevel,
            ISNULL(s.LoaiChuyen, N'Daily Count') AS MovementType,
            GETDATE() AS LoadDatetime
        FROM STG_InventoryRaw s
        INNER JOIN DimProduct p ON p.ProductCode = s.MaSP AND p.IsCurrent = 1
        INNER JOIN DimStore st ON st.StoreCode = s.MaCH AND st.TenantID = @TenantID
        CROSS JOIN DimProduct p_cost
        WHERE s.TenantID = @TenantID
          AND CAST(s.NgayChot AS DATE) = @BatchDate
    ) AS source (
        TenantID, DateKey, ProductKey, StoreKey,
        OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
        UnitCostPrice, ReorderLevel, MovementType, LoadDatetime
    )
    ON target.TenantID = source.TenantID
       AND target.DateKey = source.DateKey
       AND target.ProductKey = source.ProductKey
       AND target.StoreKey = source.StoreKey
    WHEN MATCHED THEN
        UPDATE SET
            target.OpeningQty    = source.OpeningQty,
            target.ReceivedQty  = source.ReceivedQty,
            target.SoldQty       = source.SoldQty,
            target.ReturnedQty   = source.ReturnedQty,
            target.AdjustedQty   = source.AdjustedQty,
            target.UnitCostPrice = source.UnitCostPrice,
            target.ReorderLevel  = source.ReorderLevel,
            target.MovementType  = source.MovementType,
            target.LoadDatetime  = source.LoadDatetime
    WHEN NOT MATCHED THEN
        INSERT (
            TenantID, DateKey, ProductKey, StoreKey,
            OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
            UnitCostPrice, ReorderLevel, MovementType, LoadDatetime
        )
        VALUES (
            source.TenantID, source.DateKey, source.ProductKey, source.StoreKey,
            source.OpeningQty, source.ReceivedQty, source.SoldQty,
            source.ReturnedQty, source.AdjustedQty,
            source.UnitCostPrice, source.ReorderLevel, source.MovementType,
            source.LoadDatetime
        );

    SET @RowsInserted = @@ROWCOUNT;

    -- Cap nhat cac cot tinh toan: ClosingQty, Values, DaysOfStock, StockStatus
    UPDATE fi SET
        fi.ClosingQty    = fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty,
        fi.OpeningValue  = fi.OpeningQty * fi.UnitCostPrice,
        fi.ReceivedValue = fi.ReceivedQty * fi.UnitCostPrice,
        fi.SoldValue     = fi.SoldQty * fi.UnitCostPrice,
        fi.ClosingValue  = (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty) * fi.UnitCostPrice,
        fi.DaysOfStock   = CASE
                               WHEN fi.SoldQty > 0
                               THEN CAST((fi.OpeningQty + fi.ReceivedQty) * 1.0 / fi.SoldQty AS DECIMAL(10,2))
                               ELSE 999
                           END,
        fi.StockStatus   = CASE
                               WHEN fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty = 0
                               THEN N'Out of Stock'
                               WHEN (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty) <= fi.ReorderLevel
                               THEN N'Low'
                               WHEN (fi.OpeningQty + fi.ReceivedQty - fi.SoldQty + fi.ReturnedQty + fi.AdjustedQty)
                                    > (fi.ReorderLevel * 5)
                               THEN N'Overstock'
                               ELSE N'Normal'
                           END
    FROM FactInventory fi
    WHERE fi.TenantID = @TenantID
      AND fi.DateKey = @DateKey;

    SET @RowsUpdated = @@ROWCOUNT;

    -- Ghi log
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactInventory',
        @BatchDate,
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        @RowsUpdated,
        0,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Transform_FactInventory [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Inserted=' + CAST(@RowsInserted AS VARCHAR(10))
        + ', Updated=' + CAST(@RowsUpdated AS VARCHAR(10)) + '.';
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactInventory';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Transform_FactPurchase
-- Chuyen doi du lieu tu STG_PurchaseRaw sang FactPurchase.
-- @TenantID: Bat buoc — chi xu ly du lieu cua tenant duoc chi dinh.
-- @BatchDate: Ngay can xu ly (mac dinh = hom nay).
--
-- Logic:
--   1. INSERT dong moi vao FactPurchase.
--   2. Tinh TotalCost, NetCost.
--   3. Ghi log ket qua.
-- ============================================================================
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

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsSkipped  INT = 0;
    DECLARE @RowsError    INT = 0;
    DECLARE @StartTime    DATETIME2 = GETDATE();

    -- BUOC 1: Ghi nhan ban ghi loi
    INSERT INTO STG_ErrorLog (
        TenantID, SourceTable, ErrorType, ErrorMessage,
        RawData, BatchDate, LoadDatetime
    )
    SELECT
        @TenantID,
        'STG_PurchaseRaw',
        'DIMENSION_NOT_FOUND',
        'Product or Supplier not found',
        CONCAT('MaSP=', s.MaSP, ', MaNCC=', s.MaNCC),
        @BatchDate,
        GETDATE()
    FROM STG_PurchaseRaw s
    WHERE s.TenantID = @TenantID
      AND CAST(s.NgayNhap AS DATE) = @BatchDate
      AND (
          NOT EXISTS (SELECT 1 FROM DimProduct p WHERE p.ProductCode = s.MaSP AND p.IsCurrent = 1)
          OR NOT EXISTS (SELECT 1 FROM DimSupplier sup WHERE sup.SupplierCode = s.MaNCC)
      );

    SET @RowsError = @@ROWCOUNT;

    -- BUOC 2: INSERT vao FactPurchase
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

    -- Ghi log
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Transform_FactPurchase',
        @BatchDate,
        'SUCCESS',
        @RowsInserted + @RowsSkipped + @RowsError,
        @RowsInserted,
        0,
        @RowsSkipped,
        @RowsError,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Transform_FactPurchase [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Inserted=' + CAST(@RowsInserted AS VARCHAR(10))
        + ', Errors=' + CAST(@RowsError AS VARCHAR(10)) + '.';
END;
GO

PRINT 'Created stored procedure: usp_Transform_FactPurchase';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_ClearFactData
-- Xoa du lieu trong bang Fact theo tenant va ngay.
-- Dung de rerun ETL khi can reload du lieu.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_ClearFactData')
BEGIN
    DROP PROCEDURE usp_ClearFactData;
END
GO

CREATE PROCEDURE usp_ClearFactData
    @TenantID  VARCHAR(20),
    @BatchDate DATE = NULL,
    @FactTable VARCHAR(50) = NULL  -- NULL = xoa tat ca fact tables
AS
BEGIN
    SET NOCOUNT ON;

    IF @BatchDate IS NULL
        SET @BatchDate = CAST(GETDATE() AS DATE);

    DECLARE @DateKey INT = CONVERT(INT, FORMAT(@BatchDate, 'yyyyMMdd'));
    DECLARE @RowsDeleted INT = 0;

    IF @FactTable IS NULL OR @FactTable = 'FactSales'
    BEGIN
        DELETE FROM FactSales WHERE TenantID = @TenantID AND DateKey = @DateKey;
        SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
        PRINT 'Cleared FactSales: ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows.';
    END

    IF @FactTable IS NULL OR @FactTable = 'FactInventory'
    BEGIN
        DELETE FROM FactInventory WHERE TenantID = @TenantID AND DateKey = @DateKey;
        SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
        PRINT 'Cleared FactInventory: ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows.';
    END

    IF @FactTable IS NULL OR @FactTable = 'FactPurchase'
    BEGIN
        DELETE FROM FactPurchase WHERE TenantID = @TenantID AND DateKey = @DateKey;
        SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
        PRINT 'Cleared FactPurchase: ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows.';
    END

    PRINT 'usp_ClearFactData [' + @TenantID + '][' + CONVERT(VARCHAR(10), @BatchDate, 120)
        + ']: Total deleted=' + CAST(@RowsDeleted AS VARCHAR(10)) + '.';
END;
GO

PRINT 'Created stored procedure: usp_ClearFactData';
GO

-- ============================================================================
-- XAC MINH: Doc lai cau truc bang
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: FactSales Structure ===';
SELECT
    c.column_id AS ColID,
    c.name AS ColumnName,
    t.name + CASE WHEN t.name IN ('varchar','nvarchar') THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                  WHEN t.name IN ('decimal') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                  ELSE '' END AS DataType,
    CASE WHEN ic.column_id IS NOT NULL THEN 'PK' ELSE '' END AS IsPK,
    CASE WHEN i.name IS NOT NULL THEN 'INDEX' ELSE '' END AS HasIndex
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.indexes i ON i.object_id = c.object_id AND i.is_primary_key = 0
LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id
WHERE c.object_id = OBJECT_ID('FactSales')
ORDER BY c.column_id;

PRINT '';
PRINT '=== VERIFICATION: FactInventory Structure ===';
SELECT
    c.column_id AS ColID,
    c.name AS ColumnName,
    t.name + CASE WHEN t.name IN ('varchar','nvarchar') THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                  WHEN t.name IN ('decimal') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                  ELSE '' END AS DataType,
    CASE WHEN ic.column_id IS NOT NULL THEN 'PK' ELSE '' END AS IsPK
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.indexes i ON i.object_id = c.object_id AND i.is_primary_key = 0
LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id
WHERE c.object_id = OBJECT_ID('FactInventory')
ORDER BY c.column_id;

PRINT '';
PRINT '=== VERIFICATION: FactPurchase Structure ===';
SELECT
    c.column_id AS ColID,
    c.name AS ColumnName,
    t.name + CASE WHEN t.name IN ('varchar','nvarchar') THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                  WHEN t.name IN ('decimal') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                  ELSE '' END AS DataType,
    CASE WHEN ic.column_id IS NOT NULL THEN 'PK' ELSE '' END AS IsPK
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.indexes i ON i.object_id = c.object_id AND i.is_primary_key = 0
LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id
WHERE c.object_id = OBJECT_ID('FactPurchase')
ORDER BY c.column_id;

PRINT '';
PRINT '=== VERIFICATION: Indexes Summary ===';
SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    CASE WHEN i.is_primary_key = 1 THEN 'PK' ELSE '' END AS IsPK,
    CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE '' END AS IsUnique
FROM sys.indexes i
WHERE OBJECT_NAME(i.object_id) IN ('FactSales', 'FactInventory', 'FactPurchase')
ORDER BY OBJECT_NAME(i.object_id), i.index_id;

PRINT '';
PRINT '=== VERIFICATION: Stored Procedures ===';
SELECT name AS ProcedureName, create_date, modify_date
FROM sys.procedures
WHERE name IN (
    'usp_Transform_FactSales', 'usp_Transform_FactInventory',
    'usp_Transform_FactPurchase', 'usp_ClearFactData'
)
ORDER BY name;

PRINT '';
PRINT '=== PHASE 4 COMPLETED SUCCESSFULLY ===';
GO
