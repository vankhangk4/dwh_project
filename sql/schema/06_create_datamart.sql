-- ============================================================================
-- PHASE 6: Data Mart Layer
-- File: sql/schema/06_create_datamart.sql
-- Description: Tao cac bang Data Mart (DM_*) — bang tam tong hop (aggregate)
--              da duoc tinh toan san, toi uu cho truy van dashboard.
--              Superset doc tu DM_ thay vi truc tiep Fact de tang hieu nang.
--
-- NOTE:
--   - Tat ca bang DM_* DEU CO TenantID (tuong tu nhu Fact tables).
--   - Cac DM_* duoc refresh BOI SP sau moi chu ky ETL.
--   - Khong insert truc tiep vao DM_ — chi refresh qua SP.
--   - Phu thuoc: Chay SAU Phase 1, 2, 3, 4, 5
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG 1: DM_SalesSummary
-- Tam tong hop doanh thu theo ngay / cua hang / danh muc.
-- Duoc refresh sau moi lan chay ETL (hoac theo schedule).
-- Grain: 1 dong = Tong hop 1 ngay / 1 cua hang / 1 danh muc.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DM_SalesSummary')
BEGIN
    CREATE TABLE DM_SalesSummary (
        SummaryKey       BIGINT IDENTITY(1,1) NOT NULL,

        -- Khoa ngoai
        TenantID         VARCHAR(20)       NOT NULL,
        DateKey          INT               NOT NULL,  -- Den DimDate
        StoreKey         INT               NOT NULL,  -- Den DimStore (CO TenantID)

        -- Chi chinh
        ProductKey       INT                    NULL,  -- Den DimProduct (NULL = "Tat ca san pham")
        CategoryName     NVARCHAR(100)     NULL,      -- NULL = Tong hop theo danh muc
        BrandName        NVARCHAR(100)     NULL,

        -- Do luong tong hop
        TotalRevenue     DECIMAL(18,2)     NOT NULL DEFAULT 0,
        TotalGrossProfit DECIMAL(18,2)    NOT NULL DEFAULT 0,
        TotalCost        DECIMAL(18,2)    NOT NULL DEFAULT 0,
        TotalDiscount    DECIMAL(18,2)    NOT NULL DEFAULT 0,

        -- Don vi tinh
        TotalOrders      INT               NOT NULL DEFAULT 0,   -- So hoa don (distinct)
        TotalQty         INT               NOT NULL DEFAULT 0,   -- Tong so san pham ban
        TotalReturns     INT               NOT NULL DEFAULT 0,   -- So san pham tra lai

        -- Don vi tinh mo rong
        AvgOrderValue    DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- TotalRevenue / TotalOrders
        AvgQtyPerOrder   DECIMAL(10,2)   NOT NULL DEFAULT 0,   -- TotalQty / TotalOrders
        GrossMarginPct  DECIMAL(8,4)    NOT NULL DEFAULT 0,   -- GrossProfit / Revenue

        -- Thoi gian
        YearKey          INT               NOT NULL,
        QuarterKey       TINYINT          NOT NULL,
        MonthKey         INT               NOT NULL,
        MonthName        NVARCHAR(20)      NOT NULL,

        -- Ghi nhan
        LastRefreshed    DATETIME2         NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_DM_SalesSummary PRIMARY KEY CLUSTERED (SummaryKey),
        CONSTRAINT UQ_DM_SalesSummary_Composite UNIQUE (
            TenantID, DateKey, StoreKey, ProductKey, CategoryName
        )
    );

    PRINT 'Created table: DM_SalesSummary';
END
ELSE
BEGIN
    PRINT 'Table DM_SalesSummary already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_SalesSummary_TenantID_DateKey'
    AND object_id = OBJECT_ID('DM_SalesSummary')
)
BEGIN
    CREATE INDEX IX_DM_SalesSummary_TenantID_DateKey
        ON DM_SalesSummary(TenantID, DateKey DESC);
    PRINT 'Created index: IX_DM_SalesSummary_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_SalesSummary_TenantID_Category'
    AND object_id = OBJECT_ID('DM_SalesSummary')
)
BEGIN
    CREATE INDEX IX_DM_SalesSummary_TenantID_Category
        ON DM_SalesSummary(TenantID, CategoryName, DateKey DESC);
    PRINT 'Created index: IX_DM_SalesSummary_TenantID_Category';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_SalesSummary_TenantID_StoreKey'
    AND object_id = OBJECT_ID('DM_SalesSummary')
)
BEGIN
    CREATE INDEX IX_DM_SalesSummary_TenantID_StoreKey
        ON DM_SalesSummary(TenantID, StoreKey, DateKey DESC);
    PRINT 'Created index: IX_DM_SalesSummary_TenantID_StoreKey';
END
GO

-- ============================================================================
-- BANG 2: DM_InventoryAlert
-- Tam canh bao ton kho: nhung san pham can chu y (het hang, sac xuat, qua ton).
-- Duoc refresh moi chu ky ETL.
-- Grain: 1 dong = 1 san pham / 1 cua hang tai thoi diem hien tai (DateKey = MAX).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DM_InventoryAlert')
BEGIN
    CREATE TABLE DM_InventoryAlert (
        AlertKey         BIGINT IDENTITY(1,1) NOT NULL,

        TenantID         VARCHAR(20)       NOT NULL,
        DateKey          INT               NOT NULL,  -- Den DimDate (thuong = ngay hien tai)
        ProductKey       INT               NOT NULL,  -- Den DimProduct
        StoreKey         INT               NOT NULL,  -- Den DimStore

        -- Thong tin san pham
        ProductCode      VARCHAR(50)       NOT NULL,
        ProductName      NVARCHAR(200)    NOT NULL,
        CategoryName     NVARCHAR(100)    NOT NULL,
        BrandName        NVARCHAR(100)    NULL,

        -- So luong
        CurrentQty       INT               NOT NULL DEFAULT 0,
        OpeningQty       INT               NOT NULL DEFAULT 0,
        ReceivedQty      INT               NOT NULL DEFAULT 0,
        SoldQty          INT               NOT NULL DEFAULT 0,
        ReturnedQty       INT               NOT NULL DEFAULT 0,
        AdjustedQty       INT               NOT NULL DEFAULT 0,

        -- Gia tri
        ClosingValue     DECIMAL(18,2)    NOT NULL DEFAULT 0,

        -- Nguong
        ReorderLevel     INT               NOT NULL DEFAULT 0,
        MaxStockLevel    INT               NOT NULL DEFAULT 0,

        -- Canh bao
        DaysOfStock      DECIMAL(10,2)   NOT NULL DEFAULT 0,
        AlertLevel       NVARCHAR(20)      NOT NULL DEFAULT N'Normal',
        AlertMessage     NVARCHAR(500)    NULL,
        SuggestedOrderQty INT               NOT NULL DEFAULT 0,

        -- Khoang thoi gian
        DaysSinceLastSale INT              NOT NULL DEFAULT 0,

        -- Ghi nhan
        LastRefreshed    DATETIME2         NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_DM_InventoryAlert PRIMARY KEY CLUSTERED (AlertKey),
        CONSTRAINT UQ_DM_InventoryAlert_Composite UNIQUE (
            TenantID, DateKey, ProductKey, StoreKey
        )
    );

    PRINT 'Created table: DM_InventoryAlert';
END
ELSE
BEGIN
    PRINT 'Table DM_InventoryAlert already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_InventoryAlert_TenantID_AlertLevel'
    AND object_id = OBJECT_ID('DM_InventoryAlert')
)
BEGIN
    CREATE INDEX IX_DM_InventoryAlert_TenantID_AlertLevel
        ON DM_InventoryAlert(TenantID, AlertLevel)
        WHERE AlertLevel <> N'Normal';
    PRINT 'Created index: IX_DM_InventoryAlert_TenantID_AlertLevel';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_InventoryAlert_TenantID_ProductKey'
    AND object_id = OBJECT_ID('DM_InventoryAlert')
)
BEGIN
    CREATE INDEX IX_DM_InventoryAlert_TenantID_ProductKey
        ON DM_InventoryAlert(TenantID, ProductKey);
    PRINT 'Created index: IX_DM_InventoryAlert_TenantID_ProductKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_InventoryAlert_DaysSinceLastSale'
    AND object_id = OBJECT_ID('DM_InventoryAlert')
)
BEGIN
    CREATE INDEX IX_DM_InventoryAlert_DaysSinceLastSale
        ON DM_InventoryAlert(DaysSinceLastSale DESC);
    PRINT 'Created index: IX_DM_InventoryAlert_DaysSinceLastSale';
END
GO

-- ============================================================================
-- BANG 3: DM_CustomerRFM
-- Phan tich RFM (Recency, Frequency, Monetary) cho khach hang.
-- RFM giup phan khuc khach hang theo hanh vi mua sam.
-- Recency: So ngay tu lan mua cuoi den ngay hien tai (it hon = tot hon).
-- Frequency: Tong so hoa don da mua.
-- Monetary: Tong tien da chi tieu.
-- Grain: 1 dong = 1 khach hang / 1 tenant.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DM_CustomerRFM')
BEGIN
    CREATE TABLE DM_CustomerRFM (
        RFMKey           BIGINT IDENTITY(1,1) NOT NULL,

        TenantID         VARCHAR(20)       NOT NULL,
        CustomerKey     INT               NOT NULL,  -- Den DimCustomer (CO TenantID)

        -- Thong tin khach hang
        CustomerCode     VARCHAR(50)       NOT NULL,
        FullName        NVARCHAR(200)    NOT NULL,
        City            NVARCHAR(100)     NULL,
        CustomerType    NVARCHAR(50)      NULL,
        LoyaltyTier     NVARCHAR(50)      NULL,

        -- RFM Scores (1-5, 5 = tot nhat)
        RecencyScore    TINYINT          NOT NULL DEFAULT 0,   -- 1=It recent, 5=Rat recent
        FrequencyScore  TINYINT          NOT NULL DEFAULT 0,   -- 1=It lan, 5=Nhieu lan
        MonetaryScore  TINYINT          NOT NULL DEFAULT 0,   -- 1=It tien, 5=Nhieu tien

        -- RFM Values
        RecencyDays     INT               NOT NULL DEFAULT 0,   -- So ngay tu lan mua cuoi
        FrequencyOrders INT               NOT NULL DEFAULT 0,   -- Tong so hoa don
        MonetaryAmount  DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- Tong tien da chi

        -- Chi so tinh toan
        AvgOrderValue   DECIMAL(18,2)    NOT NULL DEFAULT 0,   -- MonetaryAmount / FrequencyOrders
        RFMScore        INT               NOT NULL DEFAULT 0,   -- RecencyScore + FrequencyScore + MonetaryScore
        RFMScoreGrade   NVARCHAR(5)       NULL,               -- 'AAA', 'AAB', 'CCC', v.v.

        -- Phan khuc
        Segment         NVARCHAR(50)      NOT NULL DEFAULT N'Unclassified',
        SegmentDesc     NVARCHAR(200)     NULL,

        -- Xu huong
        LastPurchaseDate DATE             NULL,
        FirstPurchaseDate DATE           NULL,
        CustomerLifetimeDays INT          NULL,                -- Ngay tu lan mua dau tien

        -- Lich su
        Last90DaysRevenue DECIMAL(18,2) NOT NULL DEFAULT 0,  -- Doanh thu 90 ngay gan nhat
        Last30DaysRevenue DECIMAL(18,2) NOT NULL DEFAULT 0,  -- Doanh thu 30 ngay gan nhat
        ChurnRiskScore   DECIMAL(5,2)  NOT NULL DEFAULT 0,   -- Diem nguy co roi

        -- Ghi nhan
        LastRefreshed    DATETIME2         NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_DM_CustomerRFM PRIMARY KEY CLUSTERED (RFMKey),
        CONSTRAINT UQ_DM_CustomerRFM_Composite UNIQUE (
            TenantID, CustomerKey
        )
    );

    PRINT 'Created table: DM_CustomerRFM';
END
ELSE
BEGIN
    PRINT 'Table DM_CustomerRFM already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_CustomerRFM_TenantID_Segment'
    AND object_id = OBJECT_ID('DM_CustomerRFM')
)
BEGIN
    CREATE INDEX IX_DM_CustomerRFM_TenantID_Segment
        ON DM_CustomerRFM(TenantID, Segment);
    PRINT 'Created index: IX_DM_CustomerRFM_TenantID_Segment';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_CustomerRFM_RFMScore'
    AND object_id = OBJECT_ID('DM_CustomerRFM')
)
BEGIN
    CREATE INDEX IX_DM_CustomerRFM_RFMScore
        ON DM_CustomerRFM(RFMScore DESC);
    PRINT 'Created index: IX_DM_CustomerRFM_RFMScore';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_CustomerRFM_TenantID_LoyaltyTier'
    AND object_id = OBJECT_ID('DM_CustomerRFM')
)
BEGIN
    CREATE INDEX IX_DM_CustomerRFM_TenantID_LoyaltyTier
        ON DM_CustomerRFM(TenantID, LoyaltyTier);
    PRINT 'Created index: IX_DM_CustomerRFM_TenantID_LoyaltyTier';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_CustomerRFM_ChurnRiskScore'
    AND object_id = OBJECT_ID('DM_CustomerRFM')
)
BEGIN
    CREATE INDEX IX_DM_CustomerRFM_ChurnRiskScore
        ON DM_CustomerRFM(ChurnRiskScore DESC);
    PRINT 'Created index: IX_DM_CustomerRFM_ChurnRiskScore';
END
GO

-- ============================================================================
-- BANG 4: DM_EmployeePerformance
-- Do hieu suat nhan vien ban hang theo ngay / thang.
-- Grain: 1 dong = Tong hop 1 nhan vien / 1 ngay.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DM_EmployeePerformance')
BEGIN
    CREATE TABLE DM_EmployeePerformance (
        PerfKey          BIGINT IDENTITY(1,1) NOT NULL,

        TenantID         VARCHAR(20)       NOT NULL,
        DateKey          INT               NOT NULL,
        EmployeeKey     INT               NOT NULL,

        -- Thong tin nhan vien
        EmployeeCode     VARCHAR(50)       NOT NULL,
        FullName        NVARCHAR(200)    NOT NULL,
        Position        NVARCHAR(100)     NULL,
        Department      NVARCHAR(100)    NULL,
        ShiftType       NVARCHAR(20)      NULL,

        -- Do luong ban hang
        TotalRevenue     DECIMAL(18,2)    NOT NULL DEFAULT 0,
        TotalGrossProfit DECIMAL(18,2)  NOT NULL DEFAULT 0,
        TotalOrders     INT               NOT NULL DEFAULT 0,
        TotalQtySold    INT               NOT NULL DEFAULT 0,
        TotalReturns    INT               NOT NULL DEFAULT 0,

        -- Don vi tinh
        AvgOrderValue   DECIMAL(18,2)    NOT NULL DEFAULT 0,
        ConversionRate  DECIMAL(8,4)    NOT NULL DEFAULT 0,   -- Don vi tiep nhan / Don vi mua
        GrossMarginPct  DECIMAL(8,4)    NOT NULL DEFAULT 0,

        -- Top san pham
        TopProduct1Code VARCHAR(50)       NULL,
        TopProduct1Name NVARCHAR(200)    NULL,
        TopProduct1Qty  INT               NOT NULL DEFAULT 0,

        -- Ghi nhan
        LastRefreshed    DATETIME2         NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_DM_EmployeePerformance PRIMARY KEY CLUSTERED (PerfKey),
        CONSTRAINT UQ_DM_EmployeePerformance_Composite UNIQUE (
            TenantID, DateKey, EmployeeKey
        )
    );

    PRINT 'Created table: DM_EmployeePerformance';
END
ELSE
BEGIN
    PRINT 'Table DM_EmployeePerformance already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_EmployeePerformance_TenantID_DateKey'
    AND object_id = OBJECT_ID('DM_EmployeePerformance')
)
BEGIN
    CREATE INDEX IX_DM_EmployeePerformance_TenantID_DateKey
        ON DM_EmployeePerformance(TenantID, DateKey DESC);
    PRINT 'Created index: IX_DM_EmployeePerformance_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_EmployeePerformance_TenantID_Revenue'
    AND object_id = OBJECT_ID('DM_EmployeePerformance')
)
BEGIN
    CREATE INDEX IX_DM_EmployeePerformance_TenantID_Revenue
        ON DM_EmployeePerformance(TenantID, TotalRevenue DESC);
    PRINT 'Created index: IX_DM_EmployeePerformance_TenantID_Revenue';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_EmployeePerformance_TenantID_EmployeeKey'
    AND object_id = OBJECT_ID('DM_EmployeePerformance')
)
BEGIN
    CREATE INDEX IX_DM_EmployeePerformance_TenantID_EmployeeKey
        ON DM_EmployeePerformance(TenantID, EmployeeKey, DateKey DESC);
    PRINT 'Created index: IX_DM_EmployeePerformance_TenantID_EmployeeKey';
END
GO

-- ============================================================================
-- BANG 5: DM_PurchaseSummary
-- Tam tong hop nhap hang theo ngay / nha cung cap / danh muc.
-- Grain: 1 dong = Tong hop 1 ngay / 1 nha cung cap / 1 danh muc.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DM_PurchaseSummary')
BEGIN
    CREATE TABLE DM_PurchaseSummary (
        SummaryKey       BIGINT IDENTITY(1,1) NOT NULL,

        TenantID         VARCHAR(20)       NOT NULL,
        DateKey          INT               NOT NULL,
        SupplierKey     INT               NOT NULL,
        StoreKey        INT               NOT NULL,

        -- Chi tinh
        CategoryName     NVARCHAR(100)    NULL,
        SupplierCode    VARCHAR(50)       NOT NULL,
        SupplierName    NVARCHAR(200)    NOT NULL,

        -- Do luong
        TotalPurchaseCost DECIMAL(18,2)  NOT NULL DEFAULT 0,
        TotalNetCost      DECIMAL(18,2)  NOT NULL DEFAULT 0,
        TotalDiscount     DECIMAL(18,2)   NOT NULL DEFAULT 0,
        TotalTax          DECIMAL(18,2)   NOT NULL DEFAULT 0,
        TotalOrders      INT              NOT NULL DEFAULT 0,
        TotalQty         INT              NOT NULL DEFAULT 0,
        TotalReceivedQty INT              NOT NULL DEFAULT 0,
        TotalRejectedQty INT              NOT NULL DEFAULT 0,

        -- Don vi tinh
        AvgUnitCost      DECIMAL(18,2)   NOT NULL DEFAULT 0,
        FillRatePct      DECIMAL(8,4)   NOT NULL DEFAULT 0,   -- ReceivedQty / TotalQty

        -- Thanh toan
        TotalPendingPayment DECIMAL(18,2) NOT NULL DEFAULT 0,
        TotalPaidPayment    DECIMAL(18,2) NOT NULL DEFAULT 0,
        TotalOverduePayment DECIMAL(18,2) NOT NULL DEFAULT 0,

        -- Thoi gian
        YearKey          INT               NOT NULL,
        QuarterKey       TINYINT          NOT NULL,
        MonthKey         INT               NOT NULL,

        -- Ghi nhan
        LastRefreshed    DATETIME2         NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_DM_PurchaseSummary PRIMARY KEY CLUSTERED (SummaryKey),
        CONSTRAINT UQ_DM_PurchaseSummary_Composite UNIQUE (
            TenantID, DateKey, SupplierKey, StoreKey, CategoryName
        )
    );

    PRINT 'Created table: DM_PurchaseSummary';
END
ELSE
BEGIN
    PRINT 'Table DM_PurchaseSummary already exists — skipping CREATE.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_PurchaseSummary_TenantID_DateKey'
    AND object_id = OBJECT_ID('DM_PurchaseSummary')
)
BEGIN
    CREATE INDEX IX_DM_PurchaseSummary_TenantID_DateKey
        ON DM_PurchaseSummary(TenantID, DateKey DESC);
    PRINT 'Created index: IX_DM_PurchaseSummary_TenantID_DateKey';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DM_PurchaseSummary_TenantID_SupplierKey'
    AND object_id = OBJECT_ID('DM_PurchaseSummary')
)
BEGIN
    CREATE INDEX IX_DM_PurchaseSummary_TenantID_SupplierKey
        ON DM_PurchaseSummary(TenantID, SupplierKey, DateKey DESC);
    PRINT 'Created index: IX_DM_PurchaseSummary_TenantID_SupplierKey';
END
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Refresh_DM_SalesSummary
-- Refresh tam tong hop doanh thu cho 1 tenant.
-- Buoc 1: DELETE toan bo du lieu cua tenant.
-- Buoc 2: INSERT tong hop tu FactSales + DimProduct + DimStore + DimDate.
-- Tinh toan: Revenue, Profit, Orders, Margin% theo ngay/cua hang/danh muc.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_SalesSummary')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_SalesSummary;
END
GO

CREATE PROCEDURE usp_Refresh_DM_SalesSummary
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa du lieu cu cua tenant
    DELETE FROM DM_SalesSummary WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert tong hop theo ngay / cua hang / danh muc
    INSERT INTO DM_SalesSummary (
        TenantID, DateKey, StoreKey,
        ProductKey, CategoryName, BrandName,
        TotalRevenue, TotalGrossProfit, TotalCost, TotalDiscount,
        TotalOrders, TotalQty, TotalReturns,
        AvgOrderValue, AvgQtyPerOrder, GrossMarginPct,
        YearKey, QuarterKey, MonthKey, MonthName,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.StoreKey,
        CAST(NULL AS INT) AS ProductKey,
        p.CategoryName,
        p.Brand,
        SUM(f.NetSalesAmount) AS TotalRevenue,
        SUM(f.GrossProfitAmount) AS TotalGrossProfit,
        SUM(f.CostAmount) AS TotalCost,
        SUM(f.DiscountAmount) AS TotalDiscount,
        COUNT(DISTINCT f.InvoiceNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQty,
        SUM(CASE WHEN f.ReturnFlag = 1 THEN f.Quantity ELSE 0 END) AS TotalReturns,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.NetSalesAmount) / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(18,2))
             ELSE 0 END AS AvgOrderValue,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.Quantity) * 1.0 / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(10,2))
             ELSE 0 END AS AvgQtyPerOrder,

        CASE WHEN SUM(f.NetSalesAmount) > 0
             THEN CAST(SUM(f.GrossProfitAmount) / SUM(f.NetSalesAmount) * 100 AS DECIMAL(8,4))
             ELSE 0 END AS GrossMarginPct,

        d.YearKey,
        d.QuarterKey,
        d.MonthKey,
        d.MonthName,

        GETDATE()
    FROM FactSales f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
    INNER JOIN DimDate d ON d.DateKey = f.DateKey
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.StoreKey,
        p.CategoryName, p.Brand,
        d.YearKey, d.QuarterKey, d.MonthKey, d.MonthName;

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Log
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_SalesSummary',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Refresh_DM_SalesSummary [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_SalesSummary';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Refresh_DM_InventoryAlert
-- Refresh canh bao ton kho cho 1 tenant.
-- Lay du lieu tu FactInventory ngan nhat (DateKey = MAX) cho tung san pham / cua hang.
-- Xac dinh AlertLevel: OutOfStock / Low / Normal / Overstock.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_InventoryAlert')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_InventoryAlert;
END
GO

CREATE PROCEDURE usp_Refresh_DM_InventoryAlert
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa canh bao cu cua tenant
    DELETE FROM DM_InventoryAlert WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Insert canh bao
    INSERT INTO DM_InventoryAlert (
        TenantID, DateKey, ProductKey, StoreKey,
        ProductCode, ProductName, CategoryName, BrandName,
        CurrentQty, OpeningQty, ReceivedQty, SoldQty, ReturnedQty, AdjustedQty,
        ClosingValue,
        ReorderLevel, MaxStockLevel,
        DaysOfStock, AlertLevel, AlertMessage, SuggestedOrderQty,
        DaysSinceLastSale,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.ProductKey,
        f.StoreKey,
        p.ProductCode,
        p.ProductName,
        p.CategoryName,
        p.Brand,
        f.ClosingQty,
        f.OpeningQty,
        f.ReceivedQty,
        f.SoldQty,
        f.ReturnedQty,
        f.AdjustedQty,
        f.ClosingValue,
        f.ReorderLevel,
        f.ReorderLevel * 5 AS MaxStockLevel,
        f.DaysOfStock,

        CASE
            WHEN f.ClosingQty = 0 THEN N'Out of Stock'
            WHEN f.ClosingQty <= f.ReorderLevel THEN N'Low'
            WHEN f.ClosingQty > f.ReorderLevel * 5 THEN N'Overstock'
            ELSE N'Normal'
        END AS AlertLevel,

        CASE
            WHEN f.ClosingQty = 0
                THEN N'San pham [' + p.ProductName + N'] da het hang tai cua hang. Can nhap ngay!'
            WHEN f.ClosingQty <= f.ReorderLevel
                THEN N'San pham [' + p.ProductName + N'] sac xuat hon muc toi thieu ('
                     + CAST(f.ReorderLevel AS NVARCHAR(10)) + N'). Can nhap them.'
            WHEN f.ClosingQty > f.ReorderLevel * 5
                THEN N'San pham [' + p.ProductName + N'] qua ton ('
                     + CAST(f.ClosingQty AS NVARCHAR(10)) + N' > '
                     + CAST(f.ReorderLevel * 5 AS NVARCHAR(10)) + N'). Can giam nhap.'
            ELSE NULL
        END AS AlertMessage,

        CASE
            WHEN f.ClosingQty <= f.ReorderLevel
                THEN CAST((f.ReorderLevel * 2 - f.ClosingQty) AS INT)
            ELSE 0
        END AS SuggestedOrderQty,

        CASE WHEN f.SoldQty > 0
             THEN 0
             ELSE DATEDIFF(DAY, (
                 SELECT MAX(f2.DateKey) FROM FactSales f2
                 WHERE f2.TenantID = f.TenantID
                   AND f2.ProductKey = f.ProductKey
                   AND f2.StoreKey = f.StoreKey
                   AND f2.ReturnFlag = 0
             ), f.DateKey)
        END AS DaysSinceLastSale,

        GETDATE()
    FROM FactInventory f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimStore st ON st.StoreKey = f.StoreKey
    WHERE f.TenantID = @TenantID
      AND f.DateKey = (
          SELECT MAX(fi.DateKey) FROM FactInventory fi
          WHERE fi.TenantID = f.TenantID
            AND fi.ProductKey = f.ProductKey
            AND fi.StoreKey = f.StoreKey
      );

    SET @RowsInserted = @@ROWCOUNT;

    -- BUOC 3: Log
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_InventoryAlert',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Refresh_DM_InventoryAlert [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_InventoryAlert';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Refresh_DM_CustomerRFM
-- Refresh phan tich RFM cho 1 tenant.
-- Tinh toan Recency, Frequency, Monetary scores (1-5) cho tung khach hang.
-- Phan khuc: Champions / Loyal / At Risk / Lost / New / Potential.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_CustomerRFM')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_CustomerRFM;
END
GO

CREATE PROCEDURE usp_Refresh_DM_CustomerRFM
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @Today DATE = CAST(GETDATE() AS DATE);
    DECLARE @TodayInt INT = CONVERT(INT, FORMAT(GETDATE(), 'yyyyMMdd'));
    DECLARE @TodayMinus90Int INT = CONVERT(INT, FORMAT(DATEADD(DAY, -90, GETDATE()), 'yyyyMMdd'));
    DECLARE @TodayMinus30Int INT = CONVERT(INT, FORMAT(DATEADD(DAY, -30, GETDATE()), 'yyyyMMdd'));
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- BUOC 1: Xoa RFM cu cua tenant
    DELETE FROM DM_CustomerRFM WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    -- BUOC 2: Tinh RFM base values (khong can partition)
    -- Bang tam chua gia tri RFM cua tung khach hang
    -- Su dung INT yyyyMMdd cho moi phep tinh ngay de tuong thich voi DateKey
    IF OBJECT_ID('tempdb..#RFM_Base') IS NOT NULL
        DROP TABLE #RFM_Base;

    SELECT
        c.CustomerKey,
        c.CustomerCode,
        c.FullName,
        ISNULL(c.City, N'Khác') AS City,
        ISNULL(c.CustomerType, N'Khách lẻ') AS CustomerType,
        ISNULL(c.LoyaltyTier, N'Bronze') AS LoyaltyTier,

        -- Recency: So ngay tu lan mua cuoi den hom nay (INT - INT = INT)
        @TodayInt - ISNULL(MAXDate.LastPurchaseDateInt, @TodayInt) AS RecencyDays,

        -- Frequency: Tong so hoa don
        ISNULL(FreqOrder.FreqOrders, 0) AS FrequencyOrders,

        -- Monetary: Tong tien da chi
        ISNULL(MonetaryData.MonetaryAmount, 0) AS MonetaryAmount,

        -- Ngay dau tien / cuoi cung mua (INT yyyyMMdd)
        ISNULL(MAXDate.LastPurchaseDateInt, @TodayInt) AS LastPurchaseDateInt,
        ISNULL(MinDate.FirstPurchaseDateInt, @TodayInt) AS FirstPurchaseDateInt,
        @TodayInt - ISNULL(MinDate.FirstPurchaseDateInt, @TodayInt) AS CustomerLifetimeDays,

        -- Doanh thu 90 / 30 ngay
        ISNULL(R90.Revenue90, 0) AS Last90DaysRevenue,
        ISNULL(R30.Revenue30, 0) AS Last30DaysRevenue

    INTO #RFM_Base
    FROM DimCustomer c
    LEFT JOIN (
        SELECT
            f.CustomerKey,
            MAX(f.DateKey) AS LastPurchaseDateInt
        FROM FactSales f
        WHERE f.TenantID = @TenantID AND f.CustomerKey > 0 AND f.ReturnFlag = 0
        GROUP BY f.CustomerKey
    ) MaxDate ON MaxDate.CustomerKey = c.CustomerKey

    LEFT JOIN (
        SELECT
            f.CustomerKey,
            MIN(f.DateKey) AS FirstPurchaseDateInt
        FROM FactSales f
        WHERE f.TenantID = @TenantID AND f.CustomerKey > 0 AND f.ReturnFlag = 0
        GROUP BY f.CustomerKey
    ) MinDate ON MinDate.CustomerKey = c.CustomerKey

    LEFT JOIN (
        SELECT
            f.CustomerKey,
            COUNT(DISTINCT f.InvoiceNumber) AS FreqOrders
        FROM FactSales f
        WHERE f.TenantID = @TenantID AND f.CustomerKey > 0 AND f.ReturnFlag = 0
        GROUP BY f.CustomerKey
    ) FreqOrder ON FreqOrder.CustomerKey = c.CustomerKey

    LEFT JOIN (
        SELECT
            f.CustomerKey,
            SUM(f.NetSalesAmount) AS MonetaryAmount
        FROM FactSales f
        WHERE f.TenantID = @TenantID AND f.CustomerKey > 0 AND f.ReturnFlag = 0
        GROUP BY f.CustomerKey
    ) MonetaryData ON MonetaryData.CustomerKey = c.CustomerKey

    LEFT JOIN (
        SELECT
            f.CustomerKey,
            SUM(f.NetSalesAmount) AS Revenue90
        FROM FactSales f
        WHERE f.TenantID = @TenantID
          AND f.CustomerKey > 0
          AND f.ReturnFlag = 0
          AND f.DateKey >= @TodayMinus90Int
        GROUP BY f.CustomerKey
    ) R90 ON R90.CustomerKey = c.CustomerKey

    LEFT JOIN (
        SELECT
            f.CustomerKey,
            SUM(f.NetSalesAmount) AS Revenue30
        FROM FactSales f
        WHERE f.TenantID = @TenantID
          AND f.CustomerKey > 0
          AND f.ReturnFlag = 0
          AND f.DateKey >= @TodayMinus30Int
        GROUP BY f.CustomerKey
    ) R30 ON R30.CustomerKey = c.CustomerKey

    WHERE c.TenantID = @TenantID AND c.IsCurrent = 1;

    -- BUOC 3: Tinh RFM scores bang PERCENT_RANK (5 groups)
    IF OBJECT_ID('tempdb..#RFM_Scored') IS NOT NULL
        DROP TABLE #RFM_Scored;

    SELECT
        TenantID, CustomerKey, CustomerCode, FullName, City, CustomerType, LoyaltyTier,
        RecencyDays, FrequencyOrders, MonetaryAmount,
        LastPurchaseDateInt, FirstPurchaseDateInt, CustomerLifetimeDays,
        Last90DaysRevenue, Last30DaysRevenue,

        -- Recency Score: It nhat = 5 diem (muon nhat), nhieu nhat = 1 diem
        CASE
            WHEN r.PercentRank_Recency <= 0.2 THEN 5
            WHEN r.PercentRank_Recency <= 0.4 THEN 4
            WHEN r.PercentRank_Recency <= 0.6 THEN 3
            WHEN r.PercentRank_Recency <= 0.8 THEN 2
            ELSE 1
        END AS RecencyScore,

        -- Frequency Score: Nhieu nhat = 5 diem
        CASE
            WHEN f.PercentRank_Freq <= 0.2 THEN 1
            WHEN f.PercentRank_Freq <= 0.4 THEN 2
            WHEN f.PercentRank_Freq <= 0.6 THEN 3
            WHEN f.PercentRank_Freq <= 0.8 THEN 4
            ELSE 5
        END AS FrequencyScore,

        -- Monetary Score: Nhieu tien nhat = 5 diem
        CASE
            WHEN m.PercentRank_Mon <= 0.2 THEN 1
            WHEN m.PercentRank_Mon <= 0.4 THEN 2
            WHEN m.PercentRank_Mon <= 0.6 THEN 3
            WHEN m.PercentRank_Mon <= 0.8 THEN 4
            ELSE 5
        END AS MonetaryScore

    INTO #RFM_Scored
    FROM #RFM_Base b
    CROSS APPLY (
        SELECT CAST(PERCENT_RANK() OVER (
            ORDER BY RecencyDays DESC) AS DECIMAL(10,4)) AS PercentRank_Recency
    ) r
    CROSS APPLY (
        SELECT CAST(PERCENT_RANK() OVER (
            ORDER BY FrequencyOrders ASC) AS DECIMAL(10,4)) AS PercentRank_Freq
    ) f
    CROSS APPLY (
        SELECT CAST(PERCENT_RANK() OVER (
            ORDER BY MonetaryAmount ASC) AS DECIMAL(10,4)) AS PercentRank_Mon
    ) m;

    -- BUOC 4: Insert vao DM_CustomerRFM
    -- Chuyen INT yyyyMMdd trong #RFM_Scored thanh DATE khi insert
    INSERT INTO DM_CustomerRFM (
        TenantID, CustomerKey,
        CustomerCode, FullName, City, CustomerType, LoyaltyTier,
        RecencyScore, FrequencyScore, MonetaryScore,
        RecencyDays, FrequencyOrders, MonetaryAmount,
        AvgOrderValue, RFMScore, RFMScoreGrade,
        Segment, SegmentDesc,
        LastPurchaseDate, FirstPurchaseDate, CustomerLifetimeDays,
        Last90DaysRevenue, Last30DaysRevenue,
        ChurnRiskScore,
        LastRefreshed
    )
    SELECT
        TenantID,
        CustomerKey,
        CustomerCode,
        FullName,
        City,
        CustomerType,
        LoyaltyTier,
        RecencyScore,
        FrequencyScore,
        MonetaryScore,
        RecencyDays,
        FrequencyOrders,
        MonetaryAmount,

        CASE WHEN FrequencyOrders > 0
             THEN CAST(MonetaryAmount / FrequencyOrders AS DECIMAL(18,2))
             ELSE CAST(0 AS DECIMAL(18,2)) END AS AvgOrderValue,

        RecencyScore + FrequencyScore + MonetaryScore AS RFMScore,
        CAST(RecencyScore AS NVARCHAR(1))
            + CAST(FrequencyScore AS NVARCHAR(1))
            + CAST(MonetaryScore AS NVARCHAR(1)) AS RFMScoreGrade,

        CASE
            WHEN RecencyScore >= 4 AND FrequencyScore >= 4 AND MonetaryScore >= 4
                THEN N'Champions'
            WHEN RecencyScore >= 3 AND FrequencyScore >= 3
                THEN N'Loyal Customers'
            WHEN RecencyScore >= 3 AND FrequencyScore <= 2
                THEN N'Potential Loyalists'
            WHEN RecencyScore >= 4 AND FrequencyScore <= 2
                THEN N'New Customers'
            WHEN RecencyScore BETWEEN 2 AND 3 AND FrequencyScore >= 3
                THEN N'At Risk'
            WHEN RecencyScore <= 2 AND FrequencyScore >= 3
                THEN N'Can''t Lose Them'
            WHEN RecencyScore <= 2 AND FrequencyScore <= 2
                THEN N'Lost Customers'
            ELSE N'Unclassified'
        END AS Segment,

        CASE
            WHEN RecencyScore >= 4 AND FrequencyScore >= 4 AND MonetaryScore >= 4
                THEN N'Khach hang tot nhat: mua thuong xuyen, chi tieu cao, moi mua gan day.'
            WHEN RecencyScore >= 3 AND FrequencyScore >= 3
                THEN N'Khach hang trung thanh: mua deu deu, can khuyen mai giu lien lac.'
            WHEN RecencyScore >= 3 AND FrequencyScore <= 2
                THEN N'Khach hang tieu bieu: moi tham gia, co tiềm nang trung thanh.'
            WHEN RecencyScore >= 4 AND FrequencyScore <= 2
                THEN N'Khach hang moi: mua gan day, can chuong trinh khuyen mai them.'
            WHEN RecencyScore BETWEEN 2 AND 3 AND FrequencyScore >= 3
                THEN N'Khach hang nguy co: da lau khong mua, can chuong trình kich hoat lai.'
            WHEN RecencyScore <= 2 AND FrequencyScore >= 3
                THEN N'Khach hang cu: da tung mua nhieu, can thu hut quay lai.'
            WHEN RecencyScore <= 2 AND FrequencyScore <= 2
                THEN N'Khach hang mat: da ngung mua, co the da chuyen sang noi khac.'
            ELSE N'Chua phan loai duoc.'
        END AS SegmentDesc,

        CONVERT(DATE, CAST(LastPurchaseDateInt AS VARCHAR(8))) AS LastPurchaseDate,
        CONVERT(DATE, CAST(FirstPurchaseDateInt AS VARCHAR(8))) AS FirstPurchaseDate,
        CustomerLifetimeDays,
        Last90DaysRevenue,
        Last30DaysRevenue,

        CASE
            WHEN RecencyDays > 180 THEN CAST(90.0 AS DECIMAL(5,2))
            WHEN RecencyDays > 90 THEN CAST(70.0 AS DECIMAL(5,2))
            WHEN RecencyDays > 60 THEN CAST(50.0 AS DECIMAL(5,2))
            WHEN RecencyDays > 30 THEN CAST(30.0 AS DECIMAL(5,2))
            WHEN RecencyDays > 14 THEN CAST(15.0 AS DECIMAL(5,2))
            ELSE CAST(0.0 AS DECIMAL(5,2))
        END AS ChurnRiskScore,

        GETDATE()
    FROM #RFM_Scored;

    SET @RowsInserted = @@ROWCOUNT;

    -- Cleanup
    DROP TABLE #RFM_Base;
    DROP TABLE #RFM_Scored;

    -- BUOC 5: Log
    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_CustomerRFM',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Refresh_DM_CustomerRFM [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_CustomerRFM';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Refresh_DM_EmployeePerformance
-- Refresh do hieu suat nhan vien ban hang cho 1 tenant.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_EmployeePerformance')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_EmployeePerformance;
END
GO

CREATE PROCEDURE usp_Refresh_DM_EmployeePerformance
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    DELETE FROM DM_EmployeePerformance WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    INSERT INTO DM_EmployeePerformance (
        TenantID, DateKey, EmployeeKey,
        EmployeeCode, FullName, Position, Department, ShiftType,
        TotalRevenue, TotalGrossProfit, TotalOrders, TotalQtySold, TotalReturns,
        AvgOrderValue, ConversionRate, GrossMarginPct,
        TopProduct1Code, TopProduct1Name, TopProduct1Qty,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.EmployeeKey,
        e.EmployeeCode,
        e.FullName,
        e.Position,
        e.Department,
        e.ShiftType,

        SUM(f.NetSalesAmount) AS TotalRevenue,
        SUM(f.GrossProfitAmount) AS TotalGrossProfit,
        COUNT(DISTINCT f.InvoiceNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQtySold,
        SUM(CASE WHEN f.ReturnFlag = 1 THEN f.Quantity ELSE 0 END) AS TotalReturns,

        CASE WHEN COUNT(DISTINCT f.InvoiceNumber) > 0
             THEN CAST(SUM(f.NetSalesAmount) / COUNT(DISTINCT f.InvoiceNumber) AS DECIMAL(18,2))
             ELSE 0 END AS AvgOrderValue,

        CAST(100.0 AS DECIMAL(8,4)) AS ConversionRate,  -- Placeholder

        CASE WHEN SUM(f.NetSalesAmount) > 0
             THEN CAST(SUM(f.GrossProfitAmount) / SUM(f.NetSalesAmount) * 100 AS DECIMAL(8,4))
             ELSE 0 END AS GrossMarginPct,

        TopP.TopProduct1Code,
        TopP.TopProduct1Name,
        TopP.TopProduct1Qty,

        GETDATE()
    FROM FactSales f
    INNER JOIN DimEmployee e ON e.EmployeeKey = f.EmployeeKey
        AND e.TenantID = f.TenantID AND e.IsActive = 1
    LEFT JOIN (
        SELECT
            f2.TenantID,
            f2.EmployeeKey,
            f2.DateKey,
            p_top.ProductCode AS TopProduct1Code,
            p_top.ProductName AS TopProduct1Name,
            SUM(f2.Quantity) AS TopProduct1Qty,
            ROW_NUMBER() OVER (
                PARTITION BY f2.TenantID, f2.EmployeeKey, f2.DateKey
                ORDER BY SUM(f2.Quantity) DESC
            ) AS rn
        FROM FactSales f2
        INNER JOIN DimProduct p_top ON p_top.ProductKey = f2.ProductKey AND p_top.IsCurrent = 1
        WHERE f2.TenantID = @TenantID AND f2.EmployeeKey > 0 AND f2.ReturnFlag = 0
        GROUP BY f2.TenantID, f2.EmployeeKey, f2.DateKey,
                 p_top.ProductCode, p_top.ProductName
    ) TopP ON TopP.TenantID = f.TenantID
          AND TopP.EmployeeKey = f.EmployeeKey
          AND TopP.DateKey = f.DateKey
          AND TopP.rn = 1
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.EmployeeKey,
        e.EmployeeCode, e.FullName, e.Position, e.Department, e.ShiftType,
        TopP.TopProduct1Code, TopP.TopProduct1Name, TopP.TopProduct1Qty;

    SET @RowsInserted = @@ROWCOUNT;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_EmployeePerformance',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Refresh_DM_EmployeePerformance [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_EmployeePerformance';
GO

-- ============================================================================
-- STORED PROCEDURE: usp_Refresh_DM_PurchaseSummary
-- Refresh tam tong hop nhap hang cho 1 tenant.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Refresh_DM_PurchaseSummary')
BEGIN
    DROP PROCEDURE usp_Refresh_DM_PurchaseSummary;
END
GO

CREATE PROCEDURE usp_Refresh_DM_PurchaseSummary
    @TenantID VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsDeleted INT = 0;
    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    DELETE FROM DM_PurchaseSummary WHERE TenantID = @TenantID;
    SET @RowsDeleted = @@ROWCOUNT;

    INSERT INTO DM_PurchaseSummary (
        TenantID, DateKey, SupplierKey, StoreKey,
        CategoryName, SupplierCode, SupplierName,
        TotalPurchaseCost, TotalNetCost, TotalDiscount, TotalTax,
        TotalOrders, TotalQty, TotalReceivedQty, TotalRejectedQty,
        AvgUnitCost, FillRatePct,
        TotalPendingPayment, TotalPaidPayment, TotalOverduePayment,
        YearKey, QuarterKey, MonthKey,
        LastRefreshed
    )
    SELECT
        f.TenantID,
        f.DateKey,
        f.SupplierKey,
        f.StoreKey,
        p.CategoryName,
        sup.SupplierCode,
        sup.SupplierName,
        SUM(f.TotalCost) AS TotalPurchaseCost,
        SUM(f.NetCost) AS TotalNetCost,
        SUM(f.DiscountAmount) AS TotalDiscount,
        SUM(f.TaxAmount) AS TotalTax,
        COUNT(DISTINCT f.PurchaseOrderNumber) AS TotalOrders,
        SUM(f.Quantity) AS TotalQty,
        SUM(f.ReceivedQty) AS TotalReceivedQty,
        SUM(f.Quantity - f.ReceivedQty) AS TotalRejectedQty,

        CASE WHEN SUM(f.Quantity) > 0
             THEN CAST(SUM(f.NetCost) / SUM(f.Quantity) AS DECIMAL(18,2))
             ELSE 0 END AS AvgUnitCost,

        CASE WHEN SUM(f.Quantity) > 0
             THEN CAST(SUM(f.ReceivedQty) * 100.0 / SUM(f.Quantity) AS DECIMAL(8,4))
             ELSE 0 END AS FillRatePct,

        SUM(CASE WHEN f.PaymentStatus = N'Pending' THEN f.NetCost ELSE 0 END) AS TotalPendingPayment,
        SUM(CASE WHEN f.PaymentStatus = N'Paid' THEN f.NetCost ELSE 0 END) AS TotalPaidPayment,
        SUM(CASE WHEN f.PaymentStatus = N'Overdue' THEN f.NetCost ELSE 0 END) AS TotalOverduePayment,

        CONVERT(INT, LEFT(CONVERT(VARCHAR(8), CAST(f.DateKey AS VARCHAR(8))), 4)) AS YearKey,
        CONVERT(TINYINT, SUBSTRING(CONVERT(VARCHAR(8), CAST(f.DateKey AS VARCHAR(8))), 5, 1)) AS QuarterKey,
        CONVERT(INT, LEFT(CONVERT(VARCHAR(6), CAST(f.DateKey AS VARCHAR(8))), 6)) AS MonthKey,

        GETDATE()
    FROM FactPurchase f
    INNER JOIN DimProduct p ON p.ProductKey = f.ProductKey AND p.IsCurrent = 1
    INNER JOIN DimSupplier sup ON sup.SupplierKey = f.SupplierKey
    WHERE f.TenantID = @TenantID
    GROUP BY
        f.TenantID, f.DateKey, f.SupplierKey, f.StoreKey,
        p.CategoryName, sup.SupplierCode, sup.SupplierName;

    SET @RowsInserted = @@ROWCOUNT;

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime
    )
    VALUES (
        @TenantID,
        'usp_Refresh_DM_PurchaseSummary',
        CAST(GETDATE() AS DATE),
        'SUCCESS',
        @RowsInserted,
        @RowsInserted,
        0,
        @RowsDeleted,
        0,
        NULL,
        @StartTime,
        GETDATE()
    );

    PRINT 'usp_Refresh_DM_PurchaseSummary [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.';
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_PurchaseSummary';
GO

-- ============================================================================
-- XAC MINH: Doc lai cau truc
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: Data Mart Tables ===';
SELECT
    t.name AS TableName,
    p.rows AS ApproxRows,
    COUNT(c.column_id) AS TotalColumns
FROM sys.tables t
INNER JOIN sys.columns c ON c.object_id = t.object_id
INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE t.name LIKE 'DM_%'
GROUP BY t.name, p.rows
ORDER BY t.name;

PRINT '';
PRINT '=== VERIFICATION: Data Mart Columns ===';
SELECT t.name AS TableName, c.name AS ColumnName, c.column_id AS ColID,
       t2.name + CASE WHEN t2.name IN ('varchar','nvarchar')
                       THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                       WHEN t2.name IN ('decimal','numeric')
                       THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                       ELSE '' END AS DataType
FROM sys.columns c
INNER JOIN sys.tables t ON t.object_id = c.object_id
INNER JOIN sys.types t2 ON t2.user_type_id = c.user_type_id
WHERE t.name LIKE 'DM_%'
ORDER BY t.name, c.column_id;

PRINT '';
PRINT '=== VERIFICATION: Unique Constraints ===';
SELECT OBJECT_NAME(kc.parent_object_id) AS TableName, kc.name AS ConstraintName, kc.type_desc
FROM sys.key_constraints kc
WHERE OBJECT_NAME(kc.parent_object_id) LIKE 'DM_%'
ORDER BY OBJECT_NAME(kc.parent_object_id);

PRINT '';
PRINT '=== VERIFICATION: Stored Procedures ===';
SELECT name AS ProcedureName, create_date
FROM sys.procedures
WHERE name LIKE 'usp_Refresh_DM_%'
ORDER BY name;

PRINT '';
PRINT '=== PHASE 6 COMPLETED SUCCESSFULLY ===';
GO
