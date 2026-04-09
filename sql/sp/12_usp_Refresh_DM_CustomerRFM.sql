-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DM_CustomerRFM (Tenant-Specific)
-- File: sql/sp/12_usp_Refresh_DM_CustomerRFM.sql
-- Description: Refresh phan tich RFM (Recency, Frequency, Monetary)
--              cho 1 tenant.
--              Tenant-Specific — chi xu ly du lieu cua tenant duoc chi dinh.
--
-- Logic:
--   1. DELETE RFM cu cua tenant.
--   2. Tinh RecencyDays, FrequencyOrders, MonetaryAmount cho tung KH.
--   3. Tinh RFM scores (1-5) bang PERCENT_RANK().
--   4. Gan Segment: Champions / Loyal / At Risk / Lost / New / Potential.
--   5. Tinh ChurnRiskScore.
--   6. Ghi log vao ETL_RunLog.
--
-- Dependencies: DimCustomer, FactSales, DM_CustomerRFM.
-- ============================================================================

SET NOCOUNT ON;
GO

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

    -- Validate TenantID
    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Refresh_DM_CustomerRFM: TenantID is required.';
        RETURN;
    END

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

    -- BUOC 2: Tinh RFM base values trong bang tam
    IF OBJECT_ID('tempdb..#RFM_Base') IS NOT NULL
        DROP TABLE #RFM_Base;

    SELECT
        c.CustomerKey,
        c.CustomerCode,
        c.FullName,
        ISNULL(c.City, N'Khác') AS City,
        ISNULL(c.CustomerType, N'Khách lẻ') AS CustomerType,
        ISNULL(c.LoyaltyTier, N'Bronze') AS LoyaltyTier,

        -- Recency: So ngay tu lan mua cuoi den hom nay
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
    ) MAXDate ON MAXDate.CustomerKey = c.CustomerKey

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

    -- BUOC 3: Tinh RFM scores bang PERCENT_RANK()
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
                THEN N'Khách hàng tốt nhất: mua thường xuyên, chi tiêu cao, mới mua gần đây.'
            WHEN RecencyScore >= 3 AND FrequencyScore >= 3
                THEN N'Khách hàng trung thành: mua đều đều, cần khuyến mãi giữ liên lạc.'
            WHEN RecencyScore >= 3 AND FrequencyScore <= 2
                THEN N'Khách hàng tiềm năng: mới tham gia, có tiềm năng trung thành.'
            WHEN RecencyScore >= 4 AND FrequencyScore <= 2
                THEN N'Khách hàng mới: mua gần đây, cần chương trình khuyến mãi thêm.'
            WHEN RecencyScore BETWEEN 2 AND 3 AND FrequencyScore >= 3
                THEN N'Khách hàng nguy cơ: đã lâu không mua, cần kích hoạt lại.'
            WHEN RecencyScore <= 2 AND FrequencyScore >= 3
                THEN N'Khách hàng cũ: đã từng mua nhiều, cần thu hút quay lại.'
            WHEN RecencyScore <= 2 AND FrequencyScore <= 2
                THEN N'Khách hàng mất: đã ngừng mua, có thể đã chuyển nơi khác.'
            ELSE N'Chưa phân loại được.'
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

    -- BUOC 5: Ghi log
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE());

    INSERT INTO ETL_RunLog (
        TenantID, StoredProcedureName, RunDate, Status,
        RowsProcessed, RowsInserted, RowsUpdated, RowsSkipped, RowsFailed,
        ErrorMessage, StartTime, EndTime, DurationSeconds
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
        GETDATE(),
        @Duration
    );

    PRINT 'usp_Refresh_DM_CustomerRFM [' + @TenantID + ']: Deleted '
        + CAST(@RowsDeleted AS VARCHAR(10)) + ' old rows, Inserted '
        + CAST(@RowsInserted AS VARCHAR(10)) + ' new rows.'
        + ' Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's.';

    -- Xac minh
    DECLARE @AvgRFM DECIMAL(5,2);
    DECLARE @Champions INT;
    SELECT
        @AvgRFM = AVG(CAST(RFMScore AS DECIMAL(5,2))),
        @Champions = SUM(CASE WHEN Segment = N'Champions' THEN 1 ELSE 0 END)
    FROM DM_CustomerRFM WHERE TenantID = @TenantID;
    PRINT '[VERIFY] DM_CustomerRFM [' + @TenantID + '] — Total: '
        + CAST(@RowsInserted AS VARCHAR(10)) + ', Avg RFMScore: '
        + CAST(@AvgRFM AS VARCHAR(10)) + ', Champions: '
        + CAST(@Champions AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Refresh_DM_CustomerRFM';
GO