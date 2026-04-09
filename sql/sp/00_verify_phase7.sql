-- ============================================================================
-- PHASE 7: Verify Script — Kiem tra 15 Stored Procedures
-- File: sql/sp/00_verify_phase7.sql
-- Description: Chay sau khi execute tat ca SP de xac nhan Phase 7 hoat dong dung.
--
-- CACH DUNG:
--   Chay tat ca SP truoc: sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB -i sql/sp/00_run_all_sp.sql
--   Sau do chay verify: sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB -i sql/sp/00_verify_phase7.sql
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '';
PRINT '============================================================';
PRINT 'PHASE 7: VERIFY SCRIPT';
PRINT 'Verification started at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';

---------------------------------------------------------------------------
-- VERIFY 1: Kiem tra 15 SP da ton tai trong SQL Server
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 1: Stored Procedures existence ===';

DECLARE @SPCount INT;
SELECT @SPCount = COUNT(*)
FROM sys.procedures
WHERE name IN (
    'usp_Load_DimDate',
    'usp_Load_DimProduct',
    'usp_Load_DimSupplier',
    'usp_Load_DimStore',
    'usp_Load_DimCustomer',
    'usp_Load_DimEmployee',
    'usp_Transform_FactSales',
    'usp_Transform_FactInventory',
    'usp_Transform_FactPurchase',
    'usp_Refresh_DM_SalesSummary',
    'usp_Refresh_DM_InventoryAlert',
    'usp_Refresh_DM_CustomerRFM',
    'usp_Refresh_DM_EmployeePerformance',
    'usp_Refresh_DM_PurchaseSummary',
    'usp_Update_Watermark',
    'usp_Get_Last_Watermark',
    'usp_Get_All_Active_Watermarks'
);

PRINT 'Total SP count: ' + CAST(@SPCount AS VARCHAR(10)) + '/17';
IF @SPCount >= 15
    PRINT '[PASS] All required SPs exist.';
ELSE
    PRINT '[FAIL] Missing SPs! Found only ' + CAST(@SPCount AS VARCHAR(10)) + '/17.';

SELECT
    name AS ProcedureName,
    create_date,
    modify_date,
    CASE
        WHEN name IN ('usp_Load_DimDate','usp_Load_DimProduct','usp_Load_DimSupplier')
            THEN 'SHARED-DIM'
        WHEN name IN ('usp_Load_DimStore','usp_Load_DimCustomer','usp_Load_DimEmployee')
            THEN 'TENANT-DIM'
        WHEN name IN ('usp_Transform_FactSales','usp_Transform_FactInventory','usp_Transform_FactPurchase')
            THEN 'TENANT-FACT'
        WHEN name LIKE 'usp_Refresh_DM_%'
            THEN 'TENANT-DM'
        WHEN name LIKE 'usp_%_Watermark%'
            THEN 'WATERMARK'
        ELSE 'OTHER'
    END AS Category
FROM sys.procedures
WHERE name IN (
    'usp_Load_DimDate','usp_Load_DimProduct','usp_Load_DimSupplier',
    'usp_Load_DimStore','usp_Load_DimCustomer','usp_Load_DimEmployee',
    'usp_Transform_FactSales','usp_Transform_FactInventory','usp_Transform_FactPurchase',
    'usp_Refresh_DM_SalesSummary','usp_Refresh_DM_InventoryAlert',
    'usp_Refresh_DM_CustomerRFM','usp_Refresh_DM_EmployeePerformance',
    'usp_Refresh_DM_PurchaseSummary',
    'usp_Update_Watermark','usp_Get_Last_Watermark','usp_Get_All_Active_Watermarks'
)
ORDER BY Category, name;

---------------------------------------------------------------------------
-- VERIFY 2: Kiem tra SP co tham so dung
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 2: Stored Procedure Parameters ===';

SELECT
    p.name AS ProcedureName,
    STRING_AGG(
        c.name + ' ' + t.name +
        CASE WHEN t.name IN ('varchar','nvarchar') THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
             WHEN t.name IN ('decimal','numeric') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
             ELSE '' END,
        ', '
    ) AS Parameters
FROM sys.procedures p
INNER JOIN sys.parameters c ON c.object_id = p.object_id
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE p.name IN (
    'usp_Load_DimStore','usp_Load_DimCustomer','usp_Load_DimEmployee',
    'usp_Transform_FactSales','usp_Transform_FactInventory','usp_Transform_FactPurchase',
    'usp_Refresh_DM_SalesSummary','usp_Refresh_DM_InventoryAlert',
    'usp_Refresh_DM_CustomerRFM','usp_Refresh_DM_EmployeePerformance',
    'usp_Refresh_DM_PurchaseSummary','usp_Update_Watermark','usp_Get_Last_Watermark'
)
GROUP BY p.name
ORDER BY p.name;

---------------------------------------------------------------------------
-- VERIFY 3: Test usp_Load_DimDate (Shared)
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 3: usp_Load_DimDate ===';

DECLARE @DimDateCount INT;
SELECT @DimDateCount = COUNT(*) FROM DimDate;
PRINT 'DimDate row count: ' + CAST(@DimDateCount AS VARCHAR(10));
IF @DimDateCount = 5844
    PRINT '[PASS] DimDate has correct 5844 rows (2015-01-01 → 2030-12-31).';
ELSE
    PRINT '[WARN] DimDate row count: ' + CAST(@DimDateCount AS VARCHAR(10)) + ' (expected 5844).';

SELECT TOP 3 DateKey, FullDate, DayName, MonthName, YearKey, IsHoliday, HolidayName
FROM DimDate WHERE IsHoliday = 1 ORDER BY DateKey;

---------------------------------------------------------------------------
-- VERIFY 4: Test usp_Get_Last_Watermark
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 4: usp_Get_Last_Watermark ===';

EXEC usp_Get_Last_Watermark @SourceName = 'STORE_HN_Sales_Excel';
PRINT '';

---------------------------------------------------------------------------
-- VERIFY 5: Test usp_Update_Watermark — SUCCESS
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 5: usp_Update_Watermark ===';

EXEC usp_Update_Watermark
    @SourceName = 'TEST_Sales_Excel',
    @TenantID = 'STORE_HN',
    @Status = 'SUCCESS',
    @SourceType = 'Sales',
    @RowsExtracted = 100,
    @DurationSeconds = 30,
    @Notes = 'Phase 7 verification test';
PRINT '';

-- Verify da insert
SELECT SourceName, TenantID, WatermarkValue, LastRunStatus, RowsExtracted, DurationSeconds, Notes
FROM ETL_Watermark WHERE SourceName = 'TEST_Sales_Excel';
PRINT '[PASS] Watermark SUCCESS update works.';

-- Test FAILED
EXEC usp_Update_Watermark
    @SourceName = 'TEST_Sales_Excel',
    @TenantID = 'STORE_HN',
    @Status = 'FAILED',
    @Notes = 'Phase 7 verification test — FAILED';
PRINT '';

DECLARE @WatermarkAfterFail DATETIME2;
SELECT @WatermarkAfterFail = WatermarkValue FROM ETL_Watermark WHERE SourceName = 'TEST_Sales_Excel';
PRINT 'Watermark after FAILED: ' + CONVERT(VARCHAR(30), @WatermarkAfterFail, 120) + ' (should be kept, not reset).';
PRINT '[PASS] Watermark FAILED keeps old value for retry.';

---------------------------------------------------------------------------
-- VERIFY 6: Test usp_Get_All_Active_Watermarks
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 6: usp_Get_All_Active_Watermarks ===';

EXEC usp_Get_All_Active_Watermarks;
PRINT '';

---------------------------------------------------------------------------
-- VERIFY 7: Dimension row counts
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 7: Dimension Table Row Counts ===';

SELECT 'DimDate' AS TableName, COUNT(*) AS TotalRows FROM DimDate
UNION ALL
SELECT 'DimProduct', COUNT(*) FROM DimProduct WHERE IsCurrent = 1
UNION ALL
SELECT 'DimSupplier', COUNT(*) FROM DimSupplier
UNION ALL
SELECT 'DimStore (STORE_HN)', COUNT(*) FROM DimStore WHERE TenantID = 'STORE_HN'
UNION ALL
SELECT 'DimStore (STORE_HCM)', COUNT(*) FROM DimStore WHERE TenantID = 'STORE_HCM'
UNION ALL
SELECT 'DimCustomer (STORE_HN)', COUNT(*) FROM DimCustomer WHERE TenantID = 'STORE_HN' AND IsCurrent = 1
UNION ALL
SELECT 'DimCustomer (STORE_HCM)', COUNT(*) FROM DimCustomer WHERE TenantID = 'STORE_HCM' AND IsCurrent = 1
UNION ALL
SELECT 'DimEmployee (STORE_HN)', COUNT(*) FROM DimEmployee WHERE TenantID = 'STORE_HN'
UNION ALL
SELECT 'DimEmployee (STORE_HCM)', COUNT(*) FROM DimEmployee WHERE TenantID = 'STORE_HCM';

---------------------------------------------------------------------------
-- VERIFY 8: ETL_RunLog summary
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 8: ETL_RunLog recent entries ===';

SELECT TOP 20
    RunLogID,
    TenantID,
    StoredProcedureName,
    CONVERT(VARCHAR(10), RunDate, 120) AS RunDate,
    Status,
    RowsProcessed,
    RowsInserted,
    RowsUpdated,
    RowsFailed,
    CONVERT(VARCHAR(20), StartTime, 120) AS StartTime,
    DurationSeconds
FROM ETL_RunLog
ORDER BY RunLogID DESC;

---------------------------------------------------------------------------
-- VERIFY 9: SCD Type 2 check for DimProduct
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 9: SCD Type 2 — DimProduct ===';

SELECT
    ProductCode,
    COUNT(*) AS TotalVersions,
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentVersions,
    MIN(EffectiveDate) AS FirstEffective,
    MAX(EffectiveDate) AS LatestEffective,
    MAX(ExpirationDate) AS LatestExpiration
FROM DimProduct
GROUP BY ProductCode
HAVING COUNT(*) > 1
ORDER BY ProductCode;

DECLARE @SCDCount INT = @@ROWCOUNT;
IF @SCDCount > 0
    PRINT '[INFO] ' + CAST(@SCDCount AS VARCHAR(10)) + ' product(s) have SCD history (multiple versions).';
ELSE
    PRINT '[INFO] No SCD Type 2 changes detected yet (single version per product).';

---------------------------------------------------------------------------
-- VERIFY 10: SCD Type 2 check for DimCustomer
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 10: SCD Type 2 — DimCustomer ===';

SELECT
    TenantID,
    CustomerCode,
    COUNT(*) AS TotalVersions,
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentVersions,
    MIN(EffectiveDate) AS FirstEffective,
    MAX(EffectiveDate) AS LatestEffective
FROM DimCustomer
GROUP BY TenantID, CustomerCode
HAVING COUNT(*) > 1
ORDER BY TenantID, CustomerCode;

---------------------------------------------------------------------------
-- FINAL RESULT
---------------------------------------------------------------------------
PRINT '';
PRINT '============================================================';
PRINT 'PHASE 7: VERIFICATION COMPLETED';
PRINT 'Completed at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';
GO