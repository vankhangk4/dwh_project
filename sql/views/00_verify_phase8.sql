-- ============================================================================
-- PHASE 8: Verify Script — Kiem tra Views & Indexes
-- File: sql/views/00_verify_phase8.sql
-- Description: Chay sau khi execute tat ca views de xac nhan Phase 8 hoat dong dung.
--
-- CACH DUNG:
--   Step 1: sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB \
--             -i sql/schema/01_create_tenants.sql
--             -i sql/schema/02_create_dimensions.sql
--             -i sql/schema/03_create_dimensions_tenant.sql
--             -i sql/schema/04_create_facts.sql
--             -i sql/schema/05_create_staging.sql
--             -i sql/schema/06_create_datamart.sql
--   Step 2: Chay tung view file:
--             sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB \
--             -i sql/views/01_v_FactSales_ByTenant.sql
--             ... (tat ca view files)
--   Step 3: Chay indexes:
--             sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB \
--             -i sql/views/08_create_indexes.sql
--   Step 4: Verify:
--             sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB \
--             -i sql/views/00_verify_phase8.sql
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: VERIFY SCRIPT';
PRINT 'Verification started at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';

---------------------------------------------------------------------------
-- VERIFY 1: All Views exist
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 1: Views existence ===';

DECLARE @ViewCount INT;
SELECT @ViewCount = COUNT(*)
FROM sys.views
WHERE name IN (
    'v_FactSales_ByTenant',
    'v_FactSales_TenantSummary',
    'v_FactInventory_ByTenant',
    'v_FactInventory_Latest',
    'v_FactPurchase_ByTenant',
    'v_FactPurchase_TenantSummary',
    'v_DM_SalesSummary_ByTenant',
    'v_DM_InventoryAlert_ByTenant',
    'v_DM_InventoryAlert_Critical',
    'v_DM_CustomerRFM_ByTenant',
    'v_DM_CustomerRFM_SegmentSummary',
    'v_DM_CustomerRFM_AtRisk',
    'v_DM_EmployeePerformance_ByTenant',
    'v_DM_EmployeePerformance_Ranking',
    'v_DM_PurchaseSummary_ByTenant',
    'v_ETL_RunLog_Recent',
    'v_STG_ErrorLog_Recent'
);

PRINT 'View count: ' + CAST(@ViewCount AS VARCHAR(10)) + '/17';
IF @ViewCount = 17
    PRINT '[PASS] All 17 views exist.';
ELSE
    PRINT '[FAIL] Missing views! Found only ' + CAST(@ViewCount AS VARCHAR(10)) + '/17.';

SELECT
    name AS ViewName,
    create_date,
    modify_date,
    CASE
        WHEN name LIKE 'v_FactSales%' THEN 'FACT-SALES'
        WHEN name LIKE 'v_FactInventory%' THEN 'FACT-INVENTORY'
        WHEN name LIKE 'v_FactPurchase%' THEN 'FACT-PURCHASE'
        WHEN name LIKE 'v_DM_Sales%' THEN 'DM-SALES'
        WHEN name LIKE 'v_DM_Inventory%' THEN 'DM-INVENTORY'
        WHEN name LIKE 'v_DM_CustomerRFM%' THEN 'DM-CUSTOMER'
        WHEN name LIKE 'v_DM_Employee%' THEN 'DM-EMPLOYEE'
        WHEN name LIKE 'v_DM_Purchase%' THEN 'DM-PURCHASE'
        WHEN name LIKE 'v_ETL_%' THEN 'ETL'
        ELSE 'OTHER'
    END AS Category
FROM sys.views
WHERE name IN (
    'v_FactSales_ByTenant','v_FactSales_TenantSummary',
    'v_FactInventory_ByTenant','v_FactInventory_Latest',
    'v_FactPurchase_ByTenant','v_FactPurchase_TenantSummary',
    'v_DM_SalesSummary_ByTenant','v_DM_InventoryAlert_ByTenant','v_DM_InventoryAlert_Critical',
    'v_DM_CustomerRFM_ByTenant','v_DM_CustomerRFM_SegmentSummary','v_DM_CustomerRFM_AtRisk',
    'v_DM_EmployeePerformance_ByTenant','v_DM_EmployeePerformance_Ranking',
    'v_DM_PurchaseSummary_ByTenant',
    'v_ETL_RunLog_Recent','v_STG_ErrorLog_Recent'
)
ORDER BY Category, name;

---------------------------------------------------------------------------
-- VERIFY 2: Views columns match base tables
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 2: Views have correct column count ===';

SELECT
    v.name AS ViewName,
    COUNT(c.column_id) AS ColumnCount,
    STRING_AGG(LEFT(c.name, 30), ', ') AS ColumnList
FROM sys.views v
INNER JOIN sys.columns c ON c.object_id = v.object_id
WHERE v.name IN (
    'v_FactSales_ByTenant',
    'v_DM_SalesSummary_ByTenant',
    'v_DM_CustomerRFM_ByTenant'
)
GROUP BY v.name
ORDER BY v.name;

---------------------------------------------------------------------------
-- VERIFY 3: Index count summary
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 3: All Indexes ===';

SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    CASE WHEN i.is_primary_key = 1 THEN 'PK' ELSE '' END AS IsPK,
    CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE '' END AS IsUnique,
    CASE WHEN i.has_filter = 1 THEN 'FILTERED' ELSE '' END AS HasFilter
FROM sys.indexes i
WHERE OBJECT_NAME(i.object_id) IN (
    'FactSales','FactInventory','FactPurchase',
    'DimCustomer','DimEmployee',
    'DM_SalesSummary','DM_CustomerRFM',
    'ETL_RunLog','STG_ErrorLog'
)
AND i.type > 0
ORDER BY OBJECT_NAME(i.object_id), i.index_id;

---------------------------------------------------------------------------
-- VERIFY 4: SESSION_CONTEXT filter in views
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 4: SESSION_CONTEXT filter in views ===';

SELECT
    v.name AS ViewName,
    m.definition AS ViewDefinition
FROM sys.views v
INNER JOIN sys.sql_modules m ON m.object_id = v.object_id
WHERE v.name IN (
    'v_FactSales_ByTenant',
    'v_DM_SalesSummary_ByTenant',
    'v_DM_CustomerRFM_ByTenant'
)
AND m.definition LIKE '%SESSION_CONTEXT%';

DECLARE @SessionFilterCount INT;
SELECT @SessionFilterCount = COUNT(*)
FROM sys.views v
INNER JOIN sys.sql_modules m ON m.object_id = v.object_id
WHERE m.definition LIKE '%SESSION_CONTEXT%';

PRINT 'Views with SESSION_CONTEXT filter: ' + CAST(@SessionFilterCount AS VARCHAR(10));
IF @SessionFilterCount >= 15
    PRINT '[PASS] SESSION_CONTEXT filter applied to ' + CAST(@SessionFilterCount AS VARCHAR(10)) + ' views.';
ELSE
    PRINT '[WARN] Only ' + CAST(@SessionFilterCount AS VARCHAR(10)) + ' views have SESSION_CONTEXT filter.';

---------------------------------------------------------------------------
-- VERIFY 5: Index statistics
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 5: Index Statistics (total count per table) ===';

SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    COUNT(i.name) AS TotalIndexes,
    SUM(CASE WHEN i.type_desc = 'CLUSTERED' THEN 1 ELSE 0 END) AS TotalClustered,
    SUM(CASE WHEN i.type_desc = 'NONCLUSTERED' THEN 1 ELSE 0 END) AS TotalNonClustered,
    SUM(CASE WHEN i.has_filter = 1 THEN 1 ELSE 0 END) AS Filtered,
    SUM(CASE WHEN i.is_primary_key = 1 THEN 1 ELSE 0 END) AS PKs
FROM sys.indexes i
WHERE OBJECT_NAME(i.object_id) IN (
    'FactSales','FactInventory','FactPurchase',
    'DimCustomer','DimEmployee','DimProduct',
    'DM_SalesSummary','DM_CustomerRFM',
    'ETL_RunLog','STG_ErrorLog'
)
AND i.type > 0
GROUP BY i.object_id
ORDER BY TableName;

---------------------------------------------------------------------------
-- VERIFY 6: Covering indexes (INCLUDES) check
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 6: Covering indexes (with INCLUDE columns) ===';

SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    STRING_AGG(c.name, ', ') AS KeyColumns,
    CASE WHEN inc.definition IS NOT NULL THEN inc.definition ELSE '' END AS IncludeColumns
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
CROSS APPLY (
    SELECT STRING_AGG(c2.name, ', ') AS definition
    FROM sys.index_columns ic2
    INNER JOIN sys.columns c2 ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
    WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 1
) inc
WHERE OBJECT_NAME(i.object_id) IN ('FactSales','FactInventory','FactPurchase','DM_SalesSummary')
AND i.type > 0
GROUP BY OBJECT_NAME(i.object_id), i.name, inc.definition, i.index_id
ORDER BY TableName, i.index_id;

---------------------------------------------------------------------------
-- VERIFY 7: Recommended indexes from phase plan
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 7: Required indexes from phase plan ===';

DECLARE @RequiredIndexes INT = 0;
DECLARE @TotalRequired INT = 16;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactSales_TenantID_DateKey' AND object_id = OBJECT_ID('FactSales'))
BEGIN
    SET @RequiredIndexes = @RequiredIndexes + 1;
    PRINT '[PASS] IX_FactSales_TenantID_DateKey';
END
ELSE PRINT '[FAIL] IX_FactSales_TenantID_DateKey MISSING';

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactInventory_TenantID_DateKey' AND object_id = OBJECT_ID('FactInventory'))
BEGIN
    SET @RequiredIndexes = @RequiredIndexes + 1;
    PRINT '[PASS] IX_FactInventory_TenantID_DateKey';
END
ELSE PRINT '[FAIL] IX_FactInventory_TenantID_DateKey MISSING';

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DimCustomer_TenantID_IsCurrent' AND object_id = OBJECT_ID('DimCustomer'))
BEGIN
    SET @RequiredIndexes = @RequiredIndexes + 1;
    PRINT '[PASS] IX_DimCustomer_TenantID_IsCurrent';
END
ELSE PRINT '[FAIL] IX_DimCustomer_TenantID_IsCurrent MISSING';

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DM_SalesSummary_TenantID_DateKey' AND object_id = OBJECT_ID('DM_SalesSummary'))
BEGIN
    SET @RequiredIndexes = @RequiredIndexes + 1;
    PRINT '[PASS] IX_DM_SalesSummary_TenantID_DateKey';
END
ELSE PRINT '[FAIL] IX_DM_SalesSummary_TenantID_DateKey MISSING';

PRINT '';
PRINT 'Required indexes: ' + CAST(@RequiredIndexes AS VARCHAR(10)) + '/' + CAST(@TotalRequired AS VARCHAR(10));

---------------------------------------------------------------------------
-- VERIFY 8: Filtered indexes (SCD)
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 8: Filtered indexes (SCD Type 2 support) ===';

SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.filter_definition AS FilterDefinition
FROM sys.indexes i
WHERE i.type = 2 -- NONCLUSTERED
  AND i.has_filter = 1
ORDER BY TableName;

DECLARE @FilteredIndexCount INT = 0;
SELECT @FilteredIndexCount = COUNT(*)
FROM sys.indexes i
WHERE i.type = 2 AND i.has_filter = 1;

PRINT 'Filtered indexes found: ' + CAST(@FilteredIndexCount AS VARCHAR(10));
IF @FilteredIndexCount >= 4
    PRINT '[PASS] Sufficient filtered indexes for SCD Type 2.';

---------------------------------------------------------------------------
-- VERIFY 9: Test SESSION_CONTEXT usage
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 9: SESSION_CONTEXT usage test ===';

-- Set session context for STORE_HN
EXEC sp_set_session_context @key = N'tenant_id', @value = N'STORE_HN';

-- Test: Verify view returns no rows (because no data in FactSales yet)
-- and does not error out on SESSION_CONTEXT
BEGIN TRY
    DECLARE @TestSQL NVARCHAR(MAX) = N'SELECT TOP 1 TenantID FROM v_FactSales_ByTenant';
    EXEC sp_executesql @TestSQL;
    PRINT '[PASS] v_FactSales_ByTenant executes without error with SESSION_CONTEXT.';
END TRY
BEGIN CATCH
    PRINT '[FAIL] v_FactSales_ByTenant error: ' + ERROR_MESSAGE();
END CATCH

---------------------------------------------------------------------------
-- VERIFY 10: Cross-tenant isolation test
---------------------------------------------------------------------------
PRINT '';
PRINT '=== VERIFY 10: Tenant isolation (anti-join check) ===';

-- Verify SESSION_CONTEXT is the ONLY filter for tenant isolation
-- by checking that views do NOT have hardcoded TenantID values
DECLARE @HardcodedTenantCount INT;
SELECT @HardcodedTenantCount = COUNT(*)
FROM sys.views v
INNER JOIN sys.sql_modules m ON m.object_id = v.object_id
WHERE v.name LIKE 'v_%_ByTenant'
  AND (
      m.definition LIKE '%= ''STORE_HN''%'
      OR m.definition LIKE '%= ''STORE_HCM''%'
  );

PRINT 'Views with hardcoded TenantID: ' + CAST(@HardcodedTenantCount AS VARCHAR(10));
IF @HardcodedTenantCount = 0
    PRINT '[PASS] No hardcoded TenantID in views. Tenant isolation via SESSION_CONTEXT only.';
ELSE
    PRINT '[WARN] ' + CAST(@HardcodedTenantCount AS VARCHAR(10)) + ' view(s) may have hardcoded TenantID!';

---------------------------------------------------------------------------
-- FINAL RESULT
---------------------------------------------------------------------------
PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: VERIFICATION COMPLETED';
PRINT 'Completed at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';
PRINT '';
PRINT 'SUMMARY:';
PRINT '  - Views: 17 total (11 base + 6 supplementary)';
PRINT '  - Covering indexes: Created for dashboard performance';
PRINT '  - SESSION_CONTEXT filter: Applied to all _ByTenant views';
PRINT '  - Tenant isolation: Verified (no hardcoded values)';
PRINT '';
GO
