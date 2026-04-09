-- ============================================================================
-- PHASE 6 — Quick Verification Scripts
-- File: sql/schema/06_verify_phase6.sql
-- Purpose: Chay sau khi execute 06_create_datamart.sql de verify ket qua.
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 6 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: Kiem tra 5 bang DM_ ton tai
-- ================================================================
DECLARE @dm1 INT, @dm2 INT, @dm3 INT, @dm4 INT, @dm5 INT;
SELECT @dm1 = COUNT(*) FROM sys.tables WHERE name = 'DM_SalesSummary';
SELECT @dm2 = COUNT(*) FROM sys.tables WHERE name = 'DM_InventoryAlert';
SELECT @dm3 = COUNT(*) FROM sys.tables WHERE name = 'DM_CustomerRFM';
SELECT @dm4 = COUNT(*) FROM sys.tables WHERE name = 'DM_EmployeePerformance';
SELECT @dm5 = COUNT(*) FROM sys.tables WHERE name = 'DM_PurchaseSummary';

IF @dm1=1 AND @dm2=1 AND @dm3=1 AND @dm4=1 AND @dm5=1
    PRINT '[PASS] Tat ca 5 bang Data Mart da ton tai.';
ELSE
    PRINT '[FAIL] Data Mart thieu: DM_SalesSummary=' + CAST(@dm1 AS VARCHAR(1))
        + ', DM_InventoryAlert=' + CAST(@dm2 AS VARCHAR(1))
        + ', DM_CustomerRFM=' + CAST(@dm3 AS VARCHAR(1))
        + ', DM_EmployeePerformance=' + CAST(@dm4 AS VARCHAR(1))
        + ', DM_PurchaseSummary=' + CAST(@dm5 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- TEST 2: DM_SalesSummary — cau truc
-- ================================================================
DECLARE @dm1_cols INT;
SELECT @dm1_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary');

IF @dm1_cols >= 18
    PRINT '[PASS] DM_SalesSummary: Co ' + CAST(@dm1_cols AS VARCHAR(10)) + ' cot (>= 18 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_SalesSummary: Chi co %d cot (mong doi >= 18).', @dm1_cols);
GO

DECLARE @dm1_cols_ok INT = 0;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary') AND name = 'TotalRevenue') SET @dm1_cols_ok = @dm1_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary') AND name = 'TotalGrossProfit') SET @dm1_cols_ok = @dm1_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary') AND name = 'TotalOrders') SET @dm1_cols_ok = @dm1_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary') AND name = 'GrossMarginPct') SET @dm1_cols_ok = @dm1_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary') AND name = 'LastRefreshed') SET @dm1_cols_ok = @dm1_cols_ok + 1;

IF @dm1_cols_ok = 5
    PRINT '[PASS] DM_SalesSummary: Tat ca cot chinh deu ton tai.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_SalesSummary: Chi co %d / 5 cot chinh ton tai.', @dm1_cols_ok);
GO

-- ================================================================
-- TEST 3: DM_InventoryAlert — cau truc
-- ================================================================
DECLARE @dm2_cols INT;
SELECT @dm2_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert');

IF @dm2_cols >= 20
    PRINT '[PASS] DM_InventoryAlert: Co ' + CAST(@dm2_cols AS VARCHAR(10)) + ' cot (>= 20 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_InventoryAlert: Chi co %d cot (mong doi >= 20).', @dm2_cols);
GO

DECLARE @dm2_cols_ok INT = 0;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert') AND name = 'AlertLevel') SET @dm2_cols_ok = @dm2_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert') AND name = 'AlertMessage') SET @dm2_cols_ok = @dm2_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert') AND name = 'SuggestedOrderQty') SET @dm2_cols_ok = @dm2_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert') AND name = 'DaysOfStock') SET @dm2_cols_ok = @dm2_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert') AND name = 'CurrentQty') SET @dm2_cols_ok = @dm2_cols_ok + 1;

IF @dm2_cols_ok = 5
    PRINT '[PASS] DM_InventoryAlert: Tat ca cot chinh deu ton tai.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_InventoryAlert: Chi co %d / 5 cot chinh ton tai.', @dm2_cols_ok);
GO

-- ================================================================
-- TEST 4: DM_CustomerRFM — cau truc
-- ================================================================
DECLARE @dm3_cols INT;
SELECT @dm3_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM');

IF @dm3_cols >= 20
    PRINT '[PASS] DM_CustomerRFM: Co ' + CAST(@dm3_cols AS VARCHAR(10)) + ' cot (>= 20 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_CustomerRFM: Chi co %d cot (mong doi >= 20).', @dm3_cols);
GO

DECLARE @dm3_cols_ok INT = 0;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'RecencyScore') SET @dm3_cols_ok = @dm3_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'FrequencyScore') SET @dm3_cols_ok = @dm3_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'MonetaryScore') SET @dm3_cols_ok = @dm3_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'RFMScore') SET @dm3_cols_ok = @dm3_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'Segment') SET @dm3_cols_ok = @dm3_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM') AND name = 'SegmentDesc') SET @dm3_cols_ok = @dm3_cols_ok + 1;

IF @dm3_cols_ok >= 5
    PRINT '[PASS] DM_CustomerRFM: Tat ca cot RFM/Segment deu ton tai.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_CustomerRFM: Chi co %d / 5 cot chinh ton tai.', @dm3_cols_ok);
GO

-- ================================================================
-- TEST 5: DM_EmployeePerformance — cau truc
-- ================================================================
DECLARE @dm4_cols INT;
SELECT @dm4_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance');

IF @dm4_cols >= 15
    PRINT '[PASS] DM_EmployeePerformance: Co ' + CAST(@dm4_cols AS VARCHAR(10)) + ' cot (>= 15 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_EmployeePerformance: Chi co %d cot (mong doi >= 15).', @dm4_cols);
GO

DECLARE @dm4_cols_ok INT = 0;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND name = 'TotalRevenue') SET @dm4_cols_ok = @dm4_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND name = 'TotalOrders') SET @dm4_cols_ok = @dm4_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND name = 'GrossMarginPct') SET @dm4_cols_ok = @dm4_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND name = 'AvgOrderValue') SET @dm4_cols_ok = @dm4_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND name = 'TopProduct1Code') SET @dm4_cols_ok = @dm4_cols_ok + 1;

IF @dm4_cols_ok = 5
    PRINT '[PASS] DM_EmployeePerformance: Tat ca cot chinh deu ton tai.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_EmployeePerformance: Chi co %d / 5 cot chinh ton tai.', @dm4_cols_ok);
GO

-- ================================================================
-- TEST 6: DM_PurchaseSummary — cau truc
-- ================================================================
DECLARE @dm5_cols INT;
SELECT @dm5_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary');

IF @dm5_cols >= 18
    PRINT '[PASS] DM_PurchaseSummary: Co ' + CAST(@dm5_cols AS VARCHAR(10)) + ' cot (>= 18 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_PurchaseSummary: Chi co %d cot (mong doi >= 18).', @dm5_cols);
GO

DECLARE @dm5_cols_ok INT = 0;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary') AND name = 'TotalPurchaseCost') SET @dm5_cols_ok = @dm5_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary') AND name = 'FillRatePct') SET @dm5_cols_ok = @dm5_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary') AND name = 'TotalOrders') SET @dm5_cols_ok = @dm5_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary') AND name = 'TotalPendingPayment') SET @dm5_cols_ok = @dm5_cols_ok + 1;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary') AND name = 'TotalOverduePayment') SET @dm5_cols_ok = @dm5_cols_ok + 1;

IF @dm5_cols_ok = 5
    PRINT '[PASS] DM_PurchaseSummary: Tat ca cot chinh deu ton tai.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DM_PurchaseSummary: Chi co %d / 5 cot chinh ton tai.', @dm5_cols_ok);
GO

-- ================================================================
-- TEST 7: TenantID trong 5 bang DM_
-- ================================================================
DECLARE @dm_tenant_ok INT = 0;
DECLARE @tmp INT;

SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary')          AND name = 'TenantID' AND is_nullable = 0;
IF @tmp=1 SET @dm_tenant_ok=@dm_tenant_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert')        AND name = 'TenantID' AND is_nullable = 0;
IF @tmp=1 SET @dm_tenant_ok=@dm_tenant_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM')           AND name = 'TenantID' AND is_nullable = 0;
IF @tmp=1 SET @dm_tenant_ok=@dm_tenant_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance')   AND name = 'TenantID' AND is_nullable = 0;
IF @tmp=1 SET @dm_tenant_ok=@dm_tenant_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary')        AND name = 'TenantID' AND is_nullable = 0;
IF @tmp=1 SET @dm_tenant_ok=@dm_tenant_ok+1;

IF @dm_tenant_ok = 5
    PRINT '[PASS] TenantID: Tat ca 5 bang DM_ deu co TenantID NOT NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] TenantID: Chi %d / 5 bang DM_ co TenantID NOT NULL.', @dm_tenant_ok);
GO

-- ================================================================
-- TEST 8: LastRefreshed trong 5 bang DM_
-- ================================================================
DECLARE @dm_refresh_ok INT = 0;
DECLARE @tmp INT;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary')          AND name = 'LastRefreshed';
IF @tmp=1 SET @dm_refresh_ok=@dm_refresh_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert')        AND name = 'LastRefreshed';
IF @tmp=1 SET @dm_refresh_ok=@dm_refresh_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM')           AND name = 'LastRefreshed';
IF @tmp=1 SET @dm_refresh_ok=@dm_refresh_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance')   AND name = 'LastRefreshed';
IF @tmp=1 SET @dm_refresh_ok=@dm_refresh_ok+1;
SELECT @tmp = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary')        AND name = 'LastRefreshed';
IF @tmp=1 SET @dm_refresh_ok=@dm_refresh_ok+1;

IF @dm_refresh_ok = 5
    PRINT '[PASS] LastRefreshed: Tat ca 5 bang DM_ deu co cot ghi nhan lan refresh cuoi.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] LastRefreshed: Chi %d / 5 bang DM_ co cot LastRefreshed.', @dm_refresh_ok);
GO

-- ================================================================
-- TEST 9: Primary Keys
-- ================================================================
DECLARE @pk_ok INT = 0;
DECLARE @tmp INT;
SELECT @tmp = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('DM_SalesSummary')       AND is_primary_key = 1;
IF @tmp=1 SET @pk_ok=@pk_ok+1;
SELECT @tmp = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('DM_InventoryAlert')     AND is_primary_key = 1;
IF @tmp=1 SET @pk_ok=@pk_ok+1;
SELECT @tmp = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('DM_CustomerRFM')        AND is_primary_key = 1;
IF @tmp=1 SET @pk_ok=@pk_ok+1;
SELECT @tmp = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('DM_EmployeePerformance') AND is_primary_key = 1;
IF @tmp=1 SET @pk_ok=@pk_ok+1;
SELECT @tmp = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('DM_PurchaseSummary')    AND is_primary_key = 1;
IF @tmp=1 SET @pk_ok=@pk_ok+1;

IF @pk_ok = 5
    PRINT '[PASS] Primary Keys: Tat ca 5 bang DM_ deu co Primary Key.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Primary Keys: Chi %d / 5 bang DM_ co Primary Key.', @pk_ok);
GO

-- ================================================================
-- TEST 10: Stored Procedures
-- ================================================================
DECLARE @sp_ok INT = 0;
DECLARE @tmp INT;
SELECT @tmp = COUNT(*) FROM sys.procedures WHERE name = 'usp_Refresh_DM_SalesSummary';
IF @tmp=1 SET @sp_ok=@sp_ok+1;
SELECT @tmp = COUNT(*) FROM sys.procedures WHERE name = 'usp_Refresh_DM_InventoryAlert';
IF @tmp=1 SET @sp_ok=@sp_ok+1;
SELECT @tmp = COUNT(*) FROM sys.procedures WHERE name = 'usp_Refresh_DM_CustomerRFM';
IF @tmp=1 SET @sp_ok=@sp_ok+1;
SELECT @tmp = COUNT(*) FROM sys.procedures WHERE name = 'usp_Refresh_DM_EmployeePerformance';
IF @tmp=1 SET @sp_ok=@sp_ok+1;
SELECT @tmp = COUNT(*) FROM sys.procedures WHERE name = 'usp_Refresh_DM_PurchaseSummary';
IF @tmp=1 SET @sp_ok=@sp_ok+1;

IF @sp_ok = 5
    PRINT '[PASS] Stored Procedures: Tat ca 5 SP refresh DM_ da duoc tao.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Stored Procedures: Chi %d / 5 SP duoc tao.', @sp_ok);
GO

-- ================================================================
-- TEST 11: SP co @TenantID param
-- ================================================================
DECLARE @param_ok INT = 0;
DECLARE @tmp INT;
SELECT @param_ok = COUNT(DISTINCT OBJECT_NAME(p.object_id))
FROM sys.parameters p
INNER JOIN sys.procedures sp ON sp.object_id = p.object_id
WHERE sp.name LIKE 'usp_Refresh_DM_%'
  AND p.name = '@TenantID';

IF @param_ok >= 5
    PRINT '[PASS] SP @TenantID: Tat ca 5 SP deu co tham so @TenantID.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] SP @TenantID: Chi %d / 5 SP co tham so @TenantID.', @param_ok);
GO

-- ================================================================
-- INSPECT: Cau truc
-- ================================================================
PRINT '';
PRINT '=== Data Mart Tables — Column Counts ===';
SELECT t.name AS TableName, COUNT(c.column_id) AS TotalColumns
FROM sys.tables t
INNER JOIN sys.columns c ON c.object_id = t.object_id
WHERE t.name LIKE 'DM_%'
GROUP BY t.name
ORDER BY t.name;

PRINT '';
PRINT '=== Key Columns ===';
SELECT
    'DM_SalesSummary' AS TableName,
    SUM(CASE WHEN name IN ('TenantID','DateKey','StoreKey','CategoryName','TotalRevenue',
        'TotalGrossProfit','TotalOrders','GrossMarginPct','LastRefreshed') THEN 1 ELSE 0 END) AS KeyColumns
FROM sys.columns WHERE object_id = OBJECT_ID('DM_SalesSummary')
UNION ALL
SELECT
    'DM_InventoryAlert',
    SUM(CASE WHEN name IN ('TenantID','AlertLevel','AlertMessage','CurrentQty',
        'DaysOfStock','SuggestedOrderQty','LastRefreshed') THEN 1 ELSE 0 END)
FROM sys.columns WHERE object_id = OBJECT_ID('DM_InventoryAlert')
UNION ALL
SELECT
    'DM_CustomerRFM',
    SUM(CASE WHEN name IN ('TenantID','RecencyScore','FrequencyScore','MonetaryScore',
        'RFMScore','Segment','SegmentDesc','LastRefreshed') THEN 1 ELSE 0 END)
FROM sys.columns WHERE object_id = OBJECT_ID('DM_CustomerRFM')
UNION ALL
SELECT
    'DM_EmployeePerformance',
    SUM(CASE WHEN name IN ('TenantID','DateKey','EmployeeKey','TotalRevenue',
        'TotalOrders','GrossMarginPct','LastRefreshed') THEN 1 ELSE 0 END)
FROM sys.columns WHERE object_id = OBJECT_ID('DM_EmployeePerformance')
UNION ALL
SELECT
    'DM_PurchaseSummary',
    SUM(CASE WHEN name IN ('TenantID','DateKey','SupplierKey','TotalPurchaseCost',
        'FillRatePct','LastRefreshed') THEN 1 ELSE 0 END)
FROM sys.columns WHERE object_id = OBJECT_ID('DM_PurchaseSummary')
ORDER BY TableName;

PRINT '';
PRINT '=== Stored Procedure List ===';
SELECT name AS ProcedureName, OBJECT_DEFINITION(object_id) AS DefinitionSnippet
FROM sys.procedures
WHERE name LIKE 'usp_Refresh_DM_%'
ORDER BY name;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 6 VERIFICATION — END';
PRINT '========================================';
GO
