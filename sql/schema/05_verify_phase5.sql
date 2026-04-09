-- ============================================================================
-- PHASE 5 — Quick Verification Scripts
-- File: sql/schema/05_verify_phase5.sql
-- Purpose: Chay sau khi execute 05_create_staging.sql de verify ket qua.
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 5 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: Kiem tra tat ca bang Staging ton tai
-- ================================================================
DECLARE @stg_count INT;
SELECT @stg_count = COUNT(*) FROM sys.tables
WHERE name LIKE 'STG_%'
   OR name IN ('ETL_Watermark', 'ETL_RunLog', 'STG_ErrorLog');

IF @stg_count >= 8
    PRINT '[PASS] Staging + ETL tables: Co ' + CAST(@stg_count AS VARCHAR(10)) + ' bang (>= 8 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Staging + ETL tables: Chi co %d bang (mong doi >= 8).', @stg_count);
GO

-- ================================================================
-- TEST 2: Kiem tra tung bang Staging
-- ================================================================
DECLARE @t1 INT, @t2 INT, @t3 INT, @t4 INT, @t5 INT, @t6 INT, @t7 INT, @t8 INT;
SELECT @t1 = COUNT(*) FROM sys.tables WHERE name = 'STG_SalesRaw';
SELECT @t2 = COUNT(*) FROM sys.tables WHERE name = 'STG_InventoryRaw';
SELECT @t3 = COUNT(*) FROM sys.tables WHERE name = 'STG_PurchaseRaw';
SELECT @t4 = COUNT(*) FROM sys.tables WHERE name = 'STG_ProductRaw';
SELECT @t5 = COUNT(*) FROM sys.tables WHERE name = 'STG_CustomerRaw';
SELECT @t6 = COUNT(*) FROM sys.tables WHERE name = 'STG_EmployeeRaw';
SELECT @t7 = COUNT(*) FROM sys.tables WHERE name = 'STG_StoreRaw';
SELECT @t8 = COUNT(*) FROM sys.tables WHERE name = 'STG_SupplierRaw';

IF @t1=1 AND @t2=1 AND @t3=1 AND @t4=1 AND @t5=1 AND @t6=1 AND @t7=1 AND @t8=1
    PRINT '[PASS] Tat ca 8 bang STG_ da ton tai.';
ELSE
    PRINT '[FAIL] Bang STG_ thieu:'
        + ' SalesRaw=' + CAST(@t1 AS VARCHAR(1))
        + ', InventoryRaw=' + CAST(@t2 AS VARCHAR(1))
        + ', PurchaseRaw=' + CAST(@t3 AS VARCHAR(1))
        + ', ProductRaw=' + CAST(@t4 AS VARCHAR(1))
        + ', CustomerRaw=' + CAST(@t5 AS VARCHAR(1))
        + ', EmployeeRaw=' + CAST(@t6 AS VARCHAR(1))
        + ', StoreRaw=' + CAST(@t7 AS VARCHAR(1))
        + ', SupplierRaw=' + CAST(@t8 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- TEST 3: ETL_Watermark ton tai
-- ================================================================
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETL_Watermark')
    PRINT '[PASS] ETL_Watermark: Bang da ton tai.';
ELSE
    PRINT '[FAIL] ETL_Watermark: Bang chua duoc tao.';
GO

DECLARE @wm_cols INT;
SELECT @wm_cols = COUNT(*) FROM sys.columns
WHERE object_id = OBJECT_ID('ETL_Watermark');
IF @wm_cols >= 10
    PRINT '[PASS] ETL_Watermark: Co ' + CAST(@wm_cols AS VARCHAR(10)) + ' cot (>= 10 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] ETL_Watermark: Chi co %d cot (mong doi >= 10).', @wm_cols);
GO

-- ================================================================
-- TEST 4: ETL_RunLog ton tai
-- ================================================================
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETL_RunLog')
    PRINT '[PASS] ETL_RunLog: Bang da ton tai.';
ELSE
    PRINT '[FAIL] ETL_RunLog: Bang chua duoc tao.';
GO

DECLARE @runlog_cols INT;
SELECT @runlog_cols = COUNT(*) FROM sys.columns
WHERE object_id = OBJECT_ID('ETL_RunLog');
IF @runlog_cols >= 15
    PRINT '[PASS] ETL_RunLog: Co ' + CAST(@runlog_cols AS VARCHAR(10)) + ' cot (>= 15 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] ETL_RunLog: Chi co %d cot (mong doi >= 15).', @runlog_cols);
GO

-- ================================================================
-- TEST 5: STG_ErrorLog ton tai
-- ================================================================
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'STG_ErrorLog')
    PRINT '[PASS] STG_ErrorLog: Bang da ton tai.';
ELSE
    PRINT '[FAIL] STG_ErrorLog: Bang chua duoc tao.';
GO

DECLARE @errlog_cols INT;
SELECT @errlog_cols = COUNT(*) FROM sys.columns
WHERE object_id = OBJECT_ID('STG_ErrorLog');
IF @errlog_cols >= 12
    PRINT '[PASS] STG_ErrorLog: Co ' + CAST(@errlog_cols AS VARCHAR(10)) + ' cot (>= 12 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] STG_ErrorLog: Chi co %d cot (mong doi >= 12).', @errlog_cols);
GO

-- ================================================================
-- TEST 6: TenantID trong bang Staging tenant-specific
-- ================================================================
DECLARE @sales_tenant INT, @inv_tenant INT, @pur_tenant INT,
        @cust_tenant INT, @emp_tenant INT, @store_tenant INT;
SELECT @sales_tenant  = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_SalesRaw')     AND name = 'TenantID' AND is_nullable = 0;
SELECT @inv_tenant    = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_InventoryRaw') AND name = 'TenantID' AND is_nullable = 0;
SELECT @pur_tenant    = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_PurchaseRaw')   AND name = 'TenantID' AND is_nullable = 0;
SELECT @cust_tenant  = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_CustomerRaw')   AND name = 'TenantID' AND is_nullable = 0;
SELECT @emp_tenant   = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_EmployeeRaw')   AND name = 'TenantID' AND is_nullable = 0;
SELECT @store_tenant = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('STG_StoreRaw')        AND name = 'TenantID' AND is_nullable = 0;

DECLARE @tenant_ok INT = 0;
IF @sales_tenant=1 SET @tenant_ok=@tenant_ok+1;
IF @inv_tenant=1   SET @tenant_ok=@tenant_ok+1;
IF @pur_tenant=1   SET @tenant_ok=@tenant_ok+1;
IF @cust_tenant=1 SET @tenant_ok=@tenant_ok+1;
IF @emp_tenant=1   SET @tenant_ok=@tenant_ok+1;
IF @store_tenant=1 SET @tenant_ok=@tenant_ok+1;

IF @tenant_ok = 6
    PRINT '[PASS] TenantID: Tat ca 6 bang tenant-specific deu co TenantID NOT NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] TenantID: Chi %d / 6 bang co TenantID NOT NULL.', @tenant_ok);
GO

-- ================================================================
-- TEST 7: STG_ProductRaw va STG_SupplierRaw KHONG co TenantID (Shared)
-- ================================================================
DECLARE @prod_has_tenant INT, @sup_has_tenant INT;
SELECT @prod_has_tenant = COUNT(*) FROM sys.columns
    WHERE object_id = OBJECT_ID('STG_ProductRaw') AND name = 'TenantID';
SELECT @sup_has_tenant = COUNT(*) FROM sys.columns
    WHERE object_id = OBJECT_ID('STG_SupplierRaw') AND name = 'TenantID';

IF @prod_has_tenant = 0
    PRINT '[PASS] STG_ProductRaw: KHONG co TenantID (dung — Shared).';
ELSE
    PRINT '[FAIL] STG_ProductRaw: CO TenantID (sai — phai la Shared).';

IF @sup_has_tenant = 0
    PRINT '[PASS] STG_SupplierRaw: KHONG co TenantID (dung — Shared).';
ELSE
    PRINT '[FAIL] STG_SupplierRaw: CO TenantID (sai — phai la Shared).';
GO

-- ================================================================
-- TEST 8: STG_LoadDatetime ton tai trong moi bang STG_
-- ================================================================
DECLARE @load_col_missing INT = 0;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_SalesRaw')      AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_InventoryRaw')  AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_PurchaseRaw')    AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_ProductRaw')     AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_CustomerRaw')    AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_EmployeeRaw')    AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_StoreRaw')        AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('STG_SupplierRaw')    AND name = 'STG_LoadDatetime') SET @load_col_missing = @load_col_missing + 1;

IF @load_col_missing = 0
    PRINT '[PASS] STG_LoadDatetime: Tat ca 8 bang STG_ deu co cot ghi nhan thoi gian nap.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] STG_LoadDatetime: %d / 8 bang thieu cot ghi nhan.', @load_col_missing);
GO

-- ================================================================
-- TEST 9: ETL_Watermark seed data
-- ================================================================
DECLARE @wm_rows INT, @wm_hn INT, @wm_hcm INT;
SELECT @wm_rows = COUNT(*) FROM ETL_Watermark;
SELECT @wm_hn  = COUNT(*) FROM ETL_Watermark WHERE TenantID = 'STORE_HN';
SELECT @wm_hcm = COUNT(*) FROM ETL_Watermark WHERE TenantID = 'STORE_HCM';

IF @wm_rows >= 6
    PRINT '[PASS] ETL_Watermark seed: Co ' + CAST(@wm_rows AS VARCHAR(10)) + ' ban ghi (>= 6 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] ETL_Watermark seed: Chi co %d ban ghi (mong doi >= 6).', @wm_rows);

IF @wm_hn = 3
    PRINT '[PASS] ETL_Watermark: STORE_HN co 3 nguon (Sales, Inventory, Purchase).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] ETL_Watermark: STORE_HN co %d nguon (mong doi 3).', @wm_hn);

IF @wm_hcm = 3
    PRINT '[PASS] ETL_Watermark: STORE_HCM co 3 nguon (Sales, Inventory, Purchase).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] ETL_Watermark: STORE_HCM co %d nguon (mong doi 3).', @wm_hcm);
GO

-- ================================================================
-- TEST 10: Stored Procedures ton tai
-- ================================================================
DECLARE @sp1 INT, @sp2 INT, @sp3 INT, @sp4 INT, @sp5 INT;
SELECT @sp1 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Truncate_StagingTables';
SELECT @sp2 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Update_Watermark';
SELECT @sp3 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Get_Last_Watermark';
SELECT @sp4 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Get_All_Active_Watermarks';
SELECT @sp5 = COUNT(*) FROM sys.procedures WHERE name = 'usp_ClearErrorLog';

IF @sp1=1 AND @sp2=1 AND @sp3=1 AND @sp4=1 AND @sp5=1
    PRINT '[PASS] Tat ca 5 Stored Procedures da duoc tao.';
ELSE
    PRINT '[FAIL] Stored Procedures: usp_Truncate=' + CAST(@sp1 AS VARCHAR(1))
        + ', usp_Update_Watermark=' + CAST(@sp2 AS VARCHAR(1))
        + ', usp_Get_Last_Watermark=' + CAST(@sp3 AS VARCHAR(1))
        + ', usp_Get_All_Active_Watermarks=' + CAST(@sp4 AS VARCHAR(1))
        + ', usp_ClearErrorLog=' + CAST(@sp5 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- TEST 11: Primary Keys ton tai
-- ================================================================
DECLARE @pk1 INT, @pk2 INT, @pk3 INT;
SELECT @pk1 = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('STG_SalesRaw')     AND is_primary_key = 1;
SELECT @pk2 = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('ETL_Watermark')   AND is_primary_key = 1;
SELECT @pk3 = COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('ETL_RunLog')       AND is_primary_key = 1;

IF @pk1=1 AND @pk2=1 AND @pk3=1
    PRINT '[PASS] Primary Keys: STG_SalesRaw, ETL_Watermark, ETL_RunLog deu co PK.';
ELSE
    PRINT '[INFO] Primary Keys: STG_SalesRaw=' + CAST(@pk1 AS VARCHAR(1))
        + ', ETL_Watermark=' + CAST(@pk2 AS VARCHAR(1))
        + ', ETL_RunLog=' + CAST(@pk3 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- INSPECT: Cau truc tat ca bang
-- ================================================================
PRINT '';
PRINT '=== All STG_ and ETL Tables ===';
SELECT t.name AS TableName,
       p.rows AS ApproxRows,
       COUNT(c.column_id) AS TotalColumns
FROM sys.tables t
INNER JOIN sys.columns c ON c.object_id = t.object_id
INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE t.name LIKE 'STG_%' OR t.name IN ('ETL_Watermark', 'ETL_RunLog', 'STG_ErrorLog')
GROUP BY t.name, p.rows
ORDER BY t.name;

PRINT '';
PRINT '=== TenantID Flags ===';
SELECT
    t.name AS TableName,
    CASE WHEN EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = t.object_id AND c.name = 'TenantID')
         THEN CASE WHEN EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = t.object_id AND c.name = 'TenantID' AND c.is_nullable = 0)
                   THEN 'CO TenantID (NOT NULL)' ELSE 'CO TenantID (NULL)' END
         ELSE 'KHONG CO TenantID (Shared)' END AS TenantIDStatus
FROM sys.tables t
WHERE t.name LIKE 'STG_%'
ORDER BY t.name;

PRINT '';
PRINT '=== ETL_Watermark — All Records ===';
SELECT SourceName, TenantID, SourceType,
       CONVERT(VARCHAR(30), WatermarkValue, 120) AS WatermarkValue,
       LastRunStatus,
       CONVERT(VARCHAR(30), LastRunDatetime, 120) AS LastRunDatetime
FROM ETL_Watermark
ORDER BY TenantID, SourceName;

PRINT '';
PRINT '=== Stored Procedure List ===';
SELECT name AS ProcedureName, create_date, modify_date
FROM sys.procedures
WHERE name IN (
    'usp_Truncate_StagingTables', 'usp_Update_Watermark',
    'usp_Get_Last_Watermark', 'usp_Get_All_Active_Watermarks',
    'usp_ClearErrorLog'
)
ORDER BY name;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 5 VERIFICATION — END';
PRINT '========================================';
GO
