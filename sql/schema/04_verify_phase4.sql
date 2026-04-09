-- ============================================================================
-- PHASE 4 — Quick Verification Scripts
-- File: sql/schema/04_verify_phase4.sql
-- Purpose: Chay sau khi execute 04_create_facts.sql de verify ket qua.
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 4 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: FactSales — ton tai va cau truc dung
-- ================================================================
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'FactSales')
    PRINT '[PASS] FactSales: Bang da ton tai.';
ELSE
    PRINT '[FAIL] FactSales: Bang chua duoc tao.';
GO

DECLARE @fact1_cols INT;
SELECT @fact1_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('FactSales');
IF @fact1_cols >= 25
    PRINT '[PASS] FactSales: Co ' + CAST(@fact1_cols AS VARCHAR(10)) + ' cot (>= 25 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactSales: Chi co %d cot (mong doi >= 25).', @fact1_cols);
GO

DECLARE @fact1_pk INT;
SELECT @fact1_pk = COUNT(*) FROM sys.indexes
WHERE object_id = OBJECT_ID('FactSales') AND is_primary_key = 1;
IF @fact1_pk = 1
    PRINT '[PASS] FactSales: Co 1 Primary Key.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactSales: Co %d Primary Key (mong doi 1).', @fact1_pk);
GO

-- ================================================================
-- TEST 2: FactSales — TenantID bat buoc (NOT NULL)
-- ================================================================
DECLARE @tenant_null INT;
SELECT @tenant_null = COUNT(*) FROM sys.columns
WHERE object_id = OBJECT_ID('FactSales')
  AND name = 'TenantID'
  AND is_nullable = 1;
IF @tenant_null = 0
    PRINT '[PASS] FactSales: TenantID la NOT NULL.';
ELSE
    PRINT '[FAIL] FactSales: TenantID cho phep NULL.';
GO

-- ================================================================
-- TEST 3: FactSales — cac cot tinh toan ton tai
-- ================================================================
DECLARE @missing INT = 0;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactSales') AND name = 'GrossSalesAmount') SET @missing = @missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactSales') AND name = 'NetSalesAmount') SET @missing = @missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactSales') AND name = 'CostAmount') SET @missing = @missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactSales') AND name = 'GrossProfitAmount') SET @missing = @missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactSales') AND name = 'ReturnFlag') SET @missing = @missing + 1;

IF @missing = 0
    PRINT '[PASS] FactSales: Tat ca cot tinh toan deu ton tai (GrossSales, NetSales, Cost, Profit, ReturnFlag).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactSales: Thieu %d cot tinh toan.', @missing);
GO

-- ================================================================
-- TEST 4: FactInventory — ton tai va cau truc dung
-- ================================================================
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'FactInventory')
    PRINT '[PASS] FactInventory: Bang da ton tai.';
ELSE
    PRINT '[FAIL] FactInventory: Bang chua duoc tao.';
GO

DECLARE @fact2_cols INT;
SELECT @fact2_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory');
IF @fact2_cols >= 20
    PRINT '[PASS] FactInventory: Co ' + CAST(@fact2_cols AS VARCHAR(10)) + ' cot (>= 20 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactInventory: Chi co %d cot (mong doi >= 20).', @fact2_cols);
GO

-- ================================================================
-- TEST 5: FactInventory — ClosingQty, StockStatus ton tai
-- ================================================================
DECLARE @inv_missing INT = 0;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory') AND name = 'ClosingQty') SET @inv_missing = @inv_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory') AND name = 'ClosingValue') SET @inv_missing = @inv_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory') AND name = 'DaysOfStock') SET @inv_missing = @inv_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory') AND name = 'StockStatus') SET @inv_missing = @inv_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactInventory') AND name = 'ReorderLevel') SET @inv_missing = @inv_missing + 1;

IF @inv_missing = 0
    PRINT '[PASS] FactInventory: Tat ca cot tinh toan deu ton tai (ClosingQty, ClosingValue, DaysOfStock, StockStatus, ReorderLevel).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactInventory: Thieu %d cot tinh toan.', @inv_missing);
GO

-- ================================================================
-- TEST 6: FactPurchase — ton tai va cau truc dung
-- ================================================================
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactPurchase')
    PRINT '[PASS] FactPurchase: Bang da ton tai.';
ELSE
    PRINT '[FAIL] FactPurchase: Bang chua duoc tao.';
GO

DECLARE @fact3_cols INT;
SELECT @fact3_cols = COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('FactPurchase');
IF @fact3_cols >= 20
    PRINT '[PASS] FactPurchase: Co ' + CAST(@fact3_cols AS VARCHAR(10)) + ' cot (>= 20 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactPurchase: Chi co %d cot (mong doi >= 20).', @fact3_cols);
GO

-- ================================================================
-- TEST 7: FactPurchase — TotalCost, NetCost ton tai
-- ================================================================
DECLARE @pur_missing INT = 0;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactPurchase') AND name = 'TotalCost') SET @pur_missing = @pur_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactPurchase') AND name = 'NetCost') SET @pur_missing = @pur_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactPurchase') AND name = 'PaymentStatus') SET @pur_missing = @pur_missing + 1;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('FactPurchase') AND name = 'ReceivedQty') SET @pur_missing = @pur_missing + 1;

IF @pur_missing = 0
    PRINT '[PASS] FactPurchase: Tat ca cot tinh toan deu ton tai (TotalCost, NetCost, PaymentStatus, ReceivedQty).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] FactPurchase: Thieu %d cot tinh toan.', @pur_missing);
GO

-- ================================================================
-- TEST 8: Indexes — FactSales co cac index can thiet
-- ================================================================
DECLARE @idx1 INT, @idx2 INT, @idx3 INT, @idx4 INT;
SELECT @idx1 = COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactSales') AND name = 'IX_FactSales_TenantID_DateKey';
SELECT @idx2 = COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactSales') AND name = 'IX_FactSales_InvoiceNumber';
SELECT @idx3 = COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactSales') AND name = 'IX_FactSales_ProductKey';
SELECT @idx4 = COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactSales') AND name = 'IX_FactSales_StoreKey';

IF @idx1 = 1 AND @idx2 = 1 AND @idx3 = 1 AND @idx4 = 1
    PRINT '[PASS] FactSales: Co day du 4 indexes quan trong.';
ELSE
    PRINT '[FAIL] FactSales: Thieu indexes (TenantID+DateKey=' + CAST(@idx1 AS VARCHAR(1))
        + ', InvoiceNumber=' + CAST(@idx2 AS VARCHAR(1))
        + ', ProductKey=' + CAST(@idx3 AS VARCHAR(1))
        + ', StoreKey=' + CAST(@idx4 AS VARCHAR(1)) + ').';
GO

-- ================================================================
-- TEST 9: Indexes — FactInventory co IX_TenantID_DateKey
-- ================================================================
DECLARE @inv_idx INT;
SELECT @inv_idx = COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactInventory') AND name = 'IX_FactInventory_TenantID_DateKey';

IF @inv_idx = 1
    PRINT '[PASS] FactInventory: Co index IX_TenantID_DateKey.';
ELSE
    PRINT '[FAIL] FactInventory: Thieu index IX_TenantID_DateKey.';
GO

-- ================================================================
-- TEST 10: Stored Procedures ton tai
-- ================================================================
DECLARE @sp1 INT, @sp2 INT, @sp3 INT, @sp4 INT;
SELECT @sp1 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Transform_FactSales';
SELECT @sp2 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Transform_FactInventory';
SELECT @sp3 = COUNT(*) FROM sys.procedures WHERE name = 'usp_Transform_FactPurchase';
SELECT @sp4 = COUNT(*) FROM sys.procedures WHERE name = 'usp_ClearFactData';

IF @sp1 = 1 AND @sp2 = 1 AND @sp3 = 1 AND @sp4 = 1
    PRINT '[PASS] Tat ca 4 Stored Procedures da duoc tao.';
ELSE
    PRINT '[FAIL] Stored Procedures: usp_Transform_FactSales=' + CAST(@sp1 AS VARCHAR(1))
        + ', usp_Transform_FactInventory=' + CAST(@sp2 AS VARCHAR(1))
        + ', usp_Transform_FactPurchase=' + CAST(@sp3 AS VARCHAR(1))
        + ', usp_ClearFactData=' + CAST(@sp4 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- TEST 11: Stored Procedures co tham so @TenantID
-- ================================================================
DECLARE @param_count INT;
SELECT @param_count = COUNT(*) FROM sys.parameters
WHERE object_id IN (
    SELECT object_id FROM sys.procedures WHERE name IN (
        'usp_Transform_FactSales', 'usp_Transform_FactInventory', 'usp_Transform_FactPurchase'
    )
) AND name = '@TenantID';

IF @param_count >= 3
    PRINT '[PASS] Stored Procedures: Ca 3 SP transform deu co tham so @TenantID.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Stored Procedures: Chi %d / 3 SP co @TenantID.', @param_count);
GO

-- ================================================================
-- TEST 12: Unique constraints ton tai
-- ================================================================
DECLARE @uq1 INT, @uq2 INT, @uq3 INT;
SELECT @uq1 = COUNT(*) FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('FactSales') AND type = 'UQ';
SELECT @uq2 = COUNT(*) FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('FactInventory') AND type = 'UQ';
SELECT @uq3 = COUNT(*) FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('FactPurchase') AND type = 'UQ';

IF @uq1 = 1 AND @uq2 = 1 AND @uq3 = 1
    PRINT '[PASS] Tat ca 3 bang Fact deu co Unique Constraint.';
ELSE
    PRINT '[INFO] Unique constraints: FactSales=' + CAST(@uq1 AS VARCHAR(1))
        + ', FactInventory=' + CAST(@uq2 AS VARCHAR(1))
        + ', FactPurchase=' + CAST(@uq3 AS VARCHAR(1)) + '.';
GO

-- ================================================================
-- INSPECT: Hien thi cau truc chi tiet
-- ================================================================
PRINT '';
PRINT '=== FactSales — Columns ===';
SELECT c.name AS ColumnName,
       t.name + CASE WHEN t.name IN ('varchar','nvarchar')
                     THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                     WHEN t.name IN ('decimal','numeric')
                     THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                     ELSE '' END AS DataType,
       CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END AS Nullable
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID('FactSales')
ORDER BY c.column_id;

PRINT '';
PRINT '=== FactInventory — Columns ===';
SELECT c.name AS ColumnName,
       t.name + CASE WHEN t.name IN ('varchar','nvarchar')
                     THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                     WHEN t.name IN ('decimal','numeric')
                     THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                     ELSE '' END AS DataType,
       CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END AS Nullable
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID('FactInventory')
ORDER BY c.column_id;

PRINT '';
PRINT '=== FactPurchase — Columns ===';
SELECT c.name AS ColumnName,
       t.name + CASE WHEN t.name IN ('varchar','nvarchar')
                     THEN '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
                     WHEN t.name IN ('decimal','numeric')
                     THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                     ELSE '' END AS DataType,
       CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END AS Nullable
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID('FactPurchase')
ORDER BY c.column_id;

PRINT '';
PRINT '=== All Indexes on Fact Tables ===';
SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName,
       i.type_desc AS Type, i.is_primary_key AS PK, i.is_unique AS UniqueKey
FROM sys.indexes i
WHERE OBJECT_NAME(i.object_id) IN ('FactSales', 'FactInventory', 'FactPurchase')
  AND i.index_id > 0
ORDER BY OBJECT_NAME(i.object_id), i.index_id;

PRINT '';
PRINT '=== Stored Procedure Parameters ===';
SELECT p.name AS ProcName, pr.name AS ParamName, t.name AS DataType
FROM sys.parameters pr
INNER JOIN sys.procedures p ON p.object_id = pr.object_id
INNER JOIN sys.types t ON t.user_type_id = pr.user_type_id
WHERE p.name IN ('usp_Transform_FactSales', 'usp_Transform_FactInventory', 'usp_Transform_FactPurchase', 'usp_ClearFactData')
ORDER BY p.name, pr.parameter_id;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 4 VERIFICATION — END';
PRINT '========================================';
GO