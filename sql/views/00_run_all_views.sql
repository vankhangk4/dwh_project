-- ============================================================================
-- PHASE 8: Master Runner — Run All Views & Indexes
-- File: sql/views/00_run_all_views.sql
-- Description: Chay tat ca views va indexes cua Phase 8 theo dung thu tu.
--
-- THU TU CHAY:
--   1. Views (11 views total)
--   2. Indexes (bo sung ngoai Phase 1-6 da tao)
--
-- CACH DUNG:
--   docker exec -i sqlserver /opt/mssql-tools/bin/sqlcmd \
--     -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB \
--     -i sql/views/00_run_all_views.sql
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: MASTER RUNNER — Creating Views & Indexes';
PRINT 'Started at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';

---------------------------------------------------------------------------
-- PHASE 8A: Create Views
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE 8A: Creating Views ---';

PRINT '';
PRINT '[V01] Creating v_FactSales_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_FactSales_ByTenant AS SELECT * FROM FactSales WHERE 1=0');
PRINT '[V01] v_FactSales_ByTenant declared.';

PRINT '';
PRINT '[V01b] Creating v_FactSales_TenantSummary...';
EXEC('CREATE OR ALTER VIEW v_FactSales_TenantSummary AS SELECT * FROM FactSales WHERE 1=0');
PRINT '[V01b] v_FactSales_TenantSummary declared.';

PRINT '';
PRINT '[V02] Creating v_FactInventory_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_FactInventory_ByTenant AS SELECT * FROM FactInventory WHERE 1=0');
PRINT '[V02] v_FactInventory_ByTenant declared.';

PRINT '';
PRINT '[V02b] Creating v_FactInventory_Latest...';
EXEC('CREATE OR ALTER VIEW v_FactInventory_Latest AS SELECT * FROM FactInventory WHERE 1=0');
PRINT '[V02b] v_FactInventory_Latest declared.';

PRINT '';
PRINT '[V03] Creating v_FactPurchase_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_FactPurchase_ByTenant AS SELECT * FROM FactPurchase WHERE 1=0');
PRINT '[V03] v_FactPurchase_ByTenant declared.';

PRINT '';
PRINT '[V03b] Creating v_FactPurchase_TenantSummary...';
EXEC('CREATE OR ALTER VIEW v_FactPurchase_TenantSummary AS SELECT * FROM FactPurchase WHERE 1=0');
PRINT '[V03b] v_FactPurchase_TenantSummary declared.';

PRINT '';
PRINT '[V04] Creating v_DM_SalesSummary_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_DM_SalesSummary_ByTenant AS SELECT * FROM DM_SalesSummary WHERE 1=0');
PRINT '[V04] v_DM_SalesSummary_ByTenant declared.';

PRINT '';
PRINT '[V04b] Creating v_DM_InventoryAlert_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_DM_InventoryAlert_ByTenant AS SELECT * FROM DM_InventoryAlert WHERE 1=0');
PRINT '[V04b] v_DM_InventoryAlert_ByTenant declared.';

PRINT '';
PRINT '[V04c] Creating v_DM_InventoryAlert_Critical...';
EXEC('CREATE OR ALTER VIEW v_DM_InventoryAlert_Critical AS SELECT * FROM DM_InventoryAlert WHERE 1=0');
PRINT '[V04c] v_DM_InventoryAlert_Critical declared.';

PRINT '';
PRINT '[V05] Creating v_DM_CustomerRFM_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_DM_CustomerRFM_ByTenant AS SELECT * FROM DM_CustomerRFM WHERE 1=0');
PRINT '[V05] v_DM_CustomerRFM_ByTenant declared.';

PRINT '';
PRINT '[V05b] Creating v_DM_CustomerRFM_SegmentSummary...';
EXEC('CREATE OR ALTER VIEW v_DM_CustomerRFM_SegmentSummary AS SELECT * FROM DM_CustomerRFM WHERE 1=0');
PRINT '[V05b] v_DM_CustomerRFM_SegmentSummary declared.';

PRINT '';
PRINT '[V05c] Creating v_DM_CustomerRFM_AtRisk...';
EXEC('CREATE OR ALTER VIEW v_DM_CustomerRFM_AtRisk AS SELECT * FROM DM_CustomerRFM WHERE 1=0');
PRINT '[V05c] v_DM_CustomerRFM_AtRisk declared.';

PRINT '';
PRINT '[V06] Creating v_DM_EmployeePerformance_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_DM_EmployeePerformance_ByTenant AS SELECT * FROM DM_EmployeePerformance WHERE 1=0');
PRINT '[V06] v_DM_EmployeePerformance_ByTenant declared.';

PRINT '';
PRINT '[V06b] Creating v_DM_EmployeePerformance_Ranking...';
EXEC('CREATE OR ALTER VIEW v_DM_EmployeePerformance_Ranking AS SELECT * FROM DM_EmployeePerformance WHERE 1=0');
PRINT '[V06b] v_DM_EmployeePerformance_Ranking declared.';

PRINT '';
PRINT '[V07] Creating v_DM_PurchaseSummary_ByTenant...';
EXEC('CREATE OR ALTER VIEW v_DM_PurchaseSummary_ByTenant AS SELECT * FROM DM_PurchaseSummary WHERE 1=0');
PRINT '[V07] v_DM_PurchaseSummary_ByTenant declared.';

PRINT '';
PRINT '[V08] Creating v_ETL_RunLog_Recent...';
EXEC('CREATE OR ALTER VIEW v_ETL_RunLog_Recent AS SELECT * FROM ETL_RunLog WHERE 1=0');
PRINT '[V08] v_ETL_RunLog_Recent declared.';

PRINT '';
PRINT '[V09] Creating v_STG_ErrorLog_Recent...';
EXEC('CREATE OR ALTER VIEW v_STG_ErrorLog_Recent AS SELECT * FROM STG_ErrorLog WHERE 1=0');
PRINT '[V09] v_STG_ErrorLog_Recent declared.';

---------------------------------------------------------------------------
-- PHASE 8B: Create Indexes
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE 8B: Creating Additional Indexes ---';

-- Chi chay 08_create_indexes.sql
-- (da chua tat ca index bo sung)
-- Khong can them gi them o day

PRINT '';
PRINT '[OK] Indexes are managed by sql/views/08_create_indexes.sql';
PRINT '     Run separately: sqlcmd -i sql/views/08_create_indexes.sql';

---------------------------------------------------------------------------
-- COMPLETED
---------------------------------------------------------------------------
PRINT '';
PRINT '============================================================';
PRINT 'PHASE 8: MASTER RUNNER COMPLETED';
PRINT 'Note: Views created with placeholder WHERE 1=0.';
PRINT '       Run ALTER VIEW from individual files for full definitions.';
PRINT 'Finished at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';
GO
