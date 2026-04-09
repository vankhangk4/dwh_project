-- ============================================================================
-- PHASE 7: Master Loader — Run All Stored Procedures
-- File: sql/sp/00_run_all_sp.sql
-- Description: Chay tat ca 15 stored procedures theo dung thu tu dependency.
--
-- THU TU CHAY:
--   1.  Shared (khong TenantID): usp_Load_DimDate, usp_Load_DimProduct, usp_Load_DimSupplier
--   2.  Tenant-Specific Dimension Load: usp_Load_DimStore, usp_Load_DimCustomer, usp_Load_DimEmployee
--   3.  Tenant-Specific Fact Transform: usp_Transform_FactSales, usp_Transform_FactInventory, usp_Transform_FactPurchase
--   4.  Tenant-Specific DM Refresh: usp_Refresh_DM_SalesSummary, usp_Refresh_DM_InventoryAlert,
--       usp_Refresh_DM_CustomerRFM, usp_Refresh_DM_EmployeePerformance, usp_Refresh_DM_PurchaseSummary
--   5.  Watermark: usp_Update_Watermark, usp_Get_Last_Watermark, usp_Get_All_Active_Watermarks
--
-- CACH DUNG:
--   Docker exec: docker exec -i sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d DWH_DB -i sql/sp/00_run_all_sp.sql
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '';
PRINT '============================================================';
PRINT 'PHASE 7: MASTER LOADER — Running All Stored Procedures';
PRINT 'Started at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';

---------------------------------------------------------------------------
-- PHASE A: SHARED — Load Dimensions (khong TenantID)
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE A: Shared Dimension Load ---';

PRINT '';
PRINT '[A1] Running usp_Load_DimDate...';
EXEC usp_Load_DimDate;
PRINT '[A1] usp_Load_DimDate completed.';

PRINT '';
PRINT '[A2] Running usp_Load_DimProduct...';
EXEC usp_Load_DimProduct;
PRINT '[A2] usp_Load_DimProduct completed.';

PRINT '';
PRINT '[A3] Running usp_Load_DimSupplier...';
EXEC usp_Load_DimSupplier;
PRINT '[A3] usp_Load_DimSupplier completed.';

---------------------------------------------------------------------------
-- PHASE B: TENANT-SPECIFIC — Load Dimensions (@TenantID)
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE B: Tenant-Specific Dimension Load ---';

-- Chay cho STORE_HN
PRINT '';
PRINT '[B1] Running usp_Load_DimStore for STORE_HN...';
EXEC usp_Load_DimStore @TenantID = 'STORE_HN';
PRINT '[B1] usp_Load_DimStore [STORE_HN] completed.';

PRINT '';
PRINT '[B2] Running usp_Load_DimCustomer for STORE_HN...';
EXEC usp_Load_DimCustomer @TenantID = 'STORE_HN';
PRINT '[B2] usp_Load_DimCustomer [STORE_HN] completed.';

PRINT '';
PRINT '[B3] Running usp_Load_DimEmployee for STORE_HN...';
EXEC usp_Load_DimEmployee @TenantID = 'STORE_HN';
PRINT '[B3] usp_Load_DimEmployee [STORE_HN] completed.';

-- Chay cho STORE_HCM
PRINT '';
PRINT '[B1] Running usp_Load_DimStore for STORE_HCM...';
EXEC usp_Load_DimStore @TenantID = 'STORE_HCM';
PRINT '[B1] usp_Load_DimStore [STORE_HCM] completed.';

PRINT '';
PRINT '[B2] Running usp_Load_DimCustomer for STORE_HCM...';
EXEC usp_Load_DimCustomer @TenantID = 'STORE_HCM';
PRINT '[B2] usp_Load_DimCustomer [STORE_HCM] completed.';

PRINT '';
PRINT '[B3] Running usp_Load_DimEmployee for STORE_HCM...';
EXEC usp_Load_DimEmployee @TenantID = 'STORE_HCM';
PRINT '[B3] usp_Load_DimEmployee [STORE_HCM] completed.';

---------------------------------------------------------------------------
-- PHASE C: TENANT-SPECIFIC — Transform Facts (@TenantID, @BatchDate)
-- NOTE: Chi chay khi STG_SalesRaw co du lieu. Neu chua co STG data,
--       skip Phase C va chuyen thang sang Phase D (DM Refresh).
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE C: Tenant-Specific Fact Transform ---';

-- Lay ngay mac dinh = ngay hom nay
DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);

-- STORE_HN
PRINT '';
PRINT '[C1] Running usp_Transform_FactSales for STORE_HN on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactSales @TenantID = 'STORE_HN', @BatchDate = @BatchDate;
    PRINT '[C1] usp_Transform_FactSales [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[C1] usp_Transform_FactSales [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[C2] Running usp_Transform_FactInventory for STORE_HN on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactInventory @TenantID = 'STORE_HN', @BatchDate = @BatchDate;
    PRINT '[C2] usp_Transform_FactInventory [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[C2] usp_Transform_FactInventory [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[C3] Running usp_Transform_FactPurchase for STORE_HN on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactPurchase @TenantID = 'STORE_HN', @BatchDate = @BatchDate;
    PRINT '[C3] usp_Transform_FactPurchase [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[C3] usp_Transform_FactPurchase [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

-- STORE_HCM
PRINT '';
PRINT '[C1] Running usp_Transform_FactSales for STORE_HCM on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactSales @TenantID = 'STORE_HCM', @BatchDate = @BatchDate;
    PRINT '[C1] usp_Transform_FactSales [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[C1] usp_Transform_FactSales [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[C2] Running usp_Transform_FactInventory for STORE_HCM on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactInventory @TenantID = 'STORE_HCM', @BatchDate = @BatchDate;
    PRINT '[C2] usp_Transform_FactInventory [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[C2] usp_Transform_FactInventory [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[C3] Running usp_Transform_FactPurchase for STORE_HCM on ' + CONVERT(VARCHAR(10), @BatchDate, 120) + '...';
BEGIN TRY
    EXEC usp_Transform_FactPurchase @TenantID = 'STORE_HCM', @BatchDate = @BatchDate;
    PRINT '[C3] usp_Transform_FactPurchase [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[C3] usp_Transform_FactPurchase [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

---------------------------------------------------------------------------
-- PHASE D: TENANT-SPECIFIC — Refresh Data Marts (@TenantID)
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE D: Tenant-Specific Data Mart Refresh ---';

-- STORE_HN
PRINT '';
PRINT '[D1] Running usp_Refresh_DM_SalesSummary for STORE_HN...';
BEGIN TRY
    EXEC usp_Refresh_DM_SalesSummary @TenantID = 'STORE_HN';
    PRINT '[D1] usp_Refresh_DM_SalesSummary [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[D1] usp_Refresh_DM_SalesSummary [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D2] Running usp_Refresh_DM_InventoryAlert for STORE_HN...';
BEGIN TRY
    EXEC usp_Refresh_DM_InventoryAlert @TenantID = 'STORE_HN';
    PRINT '[D2] usp_Refresh_DM_InventoryAlert [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[D2] usp_Refresh_DM_InventoryAlert [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D3] Running usp_Refresh_DM_CustomerRFM for STORE_HN...';
BEGIN TRY
    EXEC usp_Refresh_DM_CustomerRFM @TenantID = 'STORE_HN';
    PRINT '[D3] usp_Refresh_DM_CustomerRFM [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[D3] usp_Refresh_DM_CustomerRFM [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D4] Running usp_Refresh_DM_EmployeePerformance for STORE_HN...';
BEGIN TRY
    EXEC usp_Refresh_DM_EmployeePerformance @TenantID = 'STORE_HN';
    PRINT '[D4] usp_Refresh_DM_EmployeePerformance [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[D4] usp_Refresh_DM_EmployeePerformance [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D5] Running usp_Refresh_DM_PurchaseSummary for STORE_HN...';
BEGIN TRY
    EXEC usp_Refresh_DM_PurchaseSummary @TenantID = 'STORE_HN';
    PRINT '[D5] usp_Refresh_DM_PurchaseSummary [STORE_HN] completed.';
END TRY
BEGIN CATCH
    PRINT '[D5] usp_Refresh_DM_PurchaseSummary [STORE_HN] ERROR: ' + ERROR_MESSAGE();
END CATCH

-- STORE_HCM
PRINT '';
PRINT '[D1] Running usp_Refresh_DM_SalesSummary for STORE_HCM...';
BEGIN TRY
    EXEC usp_Refresh_DM_SalesSummary @TenantID = 'STORE_HCM';
    PRINT '[D1] usp_Refresh_DM_SalesSummary [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[D1] usp_Refresh_DM_SalesSummary [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D2] Running usp_Refresh_DM_InventoryAlert for STORE_HCM...';
BEGIN TRY
    EXEC usp_Refresh_DM_InventoryAlert @TenantID = 'STORE_HCM';
    PRINT '[D2] usp_Refresh_DM_InventoryAlert [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[D2] usp_Refresh_DM_InventoryAlert [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D3] Running usp_Refresh_DM_CustomerRFM for STORE_HCM...';
BEGIN TRY
    EXEC usp_Refresh_DM_CustomerRFM @TenantID = 'STORE_HCM';
    PRINT '[D3] usp_Refresh_DM_CustomerRFM [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[D3] usp_Refresh_DM_CustomerRFM [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D4] Running usp_Refresh_DM_EmployeePerformance for STORE_HCM...';
BEGIN TRY
    EXEC usp_Refresh_DM_EmployeePerformance @TenantID = 'STORE_HCM';
    PRINT '[D4] usp_Refresh_DM_EmployeePerformance [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[D4] usp_Refresh_DM_EmployeePerformance [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

PRINT '';
PRINT '[D5] Running usp_Refresh_DM_PurchaseSummary for STORE_HCM...';
BEGIN TRY
    EXEC usp_Refresh_DM_PurchaseSummary @TenantID = 'STORE_HCM';
    PRINT '[D5] usp_Refresh_DM_PurchaseSummary [STORE_HCM] completed.';
END TRY
BEGIN CATCH
    PRINT '[D5] usp_Refresh_DM_PurchaseSummary [STORE_HCM] ERROR: ' + ERROR_MESSAGE();
END CATCH

---------------------------------------------------------------------------
-- PHASE E: Watermark Verification
---------------------------------------------------------------------------
PRINT '';
PRINT '--- PHASE E: Watermark Verification ---';

PRINT '';
PRINT '[E1] Running usp_Get_All_Active_Watermarks...';
EXEC usp_Get_All_Active_Watermarks;
PRINT '[E1] usp_Get_All_Active_Watermarks completed.';

---------------------------------------------------------------------------
-- COMPLETED
---------------------------------------------------------------------------
PRINT '';
PRINT '============================================================';
PRINT 'PHASE 7: MASTER LOADER COMPLETED';
PRINT 'Finished at: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================';
GO