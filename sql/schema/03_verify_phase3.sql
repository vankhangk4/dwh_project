-- ============================================================================
-- PHASE 3 — Quick Verification Scripts
-- File: sql/schema/03_verify_phase3.sql
-- Purpose: Chay sau khi execute 03_create_dimensions_tenant.sql
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 3 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: DimStore — dung so luong (4 cua hang)
-- ================================================================
DECLARE @store_count INT;
SELECT @store_count = COUNT(*) FROM DimStore;

IF @store_count >= 4
    PRINT '[PASS] DimStore: Co ' + CAST(@store_count AS VARCHAR(10)) + ' cua hang (>= 4 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimStore: %d cua hang (mong doi >= 4).', @store_count);
GO

-- ================================================================
-- TEST 2: DimStore — phan bo theo tenant
-- ================================================================
DECLARE @hn_stores INT, @hcm_stores INT;
SELECT @hn_stores  = COUNT(*) FROM DimStore WHERE TenantID = 'STORE_HN';
SELECT @hcm_stores = COUNT(*) FROM DimStore WHERE TenantID = 'STORE_HCM';

IF @hn_stores >= 2
    PRINT '[PASS] DimStore: STORE_HN co ' + CAST(@hn_stores AS VARCHAR(10)) + ' cua hang (>= 2 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimStore: STORE_HN chi co %d cua hang (mong doi >= 2).', @hn_stores);

IF @hcm_stores >= 2
    PRINT '[PASS] DimStore: STORE_HCM co ' + CAST(@hcm_stores AS VARCHAR(10)) + ' cua hang (>= 2 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimStore: STORE_HCM chi co %d cua hang (mong doi >= 2).', @hcm_stores);
GO

-- ================================================================
-- TEST 3: DimStore — TenantID khong NULL
-- ================================================================
DECLARE @null_tenantid INT;
SELECT @null_tenantid = COUNT(*) FROM DimStore WHERE TenantID IS NULL;

IF @null_tenantid = 0
    PRINT '[PASS] DimStore: Khong co TenantID NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimStore: %d dong co TenantID NULL.', @null_tenantid);
GO

-- ================================================================
-- TEST 4: DimCustomer — dung so luong (20 khach hang)
-- ================================================================
DECLARE @cust_count INT;
SELECT @cust_count = COUNT(*) FROM DimCustomer;

IF @cust_count >= 20
    PRINT '[PASS] DimCustomer: Co ' + CAST(@cust_count AS VARCHAR(10)) + ' khach hang (>= 20 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimCustomer: %d khach hang (mong doi >= 20).', @cust_count);
GO

-- ================================================================
-- TEST 5: DimCustomer — phan bo theo tenant
-- ================================================================
DECLARE @hn_cust INT, @hcm_cust INT;
SELECT @hn_cust  = COUNT(*) FROM DimCustomer WHERE TenantID = 'STORE_HN';
SELECT @hcm_cust = COUNT(*) FROM DimCustomer WHERE TenantID = 'STORE_HCM';

IF @hn_cust >= 10
    PRINT '[PASS] DimCustomer: STORE_HN co ' + CAST(@hn_cust AS VARCHAR(10)) + ' khach hang (>= 10 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimCustomer: STORE_HN chi co %d khach hang (mong doi >= 10).', @hn_cust);

IF @hcm_cust >= 10
    PRINT '[PASS] DimCustomer: STORE_HCM co ' + CAST(@hcm_cust AS VARCHAR(10)) + ' khach hang (>= 10 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimCustomer: STORE_HCM chi co %d khach hang (mong doi >= 10).', @hcm_cust);
GO

-- ================================================================
-- TEST 6: DimCustomer — SCD Type 2, IsCurrent = 1 cho moi CustomerCode
-- (truong hop seed data, moi CustomerCode chi co 1 dong IsCurrent=1)
-- ================================================================
DECLARE @total_cust INT, @current_cust INT, @cust_codes INT;
SELECT @total_cust  = COUNT(*) FROM DimCustomer;
SELECT @current_cust = COUNT(*) FROM DimCustomer WHERE IsCurrent = 1;
SELECT @cust_codes = COUNT(DISTINCT CustomerCode) FROM DimCustomer;

IF @current_cust = @total_cust
    PRINT '[PASS] DimCustomer: Tat ca ' + CAST(@total_cust AS VARCHAR(10))
        + ' dong deu IsCurrent=1 (seed data, chua co SCD history).';
ELSE
    PRINT '[INFO] DimCustomer: ' + CAST(@total_cust AS VARCHAR(10)) + ' dong, '
        + CAST(@current_cust AS VARCHAR(10)) + ' IsCurrent=1, '
        + CAST(@cust_codes AS VARCHAR(10)) + ' CustomerCode (co SCD history).';

IF @current_cust >= @cust_codes
    PRINT '[PASS] DimCustomer: So dong IsCurrent=1 >= so CustomerCode (tot).';
ELSE
    PRINT '[FAIL] DimCustomer: So IsCurrent=1 < so CustomerCode (bat thuong).';
GO

-- ================================================================
-- TEST 7: DimCustomer — TenantID khong NULL
-- ================================================================
DECLARE @cust_null_tenant INT;
SELECT @cust_null_tenant = COUNT(*) FROM DimCustomer WHERE TenantID IS NULL;

IF @cust_null_tenant = 0
    PRINT '[PASS] DimCustomer: Khong co TenantID NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimCustomer: %d dong co TenantID NULL.', @cust_null_tenant);
GO

-- ================================================================
-- TEST 8: DimCustomer — ExpirationDate NULL voi IsCurrent = 1
-- ================================================================
DECLARE @cust_bad_expiry INT;
SELECT @cust_bad_expiry = COUNT(*)
FROM DimCustomer
WHERE IsCurrent = 1 AND ExpirationDate IS NOT NULL;

IF @cust_bad_expiry = 0
    PRINT '[PASS] DimCustomer: Dong IsCurrent=1 deu co ExpirationDate = NULL (dung SCD Type 2).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimCustomer: %d dong IsCurrent=1 co ExpirationDate not NULL.', @cust_bad_expiry);
GO

-- ================================================================
-- TEST 9: DimEmployee — dung so luong (12 nhan vien)
-- ================================================================
DECLARE @emp_count INT;
SELECT @emp_count = COUNT(*) FROM DimEmployee;

IF @emp_count >= 12
    PRINT '[PASS] DimEmployee: Co ' + CAST(@emp_count AS VARCHAR(10)) + ' nhan vien (>= 12 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimEmployee: %d nhan vien (mong doi >= 12).', @emp_count);
GO

-- ================================================================
-- TEST 10: DimEmployee — phan bo theo tenant
-- ================================================================
DECLARE @hn_emp INT, @hcm_emp INT;
SELECT @hn_emp  = COUNT(*) FROM DimEmployee WHERE TenantID = 'STORE_HN';
SELECT @hcm_emp = COUNT(*) FROM DimEmployee WHERE TenantID = 'STORE_HCM';

IF @hn_emp >= 6
    PRINT '[PASS] DimEmployee: STORE_HN co ' + CAST(@hn_emp AS VARCHAR(10)) + ' nhan vien (>= 6 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimEmployee: STORE_HN chi co %d nhan vien (mong doi >= 6).', @hn_emp);

IF @hcm_emp >= 6
    PRINT '[PASS] DimEmployee: STORE_HCM co ' + CAST(@hcm_emp AS VARCHAR(10)) + ' nhan vien (>= 6 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimEmployee: STORE_HCM chi co %d nhan vien (mong doi >= 6).', @hcm_emp);
GO

-- ================================================================
-- TEST 11: DimEmployee — TenantID khong NULL
-- ================================================================
DECLARE @emp_null_tenant INT;
SELECT @emp_null_tenant = COUNT(*) FROM DimEmployee WHERE TenantID IS NULL;

IF @emp_null_tenant = 0
    PRINT '[PASS] DimEmployee: Khong co TenantID NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimEmployee: %d dong co TenantID NULL.', @emp_null_tenant);
GO

-- ================================================================
-- TEST 12: Kiem tra unique constraint tren (TenantID, Code)
-- ================================================================
DECLARE @store_dup INT, @emp_dup INT;
SELECT @store_dup = COUNT(*) FROM (
    SELECT TenantID, StoreCode FROM DimStore
    GROUP BY TenantID, StoreCode HAVING COUNT(*) > 1
) t;
SELECT @emp_dup = COUNT(*) FROM (
    SELECT TenantID, EmployeeCode FROM DimEmployee
    GROUP BY TenantID, EmployeeCode HAVING COUNT(*) > 1
) t;

IF @store_dup = 0
    PRINT '[PASS] DimStore: Khong co (TenantID, StoreCode) trung nhau.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimStore: %d cap (TenantID, StoreCode) bi trung.', @store_dup);

IF @emp_dup = 0
    PRINT '[PASS] DimEmployee: Khong co (TenantID, EmployeeCode) trung nhau.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimEmployee: %d cap (TenantID, EmployeeCode) bi trung.', @emp_dup);
GO

-- ================================================================
-- INSPECT: Xem toan bo du lieu
-- ================================================================
PRINT '';
PRINT '=== DimStore — Full List ===';
SELECT TenantID, StoreCode, StoreName, StoreType, City, Region,
       ManagerName, OpenDate, IsActive
FROM DimStore ORDER BY TenantID, StoreCode;

PRINT '';
PRINT '=== DimCustomer — Sample by Tenant ===';
SELECT TenantID, CustomerCode, FullName, Gender, City,
       CustomerType, LoyaltyTier, LoyaltyPoint, MemberSince, IsCurrent
FROM DimCustomer
WHERE IsCurrent = 1
ORDER BY TenantID, CustomerCode;

PRINT '';
PRINT '=== DimEmployee — Full List ===';
SELECT TenantID, EmployeeCode, FullName, Position, Department,
       ShiftType, HireDate, IsActive
FROM DimEmployee ORDER BY TenantID, EmployeeCode;

PRINT '';
PRINT '=== Stored Procedures ===';
SELECT name AS ProcedureName, create_date
FROM sys.procedures
WHERE name IN ('usp_Load_DimStore', 'usp_Load_DimCustomer', 'usp_Load_DimEmployee')
ORDER BY name;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 3 VERIFICATION — END';
PRINT '========================================';
GO
