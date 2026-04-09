# ============================================================================
# PHASE 1 — Quick Verification Scripts
# File: sql/schema/01_verify_phase1.sql
#
# PURPOSE: Chay sau khi execute 01_create_tenants.sql de verify ket qua.
# Run cung voi script chinh hoac chay rieng.
# ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 1 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: Kiem tra Tenants ton tai + co dung 2 ban ghi
-- ================================================================
DECLARE @tenant_count INT;
SELECT @tenant_count = COUNT(*) FROM Tenants;

IF @tenant_count = 2
    PRINT '[PASS] Tenants: Co dung 2 ban ghi (STORE_HN, STORE_HCM).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Tenants: %d ban ghi (mong doi 2).', @tenant_count);
GO

DECLARE @hn_exists INT, @hcm_exists INT;
SELECT @hn_exists = COUNT(*) FROM Tenants WHERE TenantID = 'STORE_HN';
SELECT @hcm_exists = COUNT(*) FROM Tenants WHERE TenantID = 'STORE_HCM';
IF @hn_exists = 1 AND @hcm_exists = 1
    PRINT '[PASS] Tenants: STORE_HN va STORE_HCM deu ton tai.';
ELSE
    PRINT '[FAIL] Tenants: Thieu tenant(s).';
GO

-- ================================================================
-- TEST 2: Kiem tra AppUsers ton tai + dung 3 user
-- ================================================================
DECLARE @user_count INT;
SELECT @user_count = COUNT(*) FROM AppUsers;

IF @user_count = 3
    PRINT '[PASS] AppUsers: Co dung 3 tai khoan (admin + 2 viewer).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] AppUsers: %d tai khoan (mong doi 3).', @user_count);
GO

-- ================================================================
-- TEST 3: Kiem tra role phan bo dung
-- ================================================================
DECLARE @admin_count INT, @viewer_count INT;
SELECT @admin_count  = COUNT(*) FROM AppUsers WHERE Role = 'admin';
SELECT @viewer_count = COUNT(*) FROM AppUsers WHERE Role = 'viewer';

IF @admin_count = 1
    PRINT '[PASS] AppUsers: Co dung 1 tai khoan admin.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] AppUsers: %d tai khoan admin (mong doi 1).', @admin_count);

IF @viewer_count = 2
    PRINT '[PASS] AppUsers: Co dung 2 tai khoan viewer.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] AppUsers: %d tai khoan viewer (mong doi 2).', @viewer_count);
GO

-- ================================================================
-- TEST 4: Kiem tra TenantID cua viewer
-- ================================================================
DECLARE @hn_user  INT, @hcm_user INT;
SELECT @hn_user  = COUNT(*) FROM AppUsers WHERE Username = 'viewer_hn'  AND TenantID = 'STORE_HN';
SELECT @hcm_user = COUNT(*) FROM AppUsers WHERE Username = 'viewer_hcm' AND TenantID = 'STORE_HCM';

IF @hn_user = 1
    PRINT '[PASS] AppUsers: viewer_hn thuoc tenant STORE_HN.';
ELSE
    PRINT '[FAIL] AppUsers: viewer_hn khong dung tenant.';

IF @hcm_user = 1
    PRINT '[PASS] AppUsers: viewer_hcm thuoc tenant STORE_HCM.';
ELSE
    PRINT '[FAIL] AppUsers: viewer_hcm khong dung tenant.';
GO

-- ================================================================
-- TEST 5: Kiem tra admin co TenantID = NULL
-- ================================================================
DECLARE @admin_null INT;
SELECT @admin_null = COUNT(*) FROM AppUsers WHERE Username = 'admin' AND TenantID IS NULL;

IF @admin_null = 1
    PRINT '[PASS] AppUsers: admin co TenantID = NULL (admin toàn quyền).';
ELSE
    PRINT '[FAIL] AppUsers: admin khong co TenantID = NULL.';
GO

-- ================================================================
-- TEST 6: Kiem tra Tenants.IsActive = 1
-- ================================================================
DECLARE @active_count INT;
SELECT @active_count = COUNT(*) FROM Tenants WHERE IsActive = 1;

IF @active_count = 2
    PRINT '[PASS] Tenants: Tat ca 2 tenant deu dang hoat dong (IsActive=1).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] Tenants: %d tenant active (mong doi 2).', @active_count);
GO

-- ================================================================
-- TEST 7: Kiem tra bang khong co NULL bat thuong
-- ================================================================
DECLARE @null_tenantid_users INT;
SELECT @null_tenantid_users = COUNT(*) FROM AppUsers
WHERE Role = 'viewer' AND TenantID IS NULL;

IF @null_tenantid_users = 0
    PRINT '[PASS] AppUsers: Tat ca viewer deu co TenantID (khong NULL).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] AppUsers: %d viewer co TenantID NULL (bat thuong).', @null_tenantid_users);
GO

-- ================================================================
-- INSPECT: Xem toan bo du lieu
-- ================================================================
PRINT '';
PRINT '=== Tenants (full) ===';
SELECT TenantID, TenantName, FilePath, IsActive,
       FORMAT(CreatedAt, 'yyyy-MM-dd HH:mm:ss') AS CreatedAt
FROM Tenants ORDER BY TenantID;

PRINT '';
PRINT '=== AppUsers (full, mat khau da an) ===';
SELECT UserID, Username,
       TenantID,
       FullName, Email,
       Role,
       CASE WHEN IsActive = 1 THEN 'Active' ELSE 'Inactive' END AS Status,
       FORMAT(CreatedAt, 'yyyy-MM-dd HH:mm:ss') AS CreatedAt
FROM AppUsers ORDER BY UserID;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 1 VERIFICATION — END';
PRINT '========================================';
GO
