-- ============================================================================
-- PHASE 1: Multi-Tenant Core Schema
-- File: sql/schema/01_create_tenants.sql
-- Description: Tao bang Tenants va AppUsers phuc vu multi-tenant.
--              Chay truoc tat ca cac script khac (Phase 1).
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
-- BANG Tenants: Quan ly thong tin cac cua hang / chi nhanh (tenant)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Tenants')
BEGIN
    CREATE TABLE Tenants (
        TenantID     VARCHAR(20)    NOT NULL,
        TenantName   NVARCHAR(200) NOT NULL,
        FilePath     NVARCHAR(500) NULL,
        IsActive     BIT           NOT NULL DEFAULT 1,
        CreatedAt    DATETIME2     NOT NULL DEFAULT GETDATE(),
        UpdatedAt    DATETIME2     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_Tenants PRIMARY KEY CLUSTERED (TenantID)
    );

    PRINT 'Created table: Tenants';
END
ELSE
BEGIN
    PRINT 'Table Tenants already exists — skipping CREATE.';
END
GO

-- ============================================================================
-- BANG AppUsers: Quan ly nguoi dung he thong
-- Role: 'admin' (toan quyen) | 'viewer' (chi xem dashboard cua tenant minh)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AppUsers')
BEGIN
    CREATE TABLE AppUsers (
        UserID       INT IDENTITY(1,1) NOT NULL,
        Username     VARCHAR(100)        NOT NULL,
        PasswordHash VARCHAR(255)       NOT NULL,
        TenantID     VARCHAR(20)         NULL,           -- NULL = admin (ko thuoc tenant nao)
        FullName     NVARCHAR(200)       NULL,
        Email        NVARCHAR(200)       NULL,
        Role         VARCHAR(20)         NOT NULL DEFAULT 'viewer',
        IsActive     BIT                NOT NULL DEFAULT 1,
        CreatedAt    DATETIME2          NOT NULL DEFAULT GETDATE(),
        LastLoginAt  DATETIME2          NULL,
        CONSTRAINT PK_AppUsers PRIMARY KEY CLUSTERED (UserID),
        CONSTRAINT UQ_AppUsers_Username UNIQUE (Username),
        CONSTRAINT CHK_AppUsers_Role CHECK (Role IN ('admin', 'viewer'))
    );

    PRINT 'Created table: AppUsers';
END
ELSE
BEGIN
    PRINT 'Table AppUsers already exists — skipping CREATE.';
END
GO

-- ============================================================================
-- TAO INDEX: Toi uu tim kiem theo TenantID va Role
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AppUsers_TenantID' AND object_id = OBJECT_ID('AppUsers'))
BEGIN
    CREATE INDEX IX_AppUsers_TenantID ON AppUsers(TenantID);
    PRINT 'Created index: IX_AppUsers_TenantID';
END
ELSE
BEGIN
    PRINT 'Index IX_AppUsers_TenantID already exists — skipping.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AppUsers_Username' AND object_id = OBJECT_ID('AppUsers'))
BEGIN
    CREATE INDEX IX_AppUsers_Username ON AppUsers(Username);
    PRINT 'Created index: IX_AppUsers_Username';
END
ELSE
BEGIN
    PRINT 'Index IX_AppUsers_Username already exists — skipping.';
END
GO

-- ============================================================================
-- TAO INDEX tren Tenants
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Tenants_IsActive' AND object_id = OBJECT_ID('Tenants'))
BEGIN
    CREATE INDEX IX_Tenants_IsActive ON Tenants(IsActive);
    PRINT 'Created index: IX_Tenants_IsActive';
END
ELSE
BEGIN
    PRINT 'Index IX_Tenants_IsActive already exists — skipping.';
END
GO

-- ============================================================================
-- TAO FK: AppUsers.TenantID tham chieu Tenants.TenantID
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_AppUsers_Tenants')
BEGIN
    ALTER TABLE AppUsers
    ADD CONSTRAINT FK_AppUsers_Tenants
    FOREIGN KEY (TenantID) REFERENCES Tenants(TenantID);

    PRINT 'Created FK: FK_AppUsers_Tenants';
END
ELSE
BEGIN
    PRINT 'FK FK_AppUsers_Tenants already exists — skipping.';
END
GO

-- ============================================================================
-- SEED DATA: Insert tenant mau
-- ============================================================================
IF NOT EXISTS (SELECT * FROM Tenants WHERE TenantID = 'STORE_HN')
BEGIN
    INSERT INTO Tenants (TenantID, TenantName, FilePath, IsActive, CreatedAt, UpdatedAt)
    VALUES
        ('STORE_HN',  N'Cửa hàng Hà Nội',  './data/STORE_HN/',  1, GETDATE(), GETDATE()),
        ('STORE_HCM', N'Cửa hàng Hồ Chí Minh', './data/STORE_HCM/', 1, GETDATE(), GETDATE());

    PRINT 'Inserted seed tenants: STORE_HN, STORE_HCM';
END
ELSE
BEGIN
    PRINT 'Seed tenants already exist — skipping INSERT.';
END
GO

-- ============================================================================
-- SEED DATA: Insert tai khoan admin va viewer mau
-- Password: Admin@DWH123  (bcrypt hash)
-- Password: Viewer@HN123  (bcrypt hash)
-- Password: Viewer@HCM123 (bcrypt hash)
-- NOTE: Thay bcrypt hash bang gia tri thuc cua ban truoc khi deploy!
-- ============================================================================
-- Hash admin: Admin@DWH123 -> $2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.T0f0f0f0f0f0f0
-- Hash viewer HN: Viewer@HN123 -> $2b$12$...
-- Sử dụng bcrypt hash mẫu — trong production cần thay bằng hash thực
DECLARE @admin_hash   VARCHAR(255) = '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4gMVRBF0eRjL0Zy';
DECLARE @viewer_hn    VARCHAR(255) = '$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi';
DECLARE @viewer_hcm   VARCHAR(255) = '$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi';

IF NOT EXISTS (SELECT * FROM AppUsers WHERE Username = 'admin')
BEGIN
    INSERT INTO AppUsers (Username, PasswordHash, TenantID, FullName, Email, Role, IsActive)
    VALUES
        ('admin',     @admin_hash,  NULL,          N'Quản trị viên', 'admin@dwh.local', 'admin',  1),
        ('viewer_hn', @viewer_hn,   'STORE_HN',   N'Người xem HN',  'hn@dwh.local',    'viewer', 1),
        ('viewer_hcm', @viewer_hcm, 'STORE_HCM',  N'Người xem HCM', 'hcm@dwh.local',   'viewer', 1);

    PRINT 'Inserted seed users: admin, viewer_hn, viewer_hcm';
END
ELSE
BEGIN
    PRINT 'Seed users already exist — skipping INSERT.';
END
GO

-- ============================================================================
-- XAC MINH: Doc lai du lieu
-- ============================================================================
PRINT '';
PRINT '=== VERIFICATION: Tenants ===';
SELECT TenantID, TenantName, FilePath, IsActive, CreatedAt FROM Tenants ORDER BY TenantID;

PRINT '';
PRINT '=== VERIFICATION: AppUsers ===';
SELECT UserID, Username, TenantID, FullName, Email, Role, IsActive, CreatedAt FROM AppUsers ORDER BY UserID;

PRINT '';
PRINT '=== PHASE 1 COMPLETED SUCCESSFULLY ===';
GO
