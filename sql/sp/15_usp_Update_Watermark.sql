-- ============================================================================
-- PHASE 7: SQL Stored Procedures — Watermark Management (Shared)
-- File: sql/sp/15_usp_Update_Watermark.sql
-- Description: Cap nhat trang thai va gia tri watermark sau moi lan ETL.
--              Shared — dung chung cho tat ca tenant.
--
-- Trang thai:
--   RUNNING: Dat truoc khi ETL bat dau (ghi nhan bat dau chu ky).
--   SUCCESS: Cap nhat WatermarkValue = GETDATE() sau ETL thanh cong.
--   FAILED:  Giu nguyen WatermarkValue de retry tu diem cu.
--
-- Dependencies: ETL_Watermark.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Update_Watermark')
BEGIN
    DROP PROCEDURE usp_Update_Watermark;
END
GO

CREATE PROCEDURE usp_Update_Watermark
    @SourceName       VARCHAR(100),
    @TenantID         VARCHAR(20),
    @Status           VARCHAR(20),   -- 'RUNNING', 'SUCCESS', 'FAILED'
    @SourceType       VARCHAR(50) = NULL,
    @RowsExtracted    INT = NULL,
    @DurationSeconds INT = NULL,
    @Notes            NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate
    IF @SourceName IS NULL OR LEN(@SourceName) = 0
    BEGIN
        PRINT 'usp_Update_Watermark: SourceName is required.';
        RETURN;
    END

    IF @TenantID IS NULL OR LEN(@TenantID) = 0
    BEGIN
        PRINT 'usp_Update_Watermark: TenantID is required.';
        RETURN;
    END

    IF @Status NOT IN ('RUNNING', 'SUCCESS', 'FAILED')
    BEGIN
        PRINT 'usp_Update_Watermark: Status must be RUNNING, SUCCESS, or FAILED.';
        RETURN;
    END

    DECLARE @CurrentWatermark DATETIME2;
    DECLARE @CurrentStatus VARCHAR(20);

    -- Lay trang thai hien tai (neu co)
    SELECT
        @CurrentWatermark = WatermarkValue,
        @CurrentStatus = LastRunStatus
    FROM ETL_Watermark
    WHERE SourceName = @SourceName;

    --------------------------------------------------------------------------
    -- RUNNING: Dat truoc khi ETL bat dau
    --------------------------------------------------------------------------
    IF @Status = 'RUNNING'
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                LastRunStatus   = 'RUNNING',
                LastRunDatetime = GETDATE(),
                RowsExtracted   = ISNULL(@RowsExtracted, RowsExtracted),
                DurationSeconds = ISNULL(@DurationSeconds, DurationSeconds),
                Notes           = ISNULL(@Notes, Notes)
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName,
                @TenantID,
                ISNULL(@SourceType, @SourceName),
                CAST('2020-01-01' AS DATETIME2),
                'RUNNING',
                GETDATE(),
                @RowsExtracted,
                @DurationSeconds,
                @Notes
            );
        END

        PRINT 'usp_Update_Watermark [' + @SourceName + ']: Set to RUNNING at '
            + CONVERT(VARCHAR(30), GETDATE(), 120) + '.';
        RETURN;
    END

    --------------------------------------------------------------------------
    -- SUCCESS: Cap nhat WatermarkValue = GETDATE()
    --------------------------------------------------------------------------
    IF @Status = 'SUCCESS'
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                WatermarkValue   = GETDATE(),
                LastRunStatus    = 'SUCCESS',
                LastRunDatetime  = GETDATE(),
                RowsExtracted    = ISNULL(@RowsExtracted, RowsExtracted),
                DurationSeconds  = ISNULL(@DurationSeconds, DurationSeconds),
                Notes            = ISNULL(@Notes, Notes)
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName,
                @TenantID,
                ISNULL(@SourceType, @SourceName),
                GETDATE(),
                'SUCCESS',
                GETDATE(),
                @RowsExtracted,
                @DurationSeconds,
                @Notes
            );
        END

        PRINT 'usp_Update_Watermark [' + @SourceName + ']: SUCCESS. Watermark advanced to '
            + CONVERT(VARCHAR(30), GETDATE(), 120)
            + '. RowsExtracted: ' + ISNULL(CAST(@RowsExtracted AS VARCHAR(20)), 'N/A')
            + '. Duration: ' + ISNULL(CAST(@DurationSeconds AS VARCHAR(20)), 'N/A') + 's.');
        RETURN;
    END

    --------------------------------------------------------------------------
    -- FAILED: Giu nguyen WatermarkValue de retry tu diem cu
    --------------------------------------------------------------------------
    IF @Status = 'FAILED'
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE SourceName = @SourceName)
        BEGIN
            UPDATE ETL_Watermark SET
                LastRunStatus   = 'FAILED',
                LastRunDatetime = GETDATE(),
                DurationSeconds = ISNULL(@DurationSeconds, DurationSeconds),
                Notes           = ISNULL(@Notes, Notes)
                -- KHONG update WatermarkValue khi FAILED
                -- De retry tu diem cu
            WHERE SourceName = @SourceName;
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (
                SourceName, TenantID, SourceType,
                WatermarkValue, LastRunStatus, LastRunDatetime,
                RowsExtracted, DurationSeconds, Notes
            )
            VALUES (
                @SourceName,
                @TenantID,
                ISNULL(@SourceType, @SourceName),
                CAST('2020-01-01' AS DATETIME2),
                'FAILED',
                GETDATE(),
                @RowsExtracted,
                @DurationSeconds,
                @Notes
            );
        END

        DECLARE @KeptValue VARCHAR(30);
        IF @CurrentWatermark IS NOT NULL
            SET @KeptValue = CONVERT(VARCHAR(30), @CurrentWatermark, 120);
        ELSE
            SET @KeptValue = '2020-01-01 (default)';

        PRINT 'usp_Update_Watermark [' + @SourceName + ']: FAILED. Watermark KEPT at '
            + @KeptValue + ' for retry. Notes: '
            + ISNULL(@Notes, 'N/A') + '.';
        RETURN;
    END
END;
GO

PRINT 'Created stored procedure: usp_Update_Watermark';
GO


-- ============================================================================
-- PHASE 7: SQL Stored Procedures — Get Last Watermark
-- File: sql/sp/15_usp_Update_Watermark.sql (continued)
-- Description: Doc gia tri watermark cuoi cung thanh cong.
-- Dependencies: ETL_Watermark.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Get_Last_Watermark')
BEGIN
    DROP PROCEDURE usp_Get_Last_Watermark;
END
GO

CREATE PROCEDURE usp_Get_Last_Watermark
    @SourceName VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Watermark DATETIME2;
    DECLARE @Status VARCHAR(20);
    DECLARE @RowsExtracted INT;
    DECLARE @LastRun DATETIME2;

    SELECT TOP 1
        @Watermark = WatermarkValue,
        @Status = LastRunStatus,
        @RowsExtracted = RowsExtracted,
        @LastRun = LastRunDatetime
    FROM ETL_Watermark
    WHERE SourceName = @SourceName
    ORDER BY LastRunDatetime DESC;

    IF @Watermark IS NULL
    BEGIN
        SET @Watermark = CAST('2020-01-01' AS DATETIME2);
        PRINT 'usp_Get_Last_Watermark [' + @SourceName + ']: No successful watermark found. Using default: 2020-01-01.';
    END
    ELSE
    BEGIN
        PRINT 'usp_Get_Last_Watermark [' + @SourceName + ']: Watermark='
            + CONVERT(VARCHAR(30), @Watermark, 120)
            + ', Status=' + ISNULL(@Status, 'N/A')
            + ', LastRun=' + ISNULL(CONVERT(VARCHAR(30), @LastRun, 120), 'N/A')
            + ', RowsExtracted=' + ISNULL(CAST(@RowsExtracted AS VARCHAR(20)), 'N/A') + '.';
    END

    -- Tra ve ket qua
    SELECT
        @SourceName AS SourceName,
        @Watermark AS LastSuccessfulWatermark,
        CASE WHEN @Watermark = CAST('2020-01-01' AS DATETIME2)
             THEN 'DEFAULT' ELSE 'FOUND' END AS WatermarkSource,
        @Status AS LastRunStatus,
        @RowsExtracted AS RowsExtracted,
        @LastRun AS LastRunDatetime;
END;
GO

PRINT 'Created stored procedure: usp_Get_Last_Watermark';
GO


-- ============================================================================
-- PHASE 7: SQL Stored Procedures — Get All Active Watermarks
-- File: sql/sp/15_usp_Update_Watermark.sql (continued)
-- Description: Doc tat ca watermark cua tat ca tenant dang hoat dong.
-- Dependencies: ETL_Watermark, Tenants.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Get_All_Active_Watermarks')
BEGIN
    DROP PROCEDURE usp_Get_All_Active_Watermarks;
END
GO

CREATE PROCEDURE usp_Get_All_Active_Watermarks
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        w.SourceName,
        w.TenantID,
        w.SourceType,
        w.WatermarkValue,
        w.LastRunStatus,
        w.LastRunDatetime,
        w.RowsExtracted,
        w.DurationSeconds,
        w.Notes,
        t.TenantName,
        t.FilePath
    FROM ETL_Watermark w
    INNER JOIN Tenants t ON t.TenantID = w.TenantID
    WHERE t.IsActive = 1
    ORDER BY w.TenantID, w.SourceName;

    DECLARE @Count INT = @@ROWCOUNT;
    PRINT 'usp_Get_All_Active_Watermarks: Returned ' + CAST(@Count AS VARCHAR(10)) + ' watermark(s).';
END;
GO

PRINT 'Created stored procedure: usp_Get_All_Active_Watermarks';
GO