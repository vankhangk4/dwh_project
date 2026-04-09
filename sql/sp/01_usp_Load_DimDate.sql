-- ============================================================================
-- PHASE 7: SQL Stored Procedures — DimDate & DimProduct
-- File: sql/sp/01_usp_Load_DimDate.sql
-- Description: Populate DimDate (2015-01-01 → 2030-12-31).
--              Shared — chay mot lan duy nhat hoac khi can rebuild.
--              Phu thuoc: Khong co (DimDate khong co FK).
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Load_DimDate')
BEGIN
    DROP PROCEDURE usp_Load_DimDate;
END
GO

CREATE PROCEDURE usp_Load_DimDate
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_date DATE = '2015-01-01';
    DECLARE @end_date   DATE = '2030-12-31';
    DECLARE @current_date DATE = @start_date;
    DECLARE @RowsInserted INT = 0;

    -- Neu da co du lieu, chi reload khi nguoi dung yeu cau
    IF EXISTS (SELECT TOP 1 * FROM DimDate)
    BEGIN
        PRINT 'DimDate da co du lieu. Neu can reload, goi DELETE FROM DimDate truoc.';
        RETURN;
    END

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO DimDate (
            DateKey, FullDate, DayName, DayOfWeek, DayOfMonth, DayOfYear,
            WeekOfYear, MonthKey, MonthName, MonthOfYear, QuarterKey,
            QuarterName, YearKey, YearMonth, IsWeekend, IsHoliday,
            HolidayName, FiscalYear, FiscalQuarter
        )
        SELECT
            CAST(FORMAT(@current_date, 'yyyyMMdd') AS INT),
            @current_date,
            CAST(DATENAME(WEEKDAY, @current_date) AS NVARCHAR(20)),
            CAST(DATEPART(WEEKDAY, @current_date) AS TINYINT),
            CAST(DATEPART(DAY, @current_date) AS TINYINT),
            CAST(DATEPART(DAYOFYEAR, @current_date) AS SMALLINT),
            CAST(DATEPART(WEEK, @current_date) AS TINYINT),
            CAST(FORMAT(@current_date, 'yyyyMM') AS INT),
            CAST(FORMAT(@current_date, 'MMMM') AS NVARCHAR(20)),
            CAST(MONTH(@current_date) AS TINYINT),
            DATEPART(QUARTER, @current_date),
            'Q' + CAST(DATEPART(QUARTER, @current_date) AS NVARCHAR(10)),
            YEAR(@current_date),
            FORMAT(@current_date, 'yyyy-MM'),
            CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END,
            CAST(0 AS BIT),
            NULL,
            YEAR(DATEADD(MONTH, 3, @current_date)),
            DATEPART(QUARTER, DATEADD(MONTH, 3, @current_date));

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    SET @RowsInserted = @@ROWCOUNT;

    -- Cap nhat ngay le Viet Nam
    UPDATE d SET
        d.IsHoliday   = CAST(1 AS BIT),
        d.HolidayName = h.HolidayName
    FROM DimDate d
    INNER JOIN (
        SELECT FullDate, HolidayName FROM (
            VALUES
            ('2015-01-01', N'Tết Dương lịch'),
            ('2015-02-14', N'Tết Nguyên đán 2015'),
            ('2015-02-15', N'Tết Nguyên đán 2015'),
            ('2015-02-16', N'Tết Nguyên đán 2015'),
            ('2015-04-30', N'Ngày Giải phóng miền Nam'),
            ('2015-05-01', N'Ngày Quốc tế Lao động'),
            ('2015-09-02', N'Ngày Quốc khánh'),
            ('2016-01-01', N'Tết Dương lịch'),
            ('2016-02-08', N'Tết Nguyên đán 2016'),
            ('2016-04-30', N'Ngày Giải phóng miền Nam'),
            ('2016-05-01', N'Ngày Quốc tế Lao động'),
            ('2016-09-02', N'Ngày Quốc khánh'),
            ('2017-01-01', N'Tết Dương lịch'),
            ('2017-01-28', N'Tết Nguyên đán 2017'),
            ('2017-04-30', N'Ngày Giải phóng miền Nam'),
            ('2017-05-01', N'Ngày Quốc tế Lao động'),
            ('2017-09-02', N'Ngày Quốc khánh'),
            ('2018-01-01', N'Tết Dương lịch'),
            ('2018-02-16', N'Tết Nguyên đán 2018'),
            ('2018-04-30', N'Ngày Giải phóng miền Nam'),
            ('2018-05-01', N'Ngày Quốc tế Lao động'),
            ('2018-09-02', N'Ngày Quốc khánh'),
            ('2019-01-01', N'Tết Dương lịch'),
            ('2019-02-05', N'Tết Nguyên đán 2019'),
            ('2019-04-30', N'Ngày Giải phóng miền Nam'),
            ('2019-05-01', N'Ngày Quốc tế Lao động'),
            ('2019-09-02', N'Ngày Quốc khánh'),
            ('2020-01-01', N'Tết Dương lịch'),
            ('2020-01-25', N'Tết Nguyên đán 2020'),
            ('2020-04-30', N'Ngày Giải phóng miền Nam'),
            ('2020-05-01', N'Ngày Quốc tế Lao động'),
            ('2020-09-02', N'Ngày Quốc khánh'),
            ('2021-01-01', N'Tết Dương lịch'),
            ('2021-02-12', N'Tết Nguyên đán 2021'),
            ('2021-04-30', N'Ngày Giải phóng miền Nam'),
            ('2021-05-01', N'Ngày Quốc tế Lao động'),
            ('2021-09-02', N'Ngày Quốc khánh'),
            ('2022-01-01', N'Tết Dương lịch'),
            ('2022-02-01', N'Tết Nguyên đán 2022'),
            ('2022-04-30', N'Ngày Giải phóng miền Nam'),
            ('2022-05-01', N'Ngày Quốc tế Lao động'),
            ('2022-09-02', N'Ngày Quốc khánh'),
            ('2023-01-01', N'Tết Dương lịch'),
            ('2023-01-22', N'Tết Nguyên đán 2023'),
            ('2023-04-30', N'Ngày Giải phóng miền Nam'),
            ('2023-05-01', N'Ngày Quốc tế Lao động'),
            ('2023-09-02', N'Ngày Quốc khánh'),
            ('2024-01-01', N'Tết Dương lịch'),
            ('2024-02-10', N'Tết Nguyên đán 2024'),
            ('2024-04-30', N'Ngày Giải phóng miền Nam'),
            ('2024-05-01', N'Ngày Quốc tế Lao động'),
            ('2024-09-02', N'Ngày Quốc khánh'),
            ('2025-01-01', N'Tết Dương lịch'),
            ('2025-01-29', N'Tết Nguyên đán 2025'),
            ('2025-04-30', N'Ngày Giải phóng miền Nam'),
            ('2025-05-01', N'Ngày Quốc tế Lao động'),
            ('2025-09-02', N'Ngày Quốc khánh'),
            ('2026-01-01', N'Tết Dương lịch'),
            ('2026-02-17', N'Tết Nguyên đán 2026'),
            ('2026-04-30', N'Ngày Giải phóng miền Nam'),
            ('2026-05-01', N'Ngày Quốc tế Lao động'),
            ('2026-09-02', N'Ngày Quốc khánh'),
            ('2027-01-01', N'Tết Dương lịch'),
            ('2027-02-06', N'Tết Nguyên đán 2027'),
            ('2027-04-30', N'Ngày Giải phóng miền Nam'),
            ('2027-05-01', N'Ngày Quốc tế Lao động'),
            ('2027-09-02', N'Ngày Quốc khánh'),
            ('2028-01-01', N'Tết Dương lịch'),
            ('2028-01-26', N'Tết Nguyên đán 2028'),
            ('2028-04-30', N'Ngày Giải phóng miền Nam'),
            ('2028-05-01', N'Ngày Quốc tế Lao động'),
            ('2028-09-02', N'Ngày Quốc khánh'),
            ('2029-01-01', N'Tết Dương lịch'),
            ('2029-02-13', N'Tết Nguyên đán 2029'),
            ('2029-04-30', N'Ngày Giải phóng miền Nam'),
            ('2029-05-01', N'Ngày Quốc tế Lao động'),
            ('2029-09-02', N'Ngày Quốc khánh'),
            ('2030-01-01', N'Tết Dương lịch'),
            ('2030-02-03', N'Tết Nguyên đán 2030'),
            ('2030-04-30', N'Ngày Giải phóng miền Nam'),
            ('2030-05-01', N'Ngày Quốc tế Lao động'),
            ('2030-09-02', N'Ngày Quốc khánh')
        ) AS h(FullDate, HolidayName)
    ) h ON d.FullDate = h.FullDate;

    PRINT 'usp_Load_DimDate: Populated ' + CAST(@RowsInserted AS VARCHAR(10))
        + ' rows (2015-01-01 → 2030-12-31). '
        + 'Updated Vietnam holidays.';

    -- Xac minh
    DECLARE @HolidayCount INT;
    SELECT @HolidayCount = COUNT(*) FROM DimDate WHERE IsHoliday = 1;
    PRINT '[VERIFY] DimDate holidays marked: ' + CAST(@HolidayCount AS VARCHAR(10));
END;
GO

PRINT 'Created stored procedure: usp_Load_DimDate';
GO
