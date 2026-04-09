-- ============================================================================
-- PHASE 2 — Quick Verification Scripts
-- File: sql/schema/02_verify_phase2.sql
--
-- PURPOSE: Chay sau khi execute 02_create_dimensions.sql de verify ket qua.
-- Run cung voi script chinh hoac chay rieng.
-- ============================================================================

SET NOCOUNT ON;
GO

PRINT '========================================';
PRINT ' PHASE 2 VERIFICATION — START';
PRINT '========================================';
PRINT '';

-- ================================================================
-- TEST 1: DimDate — dung so luong ngay (2015 → 2030 = 5844)
-- ================================================================
DECLARE @date_count INT;
SELECT @date_count = COUNT(*) FROM DimDate;

IF @date_count = 5844
    PRINT '[PASS] DimDate: Co dung 5844 ngay (2015-01-01 → 2030-12-31).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimDate: %d ngay (mong doi 5844).', @date_count);
GO

-- ================================================================
-- TEST 2: DimDate — kiem tra ngay dau tien va cuoi cung
-- ================================================================
DECLARE @min_date DATE, @max_date DATE;
SELECT @min_date = MIN(FullDate), @max_date = MAX(FullDate) FROM DimDate;

IF @min_date = '2015-01-01'
    PRINT '[PASS] DimDate: Ngay dau tien = 2015-01-01.';
ELSE
    PRINT '[FAIL] DimDate: Ngay dau tien khac 2015-01-01.';

IF @max_date = '2030-12-31'
    PRINT '[PASS] DimDate: Ngay cuoi cung = 2030-12-31.';
ELSE
    PRINT '[FAIL] DimDate: Ngay cuoi cung khac 2030-12-31.';
GO

-- ================================================================
-- TEST 3: DimDate — kiem tra DateKey format yyyyMMdd
-- ================================================================
DECLARE @datekey_check INT, @expected INT;
SELECT TOP 1 @datekey_check = DateKey FROM DimDate WHERE FullDate = '2024-01-15';
SET @expected = 20240115;

IF @datekey_check = @expected
    PRINT '[PASS] DimDate: DateKey format dung (2024-01-15 → 20240115).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimDate: DateKey = %d (mong doi %d).', @datekey_check, @expected);
GO

-- ================================================================
-- TEST 4: DimDate — kiem tra cac cot quan trong
-- ================================================================
DECLARE @missing_cols INT = 0;

IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = 20240101)
    SET @missing_cols = @missing_cols + 1;
IF NOT EXISTS (SELECT 1 FROM DimDate WHERE IsWeekend IS NOT NULL)
    SET @missing_cols = @missing_cols + 1;
IF NOT EXISTS (SELECT 1 FROM DimDate WHERE MonthName IS NOT NULL)
    SET @missing_cols = @missing_cols + 1;
IF NOT EXISTS (SELECT 1 FROM DimDate WHERE QuarterKey IN (1,2,3,4))
    SET @missing_cols = @missing_cols + 1;

IF @missing_cols = 0
    PRINT '[PASS] DimDate: Tat ca cot quan trong deu co du lieu hop le.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimDate: %d cot thieu du lieu.', @missing_cols);
GO

-- ================================================================
-- TEST 5: DimDate — kiem tra ngay le (IsHoliday)
-- ================================================================
DECLARE @holiday_count INT;
SELECT @holiday_count = COUNT(*) FROM DimDate WHERE IsHoliday = 1;

IF @holiday_count >= 60
    PRINT '[PASS] DimDate: Co ' + CAST(@holiday_count AS VARCHAR(10)) + ' ngay le VN (>= 60 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimDate: Chi co %d ngay le (mong doi >= 60).', @holiday_count);
GO

-- ================================================================
-- TEST 6: DimProduct — ton tai + IsCurrent = 1 cho moi ProductCode
-- SCD Type 2: moi ProductCode chi co 1 dong IsCurrent = 1
-- ================================================================
DECLARE @total_rows INT, @current_rows INT, @product_codes INT;
SELECT @total_rows = COUNT(*) FROM DimProduct;
SELECT @current_rows = COUNT(*) FROM DimProduct WHERE IsCurrent = 1;
SELECT @product_codes = COUNT(DISTINCT ProductCode) FROM DimProduct;

IF @total_rows = @current_rows
    PRINT '[PASS] DimProduct: Tat ca ' + CAST(@total_rows AS VARCHAR(10)) + ' dong deu la IsCurrent=1 (chua co SCD history).';
ELSE
    PRINT FORMATMESSAGE('[PASS] DimProduct: %d dong, %d IsCurrent=1, %d ProductCode (co SCD history).',
        @total_rows, @current_rows, @product_codes);

IF @current_rows >= @product_codes
    PRINT '[PASS] DimProduct: So dong IsCurrent=1 >= so ProductCode (tot).';
ELSE
    PRINT '[FAIL] DimProduct: So IsCurrent=1 < so ProductCode (bat thuong).';
GO

-- ================================================================
-- TEST 7: DimProduct — kiem tra gia hop le (khong am)
-- ================================================================
DECLARE @negative_price INT;
SELECT @negative_price = COUNT(*) FROM DimProduct
WHERE UnitCostPrice < 0 OR UnitListPrice < 0 OR UnitListPrice < UnitCostPrice;

IF @negative_price = 0
    PRINT '[PASS] DimProduct: Khong co gia am hoac gia ban < gia von.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimProduct: %d dong co gia bat thuong.', @negative_price);
GO

-- ================================================================
-- TEST 8: DimProduct — kiem tra ProductCode khong trung
-- (truong hop IsCurrent=1, khong nen co trung ProductCode)
-- ================================================================
DECLARE @duplicate_codes INT;
SELECT @duplicate_codes = COUNT(*) FROM (
    SELECT ProductCode FROM DimProduct
    WHERE IsCurrent = 1
    GROUP BY ProductCode
    HAVING COUNT(*) > 1
) t;

IF @duplicate_codes = 0
    PRINT '[PASS] DimProduct: Khong co ProductCode trung voi IsCurrent=1.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimProduct: %d ProductCode bi trung (IsCurrent=1).', @duplicate_codes);
GO

-- ================================================================
-- TEST 9: DimSupplier — ton tai + khong NULL cot bat buoc
-- ================================================================
DECLARE @supplier_count INT, @null_name INT, @null_code INT;
SELECT @supplier_count = COUNT(*) FROM DimSupplier;
SELECT @null_name = COUNT(*) FROM DimSupplier WHERE SupplierName IS NULL;
SELECT @null_code = COUNT(*) FROM DimSupplier WHERE SupplierCode IS NULL;

IF @supplier_count >= 5
    PRINT '[PASS] DimSupplier: Co ' + CAST(@supplier_count AS VARCHAR(10)) + ' nha cung cap (>= 5 mong doi).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimSupplier: Chi co %d nha cung cap (mong doi >= 5).', @supplier_count);

IF @null_name = 0
    PRINT '[PASS] DimSupplier: Khong co SupplierName NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimSupplier: %d dong co SupplierName NULL.', @null_name);

IF @null_code = 0
    PRINT '[PASS] DimSupplier: Khong co SupplierCode NULL.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimSupplier: %d dong co SupplierCode NULL.', @null_code);
GO

-- ================================================================
-- TEST 10: DimSupplier — SupplierCode duy nhat
-- ================================================================
DECLARE @duplicate_sup INT;
SELECT @duplicate_sup = COUNT(*) FROM (
    SELECT SupplierCode FROM DimSupplier GROUP BY SupplierCode HAVING COUNT(*) > 1
) t;

IF @duplicate_sup = 0
    PRINT '[PASS] DimSupplier: Tat ca SupplierCode deu duy nhat.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimSupplier: %d SupplierCode bi trung.', @duplicate_sup);
GO

-- ================================================================
-- TEST 11: Kiem tra cac bang SCD co ExpirationDate/EffectiveDate
-- ================================================================
DECLARE @product_no_expiry INT, @product_no_effective INT;
SELECT @product_no_expiry = COUNT(*) FROM DimProduct WHERE IsCurrent = 1 AND ExpirationDate IS NOT NULL;
SELECT @product_no_effective = COUNT(*) FROM DimProduct WHERE IsCurrent = 1 AND EffectiveDate IS NULL;

IF @product_no_expiry = 0
    PRINT '[PASS] DimProduct: Dong IsCurrent=1 co ExpirationDate = NULL (dung SCD Type 2).';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimProduct: %d dong IsCurrent=1 co ExpirationDate not NULL.', @product_no_expiry);

IF @product_no_effective = 0
    PRINT '[PASS] DimProduct: Dong IsCurrent=1 deu co EffectiveDate.';
ELSE
    PRINT FORMATMESSAGE('[FAIL] DimProduct: %d dong IsCurrent=1 thieu EffectiveDate.', @product_no_effective);
GO

-- ================================================================
-- INSPECT: Xem toan bo du lieu
-- ================================================================
PRINT '';
PRINT '=== DimDate — Sample (First 5 + Last 5) ===';
SELECT TOP 5 DateKey, FullDate, DayName, MonthName, QuarterName, YearKey,
       IsWeekend, IsHoliday, HolidayName
FROM DimDate ORDER BY DateKey;
PRINT '...';
SELECT TOP 5 DateKey, FullDate, DayName, MonthName, QuarterName, YearKey,
       IsWeekend, IsHoliday, HolidayName
FROM DimDate ORDER BY DateKey DESC;

PRINT '';
PRINT '=== DimProduct — Full List ===';
SELECT ProductKey, ProductCode, ProductName, Brand, CategoryName,
       UnitCostPrice, UnitListPrice,
       FORMAT(UnitListPrice - UnitCostPrice, '#,##0') AS GrossMargin,
       CASE WHEN (UnitListPrice - UnitCostPrice) / NULLIF(UnitListPrice, 0) >= 0.3
            THEN 'OK' ELSE 'LOW' END AS MarginFlag,
       IsCurrent, EffectiveDate, ExpirationDate
FROM DimProduct
WHERE IsCurrent = 1
ORDER BY CategoryName, ProductName;

PRINT '';
PRINT '=== DimSupplier — Full List ===';
SELECT SupplierKey, SupplierCode, SupplierName, ContactName, City, Country,
       Phone, Email, IsActive
FROM DimSupplier ORDER BY SupplierCode;

PRINT '';
PRINT '=== Stored Procedures ===';
SELECT name AS ProcedureName, create_date, modify_date
FROM sys.procedures
WHERE name IN ('usp_Load_DimDate', 'usp_Load_DimProduct', 'usp_Load_DimSupplier')
ORDER BY name;

PRINT '';
PRINT '========================================';
PRINT ' PHASE 2 VERIFICATION — END';
PRINT '========================================';
GO
