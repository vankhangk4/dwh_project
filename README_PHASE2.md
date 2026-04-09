# PHASE 2: SQL Schema — Shared Dimensions

## 📁 Files Created

```
dwh_project/
├── sql/schema/
│   ├── 02_create_dimensions.sql       ← Script chinh
│   └── 02_verify_phase2.sql          ← Script verify
└── README_PHASE2.md                   ← File nay
```

---

## 🎯 Muc tieu Phase 2

| Bang | Loai | TenantID | Mo ta |
|------|------|----------|-------|
| `DimDate` | Shared | **Khong co** | Thoi gian 2015-2030 (5844 ngay), ngay le VN |
| `DimProduct` | Shared | **Khong co** | San pham, SCD Type 2 (theo doi thay doi gia) |
| `DimSupplier` | Shared | **Khong co** | Nha cung cap, dung chung toan chuoi |

---

## 🔧 Cach chay

### SQL Server Management Studio (SSMS)

```sql
-- Database phai ton tai (tu Phase 1)
USE DWH_RetailTech;
GO

-- Chay script chinh (chon file trong SSMS → F5)
EXECUTE 02_create_dimensions.sql

-- Chay verify
EXECUTE 02_verify_phase2.sql
```

### sqlcmd (Command Line)

```bash
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/02_create_dimensions.sql"

sqlcmd -S localhost -U sa -P "YourStrong@Passw0dr" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/02_verify_phase2.sql"
```

---

## ✅ Nghiem thu Phase 2 (11 TEST cases)

Chay **`02_verify_phase2.sql`** — kỳ vọng **11 PASS**:

```
[PASS] DimDate: Co dung 5844 ngay (2015-01-01 → 2030-12-31).
[PASS] DimDate: Ngay dau tien = 2015-01-01.
[PASS] DimDate: Ngay cuoi cung = 2030-12-31.
[PASS] DimDate: DateKey format dung (2024-01-15 → 20240115).
[PASS] DimDate: Tat ca cot quan trong deu co du lieu hop le.
[PASS] DimDate: Co N ngay le VN (>= 60 mong doi).
[PASS] DimProduct: Tat ca N dong deu la IsCurrent=1 (chua co SCD history).
[PASS] DimProduct: Khong co gia am hoac gia ban < gia von.
[PASS] DimProduct: Khong co ProductCode trung voi IsCurrent=1.
[PASS] DimSupplier: Co N nha cung cap (>= 5 mong doi).
[PASS] DimSupplier: Khong co SupplierName NULL / SupplierCode NULL / trung.
[PASS] DimProduct: Dong IsCurrent=1 co ExpirationDate = NULL (dung SCD Type 2).
```

**Inspect output:**
- `DimDate`: 5844 rows, ngay le VN da danh dau
- `DimProduct`: 18 san pham, 5 danh muc, IsCurrent=1
- `DimSupplier`: 5 nha cung cap
- 3 Stored Procedures: `usp_Load_DimDate`, `usp_Load_DimProduct`, `usp_Load_DimSupplier`

---

## 📊 Bang DimDate — Cac cot

| Cot | Mo ta | Vi du |
|-----|-------|-------|
| `DateKey` | INT yyyyMMdd | `20240115` |
| `FullDate` | DATE | `2024-01-15` |
| `DayName` | Thu trong tuan | `Monday` |
| `DayOfWeek` | 1=Mon, 7=Sun | `1` |
| `DayOfMonth` | Ngay trong thang | `15` |
| `DayOfYear` | Ngay trong nam | `15` |
| `WeekOfYear` | Tuan trong nam | `3` |
| `MonthKey` | INT yyyyMM | `202401` |
| `MonthName` | Ten thang | `January` |
| `MonthOfYear` | 1-12 | `1` |
| `QuarterKey` | 1-4 | `1` |
| `QuarterName` | Q1, Q2, Q3, Q4 | `Q1` |
| `YearKey` | Nam | `2024` |
| `YearMonth` | yyyy-MM | `2024-01` |
| `IsWeekend` | 1=CN/T7 | `0` |
| `IsHoliday` | Ngay le VN | `0` |
| `HolidayName` | Ten ngay le | `NULL` |
| `FiscalYear` | Nam tai chinh | `2024` |
| `FiscalQuarter` | Quy tai chinh | `1` |

---

## 📊 Bang DimProduct — SCD Type 2

**Moi khi gia thay doi:**
1. Dong cu: `IsCurrent=0`, `ExpirationDate=hôm qua`
2. Dong moi: `IsCurrent=1`, `EffectiveDate=hôm nay`

**Business rule:** `UnitListPrice >= UnitCostPrice >= 0`

---

## 📊 Bang DimSupplier — Cac cot

| Cot | Mo ta |
|-----|-------|
| `SupplierKey` | PK auto-increment |
| `SupplierCode` | Ma NCC (unique) |
| `SupplierName` | Ten nha cung cap |
| `ContactName` | Nguoi lien he |
| `ContactTitle` | Chuc vu |
| `Phone` | SDT |
| `Email` | Email |
| `Address` | Dia chi |
| `City` | Thanh pho |
| `Country` | Quoc gia (default: Viet Nam) |
| `TaxCode` | Ma so thue |
| `PaymentTerms` | Dieu khoan thanh toan |
| `IsActive` | 1=hoat dong |

---

## 🔗 Phu thuoc

- **Require:** Phase 1 (`Tenants`, `AppUsers`) — da ton tai
- **Depend by:** Phase 3 (Dimension tenant-specific), Phase 4 (Facts)

---

## 🔜 Phase tiep theo

**Phase 3:** `sql/schema/03_create_dimensions_tenant.sql` — Tao `DimStore`, `DimCustomer` (SCD Type 2), `DimEmployee` (co TenantID).