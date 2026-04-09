# PHASE 3: SQL Schema — Tenant-Specific Dimensions

## 📁 Files Created

```
dwh_project/
├── sql/schema/
│   ├── 03_create_dimensions_tenant.sql    ← Script chinh (3 bang + 3 SP + seed)
│   └── 03_verify_phase3.sql             ← Script verify 12 test cases
└── README_PHASE3.md                      ← File nay
```

---

## 🎯 Muc tieu Phase 3

| Bang | TenantID | Loai | Mo ta |
|------|----------|------|-------|
| `DimStore` | **CO** | Tenant-Specific | Cửa hàng/chi nhánh. Unique (TenantID, StoreCode). |
| `DimCustomer` | **CO** | Tenant-Specific, SCD Type 2 | Khách hàng. Theo dõi thay đổi thông tin. Unique filtered (TenantID, CustomerCode) WHERE IsCurrent=1. |
| `DimEmployee` | **CO** | Tenant-Specific | Nhân viên. Unique (TenantID, EmployeeCode). |

**3 Stored Procedures:**

| SP | TenantID Param | Chuc nang |
|----|----------------|-----------|
| `usp_Load_DimStore` | `@TenantID` | Insert/Update cua hang theo tenant |
| `usp_Load_DimCustomer` | `@TenantID` | SCD Type 2: Dong cu + Insert moi khi thay doi |
| `usp_Load_DimEmployee` | `@TenantID` | Insert/Update nhan vien theo tenant |

---

## 🔧 Cach chay

### SSMS

```sql
USE DWH_RetailTech;
GO
-- Chay script chinh
EXECUTE sql/schema/03_create_dimensions_tenant.sql
-- Chay verify
EXECUTE sql/schema/03_verify_phase3.sql
```

### sqlcmd

```bash
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/03_create_dimensions_tenant.sql"

sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/03_verify_phase3.sql"
```

---

## ✅ Nghiem thu Phase 3 (12 TEST cases)

```
[PASS] DimStore: Co N cua hang (>= 4 mong doi).
[PASS] DimStore: STORE_HN co N cua hang (>= 2 mong doi).
[PASS] DimStore: STORE_HCM co N cua hang (>= 2 mong doi).
[PASS] DimStore: Khong co TenantID NULL.
[PASS] DimCustomer: Co N khach hang (>= 20 mong doi).
[PASS] DimCustomer: STORE_HN co N khach hang (>= 10 mong doi).
[PASS] DimCustomer: STORE_HCM co N khach hang (>= 10 mong doi).
[PASS] DimCustomer: Tat ca dong deu IsCurrent=1 (seed, chua co SCD history).
[PASS] DimCustomer: So IsCurrent=1 >= so CustomerCode (tot).
[PASS] DimCustomer: Khong co TenantID NULL.
[PASS] DimCustomer: Dong IsCurrent=1 co ExpirationDate = NULL.
[PASS] DimEmployee: Co N nhan vien (>= 12 mong doi).
[PASS] DimEmployee: STORE_HN co N nhan vien (>= 6 mong doi).
[PASS] DimEmployee: STORE_HCM co N nhan vien (>= 6 mong doi).
[PASS] DimEmployee: Khong co TenantID NULL.
[PASS] DimStore: Khong co (TenantID, StoreCode) trung nhau.
[PASS] DimEmployee: Khong co (TenantID, EmployeeCode) trung nhau.
```

**Inspect output:**
- `DimStore`: 4 cửa hàng (STORE_HN × 2, STORE_HCM × 2)
- `DimCustomer`: 20 khách hàng (STORE_HN × 10, STORE_HCM × 10), đủ LoyaltyTier
- `DimEmployee`: 12 nhân viên (STORE_HN × 6, STORE_HCM × 6), đủ Position/Department

---

## 📊 Bang DimStore — Cac cot

| Cot | Mo ta |
|-----|-------|
| `TenantID` | Bat buoc — cua hang chi thuoc 1 tenant |
| `StoreCode` | Ma cua hang |
| `StoreName` | Ten cua hang |
| `StoreType` | Loai: Cửa hàng truyền thống, Siêu thị, Kiosk |
| `Address`, `Ward`, `District`, `City` | Dia chi chi tiet |
| `Region` | Mien: Miền Bắc, Miền Trung, Miền Nam |
| `ManagerName` | Ten quan ly |
| `OpenDate`, `CloseDate` | Ngay khai truong / dong cua |
| `IsActive` | 1 = dang hoat dong |

---

## 📊 Bang DimCustomer — SCD Type 2

**Moi khi thong tin thay doi (FullName, CustomerType, City, LoyaltyTier, Phone):**
1. Dong cu: `IsCurrent=0`, `ExpirationDate=hôm qua`
2. Chen moi: `IsCurrent=1`, `EffectiveDate=hôm nay`

**LoyaltyTier:** Bronze → Silver → Gold → Platinum

---

## 📊 Bang DimEmployee — Cac cot

| Cot | Mo ta |
|-----|-------|
| `TenantID` | Bat buoc — nhan vien chi thuoc 1 tenant |
| `Position` | Chuc vu: Quản lý, Nhân viên bán hàng, Kế toán, Kho vận |
| `Department` | Phong ban: Kinh doanh, Kho vận, Kế toán |
| `ShiftType` | Ca: Sáng, Chiều, Tối, Ca đêm |
| `TerminationDate` | Ngay nghi viec (NULL = dang lam) |
| `IsActive` | 0 = da nghi |

---

## 🔗 Phu thuoc

- **Require:** Phase 1 (Tenants), Phase 2 (DimDate, DimProduct, DimSupplier)
- **Depend by:** Phase 4 (Facts), Phase 5 (Staging)

---

## 🔜 Phase tiep theo

**Phase 4:** `sql/schema/04_create_facts.sql` — Tao `FactSales`, `FactInventory`, `FactPurchase` (co TenantID).