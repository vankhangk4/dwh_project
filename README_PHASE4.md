# PHASE 4: SQL Schema — Fact Tables

## 📁 Files Created

```
dwh_project/sql/schema/
├── 04_create_facts.sql       ← 1008 dòng — Script chính
└── 04_verify_phase4.sql  ← Script verify 12 test cases
```

---

## 🎯 Muc tieu Phase 4

| Bang | Grain | TenantID | Khoa ngoai |
|------|-------|----------|------------|
| `FactSales` | 1 dong = 1 san pham / 1 hoa don | **CO** | DimDate, DimProduct, DimStore, DimCustomer, DimEmployee |
| `FactInventory` | 1 dong = 1 SP / 1 cua hang / 1 ngay | **CO** | DimDate, DimProduct, DimStore |
| `FactPurchase` | 1 dong = 1 dong phieu nhap | **CO** | DimDate, DimProduct, DimSupplier, DimStore |

**4 Stored Procedures:**

| SP | Params | Chuc nang |
|----|--------|-----------|
| `usp_Transform_FactSales` | `@TenantID`, `@BatchDate` | INSERT + ErrorLog + ETL_RunLog |
| `usp_Transform_FactInventory` | `@TenantID`, `@BatchDate` | MERGE (upsert) + calculate ClosingQty, StockStatus |
| `usp_Transform_FactPurchase` | `@TenantID`, `@BatchDate` | INSERT + ErrorLog + ETL_RunLog |
| `usp_ClearFactData` | `@TenantID`, `@BatchDate`, `@FactTable` | DELETE fact data de rerun ETL |

---

## 🔧 Cach chay

```bash
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/04_create_facts.sql"

sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/04_verify_phase4.sql"
```

---

## ✅ Nghiem thu Phase 4 (12 TEST cases)

```
[PASS] FactSales: Bang da ton tai.
[PASS] FactSales: Co N cot (>= 25 mong doi).
[PASS] FactSales: Co 1 Primary Key.
[PASS] FactSales: TenantID la NOT NULL.
[PASS] FactSales: Tat ca cot tinh toan deu ton tai.
[PASS] FactInventory: Bang da ton tai.
[PASS] FactInventory: Tat ca cot tinh toan deu ton tai.
[PASS] FactPurchase: Bang da ton tai.
[PASS] FactPurchase: Tat ca cot tinh toan deu ton tai.
[PASS] FactSales: Co day du 4 indexes quan trong.
[PASS] FactInventory: Co index IX_TenantID_DateKey.
[PASS] Tat ca 4 Stored Procedures da duoc tao.
[PASS] Stored Procedures: Ca 3 SP deu co tham so @TenantID.
[PASS] Tat ca 3 bang Fact deu co Unique Constraint.
```

**Inspect output:**
- FactSales: 27 cot, PK, UQ(InvoiceLine), 8 indexes
- FactInventory: 23 cot, PK, UQ(DateProductStore), 6 indexes
- FactPurchase: 25 cot, PK, UQ(OrderLine), 6 indexes

---

## 📊 Bang FactSales — Cot Tinh toan

| Cot | Cong thuc |
|-----|-----------|
| `GrossSalesAmount` | `Quantity * UnitPrice` |
| `NetSalesAmount` | `GrossSalesAmount - DiscountAmount` |
| `CostAmount` | `Quantity * UnitCostPrice` (tu DimProduct) |
| `GrossProfitAmount` | `NetSalesAmount - CostAmount` |

---

## 📊 Bang FactInventory — StockStatus Logic

```
ClosingQty = Opening + Received - Sold + Returned + Adjusted
DaysOfStock = (Opening + Received) / Sold   (neu Sold > 0, nguoc lai = 999)

StockStatus:
  ClosingQty = 0          → 'Out of Stock'
  ClosingQty <= ReorderLevel → 'Low'
  ClosingQty > ReorderLevel*5 → 'Overstock'
  Nguoc lai              → 'Normal'
```

---

## 🔗 Phu thuoc

- **Require:** Phase 1 (Tenants), Phase 2 (DimDate, DimProduct, DimSupplier), Phase 3 (DimStore, DimCustomer, DimEmployee)
- **Depend by:** Phase 5 (Staging Layer)

---

## 🔜 Phase tiep theo

**Phase 5:** `sql/schema/05_create_staging.sql` — Tao cac bang Staging (STG_*), bang ETL_Watermark, ETL_RunLog, STG_ErrorLog.