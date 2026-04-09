# PHASE 5: SQL Schema — Staging Layer & ETL Support Tables

## 📁 Files Created

```
dwh_project/sql/schema/
├── 05_create_staging.sql       ← Script chinh
└── 05_verify_phase5.sql     ← Script verify 11 test cases
```

---

## 🎯 Muc tieu Phase 5

**8 bảng Staging (STG_*):**

| Bang | TenantID | Mo ta |
|------|----------|-------|
| `STG_SalesRaw` | **CO** | Chi tiet hoa don |
| `STG_InventoryRaw` | **CO** | Ton kho ngay |
| `STG_PurchaseRaw` | **CO** | Phieu nhap hang |
| `STG_ProductRaw` | **Shared** | Danh muc san pham |
| `STG_CustomerRaw` | **CO** | Khach hang |
| `STG_EmployeeRaw` | **CO** | Nhan vien |
| `STG_StoreRaw` | **CO** | Cua hang |
| `STG_SupplierRaw` | **Shared** | Nha cung cap |

**3 bảng ETL Support:**

| Bang | Mo ta |
|------|-------|
| `ETL_Watermark` | Moc thoi gian incremental extraction |
| `ETL_RunLog` | Log chi tiet moi lan chay ETL |
| `STG_ErrorLog` | Log cac ban ghi bi loi |

**5 Stored Procedures:**

| SP | Chuc nang |
|----|-----------|
| `usp_Truncate_StagingTables` | TRUNCATE / DELETE toan bo Staging |
| `usp_Update_Watermark` | Cap nhat trang thai + gia tri watermark |
| `usp_Get_Last_Watermark` | Doc gia tri watermark cuoi cung thanh cong |
| `usp_Get_All_Active_Watermarks` | Doc tat ca watermark (ket hop Tenants) |
| `usp_ClearErrorLog` | Xoa error log cu hon N ngay |

---

## 🔧 Cach chay

```bash
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/05_create_staging.sql"

sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/05_verify_phase5.sql"
```

---

## ✅ Nghiem thu Phase 5 (11 TEST cases)

```
[PASS] Staging + ETL tables: Co N bang (>= 8 mong doi).
[PASS] Tat ca 8 bang STG_ da ton tai.
[PASS] ETL_Watermark: Bang da ton tai, >= 10 cot.
[PASS] ETL_RunLog: Bang da ton tai, >= 15 cot.
[PASS] STG_ErrorLog: Bang da ton tai, >= 12 cot.
[PASS] TenantID: Tat ca 6 bang tenant-specific deu co TenantID NOT NULL.
[PASS] STG_ProductRaw: KHONG co TenantID (dung — Shared).
[PASS] STG_SupplierRaw: KHONG co TenantID (dung — Shared).
[PASS] STG_LoadDatetime: Tat ca 8 bang deu co cot ghi nhan.
[PASS] ETL_Watermark seed: Co N ban ghi (>= 6), moi tenant 3 nguon.
[PASS] Tat ca 5 Stored Procedures da duoc tao.
```

---

## 🔗 Phu thuoc

- **Require:** Phase 1 (Tenants), Phase 2, Phase 3, Phase 4
- **Depend by:** Phase 6 (Data Mart Layer), Phase 7 (Views & Indexes), ETL Python code

---

## 🔜 Phase tiep theo

**Phase 6:** `sql/schema/06_create_datamart.sql` — Tao cac bang Data Mart (DM_*): DM_SalesSummary, DM_InventoryAlert, DM_CustomerRFM, DM_EmployeePerformance, DM_PurchaseSummary.