# PHASE 1: SQL Schema — Multi-Tenant Core

## 📁 File Created

```
dwh_project/
├── sql/
│   └── schema/
│       ├── 01_create_tenants.sql       ← Script chinh tao bang
│       └── 01_verify_phase1.sql        ← Script verify ket qua
├── .env.example                        ← Template bien moi truong
└── README_PHASE1.md                    ← File nay
```

---

## 🎯 Muc tieu Phase 1

- Tạo bang `Tenants` — quan ly danh sach cua hang/chi nhanh
- Tạo bang `AppUsers` — quan ly tai khoan nguoi dung (admin + viewer)
- Tao FK, Indexes, Seed data (2 tenant, 3 user)

---

## 🔧 Cach chay

### Cach 1: SQL Server Management Studio (SSMS)

1. Mo SSMS → Connect to SQL Server
2. Chon database `DWH_RetailTech` (tao neu chua co):
   ```sql
   CREATE DATABASE DWH_RetailTech;
   GO
   USE DWH_RetailTech;
   GO
   ```
3. Mo file `sql/schema/01_create_tenants.sql` → Execute (F5)
4. Mo file `sql/schema/01_verify_phase1.sql` → Execute (F5)

### Cach 2: sqlcmd (Command Line)

```bash
# Tao database
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -Q "CREATE DATABASE DWH_RetailTech; GO"

# Chay script chinh
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/01_create_tenants.sql"

# Chay script verify
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -d DWH_RetailTech \
  -i "dwh_project/sql/schema/01_verify_phase1.sql"
```

### Cach 3: Docker (neu dung container SQL Server)

```bash
# Khoi dong container SQL Server (neu chua co)
docker run -d --name sqlserver2019 \
  -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=YourStrong@Passw0rd' \
  -e 'MSSQL_PID=Developer' \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2019-latest

# Cho 10s de container khoi dong
sleep 10

# Copy script vao container
docker cp dwh_project/sql/schema/01_create_tenants.sql sqlserver2019:/01_create_tenants.sql

# Execute script trong container
docker exec sqlserver2019 /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong@Passw0rd' -C \
  -d master -i /01_create_tenants.sql

# Verify
docker exec sqlserver2019 /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong@Passw0rd' -C \
  -d DWH_RetailTech -i /01_verify_phase1.sql
```

---

## ✅ Nghiem thu Phase 1 (7 TEST cases)

Chay script `01_verify_phase1.sql`, kỳ vọng **7 PASS**:

```
[PASS] Tenants: Co dung 2 ban ghi (STORE_HN, STORE_HCM).
[PASS] Tenants: STORE_HN va STORE_HCM deu ton tai.
[PASS] AppUsers: Co dung 3 tai khoan (admin + 2 viewer).
[PASS] AppUsers: Co dung 1 tai khoan admin.
[PASS] AppUsers: Co dung 2 tai khoan viewer.
[PASS] AppUsers: viewer_hn thuoc tenant STORE_HN.
[PASS] AppUsers: viewer_hcm thuoc tenant STORE_HCM.
[PASS] AppUsers: admin co TenantID = NULL (admin toan quyen).
[PASS] Tenants: Tat ca 2 tenant deu dang hoat dong (IsActive=1).
[PASS] AppUsers: Tat ca viewer deu co TenantID (khong NULL).
```

**Ket qua hien thi cuoi cung (Inspect) gồm:**
- Bảng Tenants: 2 dòng (STORE_HN, STORE_HCM)
- Bảng AppUsers: 3 dòng (admin, viewer_hn, viewer_hcm)

---

## ⚠️ Lưu ý quan trọng

1. **Password Hash:** Script sử dụng bcrypt hash mẫu. Trước khi deploy production, cần tạo hash thực bằng Python:
   ```python
   from passlib.hash import bcrypt
   print(bcrypt.hash("Admin@DWH123"))    # hash cho admin
   print(bcrypt.hash("Viewer@HN123"))     # hash cho viewer_hn
   print(bcrypt.hash("Viewer@HCM123"))    # hash cho viewer_hcm
   ```

2. **Database:** Database `DWH_RetailTech` phải được tạo **TRƯỚC** khi chạy script (script không tự tạo database).

3. **Thứ tự:** Phase 1 phải chạy **TRƯỚC** tất cả các Phase SQL khác (vì các bảng Dimensions/Facts sẽ tham chiếu đến Tenants).

---

## 🔜 Phase tiep theo

**Phase 2:** `sql/schema/02_create_dimensions.sql` — Tạo bảng DimDate, DimProduct, DimSupplier (Shared, không có TenantID).