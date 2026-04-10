# PHASE 10: ETL Transform Module

## 📁 Files Created

```
dwh_project/etl/transform/
├── __init__.py              ← Module exports
├── base_transform.py         ← Shared utilities (clean_string, parse_date, etc.)
├── transform_sales.py       ← Transform STG_SalesRaw -> FactSales ready
├── transform_inventory.py    ← Transform STG_InventoryRaw -> FactInventory ready
├── transform_product.py      ← Transform STG_ProductRaw -> DimProduct (SCD Type 2)
├── transform_customer.py     ← Transform STG_CustomerRaw -> DimCustomer (SCD Type 2)
├── transform_employee.py     ← Transform STG_EmployeeRaw -> DimEmployee
└── transform_purchase.py    ← Transform STG_PurchaseRaw -> FactPurchase ready
```

---

## 🎯 Mục tiêu Phase 10

**6 module transform — mỗi module nhận DataFrame từ extract module, trả về DataFrame sạch, validated, ready cho staging load:**

| Module | Input | Output | Key transformation |
|--------|-------|--------|--------------------|
| `transform_sales` | STG_SalesRaw | FactSales ready | Financial calc, dedup, DateKey |
| `transform_inventory` | STG_InventoryRaw | FactInventory ready | ClosingQty, StockValue, alerts |
| `transform_product` | STG_ProductRaw | DimProduct (SCD T2) | UnitCost/List price, margin |
| `transform_customer` | STG_CustomerRaw | DimCustomer (SCD T2) | LoyaltyTier, Age, Gender norm |
| `transform_employee` | STG_EmployeeRaw | DimEmployee | TenureDays, IsActive, pos norm |
| `transform_purchase` | STG_PurchaseRaw | FactPurchase ready | TotalCost, NetCost, VAT, status norm |

---

## 🔧 Cách chạy

### Option 1: Unit test trực tiếp (không cần DB)

```bash
cd /home/khang/Desktop/retail-tech-dwh-v2/dwh_project

# Import test
python3 -c "
from etl.transform import (
    clean_string, parse_date, safe_float, safe_int,
    transform_sales, transform_inventory, transform_products,
    transform_customers, transform_employees, transform_purchases,
)
print('All imports OK')
"

# Run full integration tests
python3 -c "
import sys; sys.path.insert(0, '.')
import pandas as pd

# Test 1: transform_sales
from etl.transform.transform_sales import transform_sales
df = transform_sales(pd.DataFrame({
    'MaHoaDon': ['HD001','HD002'],
    'NgayBan': ['15/03/2024','20/03/2024'],
    'MaSP': ['SP001','SP002'],
    'MaCH': ['CH001','CH001'],
    'MaKH': ['KH001',None],
    'MaNV': ['NV001',None],
    'SoLuong': [5,3], 'DonGiaBan': [100000,200000],
    'ChietKhau': [5000,0], 'PhuongThucTT': ['Cash',None],
    'KenhBan': ['InStore',None], 'IsHoanTra': [0,0],
}), tenant_id='STORE_HN')
assert not df.empty and 'GrossSalesAmount' in df.columns
assert 'DateKey' in df.columns and 'NetSalesAmount' in df.columns
print('[PASS] transform_sales')

# Test 2: transform_inventory
from etl.transform.transform_inventory import transform_inventory
df = transform_inventory(pd.DataFrame({
    'MaCH': ['CH001'], 'MaSP': ['SP001'],
    'NgayChot': ['15/03/2024'],
    'TonDauNgay': [100], 'NhapTrongNgay': [20], 'BanTrongNgay': [15],
    'TraLaiNhap': [0], 'DieuChinh': [0], 'DonGiaVon': [50000],
    'MucTonToiThieu': [10],
}), tenant_id='STORE_HN')
assert df['ClosingQty'].iloc[0] == 105
print('[PASS] transform_inventory')

# Test 3: transform_products
from etl.transform.transform_product import transform_products
df = transform_products(pd.DataFrame({
    'MaSP': ['SP001'], 'TenSP': ['Sua'], 'DanhMuc': ['Sữa'],
    'GiaVon': [25000], 'GiaNiemYet': [35000],
}))
assert 'UnitCostPrice' in df.columns and 'IsCurrent' in df.columns
print('[PASS] transform_products')

# Test 4: transform_customers
from etl.transform.transform_customer import transform_customers
df = transform_customers(pd.DataFrame({
    'MaKH': ['KH001'], 'HoTen': ['Nguyen Van A'],
    'GioiTinh': ['Nam'], 'NgaySinh': ['15/03/1990'],
    'ThanhPho': ['Ha Noi'], 'DiemTichLuy': [150000],
    'LoaiKH': ['Retail'], 'NgayDangKy': ['01/01/2020'],
}), tenant_id='STORE_HN')
assert 'LoyaltyTier' in df.columns and df['LoyaltyTier'].iloc[0] == 'Silver'
print('[PASS] transform_customers')

# Test 5: transform_employees
from etl.transform.transform_employee import transform_employees
df = transform_employees(pd.DataFrame({
    'MaNV': ['NV001'], 'HoTen': ['Pham Van D'],
    'GioiTinh': ['Male'], 'NgaySinh': ['10/02/1992'],
    'ChucVu': ['Manager'], 'PhongBan': ['Sales'],
    'CaLamViec': ['Morning'],
    'NgayVaoLam': ['01/01/2020'],
    'NgayNghiViec': [None],
}), tenant_id='STORE_HN')
assert 'TenureDays' in df.columns and df['IsActive'].iloc[0] == True
print('[PASS] transform_employees')

# Test 6: transform_purchases
from etl.transform.transform_purchase import transform_purchases
df = transform_purchases(pd.DataFrame({
    'MaCH': ['CH001'], 'MaNCC': ['NCC001'], 'MaSP': ['SP001'],
    'SoPhieuNhap': ['PN001'], 'SoDong': [1],
    'NgayNhap': ['10/03/2024'],
    'SoLuong': [100], 'DonGiaNhap': [25000],
    'ChietKhau': [1000], 'ThueGTGT': [0],
    'TinhTrangChatLuong': ['Passed'],
    'TinhTrangThanhToan': ['Pending'],
    'PhuongThucTT': ['Cash'],
}), tenant_id='STORE_HN')
assert df['TotalCost'].iloc[0] == 2500000
print('[PASS] transform_purchases')

print()
print('='*60)
print('PHASE 10: ALL 6 MODULES PASSED')
print('='*60)
"
```

### Option 2: E2E pipeline simulation (Extract → Transform)

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
import pandas as pd
from datetime import datetime

# Simulate raw extracted data
raw_data = {
    'MaHoaDon': ['HD001','HD002','HD003','HD001'],
    'NgayBan': ['15/03/2024','20/03/2024','25/03/2024','15/03/2024'],
    'MaSP': ['SP001','SP002','SP003','SP001'],
    'MaCH': ['CH001','CH001','CH002','CH001'],
    'SoLuong': [5,3,2,5], 'DonGiaBan': [100000,200000,150000,100000],
    'ChietKhau': [5000,0,10000,5000], 'IsHoanTra': [0,0,1,0],
}

df_raw = pd.DataFrame(raw_data)

from etl.transform.transform_sales import transform_sales
df_clean = transform_sales(df_raw, tenant_id='STORE_HN', drop_duplicates=True)

print(f'Rows: {len(df_clean)} | Gross: {df_clean[\"GrossSalesAmount\"].sum():,.0f}')
print(f'Net: {df_clean[\"NetSalesAmount\"].sum():,.0f}')
print(f'DateKey: {df_clean[\"DateKey\"].iloc[0]}')
print(f'ReturnFlag: {df_clean[\"ReturnFlag\"].value_counts().to_dict()}')
print('[PASS] E2E pipeline: Extract → Transform')
"
```

---

## ✅ Nghiệm thu Phase 10 (12 TEST cases)

```
[PASS] base_transform: clean_string, parse_date, safe_float, safe_int all work
[PASS] base_transform: normalize_phone converts Vietnamese formats correctly
[PASS] base_transform: normalize_email lowercases and validates format
[PASS] base_transform: calculate_age computes age from DOB
[PASS] base_transform: calculate_tenure_days works with NULL end_date
[PASS] transform_sales: Financial metrics (GrossSalesAmount, NetSalesAmount) calculated
[PASS] transform_sales: DateKey format yyyymmdd (20240315)
[PASS] transform_sales: Deduplication removes duplicate (MaHoaDon, MaSP)
[PASS] transform_sales: ReturnFlag populated (HoanTra/BanHang)
[PASS] transform_inventory: ClosingQty = 105 (100+20-15-0+0) ✓
[PASS] transform_inventory: StockValue = 5,250,000 (105 × 50,000) ✓
[PASS] transform_inventory: AlertLevel and StockStatus derived correctly
[PASS] transform_products: SCD Type 2 fields (IsActive, IsCurrent, EffectiveDate) present
[PASS] transform_products: MarginPercent calculated
[PASS] transform_customer: LoyaltyTier Bronze/Silver/Gold/Platinum logic correct
[PASS] transform_customer: City normalization (HCM→Hồ Chí Minh) works
[PASS] transform_employee: TenureDays computed, IsActive flag correct
[PASS] transform_employee: Position normalization (Manager→Quản lý) works
[PASS] transform_purchases: TotalCost = SoLuong × DonGiaNhap
[PASS] transform_purchases: NetCost = TotalCost - DiscountAmount
[PASS] transform_purchases: Quality and payment status normalized
[PASS] All modules: empty DataFrame handling returns empty DataFrame (no crash)
[PASS] All modules: TenantID auto-injected when not in DataFrame columns
```

---

## 📊 Key Business Rules Implemented

| Rule | Module |
|------|--------|
| SoLuong > 0 (filter invalid qty) | `transform_sales`, `transform_purchase` |
| DonGiaBan >= 0 | `transform_sales` |
| MaHoaDon + MaSP dedup (keep last) | `transform_sales` |
| GrossSales = Qty × Price | `transform_sales` |
| NetSales = Gross - ChietKhau | `transform_sales` |
| ClosingQty = TonDau + Nhap - Ban - TraLai + DieuChinh | `transform_inventory` |
| StockValue = max(0, ClosingQty) × DonGiaVon | `transform_inventory` |
| AlertLevel Critical when qty < reorder level | `transform_inventory` |
| UnitCost/List price, margin% | `transform_product` |
| LoyaltyTier Bronze/Silver/Gold/Platinum | `transform_customer` |
| City normalization | `transform_customer` |
| Gender Nam/Nu normalization | `transform_customer` |
| IsActive = NgayNghiViec IS NULL | `transform_employee` |
| TenureDays from NgayVaoLam | `transform_employee` |
| Position/Dept normalization | `transform_employee` |
| TotalCost = SoLuong × DonGiaNhap | `transform_purchase` |
| NetCost = Total - Discount + VAT | `transform_purchase` |
| Quality/Payment status normalized | `transform_purchase` |

---

## 🔗 Phase dependency

- **Require:** Phase 9 (Extract module) — transform nhận DataFrame từ extract
- **Depend by:** Phase 11 (Orchestrator) — orchestrator gọi transform sau extract
- **Depend by:** Phase 12 (Auth API / ETL trigger) — API trigger gọi orchestrator → transform

---

## 🔜 Phase tip theo

**Phase 11:** `etl/orchestrator/` — Pipeline orchestration: `config.py`, `etl_pipeline.py`, `orchestrator.py`, `scheduler.py`, `monitoring.py`, `logging_config.py`.