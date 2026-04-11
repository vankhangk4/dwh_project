# PHASE 13: Superset Config & User Scripts

## Mục lục
1. [Tổng quan](#1-tổng-quan)
2. [Cấu trúc file](#2-cấu-trúc-file)
3. [Chi tiết từng file](#3-chi-tiết-từng-file)
4. [Hướng dẫn sử dụng](#4-hướng-dẫn-sử-dụng)
5. [Verify Commands](#5-verify-commands)
6. [Luồng hoạt động](#6-luồng-hoạt-động)

---

## 1. Tổng quan

Phase 13 cung cấp toàn bộ hạ tầng Superset cho Data Warehouse Multi-Tenant:
- **Docker Compose** cho standalone Superset deployment
- **Custom config** cho multi-tenant RLS, embedded dashboards, guest tokens
- **User sync script** đồng bộ AppUsers → Superset users
- **RLS role script** tạo tenant-specific roles + Row-Level Security filters
- **Dashboard seeding script** tạo 5 dashboard scaffolds
- **API routes** trong Auth Gateway để quản lý Superset

---

## 2. Cấu trúc file

```
superset/
├── docker-compose-superset.yml     # Docker Compose standalone Superset
├── superset_config.py              # Custom Superset config (RLS, auth, cache)
├── superset_client.py              # Reusable Superset API client (typed)
├── scripts/
│   ├── __init__.py
│   ├── create_users.py             # Sync DWH AppUsers → Superset users
│   ├── create_roles_rls.py        # Create tenant RLS roles & filters
│   └── seed_dashboards.py         # Seed 5 dashboard scaffolds
└── __init__.py

api/routes/superset.py              # FastAPI routes for Superset management
```

---

## 3. Chi tiết từng file

### 3.1 `docker-compose-superset.yml`

Standalone Docker Compose cho Superset với 4 services:
- **superset_db** (PostgreSQL 15): Metadata database
- **redis** (Redis 7): Cache backend + Celery broker
- **superset** (Apache Superset 3.1.1): Web application
- **superset_worker** (Celery): Async query execution
- **superset_beat** (Celery Beat): Scheduled tasks

Tính năng:
- Health checks trên tất cả services
- Persistent volumes cho data
- Memory limits (4GB sup, 2GB worker)
- Custom `superset_config.py` mounted

### 3.2 `superset_config.py`

Custom Superset configuration:
- **Security**: SECRET_KEY, WTF_CSRF, session cookies, JWT guest tokens
- **Database**: PostgreSQL connection string
- **Cache**: Redis-backed caching (stringified JSON for Docker)
- **Row-Level Security**: `FEATURE_FLAGS["ROW_LEVEL_SECURITY"] = True`
- **Embedded SDK**: `FEATURE_FLAGS["EMBEDDED_SUPERSET"] = True`
- **Auth**: User registration disabled, guest tokens enabled
- **Branding**: Custom app name, logo

### 3.3 `superset_client.py`

Fully-typed Superset REST API client với:
- Auto token management (login, refresh)
- User CRUD
- Role management
- RLS filter CRUD
- Dashboard management
- Guest token generation
- Datasource management
- `provision_tenant()` — full tenant provisioning trong 1 call
- `deprovision_tenant()` — cleanup tenant resources

### 3.4 `scripts/create_users.py`

Sync DWH AppUsers → Superset users:
- Đọc AppUsers từ SQL Server
- Map roles: `admin→Admin`, `viewer→Gamma`, `editor→Alpha`
- Store metadata (tenant_id, dwh_user_id) trong Superset `extra` field
- `--dry-run`, `--sync-tenant`, `--sync-all` flags

### 3.5 `scripts/create_roles_rls.py`

Tạo tenant-specific Superset roles + RLS filters:
- Mỗi tenant có 1 role: `Tenant_STORE_HN`, `Tenant_STORE_HCM`
- RLS filter: `TenantID = 'STORE_HN'`
- Cấp read permissions cho role
- `--init-all`, `--tenant`, `--cleanup`, `--verify` flags
- Verification: kiểm tra orphaned RLS, missing filters

### 3.6 `scripts/seed_dashboards.py`

Tạo 5 dashboard scaffolds:
1. **Sales Overview** (id=1): KPI cards, revenue trend, sales by category
2. **Inventory Management** (id=2): Low stock alerts, stock by store, days of stock
3. **Customer Analytics** (id=3): RFM segments, monetary by segment, RFM heatmap
4. **Employee Performance** (id=4): Revenue by employee, avg basket KPI, orders trend
5. **Purchase Overview** (id=5): Total cost KPI, PO count, cost by supplier, purchase trend

### 3.7 `api/routes/superset.py`

FastAPI endpoints:
- `GET /superset/health` — Superset health check
- `POST /superset/users/sync` — Sync users (admin)
- `GET /superset/users` — List Superset users
- `GET /superset/roles` — List Superset roles
- `POST /superset/roles/init` — Init tenant RLS roles (admin)
- `GET /superset/rls` — List RLS filters
- `POST /superset/rls` — Create RLS filter (admin)
- `DELETE /superset/rls/{id}` — Delete RLS filter (admin)
- `GET /superset/dashboards` — List dashboards
- `POST /superset/dashboards/seed` — Seed dashboards (admin)
- `POST /superset/provision/tenant` — Full tenant provisioning (admin)
- `DELETE /superset/provision/tenant/{tenant_id}` — Deprovision tenant (admin)

---

## 4. Hướng dẫn sử dụng

### 4.1 Khởi động Superset

```bash
# Start Superset
cd superset
docker-compose -f docker-compose-superset.yml up -d

# Wait for health check
docker-compose -f docker-compose-superset.yml ps

# Verify Superset is up
curl http://localhost:8088/health
```

### 4.2 Sync Users

```bash
# Dry run (xem trước thay đổi)
cd /home/khang/Desktop/retail-tech-dwh-v2/dwh_project
source .venv/bin/activate
python superset/scripts/create_users.py --dry-run --verbose

# Sync all users
python superset/scripts/create_users.py --sync-all --verbose

# Sync users for specific tenant
python superset/scripts/create_users.py --sync-tenant STORE_HN --verbose
```

### 4.3 Initialize RLS Roles

```bash
# Dry run
python superset/scripts/create_roles_rls.py --dry-run

# Init for all tenants
python superset/scripts/create_roles_rls.py --init-all --verbose

# Init for specific tenant
python superset/scripts/create_roles_rls.py --tenant STORE_HN

# Verify RLS setup
python superset/scripts/create_roles_rls.py --verify

# Cleanup (xóa tất cả tenant roles)
python superset/scripts/create_roles_rls.py --cleanup --dry-run
```

### 4.4 Seed Dashboards

```bash
# List current dashboards
python superset/scripts/seed_dashboards.py --list

# Dry run
python superset/scripts/seed_dashboards.py --dry-run

# Create all 5 dashboards
python superset/scripts/seed_dashboards.py --create-all

# Create specific dashboard
python superset/scripts/seed_dashboards.py --dashboard 1
```

### 4.5 Via API

```bash
# Login (get JWT)
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'

# Check Superset health
curl http://localhost:8000/superset/health \
  -H "Authorization: Bearer <token>"

# Sync users to Superset (admin)
curl -X POST http://localhost:8000/superset/users/sync \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}'

# Init tenant RLS roles (admin)
curl -X POST http://localhost:8000/superset/roles/init \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}'

# Seed dashboards (admin)
curl -X POST http://localhost:8000/superset/dashboards/seed \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false}'

# Provision tenant (admin)
curl -X POST http://localhost:8000/superset/provision/tenant \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"STORE_HN","tenant_name":"Cua Hang Ha Noi"}'
```

### 4.6 Provisioning Flow (Recommended)

```bash
# Step 1: Start Superset
docker-compose -f superset/docker-compose-superset.yml up -d

# Step 2: Wait for Superset to be ready
sleep 30
curl -f http://localhost:8088/health

# Step 3: Sync users from DWH to Superset
python superset/scripts/create_users.py --sync-all

# Step 4: Initialize tenant RLS roles
python superset/scripts/create_roles_rls.py --init-all

# Step 5: Seed dashboard scaffolds
python superset/scripts/seed_dashboards.py --create-all

# Step 6: Verify
python superset/scripts/create_roles_rls.py --verify
```

---

## 5. Verify Commands

### 5.1 Superset Health
```bash
curl -f http://localhost:8088/health | jq .
```

Expected: `{"status":"ok","version":"3.1.1"...}`

### 5.2 Check Users Synced
```bash
# Via script
python superset/scripts/create_users.py --dry-run

# Via API
curl http://localhost:8000/superset/users \
  -H "Authorization: Bearer <token>" | jq '.count'
```

### 5.3 Check RLS Filters
```bash
# Via script
python superset/scripts/create_roles_rls.py --verify

# Via API
curl http://localhost:8000/superset/rls \
  -H "Authorization: Bearer <token>" | jq '.filters[] | {clause, role_id}'
```

Expected output:
```
{"clause":"TenantID = 'STORE_HN'","role_id":5}
{"clause":"TenantID = 'STORE_HCM'","role_id":6}
```

### 5.4 Check Dashboards
```bash
# Via script
python superset/scripts/seed_dashboards.py --list

# Via API
curl http://localhost:8000/superset/dashboards \
  -H "Authorization: Bearer <token>" | jq '.dashboards[] | {id, title, published}'
```

Expected: 5 dashboards (Sales Overview, Inventory Management, Customer Analytics, Employee Performance, Purchase Overview)

### 5.5 API Endpoints Available
```bash
# Get OpenAPI spec
curl http://localhost:8000/openapi.json | jq '.paths | keys'
```

Expected: All `/superset/*` endpoints visible in OpenAPI spec.

### 5.6 Test Embedded Dashboard Token
```bash
# Login as viewer user
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"store_hn_viewer","password":"viewer123"}'

# Get guest token for dashboard
curl -X POST http://localhost:8000/auth/dashboard-token \
  -H "Authorization: Bearer <viewer_token>" \
  -H "Content-Type: application/json" \
  -d '{"username":"store_hn_viewer","roles":["Gamma"]}'
```

Expected: Guest token với RLS filter `TenantID = 'STORE_HN'` được tự động thêm.

---

## 6. Luồng hoạt động

### 6.1 User Login → Embedded Dashboard

```
1. User login Auth Gateway
   POST /auth/login → JWT token

2. User requests embedded dashboard
   POST /auth/dashboard-token → Superset guest token
   (Auth Gateway gọi Superset API, tạo guest token với RLS filter)

3. Frontend nhúng dashboard
   <iframe src="http://localhost:8088/superset/dashboard/1/?guest_token=xxx">

4. Superset kiểm tra guest token
   - Token hợp lệ ✓
   - Áp dụng RLS filter: TenantID = 'STORE_HN'
   - User chỉ thấy data của STORE_HN
```

### 6.2 Admin Provisions New Tenant

```
1. Admin creates tenant in DWH
   INSERT INTO Tenants (TenantID, TenantName) VALUES ('STORE_DN', 'Cua Hang Da Nang')

2. Admin syncs users
   POST /superset/users/sync → Users for STORE_DN created in Superset

3. Admin initializes RLS
   POST /superset/roles/init → Role "Tenant_STORE_DN" + RLS filter created

4. Admin syncs users again
   POST /superset/users/sync → STORE_DN users updated with Gamma + Tenant_STORE_DN roles

5. Done! STORE_DN viewer users now see only their data in embedded dashboards.
```

---

## Phase tiếp theo

→ **PHASE 14**: Docker Orchestration + Sample Data
- Root `docker-compose.yml` với tất cả services
- Sample Excel data files
- Makefile
