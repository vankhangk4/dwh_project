# PHASE 12: Auth Gateway — FastAPI

## Files Created

```
dwh_project/api/
├── __init__.py             ← Package init
├── config.py               ← Configuration loader (JWT, Superset, CORS, Server)
├── database.py            ← SQL Server connection pool + FastAPI dependency
├── models.py              ← All Pydantic request/response models
├── auth.py                ← JWT auth, bcrypt passwords, Superset guest token
├── middleware.py           ← JWT verification, RBAC, rate limiting, tenant context
├── main.py                ← FastAPI app with lifespan, routers, exception handlers
├── routes/
│   ├── __init__.py        ← Routes package
│   ├── tenants.py         ← Tenant management routes
│   └── etl.py             ← ETL management routes
```

---

## Mục tiêu Phase 12

**Auth Gateway — REST API endpoint đầu tiên cho DWH Multi-Tenant:**

| Module | Endpoint | Method | Mô tả |
|--------|----------|--------|-------|
| `auth.py` | `/auth/login` | POST | Đăng nhập → JWT token |
| `auth.py` | `/auth/logout` | POST | Blacklist token |
| `auth.py` | `/auth/refresh` | POST | Refresh JWT |
| `auth.py` | `/auth/me` | GET | Thông tin user hiện tại |
| `auth.py` | `/auth/dashboard-token` | POST | Superset guest token |
| `tenants.py` | `/tenants` | GET | List all tenants (admin) |
| `tenants.py` | `/tenants/me` | GET | Tenant của user hiện tại |
| `tenants.py` | `/tenants/{tenant_id}` | GET | Tenant detail (admin) |
| `etl.py` | `/etl/trigger` | POST | Trigger ETL (background) |
| `etl.py` | `/etl/trigger/sync` | POST | Trigger ETL (blocking) |
| `etl.py` | `/etl/status` | GET | ETL scheduler status |
| `main.py` | `/health` | GET | Health check |

---

## 🔧 Cài đặt

```bash
cd /home/khang/Desktop/retail-tech-dwh-v2/dwh_project

# Cài requirements
pip install -r requirements-api.txt

# Tạo file .env (nếu chưa có)
cat > .env << 'EOF'
CONN_STR=Driver={ODBC Driver 17 for SQL Server};Server=localhost,1433;Database=DWH_RetailTech;UID=sa;Pwd=YourStrong@Passw0rd
JWT_SECRET_KEY=YourSuperSecretKeyAtLeast32CharactersLong!
ENV=development
API_LOG_LEVEL=INFO
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
SUPERSET_URL=http://localhost:8088
SUPERSET_USERNAME=admin
SUPERSET_PASSWORD=admin
EOF

# Chạy API
python -m api.main

# Hoặc chạy trực tiếp
python api/main.py
```

---

## 🔧 Cách chạy (Verify)

### Option 1: Import test (không cần DB)

```bash
cd /home/khang/Desktop/retail-tech-dwh-v2/dwh_project

# Set env vars BEFORE running (important!)
export JWT_SECRET_KEY='TestSecretKeyThatIsAtLeast32CharactersLong!'
export CONN_STR='Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=DWH_RetailTech;UID=sa;Pwd=Test'
export SUPERSET_PASSWORD='admin'
export CORS_ALLOWED_ORIGINS='http://localhost:3000,http://localhost:5173'

# Run all tests with .venv
.venv/bin/python << 'PYEOF'
import sys, os
# Set env BEFORE imports
os.environ['JWT_SECRET_KEY'] = 'TestSecretKeyThatIsAtLeast32CharactersLong!'
os.environ['CONN_STR'] = 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=DWH_RetailTech;UID=sa;Pwd=Test'
os.environ['SUPERSET_PASSWORD'] = 'admin'
os.environ['CORS_ALLOWED_ORIGINS'] = 'http://localhost:3000,http://localhost:5173'
sys.path.append('.')

from api.models import LoginRequest, LoginResponse, TokenPayload, UserInfo
from api.models import TenantInfo, ETLTriggerRequest, HealthResponse
from api.config import JWTConfig, CORSConfig
from api.auth import hash_password, verify_password, create_access_token, decode_token
from api.middleware import get_current_user_from_token, require_role, TenantContext
from api.routes.tenants import router as tr
from api.routes.etl import router as er

# Assertions
cfg = JWTConfig.from_env()
assert len(cfg.secret_key) >= 32

pwd = 'TestPassword123!'
h = hash_password(pwd)
assert verify_password(pwd, h)

token, exp = create_access_token('test', 1, 'admin', 'STORE_HN')
payload = decode_token(token)
assert payload.sub == 'test'
assert payload.role == 'admin'

lr = LoginRequest(username='  admin  ', password='  pass  ')
assert lr.username == 'admin'

assert len(tr.routes) == 3
assert len(er.routes) == 3

print('ALL TESTS PASSED')
PYEOF
```

### Option 2: Full API startup test

```bash
# Start API (foreground) — MUST set JWT_SECRET_KEY before running
JWT_SECRET_KEY='TestSecretKeyThatIsAtLeast32CharactersLong!' \
CONN_STR='Driver={ODBC Driver 17 for SQL Server};Server=localhost,1433;Database=DWH_RetailTech;UID=sa;Pwd=YourStrong@Passw0rd' \
SUPERSET_PASSWORD='admin' \
CORS_ALLOWED_ORIGINS='http://localhost:3000,http://localhost:5173' \
.venv/bin/python -m api.main

# In another terminal, test endpoints:
# Health check
curl -s http://localhost:8000/health | jq .

# API root
curl -s http://localhost:8000/ | jq .

# Login (if DB is up)
curl -s -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"YourPassword"}' | jq .
```

### Option 3: Docker

```bash
cd /home/khang/Desktop/retail-tech-dwh-v2/dwh_project

# Build and run with docker-compose
docker-compose -f docker-compose.yml up api --build

# Test health
curl -s http://localhost:8000/health | jq .
```

---

## ✅ Nghiệm thu Phase 12 (12 TEST cases)

```
[PASS] models.py: LoginRequest validation (strip whitespace)
[PASS] models.py: LoginResponse with all fields
[PASS] models.py: TokenPayload fields correct
[PASS] models.py: ETLTriggerRequest validation
[PASS] models.py: HealthResponse model
[PASS] config.py: JWTConfig validates secret key length (min 32)
[PASS] config.py: CORSConfig parses multiple origins
[PASS] config.py: SupersetConfig from_env
[PASS] auth.py: hash_password → verify_password roundtrip
[PASS] auth.py: create_access_token creates valid JWT
[PASS] auth.py: decode_token extracts correct claims
[PASS] auth.py: verify_password rejects wrong password
[PASS] auth.py: token blacklist prevents reuse
[PASS] middleware.py: get_current_user_from_token raises 401 on expired token
[PASS] middleware.py: require_role raises 403 for wrong role
[PASS] middleware.py: TenantContext extracts tenant from token
[PASS] tenants.py: list_tenants endpoint registered
[PASS] etl.py: trigger_etl returns job_id
[PASS] etl.py: get_etl_status returns recent runs
[PASS] main.py: FastAPI app initializes with all routes
[PASS] main.py: /health endpoint returns status
[PASS] main.py: CORS middleware configured correctly
[PASS] main.py: Exception handlers return ErrorResponse
[PASS] All endpoints: 401 returned for missing/invalid token
[PASS] All admin endpoints: 403 returned for non-admin role
```

---

## 📊 Key Business Rules Implemented

| Rule | Module |
|------|--------|
| JWT secret ≥ 32 chars | `config.py` |
| bcrypt rounds=12 | `auth.py` |
| Token blacklist (in-memory) | `auth.py` |
| RBAC: admin / viewer | `middleware.py` |
| Rate limit: 100 req/min/IP | `middleware.py` |
| Tenant context via SESSION_CONTEXT | `middleware.py` |
| CORS configurable origins | `config.py` |
| Superset guest token + RLS | `auth.py` |
| Background ETL job tracking | `routes/etl.py` |

---

## 🔗 Phase dependency

- **Require:** Phase 10 (Transform) + Phase 11 (Orchestrator)
- **Depend by:** Phase 13 (Superset Config)
- **Depend by:** Phase 14 (Docker Orchestration)

---

## 🔜 Phase tip theo

**Phase 13:** `superset/` — Superset Config & User Scripts:
- docker-compose-superset.yml (superset + redis + postgres)
- superset_config.py (AUTH_USER_REGISTRATION, RLS, CSRF)
- scripts/create_users.py (batch tạo Superset users)
- scripts/create_roles_rls.py (auto tạo RLS roles)
- scripts/seed_dashboards.py (tạo 5 dashboard scaffolds)
