"""focms_api.py - FOCMS Data Provider REST API v0.1

Read-only API in front of FOCMS Postgres. Enforces per-request tenant context
via RLS (SET LOCAL app.current_tenant_id). Designed to run as a Render Web
Service behind PgBouncer in transaction mode.

Endpoints:
    GET /focms/v1/health
    GET /focms/v1/types
    GET /focms/v1/student/{student_id}
    GET /focms/v1/student/{student_id}/records?type=type
    GET /focms/v1/student/{student_id}/target-universities

Auth: Bearer token in Authorization header. Token maps to (tenant_id,
user_id, role) via env-configured JSON. JWT upgrade is a Phase 1 follow-up.

Environment:
    DATABASE_URL_POOLED    - pgbouncer URL (transaction mode)
    FOCMS_API_TOKENS_JSON  - JSON dict mapping token to principal
    FOCMS_API_LOG_LEVEL    - INFO (default)
"""
import json, logging, os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.responses import JSONResponse

DATABASE_URL = os.environ.get("DATABASE_URL_POOLED") or os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL_POOLED or DATABASE_URL must be set")
TOKENS = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
LOG_LEVEL = os.environ.get("FOCMS_API_LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("focms-api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """asyncpg pool. PgBouncer transaction mode requires statement_cache_size=0."""
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, statement_cache_size=0,
    )
    log.info("DB pool ready")
    try:
        yield
    finally:
        await app.state.pool.close()
        log.info("DB pool closed")


app = FastAPI(title="FOCMS Data Provider API", version="0.1.0", lifespan=lifespan)


def authenticate(authorization: str = Header(None)) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    principal = TOKENS.get(token)
    if not principal:
        raise HTTPException(401, "Invalid bearer token")
    return principal


@asynccontextmanager
async def tx(request: Request, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
    """Acquire a pooled connection, start a tx, SET LOCAL the tenant id.
    All RLS-protected reads must go through this helper."""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)",
                str(tenant_id),
            )
            yield conn


@app.get("/focms/v1/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "version": "0.1.0"}


@app.get("/focms/v1/types")
async def list_types(
    request: Request, principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Capturable types and their row counts for the authenticated tenant."""
    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch("""
            SELECT 'events' AS type_name, count(*) AS n FROM events
            UNION ALL SELECT 'assessments', count(*) FROM assessments
            UNION ALL SELECT 'affiliations', count(*) FROM affiliations
            UNION ALL SELECT 'personal_records', count(*) FROM personal_records
            UNION ALL SELECT 'goals', count(*) FROM goals
            UNION ALL SELECT 'courses', count(*) FROM courses
            UNION ALL SELECT 'digital_presence', count(*) FROM digital_presence
            UNION ALL SELECT 'target_universities', count(*) FROM target_universities
            ORDER BY type_name
        """)
        return {"types": [{"name": r["type_name"], "count": r["n"]} for r in rows]}


@app.get("/focms/v1/student/{student_id}")
async def get_student(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Return the student identity profile."""
    async with tx(request, principal["tenant_id"]) as conn:
        row = await conn.fetchrow("""
            SELECT id, preferred_name, current_grade, residence_state,
                   expected_hs_graduation_year, birth_date,
                   EXTRACT(YEAR FROM age(birth_date))::int AS age_years
            FROM students WHERE id = $1
        """, student_id)
        if not row:
            raise HTTPException(404, "Student not found")
        return {
            "id": str(row["id"]),
            "preferred_name": row["preferred_name"],
            "current_grade": row["current_grade"],
            "residence_state": row["residence_state"],
            "expected_hs_graduation_year": row["expected_hs_graduation_year"],
            "age_years": row["age_years"],
        }


# Whitelist of types we serve. Prevents SQL injection via the path parameter.
RECORD_TABLES = {
    "events": "events",
    "assessments": "assessments",
    "affiliations": "affiliations",
    "personal_records": "personal_records",
    "goals": "goals",
    "courses": "courses",
    "digital_presence": "digital_presence",
}


@app.get("/focms/v1/student/{student_id}/records")
async def get_records(
    student_id: UUID, request: Request, type: str, limit: int = 100,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Return records of the requested type for the student."""
    if type not in RECORD_TABLES:
        raise HTTPException(400, f"Unknown record type: {type}")
    table = RECORD_TABLES[type]
    limit = max(1, min(limit, 500))
    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch(
            f"""
            SELECT to_jsonb(t) AS rec FROM {table} t
            WHERE student_id = $1 AND deleted_at IS NULL
            ORDER BY COALESCE(updated_at, created_at) DESC LIMIT $2
            """,
            student_id, limit,
        )
        return {
            "type": type,
            "student_id": str(student_id),
            "count": len(rows),
            "records": [dict(r["rec"]) for r in rows],
        }


@app.get("/focms/v1/student/{student_id}/target-universities")
async def get_target_universities(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Return target universities for the student, with the universities row
    and CDS facts hydrated."""
    async with tx(request, principal["tenant_id"]) as conn:
        targets = await conn.fetch("""
            SELECT t.id, t.university_leaid, t.priority, t.notes,
                   u.name AS university_name, u.city, u.state,
                   u.us_news_rank, u.admit_rate, u.cost_attendance,
                   u.has_rotc, u.has_d1_swim, u.is_service_academy
            FROM target_universities t
            JOIN universities u ON u.leaid = t.university_leaid
            WHERE t.student_id = $1 AND t.deleted_at IS NULL
            ORDER BY u.us_news_rank NULLS LAST, u.name
        """, student_id)
        if not targets:
            return {"student_id": str(student_id), "targets": []}
        leaids = [t["university_leaid"] for t in targets]
        facts_rows = await conn.fetch("""
            SELECT university_leaid, fact_key,
                   COALESCE(fact_value_numeric::text, fact_value_text) AS value,
                   id::text AS cite_id
            FROM university_cds_facts
            WHERE university_leaid = ANY($1::text[])
        """, leaids)
        facts_by_leaid: dict[str, dict[str, dict[str, str]]] = {}
        for f in facts_rows:
            facts_by_leaid.setdefault(f["university_leaid"], {})[f["fact_key"]] = {
                "value": f["value"], "cite_id": f["cite_id"],
            }
        result = []
        for t in targets:
            leaid = t["university_leaid"]
            result.append({
                "target_id": str(t["id"]),
                "priority": t["priority"],
                "notes": t["notes"],
                "university": {
                    "leaid": leaid,
                    "name": t["university_name"],
                    "city": t["city"],
                    "state": t["state"],
                    "us_news_rank": t["us_news_rank"],
                    "admit_rate": float(t["admit_rate"]) if t["admit_rate"] else None,
                    "cost_attendance": float(t["cost_attendance"]) if t["cost_attendance"] else None,
                    "has_rotc": t["has_rotc"],
                    "has_d1_swim": t["has_d1_swim"],
                    "is_service_academy": t["is_service_academy"],
                },
                "cds_facts": facts_by_leaid.get(leaid, {}),
            })
        return {"student_id": str(student_id), "targets": result}


@app.exception_handler(asyncpg.PostgresError)
async def pg_error_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    log.error("Postgres error: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "database_error", "detail": str(exc)},
    )

