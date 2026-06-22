"""focms_api.py - FOCMS Data Provider REST API v0.2.0

Read-only API in front of FOCMS Postgres. Enforces per-request tenant context
via RLS (set_config app.current_tenant_id). Designed to run as a Render Web
Service behind PgBouncer in transaction mode.

Endpoints:
    GET /focms/v1/health
    GET /focms/v1/types
    GET /focms/v1/student/{student_id}
    GET /focms/v1/student/{student_id}/records?type=type
    GET /focms/v1/student/{student_id}/target-universities
    GET /focms/v1/student/{student_id}/computed/power-index   [NEW v0.2.0]

Auth: Bearer token in Authorization header. Token maps to (tenant_id,
user_id, role) via env-configured JSON. JWT upgrade is a Phase 1 follow-up.

Environment:
    DATABASE_URL_POOLED    - pgbouncer URL (transaction mode)
    FOCMS_API_TOKENS_JSON  - JSON dict mapping token to principal
    FOCMS_API_LOG_LEVEL    - INFO (default)

v0.2.0 (2026-06-21): Added /computed/power-index. Computes Swimcloud-style
Power Index server-side from swim_best rows in personal_records and NCAA D1
Men 2026 base times from archive_entries.reference_data. Cited, no
hardcoded reference data, deterministic.
"""
import json, logging, os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
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


app = FastAPI(title="FOCMS Data Provider API", version="0.2.0", lifespan=lifespan)


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
    return {"status": "ok", "version": "0.2.0"}


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


# ---------------------------------------------------------------------------
# v0.2.0: Computed metrics
# ---------------------------------------------------------------------------
#
# Swimcloud Power Point formula (class of 2026+ quad):
#   PP = ((time_scy / ncaa_base)^3 - 1) * 100 + 1
# Power Index = weighted average of TOP 4 (lowest) PPs at weights 100/100/25/5
PI_WEIGHTS = [1.00, 1.00, 0.25, 0.05]


def _compute_pp(seconds: float, event: str, course: str,
                base_scy: dict, lcm_to_scy_factor: float):
    """Compute Swimcloud Power Point for one event. Returns None if event
    not in NCAA base table or course not SCY/LCM."""
    base = base_scy.get(event)
    if base is None:
        return None
    if course == "SCY":
        t_scy = seconds
    elif course == "LCM":
        t_scy = seconds * lcm_to_scy_factor
    else:
        return None
    pp = ((t_scy / base) ** 3 - 1) * 100 + 1
    return round(pp, 2)


def _split_event_course(label: str):
    """'200 Breast SCY' -> ('200 Breast', 'SCY')"""
    parts = label.rsplit(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (label, "")


@app.get("/focms/v1/student/{student_id}/computed/power-index")
async def get_power_index(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Compute Swimcloud-style Power Index for the student.

    Pulls swim_best rows from personal_records and the NCAA D1 Men 2026
    qualifying standards (SCY) from archive_entries.reference_data. The
    base times source is cited in the response.

    Returns 503 if the NCAA reference row is missing.
    """
    async with tx(request, principal["tenant_id"]) as conn:
        # 1) NCAA base times from reference_data
        base_row = await conn.fetchrow("""
            SELECT detail, source_url, archive_date, source_id
            FROM archive_entries
            WHERE archive_type = 'reference_data'
              AND source_id = 'ncaa_d1_men_2026_qualifying_standards_scy'
            LIMIT 1
        """)
        if base_row is None or not base_row["detail"]:
            raise HTTPException(
                503,
                "NCAA D1 base times reference data not present in "
                "archive_entries. Cannot compute Power Index.",
            )
        base_payload = json.loads(base_row["detail"])
        base_scy = base_payload["events"]
        lcm_factor = base_payload["conversion_to_lcm_factor"]
        base_year = base_payload["effective_year"]

        # 2) This student's swim bests
        bests = await conn.fetch("""
            SELECT title, value_numeric AS seconds
            FROM personal_records
            WHERE student_id = $1
              AND record_kind = 'swim_best'
              AND deleted_at IS NULL
              AND value_numeric IS NOT NULL
        """, student_id)

    # 3) Compute Power Points per event
    pps = []
    for r in bests:
        event, course = _split_event_course(r["title"])
        pp = _compute_pp(float(r["seconds"]), event, course, base_scy, lcm_factor)
        if pp is not None:
            pps.append((r["title"], pp))

    # 4) Sort ascending (lowest = best), take top 4
    pps.sort(key=lambda x: x[1])
    top_4_raw = pps[:4]

    # 5) Weighted PI
    if top_4_raw:
        weights = PI_WEIGHTS[: len(top_4_raw)]
        total_w = sum(weights)
        pi_value = sum(top_4_raw[i][1] * weights[i] for i in range(len(top_4_raw))) / total_w
        pi_value = round(pi_value, 1)
    else:
        pi_value = None

    return {
        "student_id": str(student_id),
        "tenant_id": principal["tenant_id"],
        "power_index": {
            "value": pi_value,
            "top_4": [{"event_course": l, "pp": p} for l, p in top_4_raw],
            "total_eligible_events": len(pps),
            "method": "swimcloud_2026_quad_class_of_2026_and_later",
            "base_times_source": {
                "archive_source_id": base_row["source_id"],
                "source_url": base_row["source_url"],
                "effective_year": base_year,
                "refreshed_at": str(base_row["archive_date"]),
            },
            "formula": (
                "PP = ((time_scy / ncaa_base)^3 - 1) * 100 + 1; "
                "PI = weighted avg of top 4 (lowest) PPs at 100/100/25/5 percent"
            ),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        },
    }


@app.exception_handler(asyncpg.PostgresError)
async def pg_error_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    log.error("Postgres error: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "database_error", "detail": str(exc)},
    )
