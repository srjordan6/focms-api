"""focms_api.py - FOCMS Data Provider REST API v0.3.2

Read + write API in front of FOCMS Postgres. Enforces per-request tenant
context via RLS (SET LOCAL app.current_tenant_id). Runs as a Render Web
Service behind PgBouncer in transaction mode.

v0.3.2 (2026-06-22):
- Fix: audit_log action='create' was written even when the upsert path hit
  ON CONFLICT and actually updated an existing row. All 8 POST endpoints now
  include `(xmax = 0) AS _was_insert` in RETURNING and branch audit action
  to 'create' or 'update' accordingly. `_was_insert` is stripped from the
  HTTP response body before return.

v0.3.1 (2026-06-22):
- Fix: /records returned 500 because asyncpg + statement_cache_size=0 +
  PgBouncer leaves JSONB as a string, breaking dict(row["rec"]).
- Add: POST endpoints for affiliations, goals, courses, digital_presence
  rounding out the write surface for John's portfolio sections.

Read endpoints:
    GET    /focms/v1/health
    GET    /focms/v1/types
    GET    /focms/v1/student/{student_id}
    GET    /focms/v1/student/{student_id}/records?type=type
    GET    /focms/v1/student/{student_id}/target-universities
    GET    /focms/v1/student/{student_id}/computed/power-index

Write endpoints:
    POST   /focms/v1/archive_entries          (+ PATCH/GET/DELETE/append-detail)
    POST   /focms/v1/personal_records
    POST   /focms/v1/events
    POST   /focms/v1/assessments
    POST   /focms/v1/affiliations              [NEW v0.3.1]
    POST   /focms/v1/goals                     [NEW v0.3.1]
    POST   /focms/v1/courses                   [NEW v0.3.1]
    POST   /focms/v1/digital_presence          [NEW v0.3.1]

Auth: Bearer token in Authorization header. Token maps to (tenant_id,
user_id, role) via FOCMS_API_TOKENS_JSON. Writes require role in
{tenant_owner, tenant_admin, platform_admin}.
"""
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, AsyncIterator, Optional
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

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


app = FastAPI(title="FOCMS Data Provider API", version="0.3.2", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

# Roles that may write. Accepts back-compat token shapes (role unset or
# 'admin' / 'writer') as well as the canonical tenant_role_enum values.
WRITE_ROLES = {
    "admin", "writer",                            # back-compat aliases
    "tenant_owner", "tenant_admin", "platform_admin",  # canonical
}

# Map any incoming role string to a valid tenant_role_enum for audit_log.
_ROLE_TO_ENUM = {
    "admin": "tenant_admin",
    "writer": "tenant_admin",
    "owner": "tenant_owner",
    "viewer": "tenant_viewer",
    "platform": "platform_admin",
    "tenant_owner": "tenant_owner",
    "tenant_admin": "tenant_admin",
    "tenant_viewer": "tenant_viewer",
    "platform_admin": "platform_admin",
}


def role_to_enum(role: Optional[str]) -> str:
    """Resolve a role label to a valid tenant_role_enum value.
    Defaults to tenant_admin for unknown or missing roles (back-compat
    with v0.2.0 tokens that did not carry a role)."""
    if not role:
        return "tenant_admin"
    return _ROLE_TO_ENUM.get(role, "tenant_admin")


def authenticate(authorization: str = Header(None)) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    principal = TOKENS.get(token)
    if not principal:
        raise HTTPException(401, "Invalid bearer token")
    # Normalize the principal so callers can rely on these keys.
    if "tenant_id" not in principal:
        raise HTTPException(500, "Token misconfigured: tenant_id missing")
    principal.setdefault("role", "tenant_admin")
    return principal


def require_write(principal: dict = Depends(authenticate)) -> dict[str, Any]:
    """Authenticate AND require a role permitted to write.
    Also requires user_id on the principal so writes can be attributed."""
    role = principal.get("role", "")
    if role not in WRITE_ROLES:
        raise HTTPException(403, f"Role '{role}' is not permitted to write")
    if not principal.get("user_id"):
        raise HTTPException(
            500,
            "Token misconfigured: user_id required on principal for writes. "
            "Update FOCMS_API_TOKENS_JSON to include user_id alongside tenant_id.",
        )
    return principal


# ---------------------------------------------------------------------------
# DB tx helper (RLS-bound)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def tx(request: Request, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
    """Acquire a pooled connection, start a tx, SET LOCAL the tenant id.
    All RLS-protected reads and writes go through this helper."""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)",
                str(tenant_id),
            )
            yield conn


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


async def write_audit(
    conn: asyncpg.Connection,
    *,
    actor_user_id: str,
    actor_role: str,
    tenant_id: str,
    action: str,
    target_table: str,
    target_id: Optional[str] = None,
    target_label: Optional[str] = None,
    new_value: Optional[dict] = None,
    prior_value: Optional[dict] = None,
    request_id: Optional[str] = None,
) -> None:
    """Best-effort audit write. Failures are logged but do not raise."""
    try:
        await conn.execute(
            """
            INSERT INTO audit_log (
                actor_user_id, actor_role, tenant_id, action,
                target_table, target_id, target_label,
                new_value, prior_value, request_id
            ) VALUES ($1, $2::tenant_role_enum, $3, $4::audit_action_enum,
                      $5, $6, $7, $8::jsonb, $9::jsonb, $10)
            """,
            UUID(actor_user_id), role_to_enum(actor_role), UUID(tenant_id), action,
            target_table,
            UUID(target_id) if target_id else None,
            target_label,
            json.dumps(new_value) if new_value is not None else None,
            json.dumps(prior_value) if prior_value is not None else None,
            request_id,
        )
    except Exception as exc:
        # Audit failure is observable but never blocks the write.
        log.warning("audit_log write failed: %s", exc)


# ---------------------------------------------------------------------------
# Health + types (unchanged)
# ---------------------------------------------------------------------------


@app.get("/focms/v1/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "version": "0.3.2"}


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
            UNION ALL SELECT 'archive_entries', count(*) FROM archive_entries
            ORDER BY type_name
        """)
        return {"types": [{"name": r["type_name"], "count": r["n"]} for r in rows]}


# ---------------------------------------------------------------------------
# Student reads (unchanged)
# ---------------------------------------------------------------------------


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
            SELECT to_jsonb(t)::text AS rec FROM {table} t
            WHERE student_id = $1 AND deleted_at IS NULL
            ORDER BY COALESCE(updated_at, created_at) DESC LIMIT $2
            """,
            student_id, limit,
        )
        # to_jsonb()::text returns a JSON string; parse client-side. This is
        # necessary because PgBouncer transaction mode + statement_cache_size=0
        # prevents asyncpg from auto-decoding JSONB type values.
        return {
            "type": type,
            "student_id": str(student_id),
            "count": len(rows),
            "records": [json.loads(r["rec"]) for r in rows],
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
# Power Index (unchanged from v0.2.0)
# ---------------------------------------------------------------------------

PI_WEIGHTS = [1.00, 1.00, 0.25, 0.05]


def _compute_pp(seconds: float, event: str, course: str,
                base_scy: dict, lcm_to_scy_factor: float):
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
    parts = label.rsplit(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (label, "")


@app.get("/focms/v1/student/{student_id}/computed/power-index")
async def get_power_index(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Compute Swimcloud-style Power Index for the student."""
    async with tx(request, principal["tenant_id"]) as conn:
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
        bests = await conn.fetch("""
            SELECT title, value_numeric AS seconds
            FROM personal_records
            WHERE student_id = $1
              AND record_kind = 'swim_best'
              AND deleted_at IS NULL
              AND value_numeric IS NOT NULL
        """, student_id)
    pps = []
    for r in bests:
        event, course = _split_event_course(r["title"])
        pp = _compute_pp(float(r["seconds"]), event, course, base_scy, lcm_factor)
        if pp is not None:
            pps.append((r["title"], pp))
    pps.sort(key=lambda x: x[1])
    top_4_raw = pps[:4]
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


# ===========================================================================
# v0.3.0 — Write surface
# ===========================================================================


class ArchiveEntryCreate(BaseModel):
    """Request body for POST /focms/v1/archive_entries."""
    model_config = ConfigDict(extra="forbid")

    archive_type: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    archive_date: Optional[date] = None         # defaults to CURRENT_DATE
    version: Optional[str] = None
    pillar: Optional[str] = None
    detail: Optional[str] = None
    detail_html: Optional[str] = None
    code_kind: Optional[str] = None
    language: Optional[str] = None
    lines_count: Optional[int] = None
    related_files: Optional[list[str]] = None
    related_records: Optional[list[str]] = None
    related_entity_table: Optional[str] = None
    related_entity_id: Optional[UUID] = None
    source: Optional[str] = None                # defaults to 'native_postgres'
    source_url: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None
    visibility: Optional[str] = None            # defaults to 'private'


class ArchiveEntryPatch(BaseModel):
    """Request body for PATCH /focms/v1/archive_entries/{id}. All fields optional."""
    model_config = ConfigDict(extra="forbid")

    archive_type: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    archive_date: Optional[date] = None
    version: Optional[str] = None
    pillar: Optional[str] = None
    detail: Optional[str] = None
    detail_html: Optional[str] = None
    code_kind: Optional[str] = None
    language: Optional[str] = None
    lines_count: Optional[int] = None
    related_files: Optional[list[str]] = None
    related_records: Optional[list[str]] = None
    related_entity_table: Optional[str] = None
    related_entity_id: Optional[UUID] = None
    source_url: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None
    visibility: Optional[str] = None


class AppendDetail(BaseModel):
    """Request body for POST /focms/v1/archive_entries/{id}/append-detail."""
    model_config = ConfigDict(extra="forbid")
    text: str = Field(..., min_length=1)


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    """Convert an asyncpg Record into a JSON-safe dict."""
    out = {}
    for k, v in dict(row).items():
        if isinstance(v, UUID):
            out[k] = str(v)
        elif isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _pop_audit_action(result: dict[str, Any]) -> str:
    """Pop the `_was_insert` marker from a RETURNING dict and translate it
    to an audit action string. Insert -> 'create'. Update (via ON CONFLICT)
    -> 'update'. Defaults to 'create' when the marker is absent so
    non-upsert paths keep their existing behavior."""
    was_insert = result.pop("_was_insert", True)
    return "create" if was_insert else "update"


@app.post("/focms/v1/archive_entries", status_code=201)
async def create_archive_entry(
    body: ArchiveEntryCreate,
    request: Request,
    upsert: bool = Query(False, description="If true and source_id+source_system are set, upsert on (source_system, source_id)."),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Insert a new archive entry. Returns the row.

    If ?upsert=true and both source_system + source_id are provided, the
    insert becomes an upsert on the existing partial unique index, returning
    the updated row.
    """
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    archive_date_val = body.archive_date or date.today()

    common_cols = (
        body.archive_type, archive_date_val, body.version, body.pillar,
        body.title, body.summary, body.detail, body.detail_html,
        body.code_kind, body.language, body.lines_count,
        body.related_files, body.related_records,
        body.related_entity_table, body.related_entity_id,
        body.source or "native_postgres",
        body.source_url, body.source_system, body.source_id,
        body.visibility or "private",
        UUID(user_id), UUID(tenant_id),
    )

    insert_sql = """
        INSERT INTO archive_entries (
            archive_type, archive_date, version, pillar, title, summary,
            detail, detail_html, code_kind, language, lines_count,
            related_files, related_records,
            related_entity_table, related_entity_id,
            source, source_url, source_system, source_id,
            visibility, created_by, tenant_id
        ) VALUES (
            $1, $2, $3, $4::pillar_enum, $5, $6,
            $7, $8, $9, $10, $11,
            $12, $13,
            $14, $15,
            $16, $17, $18, $19,
            $20::visibility_enum, $21, $22
        )
    """
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            archive_type    = EXCLUDED.archive_type,
            archive_date    = EXCLUDED.archive_date,
            version         = EXCLUDED.version,
            pillar          = EXCLUDED.pillar,
            title           = EXCLUDED.title,
            summary         = EXCLUDED.summary,
            detail          = EXCLUDED.detail,
            detail_html     = EXCLUDED.detail_html,
            code_kind       = EXCLUDED.code_kind,
            language        = EXCLUDED.language,
            lines_count     = EXCLUDED.lines_count,
            related_files   = EXCLUDED.related_files,
            related_records = EXCLUDED.related_records,
            source_url      = EXCLUDED.source_url,
            visibility      = EXCLUDED.visibility,
            updated_by      = EXCLUDED.created_by,
            updated_at      = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *common_cols)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action=audit_action,
            target_table="archive_entries",
            target_id=result["id"],
            target_label=body.title,
            new_value={
                "source_id": body.source_id,
                "version": body.version,
                "archive_type": body.archive_type,
            },
        )
    return result


@app.get("/focms/v1/archive_entries/{entry_id}")
async def get_archive_entry(
    entry_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    async with tx(request, principal["tenant_id"]) as conn:
        row = await conn.fetchrow(
            "SELECT * FROM archive_entries WHERE id = $1 AND deleted_at IS NULL",
            entry_id,
        )
        if not row:
            raise HTTPException(404, "Archive entry not found")
        return _row_to_dict(row)


@app.patch("/focms/v1/archive_entries/{entry_id}")
async def patch_archive_entry(
    entry_id: UUID,
    body: ArchiveEntryPatch,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Update fields on an archive entry. Only fields present in the body are
    written. Returns the updated row."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(400, "No fields to update")

    # Build SET clause dynamically with $N positional params
    type_casts = {
        "pillar": "::pillar_enum",
        "visibility": "::visibility_enum",
    }
    set_parts = []
    params = []
    for i, (col, val) in enumerate(fields.items(), start=1):
        cast = type_casts.get(col, "")
        set_parts.append(f"{col} = ${i}{cast}")
        # date and UUID values pass through asyncpg cleanly
        params.append(val)
    set_parts.append(f"updated_by = ${len(params) + 1}")
    params.append(UUID(user_id))
    set_parts.append("updated_at = now()")
    params.append(entry_id)
    where_param = f"${len(params)}"

    sql = f"""
        UPDATE archive_entries
        SET {', '.join(set_parts)}
        WHERE id = {where_param} AND deleted_at IS NULL
        RETURNING *
    """

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        if not row:
            raise HTTPException(404, "Archive entry not found")
        result = _row_to_dict(row)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="update",
            target_table="archive_entries",
            target_id=str(entry_id),
            new_value={k: (str(v) if isinstance(v, (UUID, date, datetime)) else v) for k, v in fields.items()},
        )
    return result


@app.post("/focms/v1/archive_entries/{entry_id}/append-detail")
async def append_detail(
    entry_id: UUID,
    body: AppendDetail,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Append text to the entry's `detail` column. Lets a client grow a doc
    without round-tripping the current value through the network. Returns
    {id, new_bytes, new_sha256}."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            UPDATE archive_entries
            SET detail = COALESCE(detail, '') || $1::text,
                updated_by = $2,
                updated_at = now()
            WHERE id = $3 AND deleted_at IS NULL
            RETURNING id,
                      octet_length(detail) AS bytes,
                      encode(sha256(convert_to(detail, 'UTF8')), 'hex') AS sha256
            """,
            body.text, UUID(user_id), entry_id,
        )
        if not row:
            raise HTTPException(404, "Archive entry not found")
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="update",
            target_table="archive_entries",
            target_id=str(entry_id),
            target_label="append_detail",
            new_value={"appended_chars": len(body.text), "total_bytes": row["bytes"]},
        )
        return {
            "id": str(row["id"]),
            "bytes": row["bytes"],
            "sha256": row["sha256"],
            "appended_chars": len(body.text),
        }


@app.delete("/focms/v1/archive_entries/{entry_id}", status_code=204)
async def delete_archive_entry(
    entry_id: UUID, request: Request,
    principal: dict = Depends(require_write),
):
    """Soft-delete: sets deleted_at and deleted_by. Returns 204."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            UPDATE archive_entries
            SET deleted_at = now(), deleted_by = $1
            WHERE id = $2 AND deleted_at IS NULL
            RETURNING id
            """,
            UUID(user_id), entry_id,
        )
        if not row:
            raise HTTPException(404, "Archive entry not found or already deleted")
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="delete",
            target_table="archive_entries",
            target_id=str(entry_id),
        )
    return JSONResponse(status_code=204, content=None)


# ---------------------------------------------------------------------------
# personal_records
# ---------------------------------------------------------------------------


class PersonalRecordCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    record_kind: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    achieved_date: Optional[date] = None
    value_numeric: Optional[float] = None
    value_text: Optional[str] = None
    value_unit: Optional[str] = None
    details: Optional[dict] = None
    achieved_at_event_id: Optional[UUID] = None
    affiliation_id: Optional[UUID] = None
    prior_value_numeric: Optional[float] = None
    total_drop_numeric: Optional[float] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/personal_records", status_code=201)
async def create_personal_record(
    body: PersonalRecordCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    insert_sql = """
        INSERT INTO personal_records (
            tenant_id, student_id, record_kind, title, achieved_date,
            value_numeric, value_text, value_unit, details,
            achieved_at_event_id, affiliation_id,
            prior_value_numeric, total_drop_numeric,
            notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::record_kind_enum, $4, $5,
            $6, $7, $8, $9::jsonb,
            $10, $11,
            $12, $13,
            $14, $15, $16::visibility_enum,
            $17, $18, $19
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.record_kind, body.title,
        body.achieved_date,
        body.value_numeric, body.value_text, body.value_unit,
        json.dumps(body.details) if body.details is not None else "{}",
        body.achieved_at_event_id, body.affiliation_id,
        body.prior_value_numeric, body.total_drop_numeric,
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            record_kind         = EXCLUDED.record_kind,
            title               = EXCLUDED.title,
            achieved_date       = EXCLUDED.achieved_date,
            value_numeric       = EXCLUDED.value_numeric,
            value_text          = EXCLUDED.value_text,
            value_unit          = EXCLUDED.value_unit,
            details             = EXCLUDED.details,
            prior_value_numeric = EXCLUDED.prior_value_numeric,
            total_drop_numeric  = EXCLUDED.total_drop_numeric,
            notes               = EXCLUDED.notes,
            public_description  = EXCLUDED.public_description,
            visibility          = EXCLUDED.visibility,
            updated_by          = EXCLUDED.created_by,
            updated_at          = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action=audit_action,
            target_table="personal_records",
            target_id=result["id"],
            target_label=body.title,
            new_value={"record_kind": body.record_kind, "source_id": body.source_id},
        )
    return result


# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------


class EventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    event_type: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    event_date: date
    event_end_date: Optional[date] = None
    location_name: Optional[str] = None
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    duration_minutes: Optional[int] = None
    details: Optional[dict] = None
    affiliation_id: Optional[UUID] = None
    related_event_id: Optional[UUID] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/events", status_code=201)
async def create_event(
    body: EventCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    insert_sql = """
        INSERT INTO events (
            tenant_id, student_id, event_type, title, event_date, event_end_date,
            location_name, location_city, location_state, duration_minutes,
            details, affiliation_id, related_event_id,
            notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::event_type_enum, $4, $5, $6,
            $7, $8, $9, $10,
            $11::jsonb, $12, $13,
            $14, $15, $16::visibility_enum,
            $17, $18, $19
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.event_type, body.title,
        body.event_date, body.event_end_date,
        body.location_name, body.location_city, body.location_state, body.duration_minutes,
        json.dumps(body.details) if body.details is not None else "{}",
        body.affiliation_id, body.related_event_id,
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            event_type         = EXCLUDED.event_type,
            title              = EXCLUDED.title,
            event_date         = EXCLUDED.event_date,
            event_end_date     = EXCLUDED.event_end_date,
            location_name      = EXCLUDED.location_name,
            location_city      = EXCLUDED.location_city,
            location_state     = EXCLUDED.location_state,
            duration_minutes   = EXCLUDED.duration_minutes,
            details            = EXCLUDED.details,
            affiliation_id     = EXCLUDED.affiliation_id,
            related_event_id   = EXCLUDED.related_event_id,
            notes              = EXCLUDED.notes,
            public_description = EXCLUDED.public_description,
            visibility         = EXCLUDED.visibility,
            updated_by         = EXCLUDED.created_by,
            updated_at         = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action=audit_action,
            target_table="events",
            target_id=result["id"],
            target_label=body.title,
            new_value={"event_type": body.event_type, "source_id": body.source_id},
        )
    return result


# ---------------------------------------------------------------------------
# assessments
# ---------------------------------------------------------------------------


class AssessmentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    assessment_type: str = Field(..., min_length=1)
    instrument: str = Field(..., min_length=1)
    test_date: date
    subject: Optional[str] = None
    school_year: Optional[str] = None
    term: Optional[str] = None
    grade_at_test: Optional[str] = None
    age_at_test: Optional[float] = None
    score: Optional[float] = None
    score_type: Optional[str] = None
    percentile: Optional[float] = None
    performance_band: Optional[str] = None
    range_low: Optional[float] = None
    range_high: Optional[float] = None
    achievement_norm: Optional[str] = None
    standard_error: Optional[float] = None
    readability_type: Optional[str] = None
    readability_low: Optional[float] = None
    readability_high: Optional[float] = None
    subscores: Optional[list] = None
    cognitive_strengths: Optional[list[str]] = None
    cognitive_skills_to_support: Optional[list[str]] = None
    cognitive_invalid_domains: Optional[list[str]] = None
    projected_proficient: Optional[bool] = None
    projected_sat_on_track: Optional[bool] = None
    projected_act: Optional[float] = None
    details: Optional[dict] = None
    administered_by: Optional[str] = None
    duration_minutes: Optional[int] = None
    rapid_guessing_pct: Optional[float] = None
    report_files: Optional[list] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/assessments", status_code=201)
async def create_assessment(
    body: AssessmentCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    insert_sql = """
        INSERT INTO assessments (
            tenant_id, student_id, assessment_type, instrument, subject,
            test_date, school_year, term, grade_at_test, age_at_test,
            score, score_type, percentile, performance_band,
            range_low, range_high, achievement_norm, standard_error,
            readability_type, readability_low, readability_high,
            subscores, cognitive_strengths, cognitive_skills_to_support,
            cognitive_invalid_domains, projected_proficient,
            projected_sat_on_track, projected_act, details,
            administered_by, duration_minutes, rapid_guessing_pct,
            report_files, notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::assessment_type_enum, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20, $21,
            $22::jsonb, $23, $24,
            $25, $26,
            $27, $28, $29::jsonb,
            $30, $31, $32,
            $33::jsonb, $34, $35, $36::visibility_enum,
            $37, $38, $39
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.assessment_type, body.instrument, body.subject,
        body.test_date, body.school_year, body.term, body.grade_at_test, body.age_at_test,
        body.score, body.score_type, body.percentile, body.performance_band,
        body.range_low, body.range_high, body.achievement_norm, body.standard_error,
        body.readability_type, body.readability_low, body.readability_high,
        json.dumps(body.subscores) if body.subscores is not None else "[]",
        body.cognitive_strengths, body.cognitive_skills_to_support,
        body.cognitive_invalid_domains, body.projected_proficient,
        body.projected_sat_on_track, body.projected_act,
        json.dumps(body.details) if body.details is not None else "{}",
        body.administered_by, body.duration_minutes, body.rapid_guessing_pct,
        json.dumps(body.report_files) if body.report_files is not None else "[]",
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            instrument         = EXCLUDED.instrument,
            subject            = EXCLUDED.subject,
            test_date          = EXCLUDED.test_date,
            score              = EXCLUDED.score,
            percentile         = EXCLUDED.percentile,
            performance_band   = EXCLUDED.performance_band,
            details            = EXCLUDED.details,
            notes              = EXCLUDED.notes,
            visibility         = EXCLUDED.visibility,
            updated_by         = EXCLUDED.created_by,
            updated_at         = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action=audit_action,
            target_table="assessments",
            target_id=result["id"],
            target_label=body.instrument,
            new_value={"assessment_type": body.assessment_type, "source_id": body.source_id},
        )
    return result


# ---------------------------------------------------------------------------
# v0.3.1: affiliations, goals, courses, digital_presence
# ---------------------------------------------------------------------------


class AffiliationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    affiliation_type: str = Field(..., min_length=1)
    organization_name: str = Field(..., min_length=1)
    organization_url: Optional[str] = None
    organization_naics: Optional[str] = None
    organization_city: Optional[str] = None
    organization_state: Optional[str] = None
    organization_country: Optional[str] = None
    role: Optional[str] = None
    role_start_date: Optional[date] = None
    role_end_date: Optional[date] = None
    coach_name: Optional[str] = None
    coach_email: Optional[str] = None
    coach_phone: Optional[str] = None
    coach_role: Optional[str] = None
    weekly_hours: Optional[float] = None
    total_hours: Optional[float] = None
    verification_contact_name: Optional[str] = None
    verification_contact_email: Optional[str] = None
    verification_contact_phone: Optional[str] = None
    is_verified: Optional[bool] = None
    details: Optional[dict] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/affiliations", status_code=201)
async def create_affiliation(
    body: AffiliationCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    insert_sql = """
        INSERT INTO affiliations (
            tenant_id, student_id, affiliation_type, organization_name,
            organization_url, organization_naics, organization_city,
            organization_state, organization_country, role,
            role_start_date, role_end_date, coach_name, coach_email,
            coach_phone, coach_role, weekly_hours, total_hours,
            verification_contact_name, verification_contact_email,
            verification_contact_phone, is_verified, details, notes,
            public_description, visibility, source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::affiliation_type_enum, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20,
            $21, COALESCE($22, false), $23::jsonb, $24,
            $25, $26::visibility_enum, $27, $28, $29
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.affiliation_type, body.organization_name,
        body.organization_url, body.organization_naics, body.organization_city,
        body.organization_state, body.organization_country, body.role,
        body.role_start_date, body.role_end_date, body.coach_name, body.coach_email,
        body.coach_phone, body.coach_role, body.weekly_hours, body.total_hours,
        body.verification_contact_name, body.verification_contact_email,
        body.verification_contact_phone, body.is_verified,
        json.dumps(body.details) if body.details is not None else "{}",
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            affiliation_type    = EXCLUDED.affiliation_type,
            organization_name   = EXCLUDED.organization_name,
            organization_url    = EXCLUDED.organization_url,
            role                = EXCLUDED.role,
            role_start_date     = EXCLUDED.role_start_date,
            role_end_date       = EXCLUDED.role_end_date,
            weekly_hours        = EXCLUDED.weekly_hours,
            total_hours         = EXCLUDED.total_hours,
            details             = EXCLUDED.details,
            notes               = EXCLUDED.notes,
            visibility          = EXCLUDED.visibility,
            updated_by          = EXCLUDED.created_by,
            updated_at          = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"
    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action=audit_action, target_table="affiliations",
            target_id=result["id"], target_label=body.organization_name,
            new_value={"affiliation_type": body.affiliation_type, "source_id": body.source_id},
        )
    return result


class GoalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    title: str = Field(..., min_length=1)
    pillar: Optional[str] = None
    parent_goal_id: Optional[UUID] = None
    category: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    target_date: Optional[date] = None
    target_value_numeric: Optional[float] = None
    target_value_unit: Optional[str] = None
    current_value_numeric: Optional[float] = None
    progress_pct: Optional[float] = None
    achieved_at: Optional[date] = None
    achieved_value_numeric: Optional[float] = None
    related_university_leaid: Optional[str] = None
    related_pathway: Optional[str] = None
    related_scholarship_program_id: Optional[UUID] = None
    details: Optional[dict] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/goals", status_code=201)
async def create_goal(
    body: GoalCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    insert_sql = """
        INSERT INTO goals (
            tenant_id, student_id, parent_goal_id, pillar, category,
            title, description, status, target_date,
            target_value_numeric, target_value_unit, current_value_numeric,
            progress_pct, achieved_at, achieved_value_numeric,
            related_university_leaid, related_pathway, related_scholarship_program_id,
            details, notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3, COALESCE($4, 'cross_cutting')::pillar_enum, $5,
            $6, $7, COALESCE($8, 'active')::goal_status_enum, $9,
            $10, $11, $12,
            $13, $14, $15,
            $16, $17::pathway_enum, $18,
            $19::jsonb, $20, $21, $22::visibility_enum,
            $23, $24, $25
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.parent_goal_id, body.pillar, body.category,
        body.title, body.description, body.status, body.target_date,
        body.target_value_numeric, body.target_value_unit, body.current_value_numeric,
        body.progress_pct, body.achieved_at, body.achieved_value_numeric,
        body.related_university_leaid, body.related_pathway, body.related_scholarship_program_id,
        json.dumps(body.details) if body.details is not None else "{}",
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            title                = EXCLUDED.title,
            description          = EXCLUDED.description,
            status               = EXCLUDED.status,
            target_date          = EXCLUDED.target_date,
            target_value_numeric = EXCLUDED.target_value_numeric,
            current_value_numeric = EXCLUDED.current_value_numeric,
            progress_pct         = EXCLUDED.progress_pct,
            achieved_at          = EXCLUDED.achieved_at,
            details              = EXCLUDED.details,
            notes                = EXCLUDED.notes,
            visibility           = EXCLUDED.visibility,
            updated_by           = EXCLUDED.created_by,
            updated_at           = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"
    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action=audit_action, target_table="goals",
            target_id=result["id"], target_label=body.title,
            new_value={"pillar": body.pillar, "source_id": body.source_id},
        )
    return result


class CourseCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    course_kind: str = Field(..., min_length=1)
    course_name: str = Field(..., min_length=1)
    course_code: Optional[str] = None
    subject_area: Optional[str] = None
    rigor_level: Optional[str] = None
    provider_name: Optional[str] = None
    provider_leaid: Optional[str] = None
    provider_uni_leaid: Optional[str] = None
    school_year: Optional[str] = None
    term: Optional[str] = None
    grade_level: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    grade: Optional[str] = None
    grade_numeric: Optional[float] = None
    credits: Optional[float] = None
    weighted_credits: Optional[float] = None
    is_complete: Optional[bool] = None
    instructor_name: Optional[str] = None
    instructor_email: Optional[str] = None
    details: Optional[dict] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/courses", status_code=201)
async def create_course(
    body: CourseCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    insert_sql = """
        INSERT INTO courses (
            tenant_id, student_id, course_kind, course_name, course_code,
            subject_area, rigor_level, provider_name, provider_leaid,
            provider_uni_leaid, school_year, term, grade_level,
            start_date, end_date, grade, grade_numeric, credits,
            weighted_credits, is_complete, instructor_name, instructor_email,
            details, notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::course_kind_enum, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13,
            $14, $15, $16, $17, $18,
            $19, COALESCE($20, false), $21, $22,
            $23::jsonb, $24, $25, $26::visibility_enum,
            $27, $28, $29
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.course_kind, body.course_name, body.course_code,
        body.subject_area, body.rigor_level, body.provider_name, body.provider_leaid,
        body.provider_uni_leaid, body.school_year, body.term, body.grade_level,
        body.start_date, body.end_date, body.grade, body.grade_numeric, body.credits,
        body.weighted_credits, body.is_complete, body.instructor_name, body.instructor_email,
        json.dumps(body.details) if body.details is not None else "{}",
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            course_name      = EXCLUDED.course_name,
            course_code      = EXCLUDED.course_code,
            rigor_level      = EXCLUDED.rigor_level,
            grade            = EXCLUDED.grade,
            grade_numeric    = EXCLUDED.grade_numeric,
            credits          = EXCLUDED.credits,
            weighted_credits = EXCLUDED.weighted_credits,
            is_complete      = EXCLUDED.is_complete,
            details          = EXCLUDED.details,
            notes            = EXCLUDED.notes,
            visibility       = EXCLUDED.visibility,
            updated_by       = EXCLUDED.created_by,
            updated_at       = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"
    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action=audit_action, target_table="courses",
            target_id=result["id"], target_label=body.course_name,
            new_value={"course_kind": body.course_kind, "source_id": body.source_id},
        )
    return result


class DigitalPresenceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    student_id: UUID
    presence_kind: str = Field(..., min_length=1)
    platform: str = Field(..., min_length=1)
    handle: str = Field(..., min_length=1)
    profile_url: Optional[str] = None
    display_name: Optional[str] = None
    is_primary: Optional[bool] = None
    is_minor_managed: Optional[bool] = None
    is_public_findable: Optional[bool] = None
    started_at: Optional[date] = None
    closed_at: Optional[date] = None
    follower_count: Optional[int] = None
    follower_count_as_of: Optional[datetime] = None
    privacy_setting: Optional[str] = None
    details: Optional[dict] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    visibility: Optional[str] = None
    source_system: Optional[str] = None
    source_id: Optional[str] = None


@app.post("/focms/v1/digital_presence", status_code=201)
async def create_digital_presence(
    body: DigitalPresenceCreate,
    request: Request,
    upsert: bool = Query(False),
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    insert_sql = """
        INSERT INTO digital_presence (
            tenant_id, student_id, presence_kind, platform, handle,
            profile_url, display_name, is_primary, is_minor_managed,
            is_public_findable, started_at, closed_at,
            follower_count, follower_count_as_of, privacy_setting,
            details, notes, public_description, visibility,
            source_system, source_id, created_by
        ) VALUES (
            $1, $2, $3::presence_kind_enum, $4, $5,
            $6, $7, COALESCE($8, false), COALESCE($9, false),
            $10, $11, $12,
            $13, $14, $15,
            $16::jsonb, $17, $18, $19::visibility_enum,
            $20, $21, $22
        )
    """
    params = (
        UUID(tenant_id), body.student_id, body.presence_kind, body.platform, body.handle,
        body.profile_url, body.display_name, body.is_primary, body.is_minor_managed,
        body.is_public_findable, body.started_at, body.closed_at,
        body.follower_count, body.follower_count_as_of, body.privacy_setting,
        json.dumps(body.details) if body.details is not None else "{}",
        body.notes, body.public_description, body.visibility or "private",
        body.source_system, body.source_id, UUID(user_id),
    )
    if upsert and body.source_system and body.source_id:
        sql = insert_sql + """
        ON CONFLICT (source_system, source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
            platform         = EXCLUDED.platform,
            handle           = EXCLUDED.handle,
            profile_url      = EXCLUDED.profile_url,
            display_name     = EXCLUDED.display_name,
            is_primary       = EXCLUDED.is_primary,
            is_minor_managed = EXCLUDED.is_minor_managed,
            follower_count   = EXCLUDED.follower_count,
            details          = EXCLUDED.details,
            notes            = EXCLUDED.notes,
            visibility       = EXCLUDED.visibility,
            updated_by       = EXCLUDED.created_by,
            updated_at       = now()
        RETURNING *, (xmax = 0) AS _was_insert
        """
    else:
        sql = insert_sql + " RETURNING *, (xmax = 0) AS _was_insert"
    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, f"Duplicate: {exc.detail or exc}") from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        audit_action = _pop_audit_action(result)
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action=audit_action, target_table="digital_presence",
            target_id=result["id"], target_label=f"{body.platform}/{body.handle}",
            new_value={"presence_kind": body.presence_kind, "source_id": body.source_id},
        )
    return result


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------


@app.exception_handler(asyncpg.PostgresError)
async def pg_error_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    log.error("Postgres error: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "database_error", "detail": str(exc)},
    )
