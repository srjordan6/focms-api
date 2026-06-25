"""focms_milestones.py - Life milestones tracking for the FOCMS parent portal.

v0.8.2 (2026-06-25) - Millstones and Milestones pillar.
  - v0.8.2: fixed audit_log INSERT — schema has no 'details' column; action is an
            enum (audit_action_enum) restricted to {create, update, delete,
            visibility_change, login, logout, export, consent_grant, consent_revoke,
            role_assign, role_revoke}. Semantic action names now go in target_label
            and the payload jsonb goes in new_value.
  - v0.8.1: corrected imports to use _require_parent_token (FastAPI Depends pattern),
            request.app.state.pool for DB acquisition, _bind_tenant for RLS context.
            v0.8.0 had wrong import names (_resolve_parent_token / _get_conn).

Endpoints (all on /focms/v1/parent/students/{student_id}/milestones):

  GET    /picker
         Returns the full milestone catalog (platform 101 + any tenant custom rows)
         with per-row status (achieved/now/upcoming/backfill_candidate/available)
         plus a separate stream of custom (non-catalog) events logged for this student.

  POST   /capture
         Log an achievement against a known catalog code.
         Body: {milestone_code, event_date, event_notes?, artifact_url?, visibility?}

  POST   /custom
         Log a custom (free-text) life event. Optionally also create a tenant-scoped
         catalog row so future siblings/children can reuse the milestone definition.
         Body: {custom_title, custom_category?, event_date, event_notes?, artifact_url?,
                visibility?, add_to_catalog?, catalog_pillar?, catalog_age_band?}

  DELETE /{milestone_id}
         Soft-delete an achievement record (sets deleted_at).

Data destinations:
  - Achievement records:  student_life_milestones (RLS enforced via tenant_id)
  - Custom catalog rows:  life_milestones_catalog (tenant_id = ctx.tenant_id)
  - Audit trail:          audit_log (actor_role='tenant_admin', actor_user_id=NULL)

Token verification re-uses _require_parent_token from focms_parent_portal as a
FastAPI dependency. The audit_log write is wrapped in a SAVEPOINT so FK violations
on actor_user_id (which we intentionally pass as NULL, since parent tokens are not
users.id principals) do not poison the parent INSERT transaction. Pattern
established in v0.7.5.

Deployment:
  1. Upload this file alongside focms_parent_portal.py and focms_api.py at repo root.
  2. In focms_api.py add (near the other include_router calls):
         from focms_milestones import router as milestones_router
         app.include_router(milestones_router)
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

# Re-use the existing parent portal token resolver, RLS binder, and context type.
# v0.7.0 names: _require_parent_token (FastAPI dependency), _bind_tenant (RLS),
# ParentContext (dataclass with tenant_id, student_id, token_id, ...).
from focms_parent_portal import _require_parent_token, _bind_tenant, ParentContext

logger = logging.getLogger("focms.milestones")

router = APIRouter(prefix="/focms/v1/parent", tags=["parent-milestones"])


# ============================================================================
#  Pydantic models
# ============================================================================

class MilestoneCatalogRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    code: str
    title: str
    description: Optional[str] = None
    age_band: str
    typical_age_min: Optional[float] = None
    typical_age_max: Optional[float] = None
    pillar: str
    sub_pillar: Optional[str] = None
    category: Optional[str] = None
    universality: Optional[str] = None
    traits: Optional[list[str]] = None
    source_kind: str  # 'platform' | 'tenant_custom'
    status: str       # 'achieved' | 'now' | 'upcoming' | 'backfill_candidate' | 'available'
    first_hit: Optional[date] = None
    event_count: int = 0
    milestone_id: Optional[UUID] = None  # student_life_milestones.id when achieved


class CustomMilestoneEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    custom_title: str
    custom_category: Optional[str] = None
    event_date: date
    event_notes: Optional[str] = None
    artifact_url: Optional[str] = None
    visibility: str = "family"


class MilestonePickerResponse(BaseModel):
    student_id: UUID
    student_age_years: Optional[float] = None
    catalog_count: int
    custom_count: int
    catalog: list[MilestoneCatalogRow]
    custom_events: list[CustomMilestoneEvent]


class MilestoneCaptureRequest(BaseModel):
    milestone_code: str = Field(min_length=1, max_length=120)
    event_date: date
    event_notes: Optional[str] = Field(default=None, max_length=2000)
    artifact_url: Optional[str] = Field(default=None, max_length=500)
    visibility: str = Field(default="family")


class CustomMilestoneRequest(BaseModel):
    custom_title: str = Field(min_length=1, max_length=200)
    custom_category: Optional[str] = Field(default=None, max_length=80)
    event_date: date
    event_notes: Optional[str] = Field(default=None, max_length=2000)
    artifact_url: Optional[str] = Field(default=None, max_length=500)
    visibility: str = Field(default="family")
    # Optional — when True, also writes a tenant-scoped row to life_milestones_catalog
    add_to_catalog: bool = False
    catalog_pillar: Optional[str] = Field(default=None, max_length=80)
    catalog_age_band: Optional[str] = Field(default=None, max_length=40)


# ============================================================================
#  Helpers
# ============================================================================

_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def _slugify(s: str, max_len: int = 50) -> str:
    """Lowercase, replace non-alphanumerics with _, trim length, dedupe underscores."""
    slug = _SLUG_RE.sub("_", s.lower()).strip("_")
    slug = re.sub(r"_+", "_", slug)
    return slug[:max_len] if len(slug) > max_len else slug


def _assert_student_match(ctx: ParentContext, student_id: UUID) -> None:
    if str(ctx.student_id) != str(student_id):
        raise HTTPException(403, "Token does not authorize access to this student.")


# audit_log.action is restricted to the audit_action_enum values
# (create / update / delete / visibility_change / login / logout / export / ...).
# Milestone events map onto this set; the semantic milestone action name is preserved
# in audit_log.target_label so it remains queryable.
_AUDIT_ACTION_MAP: dict[str, str] = {
    "milestone_captured":        "create",
    "custom_milestone_captured": "create",
    "milestone_deleted":         "delete",
    "milestone_updated":         "update",
}


async def _audit_log_milestone(
    conn: asyncpg.Connection,
    ctx: ParentContext,
    action: str,
    target_table: str,
    target_id: UUID,
    details: dict[str, Any],
) -> None:
    """SAVEPOINT-wrapped audit_log insert. Never raises out of the parent transaction.

    Schema notes:
      - actor_user_id is NULL because parent tokens are not users.id principals
        (FK violation would otherwise poison the transaction; v0.7.5 pattern).
      - actor_role is 'tenant_admin' (the role granted by the access token).
      - session_id carries the token UUID as the identity correlation key.
      - audit_log.action is an enum (audit_action_enum); we map semantic milestone
        action names to one of {create, update, delete, ...} and keep the
        semantic name in target_label for downstream filtering.
      - audit_log has no generic 'details' column — the payload jsonb goes in
        new_value (a column meant for the post-change value of the row).
    """
    enum_action = _AUDIT_ACTION_MAP.get(action, "create")
    try:
        async with conn.transaction():  # SAVEPOINT
            await conn.execute(
                """
                INSERT INTO audit_log (
                    tenant_id, student_id, actor_user_id, actor_role, session_id,
                    action, target_table, target_id, target_label, new_value, occurred_at
                ) VALUES (
                    $1, $2, NULL, 'tenant_admin', $3,
                    $4::audit_action_enum, $5, $6, $7, $8::jsonb, now()
                )
                """,
                ctx.tenant_id,
                ctx.student_id,
                str(ctx.token_id),
                enum_action,
                target_table,
                target_id,
                action,  # semantic milestone action name preserved here
                json.dumps(details, default=str),
            )
    except Exception as e:
        logger.warning(
            "milestone audit_log failed (action=%s target=%s/%s): %s",
            action, target_table, target_id, e,
        )


# ============================================================================
#  Endpoints
# ============================================================================

@router.get(
    "/students/{student_id}/milestones/picker",
    response_model=MilestonePickerResponse,
)
async def get_milestone_picker(
    student_id: UUID,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> MilestonePickerResponse:
    """Return the full milestone picker payload: catalog + custom events for this student."""
    _assert_student_match(ctx, student_id)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            bd_row = await conn.fetchrow(
                "SELECT birth_date FROM students WHERE id = $1",
                student_id,
            )
            if not bd_row:
                raise HTTPException(404, "Student not found.")
            birth_date = bd_row["birth_date"]
            student_age: Optional[float] = None
            if birth_date:
                student_age = round((date.today() - birth_date).days / 365.25, 2)

            # Picker query — catalog × this student's hits
            catalog_rows = await conn.fetch(
                """
                WITH hit AS (
                    SELECT
                        milestone_code AS code,
                        MIN(event_date) AS first_hit,
                        COUNT(*)        AS event_count,
                        MIN(id)         AS milestone_id
                    FROM student_life_milestones
                    WHERE student_id = $1
                      AND tenant_id  = $2
                      AND deleted_at IS NULL
                      AND milestone_code IS NOT NULL
                    GROUP BY milestone_code
                )
                SELECT
                    c.code, c.title, c.description, c.age_band,
                    c.typical_age_min, c.typical_age_max,
                    c.pillar, c.sub_pillar, c.category, c.universality,
                    c.admission_traits_developed AS traits,
                    CASE WHEN c.tenant_id IS NULL THEN 'platform' ELSE 'tenant_custom' END AS source_kind,
                    CASE
                        WHEN h.code IS NOT NULL THEN 'achieved'
                        WHEN $3::numeric IS NULL THEN 'available'
                        WHEN c.typical_age_max < $3::numeric THEN 'backfill_candidate'
                        WHEN c.typical_age_min <= $3::numeric AND c.typical_age_max >= $3::numeric THEN 'now'
                        WHEN c.typical_age_min > $3::numeric THEN 'upcoming'
                        ELSE 'available'
                    END AS status,
                    h.first_hit,
                    COALESCE(h.event_count, 0) AS event_count,
                    h.milestone_id
                FROM life_milestones_catalog c
                LEFT JOIN hit h ON h.code = c.code
                WHERE c.deleted_at IS NULL
                  AND c.is_active = true
                  AND (c.tenant_id IS NULL OR c.tenant_id = $2)
                ORDER BY
                    c.sort_order NULLS LAST,
                    c.typical_age_min NULLS LAST,
                    c.code
                """,
                student_id,
                ctx.tenant_id,
                student_age,
            )

            # Custom (non-catalog) events for this student
            custom_rows = await conn.fetch(
                """
                SELECT id, custom_title, custom_category, event_date, event_notes,
                       artifact_url, visibility
                FROM student_life_milestones
                WHERE student_id = $1
                  AND tenant_id  = $2
                  AND deleted_at IS NULL
                  AND milestone_code IS NULL
                ORDER BY event_date DESC NULLS LAST, created_at DESC
                """,
                student_id,
                ctx.tenant_id,
            )

    catalog = []
    for r in catalog_rows:
        traits_raw = r["traits"]
        traits_list: Optional[list[str]] = None
        if traits_raw is not None:
            if isinstance(traits_raw, list):
                traits_list = traits_raw
            elif isinstance(traits_raw, str):
                try:
                    traits_list = json.loads(traits_raw)
                except json.JSONDecodeError:
                    traits_list = None
        catalog.append(
            MilestoneCatalogRow(
                code=r["code"],
                title=r["title"],
                description=r["description"],
                age_band=r["age_band"],
                typical_age_min=float(r["typical_age_min"]) if r["typical_age_min"] is not None else None,
                typical_age_max=float(r["typical_age_max"]) if r["typical_age_max"] is not None else None,
                pillar=r["pillar"],
                sub_pillar=r["sub_pillar"],
                category=r["category"],
                universality=r["universality"],
                traits=traits_list,
                source_kind=r["source_kind"],
                status=r["status"],
                first_hit=r["first_hit"],
                event_count=int(r["event_count"]),
                milestone_id=r["milestone_id"],
            )
        )

    custom_events = [
        CustomMilestoneEvent(
            id=r["id"],
            custom_title=r["custom_title"],
            custom_category=r["custom_category"],
            event_date=r["event_date"],
            event_notes=r["event_notes"],
            artifact_url=r["artifact_url"],
            visibility=r["visibility"] or "family",
        )
        for r in custom_rows
    ]

    return MilestonePickerResponse(
        student_id=student_id,
        student_age_years=student_age,
        catalog_count=len(catalog),
        custom_count=len(custom_events),
        catalog=catalog,
        custom_events=custom_events,
    )


@router.post(
    "/students/{student_id}/milestones/capture",
    status_code=201,
)
async def capture_milestone(
    student_id: UUID,
    body: MilestoneCaptureRequest,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """Log an achievement against a known catalog milestone code."""
    _assert_student_match(ctx, student_id)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            exists = await conn.fetchval(
                """
                SELECT 1 FROM life_milestones_catalog
                WHERE code = $1
                  AND deleted_at IS NULL
                  AND is_active = true
                  AND (tenant_id IS NULL OR tenant_id = $2)
                """,
                body.milestone_code,
                ctx.tenant_id,
            )
            if not exists:
                raise HTTPException(
                    404,
                    f"Milestone code '{body.milestone_code}' not found in catalog.",
                )

            inserted = await conn.fetchrow(
                """
                INSERT INTO student_life_milestones (
                    tenant_id, student_id, milestone_code,
                    event_date, event_notes, artifact_url,
                    visibility, source_system, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'parent_portal', $8)
                RETURNING id, created_at
                """,
                ctx.tenant_id,
                student_id,
                body.milestone_code,
                body.event_date,
                body.event_notes,
                body.artifact_url,
                body.visibility,
                ctx.token_id,
            )

            await _audit_log_milestone(
                conn, ctx,
                action="milestone_captured",
                target_table="student_life_milestones",
                target_id=inserted["id"],
                details={
                    "milestone_code": body.milestone_code,
                    "event_date": str(body.event_date),
                    "visibility": body.visibility,
                },
            )

    return {
        "ok": True,
        "milestone_id": str(inserted["id"]),
        "milestone_code": body.milestone_code,
        "created_at": inserted["created_at"].isoformat() if inserted["created_at"] else None,
    }


@router.post(
    "/students/{student_id}/milestones/custom",
    status_code=201,
)
async def capture_custom_milestone(
    student_id: UUID,
    body: CustomMilestoneRequest,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """Log a custom (free-text) life event. Optionally promotes it into the
    tenant-scoped catalog for future reuse."""
    _assert_student_match(ctx, student_id)

    catalog_code: Optional[str] = None
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            # Optional catalog promotion
            if body.add_to_catalog:
                if not body.catalog_pillar or not body.catalog_age_band:
                    raise HTTPException(
                        400,
                        "add_to_catalog=true requires catalog_pillar and catalog_age_band.",
                    )

                base_code = _slugify(body.custom_title)
                if not base_code:
                    raise HTTPException(400, "custom_title produced an empty slug.")

                candidate = base_code
                for attempt in range(2, 50):
                    collision = await conn.fetchval(
                        """
                        SELECT 1 FROM life_milestones_catalog
                        WHERE code = $1
                          AND deleted_at IS NULL
                          AND (tenant_id IS NULL OR tenant_id = $2)
                        """,
                        candidate, ctx.tenant_id,
                    )
                    if not collision:
                        break
                    candidate = f"{base_code}_{attempt}"
                else:
                    raise HTTPException(
                        409,
                        "Could not generate a non-colliding catalog code after 50 attempts.",
                    )

                catalog_code = candidate

                await conn.execute(
                    """
                    INSERT INTO life_milestones_catalog (
                        tenant_id, code, title, age_band, pillar, category,
                        universality, source, is_active, created_by
                    ) VALUES ($1, $2, $3, $4, $5, $6, 'common', 'parent_custom', true, $7)
                    """,
                    ctx.tenant_id,
                    catalog_code,
                    body.custom_title,
                    body.catalog_age_band,
                    body.catalog_pillar,
                    body.custom_category,
                    ctx.token_id,
                )

            inserted = await conn.fetchrow(
                """
                INSERT INTO student_life_milestones (
                    tenant_id, student_id, milestone_code,
                    custom_title, custom_category,
                    event_date, event_notes, artifact_url,
                    visibility, source_system, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'parent_portal', $10)
                RETURNING id, created_at
                """,
                ctx.tenant_id,
                student_id,
                catalog_code,
                body.custom_title,
                body.custom_category,
                body.event_date,
                body.event_notes,
                body.artifact_url,
                body.visibility,
                ctx.token_id,
            )

            await _audit_log_milestone(
                conn, ctx,
                action="custom_milestone_captured",
                target_table="student_life_milestones",
                target_id=inserted["id"],
                details={
                    "custom_title": body.custom_title,
                    "custom_category": body.custom_category,
                    "added_to_catalog": body.add_to_catalog,
                    "catalog_code": catalog_code,
                    "event_date": str(body.event_date),
                },
            )

    return {
        "ok": True,
        "milestone_id": str(inserted["id"]),
        "added_to_catalog": body.add_to_catalog,
        "catalog_code": catalog_code,
        "created_at": inserted["created_at"].isoformat() if inserted["created_at"] else None,
    }


@router.delete(
    "/students/{student_id}/milestones/{milestone_id}",
    status_code=200,
)
async def delete_milestone(
    student_id: UUID,
    milestone_id: UUID,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """Soft-delete an achievement record. Catalog rows are not deleted by this endpoint."""
    _assert_student_match(ctx, student_id)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            status = await conn.execute(
                """
                UPDATE student_life_milestones
                SET deleted_at = now(),
                    deleted_by = $3,
                    updated_at = now()
                WHERE id = $1
                  AND tenant_id = $2
                  AND deleted_at IS NULL
                """,
                milestone_id,
                ctx.tenant_id,
                ctx.token_id,
            )
            try:
                affected = int(status.split()[-1])
            except (ValueError, IndexError):
                affected = 0

            if affected == 0:
                raise HTTPException(404, "Milestone achievement record not found.")

            await _audit_log_milestone(
                conn, ctx,
                action="milestone_deleted",
                target_table="student_life_milestones",
                target_id=milestone_id,
                details={},
            )

    return {"ok": True, "milestone_id": str(milestone_id), "deleted": True}
