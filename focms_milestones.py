"""focms_milestones.py - Life milestones tracking for the FOCMS parent portal.

v0.9.0 (2026-06-25) - Artifact upload support.
  - v0.9.0: adds file artifact upload/list/stream/delete endpoints. Files are
            stored as bytea in media_files (canonical), linked to milestones
            via field_artifact_attachments (source_table='student_life_milestones').
            Visibility inherits from milestone: family -> private, public stays public.
            Requires python-multipart in requirements.txt for FastAPI File/Form.
  - v0.8.3: fixed picker SQL (UUID MIN -> array_agg ORDER BY).
  - v0.8.2: audit_log schema fixes (action enum + new_value column).
  - v0.8.1: corrected imports to use _require_parent_token + _bind_tenant.

Endpoints:
  GET    /students/{student_id}/milestones/picker
  POST   /students/{student_id}/milestones/capture
  POST   /students/{student_id}/milestones/custom
  DELETE /students/{student_id}/milestones/{milestone_id}

  POST   /students/{student_id}/milestones/{milestone_id}/artifacts   (v0.9.0)
  GET    /students/{student_id}/milestones/{milestone_id}/artifacts   (v0.9.0)
  GET    /artifacts/{artifact_id}/content                              (v0.9.0)
  DELETE /students/{student_id}/milestones/{milestone_id}/artifacts/{attachment_id} (v0.9.0)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date
from typing import Any, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel, ConfigDict, Field

from focms_parent_portal import _require_parent_token, _bind_tenant, ParentContext

logger = logging.getLogger("focms.milestones")

router = APIRouter(prefix="/focms/v1/parent", tags=["parent-milestones"])


# ============================================================================
#  Constants
# ============================================================================

MAX_ARTIFACT_BYTES = 25 * 1024 * 1024  # 25 MB per file

_ATTACHMENT_ROLES = {
    "evidence", "reference", "supporting_doc", "primary_artifact",
    "transcript", "recording", "before_after", "social_proof", "other",
}


def _kind_from_mime(mime: str) -> str:
    """Map MIME type to media_files.kind constraint (image|document|video|other)."""
    if not mime:
        return "other"
    m = mime.lower()
    if m.startswith("image/"):
        return "image"
    if m.startswith("video/"):
        return "video"
    document_prefixes = (
        "application/pdf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument",
        "application/vnd.ms-",
        "application/vnd.oasis.opendocument",
        "application/rtf",
        "application/x-rtf",
        "application/json",
        "application/xml",
        "application/zip",
        "text/",
        "message/",
    )
    if m.startswith(document_prefixes):
        return "document"
    return "other"


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
    source_kind: str
    status: str
    first_hit: Optional[date] = None
    event_count: int = 0
    milestone_id: Optional[UUID] = None
    artifact_count: int = 0


class CustomMilestoneEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    custom_title: str
    custom_category: Optional[str] = None
    event_date: date
    event_notes: Optional[str] = None
    artifact_url: Optional[str] = None
    visibility: str = "family"
    artifact_count: int = 0


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
    add_to_catalog: bool = False
    catalog_pillar: Optional[str] = Field(default=None, max_length=80)
    catalog_age_band: Optional[str] = Field(default=None, max_length=40)


# ============================================================================
#  Helpers
# ============================================================================

_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def _slugify(s: str, max_len: int = 50) -> str:
    slug = _SLUG_RE.sub("_", s.lower()).strip("_")
    slug = re.sub(r"_+", "_", slug)
    return slug[:max_len] if len(slug) > max_len else slug


def _assert_student_match(ctx: ParentContext, student_id: UUID) -> None:
    if str(ctx.student_id) != str(student_id):
        raise HTTPException(403, "Token does not authorize access to this student.")


_AUDIT_ACTION_MAP: dict[str, str] = {
    "milestone_captured":            "create",
    "custom_milestone_captured":     "create",
    "milestone_deleted":             "delete",
    "milestone_updated":             "update",
    "milestone_artifact_uploaded":   "create",
    "milestone_artifact_deleted":    "delete",
}


async def _audit_log_milestone(
    conn: asyncpg.Connection,
    ctx: ParentContext,
    action: str,
    target_table: str,
    target_id: UUID,
    details: dict[str, Any],
) -> None:
    enum_action = _AUDIT_ACTION_MAP.get(action, "create")
    try:
        async with conn.transaction():
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
                action,
                json.dumps(details, default=str),
            )
    except Exception as e:
        logger.warning(
            "milestone audit_log failed (action=%s target=%s/%s): %s",
            action, target_table, target_id, e,
        )


# ============================================================================
#  Picker
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

            catalog_rows = await conn.fetch(
                """
                WITH hit AS (
                    SELECT
                        m.milestone_code AS code,
                        MIN(m.event_date) AS first_hit,
                        COUNT(*)        AS event_count,
                        (array_agg(m.id ORDER BY m.event_date NULLS LAST))[1] AS milestone_id
                    FROM student_life_milestones m
                    WHERE m.student_id = $1
                      AND m.tenant_id  = $2
                      AND m.deleted_at IS NULL
                      AND m.milestone_code IS NOT NULL
                    GROUP BY m.milestone_code
                ),
                art AS (
                    SELECT m.milestone_code AS code, COUNT(*) AS artifact_count
                    FROM student_life_milestones m
                    JOIN field_artifact_attachments a
                      ON a.source_record_id = m.id
                     AND a.source_table = 'student_life_milestones'
                     AND a.tenant_id = m.tenant_id
                     AND a.deleted_at IS NULL
                    WHERE m.student_id = $1
                      AND m.tenant_id  = $2
                      AND m.deleted_at IS NULL
                      AND m.milestone_code IS NOT NULL
                    GROUP BY m.milestone_code
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
                    h.milestone_id,
                    COALESCE(art.artifact_count, 0)::int AS artifact_count
                FROM life_milestones_catalog c
                LEFT JOIN hit h ON h.code = c.code
                LEFT JOIN art ON art.code = c.code
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

            custom_rows = await conn.fetch(
                """
                SELECT
                    m.id, m.custom_title, m.custom_category, m.event_date, m.event_notes,
                    m.artifact_url, m.visibility,
                    COALESCE(a.cnt, 0)::int AS artifact_count
                FROM student_life_milestones m
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS cnt
                    FROM field_artifact_attachments faa
                    WHERE faa.source_record_id = m.id
                      AND faa.source_table = 'student_life_milestones'
                      AND faa.tenant_id = m.tenant_id
                      AND faa.deleted_at IS NULL
                ) a ON true
                WHERE m.student_id = $1
                  AND m.tenant_id  = $2
                  AND m.deleted_at IS NULL
                  AND m.milestone_code IS NULL
                ORDER BY m.event_date DESC NULLS LAST, m.created_at DESC
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
                artifact_count=int(r["artifact_count"]),
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
            artifact_count=int(r["artifact_count"]),
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


# ============================================================================
#  Capture
# ============================================================================

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
                raise HTTPException(404, f"Milestone code '{body.milestone_code}' not found in catalog.")

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
    _assert_student_match(ctx, student_id)

    catalog_code: Optional[str] = None
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            if body.add_to_catalog:
                if not body.catalog_pillar or not body.catalog_age_band:
                    raise HTTPException(400, "add_to_catalog=true requires catalog_pillar and catalog_age_band.")

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
                    raise HTTPException(409, "Could not generate non-colliding catalog code after 50 attempts.")

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


# ============================================================================
#  Artifacts (v0.9.0)
# ============================================================================

@router.post(
    "/students/{student_id}/milestones/{milestone_id}/artifacts",
    status_code=201,
)
async def upload_milestone_artifact(
    student_id: UUID,
    milestone_id: UUID,
    request: Request,
    file: UploadFile = File(...),
    caption: Optional[str] = Form(default=None),
    attachment_role: str = Form(default="primary_artifact"),
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """Upload a photo, video, or document and attach it to a milestone.

    Files are stored as bytea in media_files. The link to the milestone is
    a row in field_artifact_attachments. Visibility on both rows is derived
    from the milestone's visibility (family -> private, public -> public).
    Max file size: 25 MB.
    """
    _assert_student_match(ctx, student_id)

    if attachment_role not in _ATTACHMENT_ROLES:
        raise HTTPException(
            400,
            f"Invalid attachment_role '{attachment_role}'. Allowed: {sorted(_ATTACHMENT_ROLES)}",
        )

    raw = await file.read()
    if len(raw) == 0:
        raise HTTPException(400, "Empty file.")
    if len(raw) > MAX_ARTIFACT_BYTES:
        raise HTTPException(
            413,
            f"File too large: {len(raw)} bytes. Max {MAX_ARTIFACT_BYTES // (1024 * 1024)} MB.",
        )

    mime_type = file.content_type or "application/octet-stream"
    kind = _kind_from_mime(mime_type)
    sha256_hex = hashlib.sha256(raw).hexdigest()
    original_filename = (file.filename or "upload.bin")[:500]

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            ms = await conn.fetchrow(
                """
                SELECT id, visibility, milestone_code
                FROM student_life_milestones
                WHERE id = $1 AND student_id = $2 AND tenant_id = $3
                  AND deleted_at IS NULL
                """,
                milestone_id, student_id, ctx.tenant_id,
            )
            if not ms:
                raise HTTPException(404, "Milestone not found.")

            milestone_vis = (ms["visibility"] or "family").lower()
            media_vis = "public" if milestone_vis == "public" else "private"
            field_code = ms["milestone_code"] or "custom_event"

            next_order = await conn.fetchval(
                """
                SELECT COALESCE(MAX(display_order), 0) + 1
                FROM field_artifact_attachments
                WHERE source_table = 'student_life_milestones'
                  AND source_record_id = $1
                  AND tenant_id = $2
                  AND deleted_at IS NULL
                """,
                milestone_id, ctx.tenant_id,
            )

            artifact_row = await conn.fetchrow(
                """
                INSERT INTO media_files (
                    id, tenant_id, student_id, kind, mime_type,
                    original_filename, byte_size, content, visibility,
                    sha256_hex, created_by, created_at
                ) VALUES (
                    gen_random_uuid_v7(), $1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, now()
                )
                RETURNING id
                """,
                ctx.tenant_id, student_id, kind, mime_type,
                original_filename, len(raw), raw, media_vis,
                sha256_hex, ctx.token_id,
            )

            attachment_row = await conn.fetchrow(
                """
                INSERT INTO field_artifact_attachments (
                    id, tenant_id, student_id, field_code,
                    source_table, source_record_id, source_column,
                    artifact_id, attachment_role, display_order,
                    uploaded_at, uploaded_by, upload_context,
                    caption, is_featured, notes, details, visibility,
                    visibility_inherits_from_field,
                    source_system, source_id,
                    created_at, created_by, updated_at
                ) VALUES (
                    gen_random_uuid_v7(), $1, $2, $3,
                    'student_life_milestones', $4, NULL,
                    $5, $6, $7,
                    now(), $8, 'parent_portal_milestone_capture',
                    $9, false, NULL, '{}'::jsonb, $10,
                    true,
                    'parent_portal', NULL,
                    now(), $8, now()
                )
                RETURNING id
                """,
                ctx.tenant_id, student_id, field_code,
                milestone_id,
                artifact_row["id"], attachment_role, next_order,
                ctx.token_id,
                caption, media_vis,
            )

            await _audit_log_milestone(
                conn, ctx,
                action="milestone_artifact_uploaded",
                target_table="field_artifact_attachments",
                target_id=attachment_row["id"],
                details={
                    "milestone_id": str(milestone_id),
                    "artifact_id": str(artifact_row["id"]),
                    "kind": kind,
                    "mime_type": mime_type,
                    "byte_size": len(raw),
                    "filename": original_filename,
                    "sha256_hex": sha256_hex,
                },
            )

    return {
        "ok": True,
        "artifact_id": str(artifact_row["id"]),
        "attachment_id": str(attachment_row["id"]),
        "milestone_id": str(milestone_id),
        "kind": kind,
        "mime_type": mime_type,
        "original_filename": original_filename,
        "byte_size": len(raw),
        "visibility": media_vis,
        "sha256_hex": sha256_hex,
    }


@router.get(
    "/students/{student_id}/milestones/{milestone_id}/artifacts",
)
async def list_milestone_artifacts(
    student_id: UUID,
    milestone_id: UUID,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """List all artifacts attached to a milestone."""
    _assert_student_match(ctx, student_id)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)
            rows = await conn.fetch(
                """
                SELECT
                    a.id AS attachment_id,
                    a.artifact_id,
                    a.caption,
                    a.attachment_role,
                    a.display_order,
                    a.visibility AS attachment_visibility,
                    a.uploaded_at,
                    m.kind,
                    m.mime_type,
                    m.original_filename,
                    m.byte_size,
                    m.visibility AS file_visibility
                FROM field_artifact_attachments a
                JOIN media_files m
                  ON m.id = a.artifact_id
                 AND m.tenant_id = a.tenant_id
                 AND m.deleted_at IS NULL
                WHERE a.source_table = 'student_life_milestones'
                  AND a.source_record_id = $1
                  AND a.tenant_id = $2
                  AND a.deleted_at IS NULL
                ORDER BY a.display_order, a.uploaded_at
                """,
                milestone_id, ctx.tenant_id,
            )

    return {
        "milestone_id": str(milestone_id),
        "count": len(rows),
        "artifacts": [
            {
                "attachment_id": str(r["attachment_id"]),
                "artifact_id": str(r["artifact_id"]),
                "kind": r["kind"],
                "mime_type": r["mime_type"],
                "original_filename": r["original_filename"],
                "byte_size": r["byte_size"],
                "caption": r["caption"],
                "attachment_role": r["attachment_role"],
                "display_order": r["display_order"],
                "visibility": r["attachment_visibility"],
                "uploaded_at": r["uploaded_at"].isoformat() if r["uploaded_at"] else None,
            }
            for r in rows
        ],
    }


@router.get("/artifacts/{artifact_id}/content")
async def stream_artifact_content(
    artifact_id: UUID,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
):
    """Stream the binary content of an artifact. Used by <img>/<video> tags
    and download links. Token-protected via ?t= query string."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)
            row = await conn.fetchrow(
                """
                SELECT mime_type, content, original_filename, byte_size
                FROM media_files
                WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                """,
                artifact_id, ctx.tenant_id,
            )
    if not row:
        raise HTTPException(404, "Artifact not found.")

    safe_name = (row["original_filename"] or "file").replace('"', "_")
    return Response(
        content=bytes(row["content"]),
        media_type=row["mime_type"] or "application/octet-stream",
        headers={
            "Content-Length": str(row["byte_size"]),
            "Content-Disposition": f'inline; filename="{safe_name}"',
            "Cache-Control": "private, max-age=3600",
        },
    )


@router.delete(
    "/students/{student_id}/milestones/{milestone_id}/artifacts/{attachment_id}",
    status_code=200,
)
async def delete_milestone_artifact(
    student_id: UUID,
    milestone_id: UUID,
    attachment_id: UUID,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
) -> dict[str, Any]:
    """Soft-delete an artifact attachment. If no other attachment references
    the underlying media_files row, soft-delete the file too."""
    _assert_student_match(ctx, student_id)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)
            row = await conn.fetchrow(
                """
                UPDATE field_artifact_attachments
                SET deleted_at = now(), deleted_by = $3, updated_at = now()
                WHERE id = $1
                  AND tenant_id = $2
                  AND deleted_at IS NULL
                  AND source_table = 'student_life_milestones'
                  AND source_record_id = $4
                RETURNING artifact_id
                """,
                attachment_id, ctx.tenant_id, ctx.token_id, milestone_id,
            )
            if not row:
                raise HTTPException(404, "Artifact attachment not found.")

            other_refs = await conn.fetchval(
                """
                SELECT COUNT(*) FROM field_artifact_attachments
                WHERE artifact_id = $1 AND deleted_at IS NULL
                """,
                row["artifact_id"],
            )
            if other_refs == 0:
                await conn.execute(
                    "UPDATE media_files SET deleted_at = now() WHERE id = $1",
                    row["artifact_id"],
                )

            await _audit_log_milestone(
                conn, ctx,
                action="milestone_artifact_deleted",
                target_table="field_artifact_attachments",
                target_id=attachment_id,
                details={
                    "milestone_id": str(milestone_id),
                    "artifact_id": str(row["artifact_id"]),
                },
            )

    return {"ok": True, "attachment_id": str(attachment_id), "deleted": True}
