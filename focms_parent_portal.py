"""focms_parent_portal.py - Parent-facing API endpoints for FOCMS v0.7.0.

Endpoints:
  POST  /focms/v1/parent/auth/verify-token
  GET   /focms/v1/parent/students/{id}/form
  POST  /focms/v1/parent/students/{id}/save
  POST  /focms/v1/parent/admin/generate-token
  GET   /focms/v1/parent/admin/tokens
  POST  /focms/v1/parent/admin/tokens/{id}/revoke

Parents authenticate via URL-embedded token (no email/password). Tokens
passed as Authorization: Bearer parent_<token> OR ?t=<token> query param.

Save dispatch via SAVE_TABLE_CONFIG; four patterns:
  one_to_one     - INSERT ... ON CONFLICT (student_id) DO UPDATE
  keyed_upsert   - INSERT ... ON CONFLICT (student_id, <key>) DO UPDATE
  array_update   - UPDATE <table> SET col=$1 WHERE id=$record_id AND student_id=$sid
  student_direct - UPDATE students SET col=$1 WHERE id=$sid

Every save writes to audit_log with actor_role='parent' for COPPA/FERPA.

v0.7.0 (2026-06-24): initial release.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("focms.parent_portal")

router = APIRouter(prefix="/focms/v1/parent", tags=["parent-portal"])

SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

SAVE_TABLE_CONFIG: dict[str, dict] = {
    "student_personal_details": {"pattern": "one_to_one", "conflict_columns": ["student_id"]},
    "veteran_military_status":  {"pattern": "one_to_one", "conflict_columns": ["student_id"]},
    "psychological_profile":    {"pattern": "one_to_one", "conflict_columns": ["student_id"]},
    "student_addresses": {
        "pattern": "keyed_upsert", "key_column": "address_kind",
        "field_code_key_index": 1, "conflict_columns": ["student_id", "address_kind"],
    },
    "student_school_enrollments": {"pattern": "array_update"},
    "essays": {"pattern": "array_update"},
    "students": {"pattern": "student_direct"},
    "family_members":            {"pattern": "array_update"},
    "work_experiences":          {"pattern": "array_update"},
    "awards_honors":             {"pattern": "array_update"},
    "affiliations":              {"pattern": "array_update"},
    "affiliations_common_app":   {"pattern": "array_update"},
    "applications":              {"pattern": "array_update"},
    "standardized_test_scores":  {"pattern": "array_update"},
    "portfolio_artifacts":       {"pattern": "array_update"},
    "interview_log":             {"pattern": "array_update"},
    "personal_records":          {"pattern": "array_update"},
    "student_instruments":       {"pattern": "array_update"},
    "discipline_incidents":      {"pattern": "array_update"},
    "recommenders":              {"pattern": "array_update"},
    "early_decision_agreements": {"pattern": "array_update"},
    "class_rank_history":        {"pattern": "array_update"},
    "gpa_history":               {"pattern": "array_update"},
    "courses_taken":             {"pattern": "array_update"},
    "assessments":               {"pattern": "array_update"},
    "events":                    {"pattern": "array_update"},
    "recommendations":           {"pattern": "array_update"},
    "recommendation_ratings":    {"pattern": "array_update"},
}


class ParentContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    token_id: UUID
    tenant_id: UUID
    student_id: UUID
    family_member_id: Optional[UUID] = None
    display_name: str
    email: Optional[str] = None
    preferred_ui_locale: str
    can_edit: bool
    can_view: bool
    can_upload_artifacts: bool
    student_first_name: str
    student_last_name: str
    student_current_age: int


class TokenVerifyResponse(BaseModel):
    valid: bool
    token_id: Optional[UUID] = None
    student_id: Optional[UUID] = None
    family_member_id: Optional[UUID] = None
    display_name: Optional[str] = None
    preferred_ui_locale: Optional[str] = None
    student_first_name: Optional[str] = None
    student_last_name: Optional[str] = None
    student_current_age: Optional[int] = None
    can_edit: Optional[bool] = None
    permissions: Optional[dict] = None
    reason: Optional[str] = None


class FieldUpdate(BaseModel):
    field_code: str = Field(..., min_length=3, max_length=200)
    value: Optional[str] = None
    record_id: Optional[UUID] = None


class SaveRequest(BaseModel):
    updates: list[FieldUpdate] = Field(..., min_length=1, max_length=200)
    locale_of_input: str = Field("en-US")


class FieldSaveResult(BaseModel):
    field_code: str
    ok: bool
    error: Optional[str] = None
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    saved_at: Optional[datetime] = None
    detail: Optional[dict] = None


class SaveResponse(BaseModel):
    student_id: UUID
    total_updates: int
    successful: int
    failed: int
    results: list[FieldSaveResult]


class GenerateTokenRequest(BaseModel):
    student_id: UUID
    family_member_id: Optional[UUID] = None
    display_name: str = Field(..., min_length=1, max_length=200)
    email: Optional[str] = None
    preferred_ui_locale: str = "en-US"
    expires_at: Optional[datetime] = None
    can_edit: bool = True
    can_view: bool = True
    can_upload_artifacts: bool = True


class GenerateTokenResponse(BaseModel):
    token_id: UUID
    token: str
    access_url: str
    student_id: UUID
    display_name: str
    expires_at: Optional[datetime] = None


class TokenListItem(BaseModel):
    token_id: UUID
    student_id: UUID
    display_name: str
    email: Optional[str] = None
    preferred_ui_locale: str
    granted_at: datetime
    expires_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    use_count: int
    is_active: bool
    revoked_at: Optional[datetime] = None


class AdminContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    tenant_id: UUID
    user_id: UUID
    role: str


async def _bind_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        str(tenant_id),
    )


async def _require_parent_token(
    request: Request,
    authorization: Optional[str] = Header(None),
    t: Optional[str] = Query(None),
) -> ParentContext:
    token: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        raw = authorization.split(" ", 1)[1].strip()
        if raw.startswith("parent_"):
            token = raw[len("parent_"):]
    if not token and t:
        token = t
    if not token:
        raise HTTPException(401, "missing parent token (Authorization: Bearer parent_<token> or ?t=<token>)")

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            raw = await conn.fetchval(
                "SELECT fn_resolve_parent_token($1)::text", token
            )
    result = json.loads(raw) if isinstance(raw, str) else raw
    if not result.get("valid"):
        raise HTTPException(401, f"token invalid: {result.get('reason','unknown')}")

    perms = result.get("permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)

    ctx = ParentContext(
        token_id=UUID(result["token_id"]),
        tenant_id=UUID(result["tenant_id"]),
        student_id=UUID(result["student_id"]),
        family_member_id=UUID(result["family_member_id"]) if result.get("family_member_id") else None,
        display_name=result["display_name"],
        email=result.get("email"),
        preferred_ui_locale=result.get("preferred_ui_locale") or "en-US",
        can_edit=bool(perms.get("can_edit", True)),
        can_view=bool(perms.get("can_view", True)),
        can_upload_artifacts=bool(perms.get("can_upload_artifacts", True)),
        student_first_name=result["student_first_name"],
        student_last_name=result["student_last_name"],
        student_current_age=int(result["student_current_age"]),
    )

    client_ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _bind_tenant(conn, ctx.tenant_id)
                await conn.execute(
                    """UPDATE public.parent_access_tokens
                       SET last_used_at = now(), use_count = use_count + 1,
                           ip_last_used = $1, user_agent_last_used = $2
                       WHERE id = $3""",
                    client_ip, ua, ctx.token_id,
                )
    except Exception:
        logger.warning("parent token usage bump failed", exc_info=True)

    return ctx


def _admin_tokens() -> dict[str, dict[str, str]]:
    return json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))


async def _require_admin(authorization: Optional[str] = Header(None)) -> AdminContext:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer token")
    raw = authorization.split(" ", 1)[1].strip()
    if raw.startswith("parent_"):
        raise HTTPException(403, "parent tokens cannot access admin endpoints")
    info = _admin_tokens().get(raw)
    if not info:
        raise HTTPException(401, "invalid admin token")
    if info.get("role") not in {"tenant_owner", "tenant_admin", "platform_admin"}:
        raise HTTPException(403, "admin role required")
    return AdminContext(
        tenant_id=UUID(info["tenant_id"]),
        user_id=UUID(info["user_id"]),
        role=info.get("role", "tenant_admin"),
    )


def _coerce_for_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, bytes):
        return "<encrypted:" + str(len(val)) + "b>"
    if isinstance(val, list):
        return [_coerce_for_json(v) for v in val]
    if isinstance(val, dict):
        return {str(k): _coerce_for_json(v) for k, v in val.items()}
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


@router.post("/auth/verify-token", response_model=TokenVerifyResponse)
async def verify_token(request: Request, ctx: ParentContext = Depends(_require_parent_token)):
    return TokenVerifyResponse(
        valid=True,
        token_id=ctx.token_id,
        student_id=ctx.student_id,
        family_member_id=ctx.family_member_id,
        display_name=ctx.display_name,
        preferred_ui_locale=ctx.preferred_ui_locale,
        student_first_name=ctx.student_first_name,
        student_last_name=ctx.student_last_name,
        student_current_age=ctx.student_current_age,
        can_edit=ctx.can_edit,
        permissions={
            "can_edit": ctx.can_edit,
            "can_view": ctx.can_view,
            "can_upload_artifacts": ctx.can_upload_artifacts,
        },
    )


@router.get("/students/{student_id}/form")
async def get_parent_form(
    student_id: UUID,
    request: Request,
    locale: str = Query("en-US"),
    include_values: bool = Query(True),
    ctx: ParentContext = Depends(_require_parent_token),
):
    if student_id != ctx.student_id:
        raise HTTPException(403, "token does not grant access to this student")

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            form_raw = await conn.fetchval(
                "SELECT fn_render_parent_capture_form($1)::text", student_id
            )
            form = json.loads(form_raw) if isinstance(form_raw, str) else form_raw

            i18n_raw = await conn.fetchval(
                "SELECT fn_localized_strings_for_namespace($1, $2, $3)::text",
                "parent_form", locale, ctx.tenant_id,
            )
            i18n = json.loads(i18n_raw) if isinstance(i18n_raw, str) else i18n_raw

            section_labels_raw = await conn.fetchval(
                "SELECT fn_localized_strings_for_namespace($1, $2, $3)::text",
                "parent_form_sections", locale, ctx.tenant_id,
            )
            section_labels = json.loads(section_labels_raw) if isinstance(section_labels_raw, str) else section_labels_raw

            current_values: dict = {}
            if include_values:
                current_values = await _fetch_current_values(conn, student_id, form)

    for section in form.get("sections", []):
        code = section.get("section_code")
        label_key = f"section_{code}_label"
        section["section_label"] = (section_labels.get("strings") or {}).get(label_key, code.replace("_", " ").title())

    return {
        "form_metadata": form.get("form_metadata"),
        "sections": form.get("sections", []),
        "i18n_strings": (i18n.get("strings") or {}),
        "i18n_metadata": (i18n.get("metadata") or {}),
        "i18n_fallback_count": i18n.get("fallback_count", 0),
        "locale": locale,
        "current_values": current_values,
        "permissions": {
            "can_edit": ctx.can_edit,
            "can_view": ctx.can_view,
            "can_upload_artifacts": ctx.can_upload_artifacts,
        },
        "parent": {
            "display_name": ctx.display_name,
            "preferred_ui_locale": ctx.preferred_ui_locale,
        },
        "student": {
            "id": str(ctx.student_id),
            "first_name": ctx.student_first_name,
            "last_name": ctx.student_last_name,
            "current_age": ctx.student_current_age,
        },
    }


async def _fetch_current_values(conn: asyncpg.Connection, student_id: UUID, form: dict) -> dict:
    values: dict = {}

    by_table: dict[str, list[dict]] = {}
    for section in form.get("sections", []):
        for field in section.get("fields", []):
            tbl = field.get("source_table")
            if not tbl:
                continue
            by_table.setdefault(tbl, []).append(field)

    if "student_personal_details" in by_table:
        cols = [f["source_column"] for f in by_table["student_personal_details"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                and not f["source_column"].endswith("_ciphertext")]
        if cols:
            try:
                col_list = ", ".join(f'"{c}"' for c in cols)
                row = await conn.fetchrow(
                    f"SELECT {col_list} FROM public.student_personal_details WHERE student_id = $1",
                    student_id,
                )
                if row:
                    for field in by_table["student_personal_details"]:
                        col = field.get("source_column")
                        if col and col in row:
                            values[field["field_code"]] = _coerce_for_json(row[col])
            except Exception:
                logger.warning("student_personal_details read failed", exc_info=True)

    if "students" in by_table:
        cols = [f["source_column"] for f in by_table["students"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                and not f["source_column"].endswith("_ciphertext")]
        if cols:
            try:
                col_list = ", ".join(f'"{c}"' for c in cols)
                row = await conn.fetchrow(
                    f"SELECT {col_list} FROM public.students WHERE id = $1",
                    student_id,
                )
                if row:
                    for field in by_table["students"]:
                        col = field.get("source_column")
                        if col and col in row:
                            values[field["field_code"]] = _coerce_for_json(row[col])
            except Exception:
                logger.warning("students read failed", exc_info=True)

    if "student_addresses" in by_table:
        by_type: dict[str, list[dict]] = {}
        for f in by_table["student_addresses"]:
            key = f["field_code"].split(".")[1] if "." in f["field_code"] else "permanent"
            by_type.setdefault(key, []).append(f)
        for addr_type, fields in by_type.items():
            cols = [f["source_column"] for f in fields
                    if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                    and not f["source_column"].endswith("_ciphertext")]
            if cols:
                try:
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    row = await conn.fetchrow(
                        f"SELECT {col_list} FROM public.student_addresses WHERE student_id=$1 AND address_kind=$2",
                        student_id, addr_type,
                    )
                    if row:
                        for field in fields:
                            col = field.get("source_column")
                            if col and col in row:
                                values[field["field_code"]] = _coerce_for_json(row[col])
                except Exception:
                    logger.warning("student_addresses %s read failed", addr_type, exc_info=True)

    for tbl in ("veteran_military_status", "psychological_profile"):
        if tbl in by_table:
            cols = [f["source_column"] for f in by_table[tbl]
                    if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
            if cols:
                col_list = ", ".join(f'"{c}"' for c in cols)
                try:
                    row = await conn.fetchrow(
                        f"SELECT {col_list} FROM public.{tbl} WHERE student_id = $1",
                        student_id,
                    )
                    if row:
                        for field in by_table[tbl]:
                            col = field.get("source_column")
                            if col and col in row:
                                values[field["field_code"]] = _coerce_for_json(row[col])
                except Exception:
                    logger.debug("table %s read failed", tbl, exc_info=True)

    if "essays" in by_table:
        by_kind: dict[str, list[dict]] = {}
        for f in by_table["essays"]:
            key = f["field_code"].split(".")[1] if "." in f["field_code"] else "personal_statement"
            by_kind.setdefault(key, []).append(f)
        for kind, fields in by_kind.items():
            cols = [f["source_column"] for f in fields
                    if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
            if cols:
                col_list = ", ".join(f'"{c}"' for c in cols)
                try:
                    row = await conn.fetchrow(
                        f"SELECT {col_list} FROM public.essays WHERE student_id=$1 AND essay_kind=$2",
                        student_id, kind,
                    )
                    if row:
                        for field in fields:
                            col = field.get("source_column")
                            if col and col in row:
                                values[field["field_code"]] = _coerce_for_json(row[col])
                except Exception:
                    logger.debug("essays read failed", exc_info=True)

    array_tables = [t for t in by_table if SAVE_TABLE_CONFIG.get(t, {}).get("pattern") == "array_update"]
    for tbl in array_tables:
        cols = [f["source_column"] for f in by_table[tbl]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
        if cols:
            col_list = ", ".join(f'"{c}"' for c in cols)
            try:
                rows = await conn.fetch(
                    f"SELECT id, {col_list} FROM public.{tbl} WHERE student_id = $1 ORDER BY created_at ASC",
                    student_id,
                )
                values[f"__array_rows__{tbl}"] = [
                    {"id": str(r["id"]), **{c: _coerce_for_json(r[c]) for c in cols if c in r}}
                    for r in rows
                ]
            except Exception:
                logger.debug("array table %s read failed", tbl, exc_info=True)

    return values


@router.post("/students/{student_id}/save", response_model=SaveResponse)
async def save_parent_form(
    student_id: UUID,
    body: SaveRequest,
    request: Request,
    ctx: ParentContext = Depends(_require_parent_token),
):
    if student_id != ctx.student_id:
        raise HTTPException(403, "token does not grant access to this student")
    if not ctx.can_edit:
        raise HTTPException(403, "token does not grant edit permission")

    pool: asyncpg.Pool = request.app.state.pool
    results: list[FieldSaveResult] = []

    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, ctx.tenant_id)

            field_codes = list({u.field_code for u in body.updates})
            catalog_rows = await conn.fetch(
                """SELECT field_code, source_table, source_column, field_kind,
                          visibility_lock_kind, greyed_out_in_parent_form,
                          field_label
                   FROM public.field_capture_catalog
                   WHERE field_code = ANY($1::text[])
                     AND deleted_at IS NULL AND is_active = true""",
                field_codes,
            )
            catalog: dict[str, dict] = {r["field_code"]: dict(r) for r in catalog_rows}

            for update in body.updates:
                result = await _save_one_field(
                    conn, ctx, student_id, update, catalog.get(update.field_code)
                )
                results.append(result)

    successful = sum(1 for r in results if r.ok)
    return SaveResponse(
        student_id=student_id,
        total_updates=len(results),
        successful=successful,
        failed=len(results) - successful,
        results=results,
    )


async def _save_one_field(
    conn: asyncpg.Connection,
    ctx: ParentContext,
    student_id: UUID,
    update: FieldUpdate,
    catalog: Optional[dict],
) -> FieldSaveResult:
    if not catalog:
        return FieldSaveResult(
            field_code=update.field_code, ok=False,
            error="field_code not in catalog",
        )

    src_table = catalog["source_table"]
    src_col = catalog["source_column"]

    if not src_table or not src_col:
        return FieldSaveResult(
            field_code=update.field_code, ok=False,
            error="catalog entry missing source_table or source_column",
            source_table=src_table, source_column=src_col,
        )

    if not SAFE_IDENTIFIER.match(src_col):
        return FieldSaveResult(
            field_code=update.field_code, ok=False,
            error="source_column failed identifier safety check",
            source_table=src_table, source_column=src_col,
        )

    config = SAVE_TABLE_CONFIG.get(src_table)
    if not config:
        return FieldSaveResult(
            field_code=update.field_code, ok=False,
            error=f"source_table {src_table!r} not configured for save (Phase 2)",
            source_table=src_table, source_column=src_col,
        )

    pattern = config["pattern"]
    prior_value = None

    try:
        if pattern == "one_to_one":
            row = await conn.fetchrow(
                f'SELECT "{src_col}" AS v FROM public.{src_table} WHERE student_id = $1',
                student_id,
            )
            prior_value = row["v"] if row else None

            sql = f'''INSERT INTO public.{src_table} (student_id, tenant_id, "{src_col}")
                     VALUES ($1, $2, $3)
                     ON CONFLICT (student_id) DO UPDATE
                     SET "{src_col}" = EXCLUDED."{src_col}", updated_at = now()'''
            await conn.execute(sql, student_id, ctx.tenant_id, update.value)

        elif pattern == "keyed_upsert":
            key_col = config["key_column"]
            key_idx = config["field_code_key_index"]
            parts = update.field_code.split(".")
            if len(parts) <= key_idx:
                return FieldSaveResult(
                    field_code=update.field_code, ok=False,
                    error=f"field_code missing key segment at index {key_idx}",
                    source_table=src_table, source_column=src_col,
                )
            key_val = parts[key_idx]

            row = await conn.fetchrow(
                f'SELECT "{src_col}" AS v FROM public.{src_table} '
                f'WHERE student_id = $1 AND "{key_col}" = $2',
                student_id, key_val,
            )
            prior_value = row["v"] if row else None

            sql = f'''INSERT INTO public.{src_table} (student_id, tenant_id, created_by, "{key_col}", "{src_col}")
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (student_id, "{key_col}") DO UPDATE
                     SET "{src_col}" = EXCLUDED."{src_col}", updated_at = now()'''
            await conn.execute(sql, student_id, ctx.tenant_id, ctx.token_id, key_val, update.value)

        elif pattern == "student_direct":
            row = await conn.fetchrow(
                f'SELECT "{src_col}" AS v FROM public.students WHERE id = $1',
                student_id,
            )
            prior_value = row["v"] if row else None

            sql = f'UPDATE public.students SET "{src_col}" = $1, updated_at = now() WHERE id = $2'
            await conn.execute(sql, update.value, student_id)

        elif pattern == "array_update":
            if not update.record_id:
                return FieldSaveResult(
                    field_code=update.field_code, ok=False,
                    error="record_id required for array_update tables (Phase 2 will support INSERT)",
                    source_table=src_table, source_column=src_col,
                )
            row = await conn.fetchrow(
                f'SELECT "{src_col}" AS v FROM public.{src_table} '
                f'WHERE id = $1 AND student_id = $2',
                update.record_id, student_id,
            )
            if not row:
                return FieldSaveResult(
                    field_code=update.field_code, ok=False,
                    error="record_id not found or not owned by this student",
                    source_table=src_table, source_column=src_col,
                )
            prior_value = row["v"]

            sql = f'UPDATE public.{src_table} SET "{src_col}" = $1, updated_at = now() WHERE id = $2 AND student_id = $3'
            await conn.execute(sql, update.value, update.record_id, student_id)

        else:
            return FieldSaveResult(
                field_code=update.field_code, ok=False,
                error=f"unknown save pattern {pattern!r}",
                source_table=src_table, source_column=src_col,
            )

        try:
            await conn.execute(
                """INSERT INTO public.audit_log (
                    actor_user_id, actor_role, actor_ip, occurred_at, action,
                    tenant_id, student_id, target_table, target_id, field_changed,
                    prior_value, new_value, reason
                ) VALUES ($1, 'tenant_admin', $2, now(), 'update', $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, 'parent_form_save')""",
                ctx.token_id,
                None,
                ctx.tenant_id, student_id, src_table, update.record_id, src_col,
                json.dumps({"value": _coerce_for_json(prior_value)}),
                json.dumps({"value": update.value}),
            )
        except Exception:
            logger.warning("audit_log write failed for %s", update.field_code, exc_info=True)

        return FieldSaveResult(
            field_code=update.field_code, ok=True,
            source_table=src_table, source_column=src_col,
            saved_at=datetime.now(timezone.utc),
            detail={"pattern": pattern},
        )

    except Exception as exc:
        logger.exception("save failed for %s", update.field_code)
        return FieldSaveResult(
            field_code=update.field_code, ok=False,
            error=f"db error: {str(exc)[:200]}",
            source_table=src_table, source_column=src_col,
        )


@router.post("/admin/generate-token", response_model=GenerateTokenResponse)
async def admin_generate_token(
    body: GenerateTokenRequest,
    request: Request,
    admin: AdminContext = Depends(_require_admin),
):
    raw_token = secrets.token_urlsafe(48)

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, admin.tenant_id)

            student_row = await conn.fetchrow(
                "SELECT id, tenant_id, first_name, last_name FROM public.students WHERE id = $1",
                body.student_id,
            )
            if not student_row:
                raise HTTPException(404, f"student {body.student_id} not found")
            if str(student_row["tenant_id"]) != str(admin.tenant_id):
                raise HTTPException(403, "student belongs to a different tenant")

            permissions = json.dumps({
                "can_edit": body.can_edit,
                "can_view": body.can_view,
                "can_upload_artifacts": body.can_upload_artifacts,
            })

            token_id = await conn.fetchval(
                """INSERT INTO public.parent_access_tokens (
                    tenant_id, student_id, family_member_id, token, display_name,
                    email, preferred_ui_locale, permissions, granted_by, expires_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
                RETURNING id""",
                admin.tenant_id, body.student_id, body.family_member_id, raw_token,
                body.display_name, body.email, body.preferred_ui_locale,
                permissions, admin.user_id, body.expires_at,
            )

    base_url = os.environ.get("OUTCOMESTAR_BASE_URL", "https://outcomestar.app")
    student_slug = student_row["first_name"].lower() if student_row["first_name"] else str(body.student_id)
    access_url = f"{base_url}/parent/{student_slug}?t={raw_token}"

    return GenerateTokenResponse(
        token_id=token_id,
        token=raw_token,
        access_url=access_url,
        student_id=body.student_id,
        display_name=body.display_name,
        expires_at=body.expires_at,
    )


@router.get("/admin/tokens", response_model=list[TokenListItem])
async def admin_list_tokens(
    request: Request,
    student_id: Optional[UUID] = Query(None),
    include_revoked: bool = Query(False),
    admin: AdminContext = Depends(_require_admin),
):
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, admin.tenant_id)

            where_clauses = ["tenant_id = $1"]
            params: list = [admin.tenant_id]
            if student_id:
                where_clauses.append(f"student_id = ${len(params)+1}")
                params.append(student_id)
            if not include_revoked:
                where_clauses.append("revoked_at IS NULL")

            rows = await conn.fetch(
                f"""SELECT id AS token_id, student_id, display_name, email,
                          preferred_ui_locale, granted_at, expires_at,
                          last_used_at, use_count, is_active, revoked_at
                    FROM public.parent_access_tokens
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY granted_at DESC""",
                *params,
            )

    return [TokenListItem(**dict(r)) for r in rows]


@router.post("/admin/tokens/{token_id}/revoke")
async def admin_revoke_token(
    token_id: UUID,
    request: Request,
    reason: Optional[str] = Query(None),
    admin: AdminContext = Depends(_require_admin),
):
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, admin.tenant_id)
            result = await conn.execute(
                """UPDATE public.parent_access_tokens
                   SET revoked_at = now(), revoked_by = $1, revoked_reason = $2,
                       is_active = false, updated_at = now()
                   WHERE id = $3 AND tenant_id = $4 AND revoked_at IS NULL""",
                admin.user_id, reason, token_id, admin.tenant_id,
            )
    if result.endswith("0"):
        raise HTTPException(404, "token not found, already revoked, or wrong tenant")
    return {"ok": True, "token_id": str(token_id), "revoked_at": datetime.now(timezone.utc).isoformat()}
