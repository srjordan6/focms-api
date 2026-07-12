"""
focms_form_schemas.py — Schema-driven form definitions + entry writer.

v0.12.116 · Private Tutoring course_type (subject, school, teacher, teacher email,
         school year, notes, skills gained, course description, grade received OR
         award/certificate of completion) on courses_taken; universal course
         skills-gained -> student_skills; meta-skill inference rule R11
         (course subject/name/description/skills -> meta-skills, internal-only);
         removed TEMP _debug_events endpoint.

v0.12.52 · feat: SCED subject taxonomy — /catalogs/subjects endpoint; courses_taken.subject check now SCED codes 01-23 + other.
         v0.12.51 · fix: courses subject must match check-constraint enum (invalid->other); add courses_taken.subject_other free-text; GRANT DELETE done via MCP.
         v0.12.50 · feat: job_experiences.supervisor_email (Ask-for-Recommendation mailto); remove temporary debug_raw passthrough.
         v0.12.49 · fix: coerce em-dash placeholder values to null before parse.
         v0.12.48 · fix: sanitize deepseek `"key": word: N` pollution before JSON parse.
         v0.12.47 · debug: surface raw LLM text on parse failure (temporary).
         v0.12.46 · fix: request response_format json_object for analyze/extract; strip injected <span> tags before JSON parse (deepseek pollution).
         v0.12.45 · fix: openai_compatible reads message.reasoning when content is empty (thinking models like qwen3.5:cloud return reasoning-only).
         v0.12.44 · fix: _extract_json tolerates thinking models (strip <think>, code fences, balanced-brace scan) so qwen3.5:cloud analysis parses.
         v0.12.43d · fix: default application_type must be common_app (check constraint); common_app_personal rejected.
         v0.12.43c · fix: also default status (NOT NULL) on insert; explicit NULL was overriding column default.
         v0.12.43b · fix: default application_type when unset (NOT NULL) so Studio autosave can create rows.
         v0.12.43 · fix: essays.topic_themes is jsonb not text[] - cast ::jsonb on write, json-decode on read (was blocking all essay creation).
         v0.12.42 · Essay analysis (/essays/{id}/analyze) + exemplar corpus (/catalogs/essay-exemplars, /admin/essay-exemplars); shared _llm_complete adapter shim (provider-swappable). 
         v0.12.41 · Essay Studio: /catalogs/essay-guidance, /essays/sample (AI, env-swappable), /essays/{id}/autosave (versioned), /essays/{id}/versions.
         v0.12.40 · Higher-Ed: essays + recommenders + financial-aid + college-tests GET/POST.
         v0.12.39 · Career: job_description field + meta-skill inference rule R10 (job title/description/skills -> meta-skills).
         v0.12.38 · Career pillar: career-profile + job-experiences + references (covers standard employment application fields).
         v0.12.37b · languages: seed carries codes + is never cached (avoids poisoning). fix: import List (Pydantic rebuild). UI-string runtime translation via Google (DB-cached per locale) — every Google language works, English is source.
         v0.12.36b · fix: use _pp_os (os not imported at module top). Languages catalog accepts GOOGLE_TRANSLATE_API_KEY or GOOGLE_PLACES_API_KEY.
         v0.12.35 · Tenant locale GET/POST (UI language en-US/es-ES).
         v0.12.34 · Personal: residence_country persisted on SPD; used as global default country for all address/school pickers.
         v0.12.33a · school-search K-12 queries k12_schools (per-school CCD, pg_trgm); state optional, no live DOE call.
         v0.12.32 · Teacher registry (GET/POST) + universal band-aware school search proxy (DOE k12 / IPEDS college).
         v0.12.31 · Academics: per-grade year records with mid-year school-transfer support.
         v0.12.30 · Academics: full school profile (CEEB, grading scale, class size, boarding, counselor) + report_cards GET/POST.
         v0.12.29 · Academics: current-school helper for prefill (name, address, phone).
         v0.12.28a · Academics band summary: coerce text grade values (PK, K, "9") to int before comparison.
         v0.12.28 · Academics grade-band scoping: summary + courses GET/POST filtered by band (preschool/elementary/middle/high).
         v0.12.27a · fix: details column arrives as str via asyncpg; json-decode before .get.
         v0.12.27 · Higher Education: applications GET/POST + CIP majors catalog.
         v0.12.26 · Higher Education: universities catalog + target-schools GET/POST.
         v0.12.25 · universal activity fields: skills_gained + show_on_showcase across affiliations, awards, sessions.
         v0.12.24 · Extracurricular expansion: programs picker, named-awards catalog, EC milestones catalog, awards GET/POST, sessions log GET/POST.
         v0.12.23 · Extra Curricular pillar: affiliations GET/POST for programs, activities, service orgs, coach relationships.
         v0.12.22 · SPS skills bucket is age-aware: denominator = age-appropriate + evidenced requirements; ahead-of-age skills count fully.
         v0.12.21 · fix: tenant GUC now set via SET LOCAL inside one transaction per handler (_tenant_conn); cures intermittent empty RLS reads through PgBouncer.
         v0.12.20 · fix: enum-safe event_type cast in inference engine (500 on auto-run).
         v0.12.19 · SPS fixes: skills bucket excluded when no student signal; inference engine auto-runs when empty (internal).
         v0.12.18 · Success Predictor Score: weighted A/E/S/M buckets per major with meta-alignment boost.
         v0.12.17 · meta-skills internal-only: parent-portal scope blocked from all meta endpoints; major-gap serves hard-skills-only basis to parent audiences.
         v0.12.16 · evidence-based meta-skill inference engine (200-skill taxonomy).
         v0.12.15 · meta-skills tracking + major-gap report engine.
         v0.12.2 · Session 1 of the schema-driven parent portal build.
         v0.12.1 fixes veteran_military_status placeholder alignment.
         v0.12.2 adds GET /entries/{student_id} for form pre-population.

Endpoints:
  GET  /focms/v1/form-schemas               Full catalog for form rendering
  POST /focms/v1/entries                    Schema-driven write to profile tables

GET returns every active row from field_capture_catalog (all pillars) plus
the eight reference catalogs the parent portal needs for autocomplete pickers:
  life_milestones, named_awards, standardized_tests, courses,
  interview_types, employer_types, artifact_types, psychological_indicators.

POST routes each field_code to the correct table+column and upserts.
Session 1 handlers:  students, student_personal_details,
                     student_addresses (by address_kind),
                     veteran_military_status.
Session 2 will add:  family_members, student_external_identifiers,
                     events, affiliations, personal_records.

Auth: Bearer token in Authorization header, tenant UUID in X-Tenant-Id,
verified against api_tokens by focms_api.get_context (imported below).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

log = logging.getLogger("focms-form-schemas")
router = APIRouter(prefix="/focms/v1", tags=["form-schemas"])

# v0.12.21: tenant-scoped connection. SELECT set_config(...,true) is
# transaction-local; through PgBouncer transaction pooling each statement can
# land on a different server connection, so the GUC silently evaporates and
# RLS reads come back empty (playbook rule: f-string SET LOCAL inside one
# explicit transaction). Every handler now does all DB work inside a single
# transaction opened by this context manager.
import uuid as _uuid
from contextlib import asynccontextmanager

@asynccontextmanager
async def _tenant_conn(pool, tenant_id: str):
    _uuid.UUID(tenant_id)  # validate before literal interpolation
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            yield conn


# ---------------------------------------------------------------------------
# Auth dependency — reuse focms_api.authenticate (validates against
# FOCMS_API_TOKENS_JSON env var, the same mechanism every other endpoint uses)
# ---------------------------------------------------------------------------

async def _resolve_context(request: Request) -> dict:
    """
    v0.12.81: env-registry token via focms_api.authenticate (sync shim), then
    api_tokens fallback via focms_api.db_token_principal - signup-minted
    parent-portal tokens now work on every form-schemas endpoint.
    """
    from focms_api import authenticate as _authenticate, db_token_principal
    authorization = request.headers.get("authorization")
    try:
        ctx = _authenticate(authorization=authorization)
    except HTTPException as exc:
        if exc.status_code != 401 or not (authorization or "").startswith("Bearer "):
            raise
        ctx = await db_token_principal(
            request.app.state.pool, authorization.removeprefix("Bearer ").strip())
        if not ctx:
            raise
    # authenticate returns {"tenant_id", "user_id", "role"} plus maybe more.
    # Normalize to the shape the endpoints expect.
    # v0.12.107: billing hold blocks all writes (public site is gated separately).
    if ctx.get("billing_hold") and request.method in ("POST", "PUT", "PATCH", "DELETE"):
        raise HTTPException(402, {
            "error": "billing_hold",
            "message": "Membership payment for the new age band did not go through. "
                       "Update the card in Storage & Billing - editing unlocks the moment "
                       "payment succeeds. Nothing has been deleted.",
        })
    return {
        "token_id":    ctx.get("token_id"),
        "tenant_id":   ctx.get("tenant_id"),
        "user_id":     ctx.get("user_id"),
        "scope":       ctx.get("role") or ctx.get("scope"),
        "student_ids": ctx.get("student_ids") or [],
    }


# ===========================================================================
# GET /focms/v1/form-schemas
# ===========================================================================

CATALOG_QUERIES: dict[str, str] = {
    "life_milestones": """
        SELECT id, code, title, description, age_band, typical_age_min,
               typical_age_max, pillar, sub_pillar, category, universality,
               developmental_significance, typical_capture_fields,
               admission_traits_developed, sort_order
          FROM life_milestones_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, title
    """,
    "named_awards": """
        SELECT id, code, award_name, granting_organization, description,
               level, category, sub_category, prestige_tier,
               typical_age_band, admissions_weight, selection_criteria,
               typical_capture_fields, sort_order
          FROM named_awards_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, award_name
    """,
    "standardized_tests": """
        SELECT id, code, test_name, granting_body, description,
               test_kind, typical_age_band, max_score, score_components,
               retake_allowed, superscore_allowed, admissions_weight,
               sort_order
          FROM standardized_tests_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, test_name
    """,
    "courses": """
        SELECT id, code, course_name, granting_body, description,
               course_type, subject, typical_grade_level, credit_value,
               weight_multiplier, admissions_weight, is_rigor_marker,
               sort_order
          FROM courses_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, course_name
    """,
    "interview_types": """
        SELECT id, code, type_name, category, description,
               typical_duration_minutes, typical_format,
               preparation_recommended, thank_you_required,
               admissions_weight, sort_order
          FROM interview_types_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, type_name
    """,
    "employer_types": """
        SELECT id, code, type_name, description, category,
               is_paid_default, admissions_weight, typical_age_band,
               sort_order
          FROM employer_types_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, type_name
    """,
    "artifact_types": """
        SELECT id, code, type_name, category, description,
               typical_file_formats, admissions_use, sort_order
          FROM artifact_types_catalog
         WHERE is_active AND deleted_at IS NULL
         ORDER BY sort_order NULLS LAST, type_name
    """,
    "psychological_indicators": """
        SELECT i.id, i.code, i.pillar_code, i.indicator_name, i.description,
               i.spectrum_low_label, i.spectrum_high_label,
               i.spectrum_midpoint_meaning, i.college_fit_implications,
               i.feeds_admissions_traits, i.sort_order,
               p.pillar_name AS pillar_name
          FROM psychological_indicators_catalog i
     LEFT JOIN psychological_pillars_catalog p ON p.code = i.pillar_code
         WHERE i.is_active AND i.deleted_at IS NULL
         ORDER BY p.sort_order NULLS LAST, i.sort_order NULLS LAST, i.indicator_name
    """,
}


@router.get("/form-schemas")
async def get_form_schemas(
    request: Request,
    pillar: Optional[str] = None,
    include_catalogs: bool = True,
):
    """
    Return every active parent-form field definition, plus reference catalogs.

    Query params:
      pillar             optional: filter to one pillar
                         ('personal', 'academics', 'extracurricular',
                          'career', 'higher_education', 'cross_cutting')
      include_catalogs   default true; set false to skip reference catalogs
                         if the client already has them cached
    """
    context = await _resolve_context(request)
    tenant_id = context["tenant_id"]

    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, str(tenant_id)) as conn:

        where = ["is_active = true", "shown_in_parent_form = true"]
        args: list[Any] = []
        if pillar:
            args.append(pillar)
            where.append(f"pillar = ${len(args)}")

        field_rows = await conn.fetch(
            f"""
            SELECT
              field_code, field_label, field_description, helper_text,
              pillar, sub_pillar, source_table, source_column, source_jsonb_path,
              is_record_level, field_kind, choice_options,
              required_for_uca, required_for_common_app,
              required_for_service_academy, required_for_rotc,
              required_for_athletic_recruiting,
              is_pii, is_sensitive, is_encrypted_at_rest,
              accepts_artifact, artifact_kinds_allowed,
              artifact_max_size_mb, artifact_helper_text,
              default_visibility, visibility_lock_kind,
              visibility_max_level_under_lock, visibility_lock_reason,
              visibility_lock_lifts_at_age, visibility_lock_lifts_at_event,
              parent_form_section, parent_form_subsection,
              parent_form_section_order, parent_form_field_order,
              shown_in_child_dashboard, shown_in_public_site,
              greyed_out_in_parent_form, greyed_out_reason,
              is_array_capture, related_capture_group,
              notes, details,
              validation_required, validation_provider_codes,
              validation_blocking,
              autocomplete_min_chars, autocomplete_debounce_ms,
              suggests_via_provider
            FROM field_capture_catalog
            WHERE {' AND '.join(where)}
            ORDER BY
              parent_form_section_order NULLS LAST,
              parent_form_field_order   NULLS LAST,
              field_code
            """,
            *args,
        )

        fields = [_row_to_dict(r) for r in field_rows]

        catalogs: dict[str, list[dict]] = {}
        if include_catalogs:
            for name, sql in CATALOG_QUERIES.items():
                rows = await conn.fetch(sql)
                catalogs[name] = [_row_to_dict(r) for r in rows]

    return {
        "version": "0.12.2",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "field_count": len(fields),
        "fields": fields,
        "catalogs": catalogs if include_catalogs else None,
    }


# ===========================================================================
# GET /focms/v1/entries/{student_id}
# ===========================================================================
# Read-back: returns a { field_code: value } map for a given student, so the
# parent portal can pre-populate the form with what's already saved. Only
# reads Session 1 tables. Ciphertext columns are omitted.

@router.get("/entries/{student_id}")
async def get_entries(
    request: Request,
    student_id: str,
    pillar: Optional[str] = None,
):
    """Return existing field values keyed by field_code, for form pre-population."""
    context = await _resolve_context(request)
    tenant_id = context["tenant_id"]

    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, str(tenant_id)) as conn:

        # Fetch all catalog fields (optionally filtered by pillar)
        where = ["is_active = true", "shown_in_parent_form = true"]
        args: list[Any] = []
        if pillar:
            args.append(pillar)
            where.append(f"pillar = ${len(args)}")

        catalog_rows = await conn.fetch(
            f"""SELECT field_code, source_table, source_column, is_encrypted_at_rest
                  FROM field_capture_catalog
                 WHERE {' AND '.join(where)}""",
            *args,
        )

        # Pre-fetch the singleton rows we might read from
        students_row = await conn.fetchrow(
            "SELECT * FROM students WHERE id = $1", student_id
        )
        spd_row = await conn.fetchrow(
            "SELECT * FROM student_personal_details WHERE student_id = $1",
            student_id,
        )
        vet_row = await conn.fetchrow(
            "SELECT * FROM veteran_military_status WHERE student_id = $1",
            student_id,
        )
        addr_rows = await conn.fetch(
            """SELECT * FROM student_addresses
                WHERE student_id = $1
                  AND is_current = true
                  AND deleted_at IS NULL""",
            student_id,
        )
        # Bucket addresses by kind
        addr_by_kind: dict[str, dict] = {}
        for r in addr_rows:
            addr_by_kind[r["address_kind"]] = dict(r)

    # Reverse the COLUMN_ALIASES for reads (catalog code -> physical col already
    # in COLUMN_ALIASES; we look up by (table, catalog_col) -> physical_col below).

    values: dict[str, Any] = {}
    for cat in catalog_rows:
        code = cat["field_code"]
        table = cat["source_table"]
        col = cat["source_column"]

        # Skip ciphertext columns (Session 3 will handle)
        if cat["is_encrypted_at_rest"] or (col and col.endswith("_ciphertext")):
            continue

        # Apply alias for read
        real_col = COLUMN_ALIASES.get((table, col), col)

        # Route to the appropriate row source
        row_source: Optional[dict] = None
        if table in ("students", "student"):
            row_source = dict(students_row) if students_row else None
        elif table == "student_personal_details":
            row_source = dict(spd_row) if spd_row else None
        elif table == "veteran_military_status":
            row_source = dict(vet_row) if vet_row else None
        elif table == "student_addresses":
            # Parse the address_kind out of field_code: 'student_addresses.<kind>.<col>'
            parts = code.split(".")
            if len(parts) == 3:
                kind = parts[1]
                col_read = parts[2]
                real_col = ADDR_ALIASES.get(col_read, col_read)
                row_source = addr_by_kind.get(kind)

        if row_source is None:
            continue

        if real_col in row_source:
            v = row_source[real_col]
            # asyncpg date/UUID → JSON-friendly string
            if isinstance(v, (datetime,)):
                values[code] = v.isoformat()
            elif isinstance(v, UUID):
                values[code] = str(v)
            elif hasattr(v, "isoformat"):  # date, time
                values[code] = v.isoformat()
            else:
                values[code] = v

    return {
        "student_id": student_id,
        "pillar": pillar,
        "field_count": len(values),
        "values": values,
    }


# ===========================================================================
# POST /focms/v1/entries
# ===========================================================================

class EntryValue(BaseModel):
    field_code: str = Field(..., description="Dot-path like 'students.first_name'")
    value: Any = Field(None, description="Field value; type depends on field_kind")


class EntriesRequest(BaseModel):
    student_id: str
    entries: list[EntryValue]


class EntriesResponse(BaseModel):
    saved: int
    deferred: int
    errors: list[dict]
    touched_records: dict


# Supported source_table routes for Session 1.
# Everything else returns status='deferred_to_session_2'.
SESSION_1_TABLES = {
    "students",
    "student",             # catalog uses this alias for the students table
    "student_personal_details",
    "student_addresses",
    "veteran_military_status",
}

# Column alias fixes where field_capture_catalog uses a name that doesn't
# match the physical column. Format: (source_table, source_column) -> real_col.
COLUMN_ALIASES: dict[tuple[str, str], str] = {
    ("students",             "legal_first_name"):  "first_name",
    ("students",             "legal_middle_name"): "middle_name",
    ("students",             "legal_last_name"):   "last_name",
    ("student",              "legal_first_name"):  "first_name",
    ("student",              "legal_middle_name"): "middle_name",
    ("student",              "legal_last_name"):   "last_name",
}

# Whitelisted columns per table so a malicious field_code cannot write
# anywhere it shouldn't. Column names verified against the live schema.
STUDENTS_COLS = {
    "first_name", "middle_name", "last_name", "preferred_name",
    "display_name", "pronouns", "birth_date", "birth_country",
    "primary_citizenship", "secondary_citizenship",
    "current_school_leaid", "current_grade",
    "expected_hs_graduation_year",
    "residence_state", "residence_country", "headline", "bio",
}
SPD_COLS = {
    "chosen_name", "previous_last_names", "legal_sex_at_birth",
    "residence_country",
    "pronouns", "gender_identity", "marital_status",
    "place_of_birth_city", "place_of_birth_state_province",
    "place_of_birth_country", "place_of_birth_country_iso2",
    "citizenship_status",
    "dual_citizenship_other_country", "dual_citizenship_other_country_iso2",
    "permanent_resident_origin_country",
    "permanent_resident_origin_country_iso2",
    "visa_type", "years_in_us",
    "is_hispanic_or_latino", "hispanic_country_of_origin",
    "hispanic_country_of_origin_iso2",
    "racial_background", "asian_country_of_origin",
    "asian_country_of_origin_iso2",
    "american_indian_tribal_affiliation", "is_enrolled_in_tribe",
    "language_spoken_at_home", "first_language_native",
    "email_primary", "email_secondary",
    "phone_primary", "phone_primary_e164", "phone_primary_dial_code",
    "phone_alternate", "phone_alternate_e164", "phone_alternate_dial_code",
    "preferred_address_locale", "preferred_ui_locale",
    "public_site_locale", "native_language_locale",
    "preferred_name_script", "legal_name_native",
    "legal_name_native_script", "legal_name_native_locale",
    "preferred_name_native", "preferred_name_native_locale",
    "name_romanization_source",
}
ADDR_COLS = {
    "street_address", "street_address_line_2", "street_address_line_3",
    "apt_unit", "building_or_district",
    "city_town", "state_province", "country", "country_iso2",
    "zip_postal_code", "subdivision_iso", "subdivision_name",
    "phone_at_address", "phone_at_address_e164", "phone_at_address_dial_code",
    "script", "transliterated_address", "notes",
}
# Address catalog codes use different names — map to physical columns.
ADDR_ALIASES = {
    "street":    "street_address",
    "street_line_2": "street_address_line_2",
    "street_line_3": "street_address_line_3",
    "apt":       "apt_unit",
    "city":      "city_town",
    "state":     "state_province",
    "zip":       "zip_postal_code",
}
MIL_COLS = {
    "is_veteran", "is_active_us_military", "is_dependent_of_us_veteran",
    "is_national_guard_or_active_reserve", "service_branches",
    "planning_to_use_veteran_education_benefits",
    "honorably_discharged", "discharge_explanation",
    "service_start_date", "service_end_date",
    "rank_at_separation", "applicable_dependent_relationship",
    "applicable_dependent_to_branches", "notes",
}


@router.post("/entries", response_model=EntriesResponse)
async def post_entries(request: Request, body: EntriesRequest):
    """
    Route each field_code to the correct table/column and upsert.

    Encrypted-at-rest columns are NOT written by this endpoint — that lives
    in a dedicated encrypted-write path (Session 3). If the client submits
    a value for a `*_ciphertext` column here, it's rejected.
    """
    context = await _resolve_context(request)
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    scope = context.get("scope")
    student_ids = context.get("student_ids") or []

    # Verify token has access to this student
    if scope == "parent_portal" and body.student_id not in student_ids:
        raise HTTPException(status_code=403, detail="student_not_authorized")

    # Bucket entries by target table
    buckets: dict[str, list[dict]] = {
        "students": [],
        "student_personal_details": [],
        "student_addresses_by_kind": [],   # nested: [{kind: 'permanent', col: 'street_address', value: ...}]
        "veteran_military_status": [],
        "deferred": [],
    }
    errors: list[dict] = []

    for entry in body.entries:
        parsed = _parse_field_code(entry.field_code)
        if not parsed:
            errors.append({"field_code": entry.field_code,
                           "error": "unparseable_field_code"})
            continue
        table, scope_key, column = parsed

        # Reject encrypted columns from this endpoint
        if column.endswith("_ciphertext"):
            errors.append({"field_code": entry.field_code,
                           "error": "encrypted_column_not_writable_here"})
            continue

        if table not in SESSION_1_TABLES:
            buckets["deferred"].append({"field_code": entry.field_code})
            continue

        # Apply aliases
        real_col = COLUMN_ALIASES.get((table, column), column)

        if table in ("students", "student"):
            if real_col not in STUDENTS_COLS:
                errors.append({"field_code": entry.field_code,
                               "error": f"column_not_writable: {real_col}"})
                continue
            buckets["students"].append({"col": real_col, "val": entry.value})

        elif table == "student_personal_details":
            if real_col not in SPD_COLS:
                errors.append({"field_code": entry.field_code,
                               "error": f"column_not_writable: {real_col}"})
                continue
            buckets["student_personal_details"].append(
                {"col": real_col, "val": entry.value}
            )

        elif table == "student_addresses":
            # scope_key is the address_kind: 'permanent', 'mailing', etc.
            if not scope_key:
                errors.append({"field_code": entry.field_code,
                               "error": "address_kind_required_in_field_code"})
                continue
            real_col = ADDR_ALIASES.get(column, column)
            if real_col not in ADDR_COLS:
                errors.append({"field_code": entry.field_code,
                               "error": f"column_not_writable: {real_col}"})
                continue
            buckets["student_addresses_by_kind"].append(
                {"kind": scope_key, "col": real_col, "val": entry.value}
            )

        elif table == "veteran_military_status":
            if real_col not in MIL_COLS:
                errors.append({"field_code": entry.field_code,
                               "error": f"column_not_writable: {real_col}"})
                continue
            buckets["veteran_military_status"].append(
                {"col": real_col, "val": entry.value}
            )

    touched: dict[str, list[str]] = {}
    saved_count = 0

    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, str(tenant_id)) as conn:

        async with conn.transaction():
            # ----- students (UPDATE only; row already exists) -----
            if buckets["students"]:
                cols = [b["col"] for b in buckets["students"]]
                vals = [b["val"] for b in buckets["students"]]
                set_clause = ", ".join(
                    f"{c} = ${i + 2}" for i, c in enumerate(cols)
                )
                sql = f"""
                    UPDATE students
                       SET {set_clause},
                           updated_at = now(),
                           updated_by = ${len(cols) + 2}
                     WHERE id = $1
                       AND tenant_id = ${len(cols) + 3}
                     RETURNING id
                """
                row = await conn.fetchrow(
                    sql, body.student_id, *vals, user_id, tenant_id
                )
                if row:
                    saved_count += len(cols)
                    touched["students"] = [str(row["id"])]
                else:
                    errors.append({"table": "students",
                                   "error": "student_not_found"})

            # ----- student_personal_details (UPSERT on student_id) -----
            if buckets["student_personal_details"]:
                cols = [b["col"] for b in buckets["student_personal_details"]]
                vals = [b["val"] for b in buckets["student_personal_details"]]
                col_list = ", ".join(cols)
                placeholders = ", ".join(
                    f"${i + 3}" for i in range(len(cols))
                )
                update_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in cols
                )
                sql = f"""
                    INSERT INTO student_personal_details
                        (student_id, tenant_id, {col_list},
                         created_by, updated_by, visibility)
                    VALUES ($1, $2, {placeholders},
                            ${len(cols) + 3}, ${len(cols) + 3}, 'private')
                    ON CONFLICT (student_id) DO UPDATE
                       SET {update_clause},
                           updated_at = now(),
                           updated_by = EXCLUDED.updated_by
                    RETURNING student_id
                """
                row = await conn.fetchrow(
                    sql, body.student_id, tenant_id, *vals, user_id
                )
                if row:
                    saved_count += len(cols)
                    touched["student_personal_details"] = [str(row["student_id"])]

            # ----- veteran_military_status (UPSERT on student_id) -----
            if buckets["veteran_military_status"]:
                cols = [b["col"] for b in buckets["veteran_military_status"]]
                vals = [b["val"] for b in buckets["veteran_military_status"]]
                update_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in cols
                )
                # Non-null defaults for required cols not being written.
                required_defaults = {
                    "is_veteran": False,
                    "is_active_us_military": False,
                    "is_dependent_of_us_veteran": False,
                    "is_national_guard_or_active_reserve": False,
                    "service_branches": [],
                }
                defaults_cols = [c for c in required_defaults if c not in cols]
                defaults_vals = [required_defaults[c] for c in defaults_cols]

                # Layout: $1=student_id, $2=tenant_id, then vals, then defaults, then user_id.
                all_cols = cols + defaults_cols
                col_list = ", ".join(all_cols)
                placeholders = ", ".join(
                    f"${i + 3}" for i in range(len(all_cols))
                )
                user_id_ph = f"${3 + len(all_cols)}"

                sql = f"""
                    INSERT INTO veteran_military_status
                        (student_id, tenant_id, {col_list},
                         created_by, updated_by, visibility)
                    VALUES ($1, $2, {placeholders},
                            {user_id_ph}, {user_id_ph}, 'private')
                    ON CONFLICT (student_id) DO UPDATE
                       SET {update_clause},
                           updated_at = now(),
                           updated_by = EXCLUDED.updated_by
                    RETURNING student_id
                """
                row = await conn.fetchrow(
                    sql, body.student_id, tenant_id,
                    *vals, *defaults_vals, user_id
                )
                if row:
                    saved_count += len(cols)
                    touched["veteran_military_status"] = [str(row["student_id"])]

            # ----- student_addresses (UPSERT on student_id, address_kind) -----
            if buckets["student_addresses_by_kind"]:
                by_kind: dict[str, list[dict]] = {}
                for b in buckets["student_addresses_by_kind"]:
                    by_kind.setdefault(b["kind"], []).append(b)

                touched["student_addresses"] = []
                for kind, entries_for_kind in by_kind.items():
                    cols = [b["col"] for b in entries_for_kind]
                    vals = [b["val"] for b in entries_for_kind]
                    col_list = ", ".join(cols)
                    placeholders = ", ".join(
                        f"${i + 4}" for i in range(len(cols))
                    )
                    update_clause = ", ".join(
                        f"{c} = EXCLUDED.{c}" for c in cols
                    )
                    sql = f"""
                        INSERT INTO student_addresses
                            (student_id, tenant_id, address_kind,
                             is_current, {col_list},
                             created_by, updated_by, visibility)
                        VALUES ($1, $2, $3, true, {placeholders},
                                ${len(cols) + 4}, ${len(cols) + 4}, 'private')
                        ON CONFLICT (student_id, address_kind)
                           WHERE is_current = true AND deleted_at IS NULL
                           DO UPDATE
                           SET {update_clause},
                               updated_at = now(),
                               updated_by = EXCLUDED.updated_by
                        RETURNING id
                    """
                    try:
                        row = await conn.fetchrow(
                            sql, body.student_id, tenant_id, kind,
                            *vals, user_id
                        )
                        if row:
                            saved_count += len(cols)
                            touched["student_addresses"].append(str(row["id"]))
                    except asyncpg.exceptions.UniqueViolationError:
                        # No partial unique index for the ON CONFLICT clause.
                        # Fall back to explicit lookup + update.
                        existing = await conn.fetchrow(
                            """SELECT id FROM student_addresses
                                WHERE student_id = $1
                                  AND address_kind = $2
                                  AND is_current = true
                                  AND deleted_at IS NULL
                                LIMIT 1""",
                            body.student_id, kind,
                        )
                        if existing:
                            set_clause = ", ".join(
                                f"{c} = ${i + 2}" for i, c in enumerate(cols)
                            )
                            await conn.execute(
                                f"""UPDATE student_addresses
                                       SET {set_clause},
                                           updated_at = now(),
                                           updated_by = ${len(cols) + 2}
                                     WHERE id = $1""",
                                existing["id"], *vals, user_id,
                            )
                            saved_count += len(cols)
                            touched["student_addresses"].append(str(existing["id"]))
                        else:
                            errors.append({"field_code": f"student_addresses.{kind}.*",
                                           "error": "upsert_conflict"})

    log.info(
        "post_entries student=%s saved=%d deferred=%d errors=%d",
        body.student_id, saved_count,
        len(buckets["deferred"]), len(errors),
    )

    return EntriesResponse(
        saved=saved_count,
        deferred=len(buckets["deferred"]),
        errors=errors,
        touched_records=touched,
    )


# ===========================================================================
# Helpers
# ===========================================================================

def _parse_field_code(code: str) -> Optional[tuple[str, Optional[str], str]]:
    """
    Parse a field_code from field_capture_catalog into (table, scope, column).

    Two supported forms:
      "table.column"                 -> (table, None,  column)
      "table.scope.column"           -> (table, scope, column)   (e.g. addresses)

    Returns None if the code doesn't split cleanly.
    """
    parts = code.split(".")
    if len(parts) == 2:
        return parts[0], None, parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None


def _row_to_dict(row: asyncpg.Record) -> dict:
    """Convert asyncpg Record to plain dict, JSON-serializing UUIDs/datetimes."""
    out: dict = {}
    for k, v in dict(row).items():
        if isinstance(v, UUID):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, (dict, list)) or v is None:
            out[k] = v
        else:
            out[k] = v
    return out


# ===========================================================================
# Parent-portal capture endpoints (v0.12.35)
#   + identity-documents: proof of age gates under-10 free access
# ===========================================================================
from datetime import date as _pp_date


# --------------------------------- helpers ---------------------------------

def _pp_parse_date(s):
    if not s:
        return None
    try:
        return _pp_date.fromisoformat(str(s).strip())
    except Exception:
        return None


def _pp_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _pp_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def _pp_skills(v):
    """Normalize a skills list to a list[str]."""
    if not v:
        return []
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            v = [v]
    return [str(x).strip() for x in v if str(x).strip()]


def _pp_artifacts(v):
    """Normalize an artifact-id list to list[str]."""
    if not v:
        return []
    return [str(x).strip() for x in v if str(x).strip()]


_GRADE_LETTERS = {
    "A+": 4.0, "A": 4.0, "A-": 3.7, "B+": 3.3, "B": 3.0, "B-": 2.7,
    "C+": 2.3, "C": 2.0, "C-": 1.7, "D+": 1.3, "D": 1.0, "D-": 0.7, "F": 0.0,
}
_RIGOR_BONUS = {"ap": 1.0, "ib": 1.0, "dual": 1.0, "honors": 0.5, "regular": 0.0}


def _pp_grade_points(grade):
    """Map a letter or numeric grade to unweighted 4.0 points, or None."""
    if grade is None:
        return None
    g = str(grade).strip().upper()
    if not g:
        return None
    if g in _GRADE_LETTERS:
        return _GRADE_LETTERS[g]
    try:
        n = float(g)
    except Exception:
        return None
    if n >= 93: return 4.0
    if n >= 90: return 3.7
    if n >= 87: return 3.3
    if n >= 83: return 3.0
    if n >= 80: return 2.7
    if n >= 77: return 2.3
    if n >= 73: return 2.0
    if n >= 70: return 1.7
    if n >= 67: return 1.3
    if n >= 63: return 1.0
    if n >= 60: return 0.7
    return 0.0


async def _pp_context(request: Request, student_id: str):
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal" and student_id not in (ctx.get("student_ids") or []):
        raise HTTPException(status_code=403, detail="student_not_authorized")
    uid = ctx.get("user_id")
    return str(ctx["tenant_id"]), (str(uid) if uid else None)


async def _pp_internal_context(request: Request, student_id: str):
    """Like _pp_context but rejects parent-portal tokens outright.
    Meta-skills are INTERNAL engine signal - parents never see or set them
    (decision of record 2026-07-02)."""
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal":
        raise HTTPException(status_code=403, detail="internal_only")
    uid = ctx.get("user_id")
    return str(ctx["tenant_id"]), (str(uid) if uid else None)


async def _pp_current_school_name(conn, student_id: str):
    row = await conn.fetchrow(
        "SELECT school_name FROM student_school_enrollments "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL "
        "ORDER BY is_current_school DESC, updated_at DESC NULLS LAST LIMIT 1",
        student_id,
    )
    return row["school_name"] if row else None


# ------------------------- Millstones & Milestones -------------------------

class MilestoneItem(BaseModel):
    milestone_code: Optional[str] = None
    custom_title: Optional[str] = None
    custom_category: Optional[str] = None
    happened: bool = True
    event_date: Optional[str] = None
    event_notes: Optional[str] = None
    artifact_url: Optional[str] = None


class MilestonesRequest(BaseModel):
    items: list[MilestoneItem] = Field(default_factory=list)


@router.get("/student/{student_id}/milestones")
async def get_student_milestones(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT milestone_code, custom_title, custom_category, happened, event_date, event_notes, "
            "artifact_url FROM student_life_milestones WHERE student_id=$1::uuid AND deleted_at IS NULL",
            student_id)
    catalog, custom = [], []
    for r in rows:
        d = {"milestone_code": r["milestone_code"], "custom_title": r["custom_title"],
             "custom_category": r["custom_category"], "happened": r["happened"],
             "event_date": r["event_date"].isoformat() if r["event_date"] else None,
             "event_notes": r["event_notes"], "artifact_url": r["artifact_url"]}
        (catalog if r["milestone_code"] else custom).append(d)
    return {"student_id": student_id, "milestones": catalog, "custom": custom}


@router.post("/student/{student_id}/milestones")
async def post_student_milestones(request: Request, student_id: str, body: MilestonesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = cleared = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            # replace all parent-entered CUSTOM rows (no catalog code) up front
            await conn.execute(
                "DELETE FROM student_life_milestones WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                "AND source_system='parent_portal' AND milestone_code IS NULL", tenant_id, student_id)
            for item in body.items:
                code = (item.milestone_code or "").strip()
                notes = item.event_notes.strip() if item.event_notes and item.event_notes.strip() else None
                art = (item.artifact_url or "").strip() or None
                d = _pp_parse_date(item.event_date)
                if code:
                    await conn.execute(
                        "DELETE FROM student_life_milestones WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                        "AND milestone_code=$3", tenant_id, student_id, code)
                    if not item.happened and not d and not notes and not art:
                        cleared += 1
                        continue
                    await conn.execute(
                        "INSERT INTO student_life_milestones (tenant_id, student_id, milestone_code, "
                        "happened, event_date, event_notes, artifact_url, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,'parent_portal',$8::uuid,$8::uuid)",
                        tenant_id, student_id, code, bool(item.happened), d, notes, art, user_id)
                    saved += 1
                else:
                    title = (item.custom_title or "").strip()
                    if not title:
                        continue
                    await conn.execute(
                        "INSERT INTO student_life_milestones (tenant_id, student_id, milestone_code, "
                        "custom_title, custom_category, happened, event_date, event_notes, artifact_url, "
                        "source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,NULL,$3,$4,$5,$6,$7,$8,'parent_portal',$9::uuid,$9::uuid)",
                        tenant_id, student_id, title, (item.custom_category or None),
                        bool(item.happened), d, notes, art, user_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "cleared": cleared}


# ------------------------------- Academics ---------------------------------

class AcademicsSchool(BaseModel):
    school_name: Optional[str] = None
    school_ceeb_code: Optional[str] = None
    school_type: Optional[str] = None
    counselor_name: Optional[str] = None
    counselor_email: Optional[str] = None
    start_date: Optional[str] = None
    expected_graduation_date: Optional[str] = None


class AcademicsGpa(BaseModel):
    unweighted: Optional[float] = None
    weighted: Optional[float] = None


class AcademicsRank(BaseModel):
    position: Optional[int] = None
    size: Optional[int] = None


class AcademicsRequest(BaseModel):
    school: AcademicsSchool = Field(default_factory=AcademicsSchool)
    gpa: AcademicsGpa = Field(default_factory=AcademicsGpa)
    rank: AcademicsRank = Field(default_factory=AcademicsRank)


@router.get("/student/{student_id}/academics")
async def get_student_academics(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        school = await conn.fetchrow(
            "SELECT school_name, school_ceeb_code, school_type, counselor_name, counselor_email, "
            "start_date, expected_graduation_date FROM student_school_enrollments "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY is_current_school DESC, updated_at DESC NULLS LAST LIMIT 1", student_id)
        gpa = await conn.fetchrow(
            "SELECT unweighted_gpa_value, gpa_value, weighted_gpa_value FROM gpa_history "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL AND source_system='parent_portal' "
            "ORDER BY as_of_date DESC NULLS LAST, updated_at DESC NULLS LAST LIMIT 1", student_id)
        rank = await conn.fetchrow(
            "SELECT rank_position, class_size FROM class_rank_history "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL AND source_system='parent_portal' "
            "ORDER BY as_of_date DESC NULLS LAST, updated_at DESC NULLS LAST LIMIT 1", student_id)
        est = await conn.fetchrow(
            "SELECT sum(grade_points_4_0 * COALESCE(credit_hours,1)) "
            "        / NULLIF(sum(COALESCE(credit_hours,1)),0) AS uw, "
            "       sum(grade_points_weighted * COALESCE(credit_hours,1)) "
            "        / NULLIF(sum(COALESCE(credit_hours,1)),0) AS wt "
            "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "AND grade_points_4_0 IS NOT NULL", student_id)
    return {
        "student_id": student_id,
        "school": dict(school) if school else {},
        "gpa": {
            "official_unweighted": float(gpa["unweighted_gpa_value"]) if gpa and gpa["unweighted_gpa_value"] is not None
                else (float(gpa["gpa_value"]) if gpa and gpa["gpa_value"] is not None else None),
            "official_weighted": float(gpa["weighted_gpa_value"]) if gpa and gpa["weighted_gpa_value"] is not None else None,
            "est_unweighted": round(float(est["uw"]), 3) if est and est["uw"] is not None else None,
            "est_weighted": round(float(est["wt"]), 3) if est and est["wt"] is not None else None,
        },
        "rank": {"position": rank["rank_position"] if rank else None,
                 "size": rank["class_size"] if rank else None},
    }


@router.post("/student/{student_id}/academics")
async def post_student_academics(request: Request, student_id: str, body: AcademicsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    written = []
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            sname = (body.school.school_name or "").strip()
            if sname:
                await conn.execute(
                    "DELETE FROM student_school_enrollments WHERE tenant_id=$1::uuid "
                    "AND student_id=$2::uuid AND source_system='parent_portal'", tenant_id, student_id)
                await conn.execute(
                    "INSERT INTO student_school_enrollments (tenant_id, student_id, school_name, "
                    "school_ceeb_code, school_type, counselor_name, counselor_email, start_date, "
                    "expected_graduation_date, is_current_school, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,true,'parent_portal',$10::uuid,$10::uuid)",
                    tenant_id, student_id, sname, (body.school.school_ceeb_code or None),
                    (body.school.school_type or None), (body.school.counselor_name or None),
                    (body.school.counselor_email or None), _pp_parse_date(body.school.start_date),
                    _pp_parse_date(body.school.expected_graduation_date), user_id)
                written.append("school")
            uw = _pp_num(body.gpa.unweighted)
            wt = _pp_num(body.gpa.weighted)
            if uw is not None or wt is not None:
                await conn.execute(
                    "DELETE FROM gpa_history WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                    "AND source_system='parent_portal'", tenant_id, student_id)
                await conn.execute(
                    "INSERT INTO gpa_history (tenant_id, student_id, as_of_date, gpa_value, "
                    "unweighted_gpa_value, weighted_gpa_value, is_weighted, is_official, reported_by_role, "
                    "source_system, created_by, updated_by) VALUES ($1::uuid,$2::uuid,CURRENT_DATE,$3,$4,$5,"
                    "$6,false,'parent','parent_portal',$7::uuid,$7::uuid)",
                    tenant_id, student_id, (uw if uw is not None else wt), uw, wt, (wt is not None), user_id)
                written.append("gpa")
            pos = _pp_int(body.rank.position)
            size = _pp_int(body.rank.size)
            if pos is not None or size is not None:
                await conn.execute(
                    "DELETE FROM class_rank_history WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                    "AND source_system='parent_portal'", tenant_id, student_id)
                await conn.execute(
                    "INSERT INTO class_rank_history (tenant_id, student_id, as_of_date, rank_position, "
                    "class_size, is_official, reported_by_role, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,CURRENT_DATE,$3,$4,false,'parent','parent_portal',$5::uuid,$5::uuid)",
                    tenant_id, student_id, pos, size, user_id)
                written.append("rank")
    return {"student_id": student_id, "written": written}


# ------------------------------- Coursework --------------------------------

_RIGOR = {"regular", "honors", "ap", "ib", "dual"}


class CourseItem(BaseModel):
    # v0.12.126: extra="forbid" - an unknown field is now a loud 422 instead of a
    # silent drop. Silent drops destroyed real report-card data (see v0.12.124).
    model_config = ConfigDict(extra="forbid")

    id: Optional[str] = None                       # v0.12.116 (upsert mode)
    course_name: Optional[str] = None
    course_code: Optional[str] = None
    sced_code: Optional[str] = None
    school_name: Optional[str] = None
    subject: Optional[str] = None
    subject_other: Optional[str] = None
    school_year: Optional[str] = None
    grade_level: Optional[int] = None
    term: Optional[str] = None
    grade_received: Optional[str] = None
    credit_hours: Optional[float] = None
    rigor: Optional[str] = None
    # v0.12.126: the course form sends these checkboxes; they were being silently
    # dropped, so ticking AP / Honors / IB / Dual Credit did nothing at all.
    is_honors: Optional[bool] = None
    is_ap: Optional[bool] = None
    is_ib: Optional[bool] = None
    is_dual_credit: Optional[bool] = None
    grade_points_4_0: Optional[float] = None
    grade_points_weighted: Optional[float] = None
    ap_exam_score: Optional[int] = None
    teacher_name: Optional[str] = None
    teacher_id: Optional[str] = None               # v0.12.120 (teacher registry)
    teacher_email: Optional[str] = None            # v0.12.116
    period: Optional[str] = None                   # v0.12.120 (class period)
    school_id: Optional[str] = None                # v0.12.120 (school profile link)
    course_type: Optional[str] = None              # v0.12.116 'regular'|'private_tutoring'
    course_description: Optional[str] = None       # v0.12.116 subject/course description
    completion_award: Optional[str] = None         # v0.12.116 award/certificate of completion
    notes: Optional[str] = None
    skills: list[str] = Field(default_factory=list)
    artifact_ids: list[str] = Field(default_factory=list)
    # v0.12.126: the form also posts these; declare them so extra="forbid" does not
    # reject a legitimate save. show_on_showcase is handled by the visibility flag;
    # skills_gained_custom is a UI scratch field.
    show_on_showcase: Optional[bool] = None
    skills_gained: list[str] = Field(default_factory=list)
    skills_gained_custom: Optional[str] = None


class CoursesRequest(BaseModel):
    items: list[CourseItem] = Field(default_factory=list)
    delete_ids: list[str] = Field(default_factory=list)   # v0.12.116
    mode: Optional[str] = None                            # v0.12.116 'replace' (default) | 'upsert'


@router.get("/student/{student_id}/courses")
async def get_student_courses(request: Request, student_id: str, band: Optional[str] = None):
    tenant_id, _ = await _pp_context(request, student_id)
    bounds = _band_bounds(band) if band else None
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        base = ("SELECT id::text AS id, course_name, course_code, sced_code, school_name, subject, subject_other, school_year, grade_level, term, grade_received, "
                "credit_hours, course_type, is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, "
                "teacher_name, teacher_email, course_description, details, "
                "period, school_id::text AS school_id, teacher_id::text AS teacher_id, "
                "notes, admission_traits_developed, evidence_artifact_ids, "
                "(visibility='public') AS show_on_showcase "
                "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL ")
        if bounds:
            rows = await conn.fetch(base + "AND grade_level BETWEEN $2 AND $3 ORDER BY grade_level NULLS LAST, course_name",
                                    student_id, bounds[0], bounds[1])
        else:
            rows = await conn.fetch(base + "ORDER BY grade_level NULLS LAST, course_name", student_id)

    def rigor_of(r):
        if r["is_ap"]: return "ap"
        if r["is_ib"]: return "ib"
        if r["is_dual_credit"]: return "dual"
        if r["is_honors"]: return "honors"
        # v0.12.116: course_type now carries the record TYPE, not the rigor.
        ct = r["course_type"]
        return ct if ct in _RIGOR else "regular"

    def _cdet(r):
        d = r["details"]
        if isinstance(d, str):
            try:
                return json.loads(d)
            except Exception:
                return {}
        return d or {}

    out = [
        {"id": r["id"], "course_name": r["course_name"], "course_code": r["course_code"], "sced_code": r["sced_code"], "school_name": r["school_name"], "subject": r["subject"],
         "subject_other": r["subject_other"],
         "school_year": r["school_year"], "grade_level": r["grade_level"], "term": r["term"],
         "grade_received": r["grade_received"],
         "credit_hours": float(r["credit_hours"]) if r["credit_hours"] is not None else None,
         "rigor": rigor_of(r), "is_honors": r["is_honors"], "is_ap": r["is_ap"], "is_ib": r["is_ib"], "is_dual_credit": r["is_dual_credit"],
         "ap_exam_score": r["ap_exam_score"], "teacher_name": r["teacher_name"],
         "teacher_email": r["teacher_email"],
         "teacher_id": r["teacher_id"],
         "period": r["period"],
         "school_id": r["school_id"],
         "course_type": r["course_type"],
         "course_description": r["course_description"] or _cdet(r).get("course_description"),
         "completion_award": _cdet(r).get("completion_award"),
         "is_private_tutoring": (r["course_type"] == "private_tutoring") or bool(_cdet(r).get("is_private_tutoring")),
         "show_on_showcase": r["show_on_showcase"],
         "notes": r["notes"], "skills": _pp_skills(r["admission_traits_developed"]),
         "artifact_ids": _pp_artifacts(r["evidence_artifact_ids"])} for r in rows]
    return {"student_id": student_id, "band": band, "items": out, "courses": out}


# --------------------- Recommendation letters + direct-submit ---------------------

class RecLetterItem(BaseModel):
    id: Optional[str] = None
    recommender_id: Optional[str] = None
    letter_text: Optional[str] = None
    file_name: Optional[str] = None
    file_mime: Optional[str] = None
    file_data: Optional[str] = None
    artifact_id: Optional[str] = None
    source: Optional[str] = None            # email_paste | upload | direct_form
    submitter_name: Optional[str] = None
    submitter_email: Optional[str] = None
    relationship: Optional[str] = None
    years_known: Optional[float] = None
    status: Optional[str] = None


class RecLettersRequest(BaseModel):
    items: List[RecLetterItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/recommendation-letters")
async def get_rec_letters(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, recommender_id::text AS recommender_id, letter_text, "
            "artifact_id::text AS artifact_id, source, submitter_name, submitter_email, file_name, file_mime, "
            "relationship, years_known, ratings, status, submitted_at "
            "FROM recommendation_letters WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY submitted_at DESC", student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["submitted_at"] = d["submitted_at"].isoformat() if d["submitted_at"] else None
        d["years_known"] = float(d["years_known"]) if d["years_known"] is not None else None
        d["ratings"] = json.loads(d["ratings"]) if d["ratings"] else None
        out.append(d)
    return {"student_id": student_id, "letters": out}


@router.post("/student/{student_id}/recommendation-letters")
async def post_rec_letters(request: Request, student_id: str, body: RecLettersRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = deleted = 0
    _SRC = {"email_paste", "upload", "direct_form"}
    _ST = {"received", "reviewed", "submitted_to_school"}
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                await conn.execute("UPDATE recommendation_letters SET deleted_at=now() "
                                   "WHERE id=$1::uuid AND student_id=$2::uuid", did, student_id)
                deleted += 1
            for it in body.items or []:
                if not (it.letter_text or it.artifact_id or it.file_data): continue
                src = it.source if it.source in _SRC else "email_paste"
                st = it.status if it.status in _ST else "received"
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    await conn.execute(
                        "UPDATE recommendation_letters SET letter_text=$3, artifact_id=$4::uuid, "
                        "source=$5, submitter_name=$6, submitter_email=$7, relationship=$8, "
                        "years_known=$9, status=$10, recommender_id=$11::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.letter_text, it.artifact_id, src,
                        it.submitter_name, it.submitter_email, it.relationship,
                        it.years_known, st, it.recommender_id)
                else:
                    await conn.execute(
                        "INSERT INTO recommendation_letters (tenant_id, student_id, recommender_id, "
                        "letter_text, artifact_id, source, submitter_name, submitter_email, "
                        "relationship, years_known, status, created_by, file_name, file_mime, file_data) "
                        "VALUES ($1::uuid,$2::uuid,$3::uuid,$4,$5::uuid,$6,$7,$8,$9,$10,$11,$12::uuid,$13,$14,$15)",
                        tenant_id, student_id, it.recommender_id, it.letter_text, it.artifact_id,
                        src, it.submitter_name, it.submitter_email, it.relationship,
                        it.years_known, st, user_id, it.file_name, it.file_mime, it.file_data)
                saved += 1
    return {"student_id": student_id, "saved": saved, "deleted": deleted}


@router.get("/student/{student_id}/recommendation-letters/{letter_id}/file")
async def get_rec_letter_file(request: Request, student_id: str, letter_id: str):
    from fastapi.responses import Response
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT file_name, file_mime, file_data FROM recommendation_letters "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL", letter_id, student_id)
    if not row or not row["file_data"]:
        raise HTTPException(status_code=404, detail="no file")
    import base64 as _b64
    data = _b64.b64decode(row["file_data"])
    return Response(content=data, media_type=row["file_mime"] or "application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{row["file_name"] or "letter"}"'})


# --------------------- Verified documents (Parchment-shared records) ---------------------

class VerifiedDocItem(BaseModel):
    id: Optional[str] = None
    doc_type: Optional[str] = None      # transcript|diploma|certificate|badge|other
    source: Optional[str] = None        # parchment_link|upload
    source_url: Optional[str] = None
    file_name: Optional[str] = None
    file_mime: Optional[str] = None
    file_data: Optional[str] = None     # base64
    notes: Optional[str] = None


class VerifiedDocsRequest(BaseModel):
    items: List[VerifiedDocItem] = []
    delete_ids: List[str] = []


def _pdf_signature_scan(data: bytes) -> bool:
    """Heuristic: PDF contains an embedded digital signature dictionary."""
    return (b"/ByteRange" in data) and (b"/Sig" in data or b"adbe.pkcs7" in data or b"ETSI.CAdES" in data)


@router.get("/student/{student_id}/verified-documents")
async def get_verified_docs(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, doc_type, source, source_url, file_name, file_mime, "
            "sha256, signature_present, verification_status, notes, created_at "
            "FROM verified_documents WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY created_at DESC", student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        out.append(d)
    return {"student_id": student_id, "documents": out}


@router.post("/student/{student_id}/verified-documents")
async def post_verified_docs(request: Request, student_id: str, body: VerifiedDocsRequest):
    import base64 as _b64, hashlib as _hash, urllib.request as _url
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = deleted = 0
    _DT = {"transcript","diploma","certificate","badge","other"}
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                await conn.execute("UPDATE verified_documents SET deleted_at=now() "
                                   "WHERE id=$1::uuid AND student_id=$2::uuid", did, student_id)
                deleted += 1
            for it in body.items or []:
                raw = None
                src = "upload"
                fname = it.file_name
                fmime = it.file_mime or "application/pdf"
                if it.file_data:
                    try: raw = _b64.b64decode(it.file_data)
                    except Exception: raise HTTPException(status_code=400, detail="bad base64")
                elif it.source_url and it.source_url.lower().startswith("https://"):
                    src = "parchment_link"
                    try:
                        req = _url.Request(it.source_url, headers={"User-Agent": "FOCMS/1.0"})
                        with _url.urlopen(req, timeout=30) as resp:
                            ct = resp.headers.get("Content-Type","")
                            raw = resp.read(6*1024*1024)
                        if "pdf" not in ct.lower() and not raw.startswith(b"%PDF"):
                            raise HTTPException(status_code=400,
                                detail="link did not return a PDF; download the PDF from Parchment and upload it instead")
                        fmime = "application/pdf"
                        fname = fname or "parchment_document.pdf"
                    except HTTPException: raise
                    except Exception as e:
                        raise HTTPException(status_code=400, detail=f"could not fetch link: {e}")
                else:
                    continue
                if raw is None or len(raw) == 0: continue
                if len(raw) > 5*1024*1024:
                    raise HTTPException(status_code=400, detail="file too large (max 5 MB)")
                sig = _pdf_signature_scan(raw) if raw.startswith(b"%PDF") else False
                status = "signature_present" if sig else "unverified"
                dt = it.doc_type if it.doc_type in _DT else "transcript"
                await conn.execute(
                    "INSERT INTO verified_documents (tenant_id, student_id, doc_type, source, "
                    "source_url, file_name, file_mime, file_data, sha256, signature_present, "
                    "verification_status, notes, created_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::uuid)",
                    tenant_id, student_id, dt, src, it.source_url, fname, fmime,
                    _b64.b64encode(raw).decode(), _hash.sha256(raw).hexdigest(), sig,
                    status, it.notes, user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved, "deleted": deleted}


@router.post("/student/{student_id}/verified-documents/{doc_id}/extract")
async def extract_verified_doc(request: Request, student_id: str, doc_id: str):
    """Extract courses/GPA from a stored transcript PDF via LLM. Returns a proposal; nothing is written."""
    import base64 as _b64
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT file_name, file_data FROM verified_documents "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL", doc_id, student_id)
    if not row or not row["file_data"]:
        raise HTTPException(status_code=404, detail="no file")
    raw = _b64.b64decode(row["file_data"])
    try:
        from pypdf import PdfReader
        import io as _io
        reader = PdfReader(_io.BytesIO(raw))
        text = "\n".join((pg.extract_text() or "") for pg in reader.pages)[:20000]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"pdf text extraction failed: {e}")
    if len(text.strip()) < 50:
        raise HTTPException(status_code=400, detail="no extractable text (scanned image PDF); OCR not yet supported")
    sys_p = ("You extract structured data from US school transcripts. "
             "Respond ONLY with JSON, no prose, no code fences. Schema: "
             '{"gpa_unweighted": number|null, "gpa_weighted": number|null, '
             '"courses": [{"course_name": str, "grade_level": int|null, "school_year": str|null, '
             '"term": str|null, "grade_received": str|null, "credit_hours": number|null, '
             '"is_honors": bool, "is_ap": bool, "is_ib": bool, "is_dual_credit": bool}]}. '
             "grade_level is 0-12 (K=0). Omit nothing; use null when unknown.")
    res = await _llm_complete(sys_p, "TRANSCRIPT TEXT:\n" + text, max_tokens=3000, want_json=True)
    data = res.get("json") or {}
    courses = data.get("courses") or []
    clean = []
    for c in courses[:80]:
        if not isinstance(c, dict) or not (c.get("course_name") or "").strip(): continue
        gl = c.get("grade_level")
        clean.append({
            "course_name": str(c["course_name"]).strip()[:200],
            "grade_level": gl if isinstance(gl, int) and 0 <= gl <= 12 else None,
            "school_year": (str(c["school_year"]).strip()[:20] if c.get("school_year") else None),
            "term": (str(c["term"]).strip()[:40] if c.get("term") else None),
            "grade_received": (str(c["grade_received"]).strip()[:20] if c.get("grade_received") else None),
            "credit_hours": c.get("credit_hours") if isinstance(c.get("credit_hours"), (int, float)) else None,
            "is_honors": bool(c.get("is_honors")), "is_ap": bool(c.get("is_ap")),
            "is_ib": bool(c.get("is_ib")), "is_dual_credit": bool(c.get("is_dual_credit")),
        })
    return {"document": row["file_name"], "gpa_unweighted": data.get("gpa_unweighted"),
            "gpa_weighted": data.get("gpa_weighted"), "courses": clean,
            "raw_available": bool(res.get("json"))}


@router.get("/student/{student_id}/verified-documents/{doc_id}/file")
async def get_verified_doc_file(request: Request, student_id: str, doc_id: str):
    from fastapi.responses import Response
    import base64 as _b64
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT file_name, file_mime, file_data FROM verified_documents "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL", doc_id, student_id)
    if not row or not row["file_data"]:
        raise HTTPException(status_code=404, detail="no file")
    data = _b64.b64decode(row["file_data"])
    return Response(content=data, media_type=row["file_mime"] or "application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{row["file_name"] or "document.pdf"}"'})


# --------------------- UCA form instances (v0.12.96 pillar-level publish gates + is_public master + Skills/Applications/Reports blacklisted; v0.12.95 real section content mapping (athlete_tracker/leadership/portfolio/essays); v0.12.94 latest/section endpoints; v0.12.93 site hero photo endpoints + hero_url in feed; v0.12.92 all 30 themes marked built (theme sprint shipped: token-driven ThemedSite + /{slug}/{lang} translated sites in showcase); v0.12.91 website-config returns site_slug + site URLs (secondary at /{slug}/{lang}); v0.12.90 zip geography + occupations catalogs, home/mobile/work phones; v0.12.89 family relationship enum fix (parent + parent_role) - father/mother saves were 500ing on the check constraint; v0.12.88 ISO 3166-2 subdivisions catalog + county of residence; v0.12.87 family education_level; v0.12.86 current_mailing kind fix + address row ids for validation; v0.12.85 student+family physical/mailing addresses, address fields server-locked from public; v0.12.84 middle name; v0.12.83 legal first/last name on personal-details; v0.12.82 anonymous /public/site/{slug} for showcase renderer; v0.12.81 signup-token auth fallback; v0.12.80 dual-language sites; v0.12.79 universal front-page PII + slug guardrails; v0.12.78 age-banded theme catalog (10 per band) + theme_key; v0.12.77 website pillar config; v0.12.76 adds /report-compose; v0.12.75 20-rule resume standard; v0.12.74 ATS-shape tailoring; v0.12.73 (adds /resume-tailor); v0.12.72) ---------------------

# --------------------- Website pillar config (v0.12.80) ---------------------

_SITE_PILLARS = [
    {"code": "personal", "label": "Personal"},
    {"code": "academics", "label": "Academics"},
    {"code": "extracurricular", "label": "Extra Curricular"},
    {"code": "career", "label": "Career"},
    {"code": "higher_education", "label": "Higher Education"},
]
_BLOCKED_PILLARS = {"skills", "applications_resumes_reports", "reports"}


def _default_pillars() -> dict:
    return {p["code"]: True for p in _SITE_PILLARS}


_WEBSITE_BANDS = {
    "band_1_5": {
        "label": "Ages 1-5 \u00b7 Family Memory Book", "control_mode": "parent_managed",
        "sections": [
            {"code": "growth_timeline",  "title": "Growth & Milestone Timeline",  "source": "milestones + student_skills", "pillar": "personal", "default": True},
            {"code": "art_gallery",      "title": "Digital Art & Project Gallery", "source": "media_files",                "pillar": "extracurricular", "default": True},
            {"code": "memory_scrapbook", "title": "Memory Scrapbook",              "source": "events + media_files",       "pillar": "personal", "default": True},
            {"code": "birthday_interviews", "title": "Birthday Interview Log",     "source": "events (annual interview)",  "pillar": "personal", "default": True},
            {"code": "family_excursions",  "title": "Family Excursions & Vacations", "source": "events + media_files",     "pillar": "personal", "default": False},
        ],
        "privacy_forced": {"password_protected": True, "hide_from_search": True, "pii_locked": True, "comments_disabled": True},
        "privacy_optional": [],
        "themes": [
            {"key": "sketchbook", "name": "Sketchbook", "vibe": "Watercolor, hand-drawn", "built": True},
            {"key": "storybook", "name": "Storybook", "vibe": "Picture-book pages, gentle serif", "built": True},
            {"key": "nursery", "name": "Nursery", "vibe": "Soft pastels, rounded shapes", "built": True},
            {"key": "scrapbook", "name": "Scrapbook", "vibe": "Taped photos, paper textures", "built": True},
            {"key": "growth-chart", "name": "Growth Chart", "vibe": "Ruler motifs, milestone markers", "built": True},
            {"key": "toy-box", "name": "Toy Box", "vibe": "Bright primary blocks", "built": True},
            {"key": "picture-frame", "name": "Picture Frame", "vibe": "Gallery-wall photo grid", "built": True},
            {"key": "lullaby", "name": "Lullaby", "vibe": "Night-sky calm, stars", "built": True},
            {"key": "garden", "name": "Garden", "vibe": "Botanical growth motifs", "built": True},
            {"key": "crayon", "name": "Crayon", "vibe": "Kid-drawn strokes, bold color", "built": True},
        ],
    },
    "band_6_12": {
        "label": "Ages 6-12 \u00b7 Developmental Portfolio", "control_mode": "shared",
        "sections": [
            {"code": "athlete_tracker",   "title": "Student-Athlete Tracker (PRs, times)", "source": "personal_records + events + power index", "pillar": "extracurricular", "default": True},
            {"code": "leadership_milestones", "title": "Leadership & Group Milestones",    "source": "affiliations + milestones", "pillar": "extracurricular", "default": True},
            {"code": "fine_arts",         "title": "Fine Arts Showcase",                   "source": "affiliations (music) + media", "pillar": "extracurricular", "default": True},
            {"code": "stem_portfolio",    "title": "Academic & STEM Portfolio",            "source": "events + courses_taken", "pillar": "academics", "default": True},
            {"code": "writing_book_log",  "title": "Writing & Book Log",                   "source": "events (reading log)", "pillar": "academics", "default": False},
        ],
        "privacy_forced": {"hide_from_search": True, "pii_locked": True, "comment_moderation": True},
        "privacy_optional": ["two_gate_private_portal", "password_protected"],
        "themes": [
            {"key": "mission-control", "name": "Mission Control", "vibe": "Dark space telemetry, HUD readouts", "built": True},
            {"key": "trading-card", "name": "Trading Card", "vibe": "Foil-textured stat cards, rookie badges", "built": True},
            {"key": "arcade", "name": "Arcade", "vibe": "Neon pixel-art leaderboard", "built": True},
            {"key": "comic-book", "name": "Comic Book", "vibe": "Halftone dots, action callouts", "built": True},
            {"key": "stadium", "name": "Stadium", "vibe": "Sports broadcast graphics", "built": True},
            {"key": "field-notes", "name": "Field Notes", "vibe": "Graph paper, hand-drawn annotations", "built": True},
            {"key": "treasure-map", "name": "Treasure Map", "vibe": "Adventure chart, waypoints", "built": True},
            {"key": "science-lab", "name": "Science Lab", "vibe": "Lab-notebook experiments", "built": True},
            {"key": "game-day", "name": "Game Day", "vibe": "Scoreboard energy", "built": True},
            {"key": "clubhouse", "name": "Clubhouse", "vibe": "Team locker-room boards", "built": True},
        ],
    },
    "band_13_18": {
        "label": "Ages 13-18 \u00b7 Professional Launchpad", "control_mode": "student_led",
        "sections": [
            {"code": "academic_capstone", "title": "Academic Capstone & Research",       "source": "events + essays + verified_documents", "pillar": "academics", "default": True},
            {"code": "essay_vault",       "title": "Essay & Writing Vault",              "source": "essays", "pillar": "higher_education", "default": False},
            {"code": "recruitment_portal", "title": "Athletics Recruitment Portal",      "source": "swim bests + power index + coach contacts + grad year", "pillar": "extracurricular", "default": True},
            {"code": "highlight_reel",    "title": "Highlight Reel (video)",             "source": "media_files (video)", "pillar": "extracurricular", "default": False},
            {"code": "leadership_impact", "title": "Leadership & Extracurricular Impact", "source": "affiliations + service logs", "pillar": "extracurricular", "default": True},
            {"code": "branding",          "title": "Professional Branding (domain, LinkedIn)", "source": "digital_presence", "pillar": "career", "default": True},
        ],
        "privacy_forced": {},
        "privacy_optional": ["two_gate_private_portal", "hide_from_search", "password_protected"],
        "themes": [
            {"key": "resume-mode", "name": "Resume Mode", "vibe": "Admissions-reader editorial", "built": True},
            {"key": "studio", "name": "Studio", "vibe": "Concert-poster typography", "built": True},
            {"key": "broadsheet", "name": "Broadsheet", "vibe": "Newspaper editorial layout", "built": True},
            {"key": "portfolio", "name": "Portfolio", "vibe": "Gallery-grade project grid", "built": True},
            {"key": "blueprint", "name": "Blueprint", "vibe": "Engineering drawings, cyan lines", "built": True},
            {"key": "varsity", "name": "Varsity", "vibe": "Recruitment profile, letterman accents", "built": True},
            {"key": "command-brief", "name": "Command Brief", "vibe": "Service-academy briefing style", "built": True},
            {"key": "ledger", "name": "Ledger", "vibe": "Minimal monochrome precision", "built": True},
            {"key": "spotlight", "name": "Spotlight", "vibe": "Stage-lit performance focus", "built": True},
            {"key": "summit", "name": "Summit", "vibe": "Expedition progress, elevation lines", "built": True},
        ],
    },
}


def _website_band_for_age(age: Optional[float]) -> str:
    if age is None:
        return "band_6_12"
    if age <= 5:
        return "band_1_5"
    if age <= 12:
        return "band_6_12"
    return "band_13_18"


class WebsiteConfigRequest(BaseModel):
    age_band: Optional[str] = None
    control_mode: Optional[str] = None
    sections: dict = {}
    privacy: dict = {}
    domain: Optional[str] = None
    notes: Optional[str] = None
    theme_key: Optional[str] = None
    language_primary: Optional[str] = None    # BCP-47, e.g. en, es, zh
    language_secondary: Optional[str] = None  # enables a second site in the native language
    is_public: Optional[bool] = None          # master switch (v0.12.96)
    pillars: Optional[dict] = None            # {pillar_code: bool} (v0.12.96)


@router.get("/student/{student_id}/website-config")
async def get_website_config(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    age = None
    async with _tenant_conn(pool, tenant_id) as conn:
        try:
            age = await _pp_student_age(conn, student_id)
        except Exception:
            age = None
        row = await conn.fetchrow(
            "SELECT age_band, control_mode, sections, privacy, domain, notes, theme_key, "
            "language_primary, language_secondary, is_public, pillars, updated_at "
            "FROM website_configs WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
        slug = await conn.fetchval("SELECT slug FROM tenants WHERE id=$1::uuid", tenant_id)
    band = (row and row["age_band"]) or _website_band_for_age(age)
    if band not in _WEBSITE_BANDS:
        band = _website_band_for_age(age)
    cfg = None
    if row:
        cfg = {"age_band": band, "control_mode": row["control_mode"],
               "sections": json.loads(row["sections"]) if isinstance(row["sections"], str) else (row["sections"] or {}),
               "privacy": json.loads(row["privacy"]) if isinstance(row["privacy"], str) else (row["privacy"] or {}),
               "domain": row["domain"], "notes": row["notes"], "theme_key": row["theme_key"],
               "language_primary": row["language_primary"], "language_secondary": row["language_secondary"],
               "is_public": row["is_public"] if row["is_public"] is not None else True,
               "pillars": json.loads(row["pillars"]) if isinstance(row["pillars"], str) else (row["pillars"] or _default_pillars())}
    lang2 = cfg and cfg.get("language_secondary")
    return {"student_id": student_id, "student_age": age, "computed_band": _website_band_for_age(age),
            "band_catalog": _WEBSITE_BANDS, "config": cfg, "pillars_catalog": _SITE_PILLARS, "blocked_pillars": sorted(_BLOCKED_PILLARS),
            "site_slug": slug,
            "site_url": f"https://app.outcomestar.app/{slug}" if slug else None,
            "site_url_secondary": (f"https://app.outcomestar.app/{slug}/{lang2}" if (slug and lang2) else None)}


@router.post("/student/{student_id}/website-config")
async def post_website_config(request: Request, student_id: str, body: WebsiteConfigRequest):
    tenant_id, _ = await _pp_context(request, student_id)
    band = body.age_band if body.age_band in _WEBSITE_BANDS else None
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        if band is None:
            try:
                band = _website_band_for_age(await _pp_student_age(conn, student_id))
            except Exception:
                band = "band_6_12"
        forced = dict(_WEBSITE_BANDS[band]["privacy_forced"])
        # Platform-wide rules (decision 2026-07-05): law > parent > child authority;
        # public front page shows first name ONLY; nondescript slug for minors.
        forced.update({"first_name_only_public": True, "no_address_or_phone_public": True,
                       "nondescript_slug_required": True})
        privacy = dict(body.privacy or {})
        privacy.update(forced)  # server-side lock: forced guardrails always win
        mode = body.control_mode or _WEBSITE_BANDS[band]["control_mode"]
        sj, pj = json.dumps(body.sections or {}), json.dumps(privacy)
        row = await conn.fetchrow(
            "SELECT id FROM website_configs WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
        valid_themes = {t["key"] for t in _WEBSITE_BANDS[band].get("themes", [])}
        theme = body.theme_key if body.theme_key in valid_themes else None
        lp = (body.language_primary or "en").strip().lower()[:12]
        ls = (body.language_secondary or "").strip().lower()[:12] or None
        if ls == lp:
            ls = None
        is_public_val = True if body.is_public is None else bool(body.is_public)
        pillars_in = body.pillars or {}
        pillars_val = {p["code"]: bool(pillars_in.get(p["code"], True)) for p in _SITE_PILLARS}
        pj_pillars = json.dumps(pillars_val)
        if row:
            await conn.execute(
                "UPDATE website_configs SET age_band=$2, control_mode=$3, sections=$4::jsonb, "
                "privacy=$5::jsonb, domain=$6, notes=$7, theme_key=COALESCE($8, theme_key), "
                "language_primary=$9, language_secondary=$10, is_public=$11, pillars=$12::jsonb, "
                "updated_at=now() WHERE id=$1",
                row["id"], band, mode, sj, pj, body.domain, body.notes, theme, lp, ls,
                is_public_val, pj_pillars)
        else:
            await conn.execute(
                "INSERT INTO website_configs (tenant_id, student_id, age_band, control_mode, sections, privacy, "
                "domain, notes, theme_key, language_primary, language_secondary, is_public, pillars) "
                "VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10, $11, $12, $13::jsonb)",
                tenant_id, student_id, band, mode, sj, pj, body.domain, body.notes, theme, lp, ls,
                is_public_val, pj_pillars)
    return {"ok": True, "age_band": band, "control_mode": mode, "privacy": privacy, "theme_key": theme,
            "language_primary": lp, "language_secondary": ls,
            "is_public": is_public_val, "pillars": pillars_val}


# --------------------- Public site config (v0.12.82) ---------------------
# Anonymous endpoint consumed by the outcomestar showcase renderer. Returns
# only publish-safe fields: first name (universal first-name-only rule),
# graduation year, age band, theme, enabled sections, languages. 404 when the
# family has not configured a website (config row is the publish switch).

@router.get("/public/site/{slug}/latest")
async def public_site_latest(request: Request, slug: str):
    """v0.12.94: latest public activity across visibility=public records."""
    pool: asyncpg.Pool = request.app.state.pool
    slug = slug.strip().lower()
    tables = ['events','awards_honors','personal_records','assessments','essays','work_experiences','portfolio_artifacts']
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow("SELECT t.id FROM tenants t WHERE t.slug=$1 AND t.status='active' AND NOT coalesce((SELECT (ts.feature_flags->>'billing_hold')::bool FROM tenant_settings ts WHERE ts.tenant_id=t.id), false)", slug)
        if not tenant:
            raise HTTPException(404, {"error": "not_found"})
        async with _tenant_conn(pool, str(tenant["id"])) as tconn:
            student = await tconn.fetchrow(
                "SELECT id FROM students WHERE tenant_id=$1::uuid ORDER BY created_at LIMIT 1",
                str(tenant["id"]))
            if not student:
                return {"latest": None}
            parts = " UNION ALL ".join(
                f"SELECT MAX(updated_at) AS ts, '{t}' AS kind FROM {t} "
                f"WHERE student_id=$1::uuid AND visibility='public'" for t in tables)
            row = await tconn.fetchrow(
                f"SELECT ts, kind FROM ({parts}) x WHERE ts IS NOT NULL ORDER BY ts DESC LIMIT 1",
                student["id"])
    if not row:
        return {"latest": None}
    return {"latest": {"date": row["ts"].isoformat(), "kind": row["kind"]}}


SECTION_TITLES = {
    "athlete_tracker": "Student-Athlete Tracker",
    "leadership_milestones": "Leadership & Group Milestones",
    "fine_arts": "Fine Arts Showcase",
    "stem_portfolio": "Academic & STEM Portfolio",
    "writing_book_log": "Writing & Book Log",
    "resume_cv": "Resume / CV",
    "academic_capstone": "Academic Capstone & Research",
    "essay_vault": "Essay & Writing Vault",
    "athletics_recruitment": "Athletics Recruitment Profile",
    "highlight_reel": "Highlight Reel",
    "leadership_extracurricular": "Leadership & Extracurricular Impact",
    "professional_branding": "Professional Branding",
}


async def _section_items(tconn, student_id: str, code: str) -> list[dict]:
    """v0.12.95: return public rows for a section code across relevant tables."""
    items: list[dict] = []
    if code == "athlete_tracker":
        prs = await tconn.fetch(
            "SELECT title, achieved_date, value_text, value_numeric, value_unit, details, "
            "record_kind::text AS record_kind FROM personal_records "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "AND record_kind = 'swim_best' "
            "ORDER BY achieved_date DESC NULLS LAST", student_id)
        for r in prs:
            d = r["details"] if isinstance(r["details"], dict) else (json.loads(r["details"]) if r["details"] else {})
            items.append({
                "title": r["title"],
                "date": r["achieved_date"].isoformat() if r["achieved_date"] else None,
                "body": r["value_text"],
                "meta": {
                    "stroke": d.get("stroke"),
                    "course": d.get("course"),
                    "event": d.get("event"),
                    "best_time": r["value_text"],
                    "first_time": d.get("first_time"),
                    "power_points": d.get("power_points"),
                    "usa_standard": d.get("usa_standard"),
                },
            })
    elif code in ("leadership_milestones", "leadership_extracurricular"):
        rows = await tconn.fetch(
            "SELECT title, event_date, public_description FROM events "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY event_date DESC NULLS LAST LIMIT 50", student_id)
        for r in rows:
            items.append({"title": r["title"] or "Event",
                          "date": r["event_date"].isoformat() if r["event_date"] else None,
                          "body": r["public_description"]})
    elif code in ("fine_arts", "highlight_reel"):
        # Music-performance events + portfolio artifacts (v0.12.115 real-source fix).
        ev = await tconn.fetch(
            "SELECT title, event_date, public_description FROM events "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "AND event_type = 'music_performance' "
            "ORDER BY event_date DESC NULLS LAST LIMIT 50", student_id)
        for r in ev:
            items.append({"title": r["title"] or "Performance",
                          "date": r["event_date"].isoformat() if r["event_date"] else None,
                          "body": r["public_description"]})
        pa = await tconn.fetch(
            "SELECT artifact_title, created_at, public_description FROM portfolio_artifacts "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY created_at DESC LIMIT 50", student_id)
        for r in pa:
            items.append({"title": r["artifact_title"] or "Portfolio item",
                          "date": r["created_at"].date().isoformat() if r["created_at"] else None,
                          "body": r["public_description"]})
    elif code in ("stem_portfolio", "academic_capstone"):
        # STEM/experience/competition events + public courses + portfolio (v0.12.115 real-source fix).
        ev = await tconn.fetch(
            "SELECT title, event_date, public_description FROM events "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "AND event_type IN ('stem_event','competition','summer_experience') "
            "ORDER BY event_date DESC NULLS LAST LIMIT 50", student_id)
        for r in ev:
            items.append({"title": r["title"] or "Experience",
                          "date": r["event_date"].isoformat() if r["event_date"] else None,
                          "body": r["public_description"]})
        crs = await tconn.fetch(
            "SELECT course_name, term, school_year FROM courses_taken "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY created_at DESC LIMIT 50", student_id)
        for r in crs:
            items.append({"title": r["course_name"] or "Course",
                          "date": None,
                          "body": r["term"] or r["school_year"]})
        pa = await tconn.fetch(
            "SELECT artifact_title, created_at, public_description FROM portfolio_artifacts "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY created_at DESC LIMIT 50", student_id)
        for r in pa:
            items.append({"title": r["artifact_title"] or "Portfolio item",
                          "date": r["created_at"].date().isoformat() if r["created_at"] else None,
                          "body": r["public_description"]})
    elif code in ("writing_book_log", "essay_vault"):
        rows = await tconn.fetch(
            "SELECT title, updated_at, public_description FROM essays "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY updated_at DESC LIMIT 50", student_id)
        for r in rows:
            items.append({"title": r["title"] or "Essay",
                          "date": r["updated_at"].date().isoformat() if r["updated_at"] else None,
                          "body": r["public_description"]})
        ev = await tconn.fetch(
            "SELECT title, event_date, public_description FROM events "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "AND event_type = 'essay_draft' "
            "ORDER BY event_date DESC NULLS LAST LIMIT 50", student_id)
        for r in ev:
            items.append({"title": r["title"] or "Writing",
                          "date": r["event_date"].isoformat() if r["event_date"] else None,
                          "body": r["public_description"]})
    elif code == "resume_cv":
        rows = await tconn.fetch(
            "SELECT title, event_date, public_description FROM events "
            "WHERE student_id=$1::uuid AND visibility='public' AND deleted_at IS NULL "
            "ORDER BY event_date DESC NULLS LAST LIMIT 20", student_id)
        for r in rows:
            items.append({"title": r["title"] or "Event",
                          "date": r["event_date"].isoformat() if r["event_date"] else None,
                          "body": r["public_description"]})
    return items


@router.get("/public/site/{slug}/section/{code}")
async def public_site_section(request: Request, slug: str, code: str):
    """v0.12.95: section detail - returns section title + items (public rows only)."""
    pool: asyncpg.Pool = request.app.state.pool
    slug = slug.strip().lower()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow("SELECT t.id FROM tenants t WHERE t.slug=$1 AND t.status='active' AND NOT coalesce((SELECT (ts.feature_flags->>'billing_hold')::bool FROM tenant_settings ts WHERE ts.tenant_id=t.id), false)", slug)
        if not tenant:
            raise HTTPException(404, {"error": "not_found"})
        async with _tenant_conn(pool, str(tenant["id"])) as tconn:
            student = await tconn.fetchrow(
                "SELECT id, first_name FROM students WHERE tenant_id=$1::uuid ORDER BY created_at LIMIT 1",
                str(tenant["id"]))
            if not student:
                raise HTTPException(404, {"error": "not_found"})
            cfg = await tconn.fetchrow(
                "SELECT sections FROM website_configs WHERE tenant_id=$1::uuid AND student_id=$2",
                str(tenant["id"]), student["id"])
            if not cfg:
                raise HTTPException(404, {"error": "no_website_config"})
            sections_dict = cfg["sections"] or {}
            if isinstance(sections_dict, dict):
                if not sections_dict.get(code, False):
                    raise HTTPException(404, {"error": "section_not_enabled"})
            wc = await tconn.fetchrow(
                "SELECT age_band, is_public, pillars FROM website_configs "
                "WHERE tenant_id=$1::uuid AND student_id=$2",
                str(tenant["id"]), student["id"])
            if wc and wc["is_public"] is False:
                raise HTTPException(404, {"error": "not_public"})
            band_cat = _WEBSITE_BANDS.get(
                wc["age_band"] if wc else "band_6_12", _WEBSITE_BANDS["band_6_12"])
            sec_meta = next((x for x in band_cat["sections"] if x["code"] == code), None)
            pillar_code = (sec_meta or {}).get("pillar", "personal")
            if pillar_code in _BLOCKED_PILLARS:
                raise HTTPException(404, {"error": "pillar_blocked"})
            p_cfg = wc["pillars"] if wc and wc["pillars"] else _default_pillars()
            if isinstance(p_cfg, str):
                import json as _json
                p_cfg = _json.loads(p_cfg)
            if not p_cfg.get(pillar_code, True):
                raise HTTPException(404, {"error": "pillar_disabled"})
            title = SECTION_TITLES.get(code, code.replace("_", " ").title())
            items = await _section_items(tconn, student["id"], code)
    return {"slug": slug, "student_first_name": student["first_name"],
            "code": code, "title": title, "items": items}


class SiteHeroRequest(BaseModel):
    content_type: str
    data_base64: str


@router.post("/student/{student_id}/site-hero")
async def post_site_hero(request: Request, student_id: str, body: SiteHeroRequest):
    """v0.12.93: student photo / graphic for the public site hero (JPG/PNG/WebP, 2 MB max)."""
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    if body.content_type not in ("image/jpeg", "image/png", "image/webp"):
        raise HTTPException(400, {"error": "bad_type", "message": "Use a JPG, PNG, or WebP image."})
    import base64 as _b64
    try:
        blob = _b64.b64decode(body.data_base64, validate=True)
    except Exception:
        raise HTTPException(400, {"error": "bad_data"})
    if len(blob) > 2_000_000:
        raise HTTPException(400, {"error": "too_large", "message": "Max photo size is 2 MB."})
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                """INSERT INTO site_assets (tenant_id, student_id, kind, content_type, content)
                   VALUES ($1::uuid, $2::uuid, 'hero', $3, $4)
                   ON CONFLICT (student_id, kind)
                   DO UPDATE SET content_type=EXCLUDED.content_type, content=EXCLUDED.content, updated_at=now()""",
                tenant_id, student_id, body.content_type, blob)
    return {"saved": True, "bytes": len(blob)}


@router.get("/public/site/{slug}/hero")
async def public_site_hero(request: Request, slug: str):
    from fastapi.responses import Response
    pool: asyncpg.Pool = request.app.state.pool
    slug = slug.strip().lower()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow("SELECT t.id FROM tenants t WHERE t.slug=$1 AND t.status='active' AND NOT coalesce((SELECT (ts.feature_flags->>'billing_hold')::bool FROM tenant_settings ts WHERE ts.tenant_id=t.id), false)", slug)
        if not tenant:
            raise HTTPException(404, {"error": "not_found"})
        async with _tenant_conn(pool, str(tenant["id"])) as tconn:
            row = await tconn.fetchrow(
                "SELECT sa.content_type, sa.content FROM site_assets sa "
                "JOIN students st ON st.id = sa.student_id "
                "WHERE st.tenant_id=$1::uuid AND sa.kind='hero' "
                "ORDER BY st.created_at LIMIT 1", str(tenant["id"]))
    if not row:
        raise HTTPException(404, {"error": "not_found"})
    return Response(content=bytes(row["content"]), media_type=row["content_type"],
                    headers={"Cache-Control": "public, max-age=300"})


@router.get("/public/site/{slug}/badges")
async def public_site_badges(request: Request, slug: str):
    """v0.12.115 (roadmap A4): virtual trophy case. Derives achievement badges
    from records the family has ALREADY made public - no new data entry. Only
    public personal_records / affiliations / public events feed this, so the
    three-layer visibility gate is honored automatically."""
    pool: asyncpg.Pool = request.app.state.pool
    slug = slug.strip().lower()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow(
            "SELECT t.id FROM tenants t WHERE t.slug=$1 AND t.status='active' "
            "AND NOT coalesce((SELECT (ts.feature_flags->>'billing_hold')::bool "
            "FROM tenant_settings ts WHERE ts.tenant_id=t.id), false)", slug)
        if not tenant:
            raise HTTPException(404, {"error": "not_found"})
        tid = str(tenant["id"])
        async with _tenant_conn(pool, tid) as tconn:
            student = await tconn.fetchrow(
                "SELECT id FROM students WHERE tenant_id=$1::uuid ORDER BY created_at LIMIT 1", tid)
            if not student:
                raise HTTPException(404, {"error": "not_found"})
            sid = student["id"]
            pr_count = await tconn.fetchval(
                "SELECT count(*) FROM personal_records "
                "WHERE student_id=$1 AND visibility='public'", sid) or 0
            best_drop = await tconn.fetchval(
                "SELECT max(total_drop_numeric) FROM personal_records "
                "WHERE student_id=$1 AND visibility='public' AND total_drop_numeric IS NOT NULL", sid)
            race_count = await tconn.fetchval(
                "SELECT count(*) FROM events "
                "WHERE student_id=$1 AND event_type='swim_race' AND visibility='public'", sid) or 0
            affil_count = await tconn.fetchval(
                "SELECT count(*) FROM affiliations "
                "WHERE student_id=$1 AND visibility='public'", sid) or 0
            years_active = await tconn.fetchval(
                "SELECT count(DISTINCT extract(year from achieved_date)) FROM personal_records "
                "WHERE student_id=$1 AND visibility='public' AND achieved_date IS NOT NULL", sid) or 0
            top_pr = await tconn.fetchrow(
                "SELECT title, public_description FROM personal_records "
                "WHERE student_id=$1 AND visibility='public' "
                "ORDER BY achieved_date DESC NULLS LAST LIMIT 1", sid)
    badges: list[dict] = []
    def add(icon, label, sub):
        badges.append({"icon": icon, "label": label, "sub": sub})
    if pr_count >= 1:
        add("medal", f"{pr_count} Personal Record{'s' if pr_count != 1 else ''}", "Logged and verified")
    for threshold, star in ((25, "gold"), (10, "silver"), (5, "bronze")):
        if pr_count >= threshold:
            add(star, f"{threshold}+ Records Club", "Consistency badge")
            break
    if race_count >= 1:
        add("lane", f"{race_count} Race{'s' if race_count != 1 else ''} Swum", "Every start counts")
    if best_drop and best_drop > 0:
        add("bolt", "Time Dropper", f"Best drop {round(float(best_drop), 2)}s")
    if affil_count >= 1:
        add("team", f"{affil_count} Team{'s' if affil_count != 1 else ''} & Group{'s' if affil_count != 1 else ''}", "Shows up and belongs")
    if years_active >= 2:
        add("calendar", f"{years_active}-Year Streak", "Long-haul dedication")
    if top_pr:
        add("trophy", "Latest Milestone", top_pr["public_description"] or top_pr["title"])
    return {"slug": slug, "badges": badges}


@router.get("/public/site/{slug}")
async def public_site_config(request: Request, slug: str):
    pool: asyncpg.Pool = request.app.state.pool
    slug = slug.strip().lower()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow(
            "SELECT t.id FROM tenants t WHERE t.slug = $1 AND t.status = 'active' "
            "AND NOT coalesce((SELECT (ts.feature_flags->>'billing_hold')::bool "
            "FROM tenant_settings ts WHERE ts.tenant_id=t.id), false)", slug)
        if not tenant:
            raise HTTPException(404, {"error": "site_not_found"})
        tenant_id = str(tenant["id"])
        async with _tenant_conn(pool, tenant_id) as tconn:
            student = await tconn.fetchrow(
                "SELECT id, first_name, expected_hs_graduation_year, "
                "extract(year from age(birth_date))::int AS age "
                "FROM students WHERE tenant_id = $1::uuid ORDER BY created_at LIMIT 1",
                tenant_id)
            if not student:
                raise HTTPException(404, {"error": "site_not_found"})
            cfg = await tconn.fetchrow(
                "SELECT age_band, control_mode, sections, privacy, theme_key, "
                "language_primary, language_secondary, is_public, pillars "
                "FROM website_configs WHERE tenant_id = $1::uuid AND student_id = $2",
                tenant_id, student["id"])
            has_hero = bool(await tconn.fetchval(
                "SELECT 1 FROM site_assets WHERE student_id=$1 AND kind='hero'", student["id"]))
    if not cfg:
        raise HTTPException(404, {"error": "site_not_published",
                                  "message": "This family has not published a website yet."})
    band = cfg["age_band"] or _website_band_for_age(student["age"])
    cat = _WEBSITE_BANDS.get(band, _WEBSITE_BANDS["band_6_12"])
    saved_sections = cfg["sections"] if isinstance(cfg["sections"], dict) else json.loads(cfg["sections"] or "{}")
    if cfg and cfg["is_public"] is False:
        raise HTTPException(404, {"error": "not_public"})
    pillar_cfg = cfg["pillars"] if cfg and cfg["pillars"] else _default_pillars()
    if isinstance(pillar_cfg, str):
        import json as _json
        pillar_cfg = _json.loads(pillar_cfg)
    enabled = [
        {"code": sdef["code"], "title": sdef["title"], "pillar": sdef.get("pillar", "personal")}
        for sdef in cat["sections"]
        if saved_sections.get(sdef["code"], sdef["default"])
        and pillar_cfg.get(sdef.get("pillar", "personal"), True)
        and sdef.get("pillar", "personal") not in _BLOCKED_PILLARS
    ]
    # v0.12.115: attach live item count + first preview so cards show real
    # depth instead of the generic grow-note when a section has public rows.
    async with _tenant_conn(pool, tenant_id) as tconn2:
        for sec in enabled:
            try:
                rows = await _section_items(tconn2, str(student["id"]), sec["code"])
            except Exception:
                rows = []
            sec["count"] = len(rows)
            sec["preview"] = rows[0]["title"] if rows else None
    theme = next((t for t in cat["themes"] if t["key"] == cfg["theme_key"]), None)         or (cat["themes"][0] if cat["themes"] else None)
    return {
        "slug": slug,
        "hero_url": (f"https://focms-api.onrender.com/focms/v1/public/site/{slug}/hero" if has_hero else None),
        "is_public": bool(cfg["is_public"]) if cfg else True,
        "pillars_enabled": pillar_cfg,
        "student_first_name": student["first_name"],
        "graduation_year": student["expected_hs_graduation_year"],
        "age_band": band,
        "band_label": cat["label"],
        "control_mode": cfg["control_mode"] or cat["control_mode"],
        "theme": theme,
        "sections": enabled,
        "language_primary": cfg["language_primary"] or "en",
        "language_secondary": cfg["language_secondary"],
    }


# --------------------- Custom report composer (v0.12.80) ---------------------

class ReportComposeRequest(BaseModel):
    title: Optional[str] = None
    instructions: str
    sections: List[dict] = []


@router.post("/student/{student_id}/report-compose")
async def report_compose(request: Request, student_id: str, body: ReportComposeRequest):
    await _pp_context(request, student_id)
    ins = (body.instructions or "").strip()[:4000]
    if not ins:
        raise HTTPException(status_code=400, detail="instructions required")
    src = json.dumps({"sections": body.sections})[:16000]
    system = (
        "You compose family-facing student reports from a complete student record given as JSON sections. "
        "Follow the report request: select ONLY the relevant data, organize it into clear sections, and add "
        "a short factual OVERVIEW section (2-4 sentences) summarizing what the report shows. "
        "NEVER invent facts, numbers, dates, or achievements not present in the source; never speculate, "
        "predict outcomes, or evaluate the student negatively \u2014 this is an informational report, not a verdict. "
        "Return ONLY JSON: {\"sections\": [{\"title\": str, \"rows\": [[label, detail], ...]}]}. "
        "First section must be titled OVERVIEW with rows [[\"Summary\", text]]. "
        "Keep section titles short and reader-friendly. Detail values may contain newlines for lists."
    )
    user = "REPORT REQUEST:\n" + ((body.title or "") + "\n" + ins).strip() + "\n\nSTUDENT RECORD (JSON):\n" + src
    res = await _llm_complete(system, user, max_tokens=2600, want_json=True)
    if res.get("unavailable"):
        raise HTTPException(status_code=503, detail=res.get("reason", "LLM unavailable"))
    obj = _extract_json(res.get("text", "")) or {}
    secs = obj.get("sections")
    if not isinstance(secs, list) or not secs:
        raise HTTPException(status_code=502, detail="composition failed")
    clean = []
    for sec in secs[:15]:
        if not isinstance(sec, dict):
            continue
        rows = [[str(r[0])[:200], str(r[1])[:1200]] for r in (sec.get("rows") or [])
                if isinstance(r, (list, tuple)) and len(r) >= 2][:40]
        if rows:
            clean.append({"title": str(sec.get("title", ""))[:80], "rows": rows})
    if not clean:
        raise HTTPException(status_code=502, detail="composition failed")
    return {"sections": clean}


# --------------------- Resume tailoring (v0.12.80) ---------------------

class ResumeTailorRequest(BaseModel):
    resume_kind: Optional[str] = None          # resume_academic | resume_career
    job_description: str
    sections: List[dict] = []                  # [{title, rows:[[label, detail],...]}]


@router.post("/student/{student_id}/resume-tailor")
async def resume_tailor(request: Request, student_id: str, body: ResumeTailorRequest):
    await _pp_context(request, student_id)
    kind = (body.resume_kind or "resume_career").strip()
    jd = (body.job_description or "").strip()[:8000]
    if not jd:
        raise HTTPException(status_code=400, detail="job_description required")
    src = json.dumps({"sections": body.sections})[:12000]
    audience = "an academic program or scholarship committee" if kind == "resume_academic" else "a hiring manager"
    system = (
        "You tailor student resumes into ATS-optimized professional format. You are given the student's "
        "real record as JSON sections and a target description. NEVER invent facts, employers, dates, "
        "scores, or accomplishments not present in the source. "
        "Return ONLY JSON: {\"summary\": str, \"sections\": [{\"title\": str, \"rows\": [[label, detail], ...]}]}. "
        "Apply these resume rules strictly: "
        "(1) summary = tailored 3-4 sentence professional pitch using keywords from the target description; "
        "(2) weave exact keywords and phrases from the target description into summary, skills, and bullets "
        "wherever the source record truthfully supports them (ATS keyword matching); "
        "(3) CORE COMPETENCIES section: 6-12 rows [\"\", skill], curated hard skills first then soft skills, "
        "all evidenced by the record and relevant to the target; "
        "(4) experience section (PROFESSIONAL EXPERIENCE, or ACTIVITIES & LEADERSHIP for younger students): "
        "reverse-chronological rows [\"Role \u2014 Organization\", \"Location | Mon YYYY \u2013 Mon YYYY\\n- bullet\\n- bullet\"]; "
        "(5) every bullet starts with a strong action verb (Led, Developed, Achieved, Competed, Mentored) \u2014 never "
        "\"Responsible for\"; use X-Y-Z structure (accomplished X, measured by Y, by doing Z) and include real numbers "
        "from the source (race counts, times dropped, standards earned, hours/week, years) as quantified impact; "
        "(6) bullets are 1-2 lines max, punchy; "
        "(7) past tense for ended roles, present tense for current roles; "
        "(8) relevance over recency: include only entries that matter to this target, omit the rest; "
        "(9) EDUCATION after experience: institution, location, expected graduation if known; "
        "(10) include a CERTIFICATIONS or HONORS section only if the source contains them. "
        "Keep total content sized for a 1-page resume. " + ("Frame for " + audience + ".")
    )
    user = "TARGET DESCRIPTION:\n" + jd + "\n\nSTUDENT RECORD (JSON):\n" + src
    res = await _llm_complete(system, user, max_tokens=2200, want_json=True)
    if res.get("unavailable"):
        raise HTTPException(status_code=503, detail=res.get("reason", "LLM unavailable"))
    obj = _extract_json(res.get("text", "")) or {}
    secs = obj.get("sections")
    if not isinstance(secs, list) or not secs:
        raise HTTPException(status_code=502, detail="tailoring failed; use the standard resume")
    clean = []
    for sec in secs[:12]:
        if not isinstance(sec, dict):
            continue
        rows = [[str(r[0])[:200], str(r[1])[:600]] for r in (sec.get("rows") or [])
                if isinstance(r, (list, tuple)) and len(r) >= 2][:25]
        if rows:
            clean.append({"title": str(sec.get("title", ""))[:80], "rows": rows})
    if not clean:
        raise HTTPException(status_code=502, detail="tailoring failed; use the standard resume")
    summary = str(obj.get("summary", ""))[:800]
    if summary:
        clean.insert(0, {"title": "PROFESSIONAL SUMMARY", "rows": [["Profile", summary]]})
    return {"sections": clean}


class UcaFormItem(BaseModel):
    id: Optional[str] = None
    form_code: Optional[str] = None
    application_id: Optional[str] = None
    title: Optional[str] = None
    data: Optional[dict] = None


class UcaFormsRequest(BaseModel):
    items: List[UcaFormItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/uca-forms")
async def get_uca_forms(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, form_code, application_id::text AS application_id, title, data, "
            "created_at, updated_at FROM uca_form_instances "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL ORDER BY updated_at DESC", student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["data"] = json.loads(d["data"]) if isinstance(d["data"], str) else (d["data"] or {})
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        d["updated_at"] = d["updated_at"].isoformat() if d["updated_at"] else None
        out.append(d)
    return {"student_id": student_id, "forms": out}


@router.post("/student/{student_id}/uca-forms")
async def post_uca_forms(request: Request, student_id: str, body: UcaFormsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                await conn.execute("UPDATE uca_form_instances SET deleted_at=now() "
                                   "WHERE id=$1::uuid AND student_id=$2::uuid", did, student_id)
                deleted += 1
            for it in body.items or []:
                if not it.form_code: continue
                dj = json.dumps(it.data or {})
                app_id = it.application_id if it.application_id else None
                if app_id:
                    try: _uuid.UUID(app_id)
                    except Exception: app_id = None
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    await conn.execute(
                        "UPDATE uca_form_instances SET title=$3, data=$4::jsonb, application_id=$5::uuid, "
                        "updated_at=now() WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.title, dj, app_id)
                else:
                    await conn.execute(
                        "INSERT INTO uca_form_instances (tenant_id, student_id, form_code, application_id, "
                        "title, data, created_by) VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5,$6::jsonb,$7::uuid)",
                        tenant_id, student_id, it.form_code, app_id, it.title, dj, user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved, "deleted": deleted}


class RecTokenRequest(BaseModel):
    recommender_id: Optional[str] = None
    recommender_name: Optional[str] = None
    recommender_email: Optional[str] = None
    role: Optional[str] = None              # teacher | employer


@router.post("/student/{student_id}/recommendation-links")
async def create_rec_link(request: Request, student_id: str, body: RecTokenRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    import secrets as _secrets
    token = _secrets.token_urlsafe(24)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "INSERT INTO recommendation_request_tokens (token, tenant_id, student_id, "
                "recommender_id, recommender_name, recommender_email, role, expires_at, created_by) "
                "VALUES ($1,$2::uuid,$3::uuid,$4::uuid,$5,$6,$7, now() + interval '30 days', $8::uuid)",
                token, tenant_id, student_id, body.recommender_id, body.recommender_name,
                body.recommender_email, body.role or "teacher", user_id)
    return {"token": token, "url": "https://focms-api.onrender.com/focms/v1/recommend/" + token,
            "expires_days": 30}


class RecSubmitBody(BaseModel):
    submitter_name: str
    submitter_email: Optional[str] = None
    relationship: Optional[str] = None
    years_known: Optional[float] = None
    letter_text: str


@router.get("/recommend/{token}")
async def rec_form_page(request: Request, token: str):
    """Public tokenized form — no auth. Teacher fills and submits."""
    from fastapi.responses import HTMLResponse
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        tok = await conn.fetchrow(
            "SELECT token, tenant_id, student_id, recommender_name, role, expires_at, used_at "
            "FROM recommendation_request_tokens WHERE token=$1", token)
        student_name = None
        if tok and not tok["used_at"]:
            async with conn.transaction():
                await conn.execute(f"SET LOCAL app.current_tenant_id = '{tok['tenant_id']}'")
                student_name = await conn.fetchval(
                    "SELECT COALESCE(display_name, first_name || ' ' || last_name) FROM students WHERE id=$1::uuid",
                    tok["student_id"])
                school_rows = await conn.fetch(
                    "SELECT school_name FROM student_school_enrollments WHERE student_id=$1::uuid "
                    "AND deleted_at IS NULL AND school_name IS NOT NULL "
                    "ORDER BY is_current_school DESC, created_at DESC", tok["student_id"])
                school_names = [r["school_name"] for r in school_rows]
    import datetime as _dt
    if not tok or tok["used_at"] or tok["expires_at"] < _dt.datetime.now(_dt.timezone.utc):
        return HTMLResponse("<h2 style='font-family:sans-serif'>This recommendation link is invalid or has expired.</h2>", status_code=404)
    student = student_name or "the student"
    rn = tok["recommender_name"] or ""
    _opts = "".join("<option value=\"{0}\">{0}</option>".format(n.replace('"','&quot;').replace('<','&lt;')) for n in school_names)
    sch_select = ("<select id='o'>" + _opts + "<option value=''>Other / not listed</option></select>") if school_names else "<input id='o' autocomplete='off' data-lpignore='true' data-1p-ignore readonly onfocus=\"this.removeAttribute('readonly')\">"
    html = f"""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Recommendation for {student}</title>
<link href='https://fonts.googleapis.com/css2?family=Lora:wght@500;600&family=Poppins:wght@400;600&display=swap' rel='stylesheet'>
<style>
:root{{--navy:#201868;--orange:#F07800;--gray:#7A8A9E;}}
body{{font-family:Poppins,sans-serif;background:#FAFAF7;color:#1a1a2e;max-width:760px;margin:0 auto;padding:32px 20px}}
h1{{font-family:Lora,serif;color:var(--navy);font-size:26px;margin:0 0 6px}}
.sub{{color:var(--gray);font-size:14px;margin-bottom:22px}}
label{{display:block;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--gray);margin:14px 0 4px;font-weight:600}}
input,select{{width:100%;padding:10px 12px;border:1px solid #E5E7EB;border-radius:8px;font-family:Poppins,sans-serif;font-size:14px;box-sizing:border-box;background:#fff}}
.two{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.edwrap{{border:1px solid #D8D6E4;border-radius:8px;overflow:hidden;margin-top:4px;background:#fff}}
.bar{{display:flex;gap:4px;flex-wrap:wrap;background:#EEEDF7;border-bottom:1px solid #E2E0F0;padding:8px;align-items:center}}
.fb{{font-family:Poppins,sans-serif;font-size:13px;min-width:30px;height:30px;display:inline-flex;align-items:center;justify-content:center;color:var(--navy);background:#FFF;border:1px solid #D0CEE0;border-radius:5px;padding:0 9px;cursor:pointer;line-height:1;box-sizing:border-box;vertical-align:middle}}
select.fb{{padding:0 4px}}
.bar>*{{height:30px;box-sizing:border-box;vertical-align:middle;margin:0}}
.bar label.fb{{display:inline-flex;align-items:center;justify-content:center}}
.fb:hover{{background:var(--navy);color:#fff}}
.wc{{margin-left:auto;font-size:12px;color:var(--gray)}}
.ed{{min-height:320px;padding:18px 20px;font-family:Lora,serif;font-size:16px;line-height:1.75;color:#222;outline:none}}
.ed:empty:before{{content:attr(data-ph);color:#B4B4C4;font-style:italic}}
button.go{{margin-top:20px;background:var(--orange);color:#fff;border:0;border-radius:8px;padding:12px 30px;font-weight:600;font-size:14px;cursor:pointer;font-family:Poppins,sans-serif}}
.ok{{background:#DCFCE7;color:#14532D;padding:14px;border-radius:8px;margin-top:16px;display:none}}
.note{{font-size:12px;color:var(--gray);margin-top:8px}}
</style></head><body>
<h1>Letter of Recommendation for {student}</h1>
<div class='sub'>Thank you for supporting {student}. This form submits your letter directly and securely \u2014 about 10 minutes.</div>
<div class='two'>
<div><label>Your name *</label><input id='n' value="{rn}"></div>
<div><label>Your email</label><input id='e' type='email'></div>
</div>
<div class='two'>
<div><label>Your role / title</label><input id='t' placeholder='e.g. Math Teacher, Store Manager'></div>
<div><label>Organization / school</label>{sch_select}</div>
</div>
<div class='two'>
<div><label>Relationship to {student}</label><input id='r' placeholder='e.g. Taught Algebra I, Direct supervisor'></div>
<div><label>Years known</label><input id='y' type='number' step='0.5' min='0'></div>
</div>
<label>How strongly do you recommend {student}?</label>
<select id='q'><option value=''>\u2014 choose \u2014</option><option>Recommend with reservations</option><option>Recommend</option><option>Strongly recommend</option><option>One of the best I have worked with</option></select>
<label>Your letter *</label>
<div class='edwrap'>
<div class='bar'>
<select class='fb' style='max-width:120px' onchange="cv(event,'fontName',this.value)"><option>Lora</option><option>Georgia</option><option>Times New Roman</option><option>Arial</option><option>Calibri</option><option>Helvetica</option><option>Verdana</option></select>
<select class='fb' style='max-width:56px' onchange="cv(event,'fontSize',this.value)"><option value='2'>13</option><option value='3'>16</option><option value='4' selected>18</option><option value='5'>24</option><option value='6'>32</option></select>
<button class='fb' onmousedown="c(event,'bold')"><b>B</b></button>
<button class='fb' onmousedown="c(event,'italic')"><i>I</i></button>
<button class='fb' onmousedown="c(event,'underline')"><u>U</u></button>
<button class='fb' onmousedown="c(event,'strikeThrough')"><s>S</s></button>
<button class='fb' onmousedown="c(event,'subscript')">X\u2082</button>
<button class='fb' onmousedown="c(event,'superscript')">X\u00b2</button>
<label class='fb' style='position:relative;overflow:hidden'>A<input type='color' value='#201868' style='position:absolute;inset:0;opacity:0;cursor:pointer' oninput="cv(event,'foreColor',this.value)"></label>
<label class='fb' style='position:relative;overflow:hidden;background:#FFF9C4'>\u25a0<input type='color' value='#FFF176' style='position:absolute;inset:0;opacity:0;cursor:pointer' oninput="cv(event,'hiliteColor',this.value)"></label>
<button class='fb' onmousedown="c(event,'justifyLeft')">\u2261</button>
<button class='fb' onmousedown="c(event,'justifyCenter')">\u2263</button>
<button class='fb' onmousedown="c(event,'justifyRight')">\u2261</button>
<button class='fb' onmousedown="c(event,'insertUnorderedList')">\u2022</button>
<button class='fb' onmousedown="c(event,'insertOrderedList')">1.</button>
<button class='fb' onmousedown="c(event,'outdent')">\u2190|</button>
<button class='fb' onmousedown="c(event,'indent')">|\u2192</button>
<button class='fb' onmousedown="c(event,'undo')">\u21b6</button>
<button class='fb' onmousedown="c(event,'redo')">\u21b7</button>
<button class='fb' onmousedown="c(event,'removeFormat')">Clear</button>
<span class='wc' id='wc'>0 words</span>
</div>
<div class='ed' id='l' contenteditable='true' spellcheck='true' data-ph='Write or paste your letter here. Formatting is preserved.' oninput='wcU()'></div>
</div>
<div class='note'>Spelling is checked as you type (red underline \u2014 right-click for suggestions). For grammar checking, your browser's built-in writing assistance (Edge/Chrome \u201cEnhanced spell check\u201d) or Grammarly extension works in this editor.</div>
<div class='note'>Your letter is private: it goes only to {student}'s family record, not to a public site.</div>
<button class='go' onclick='go()'>Submit recommendation</button>
<div class='ok' id='ok'>Received \u2014 thank you. You can close this page.</div>
<script>
function c(ev,cmd){{ev.preventDefault();document.getElementById('l').focus();document.execCommand(cmd,false,null);wcU();}}
function cv(ev,cmd,val){{ev.preventDefault();document.getElementById('l').focus();document.execCommand(cmd,false,val);wcU();}}
function wcU(){{var t=document.getElementById('l').textContent||'';var w=(t.trim().match(/\\S+/g)||[]).length;document.getElementById('wc').textContent=w+' words';}}
async function go(){{
 var ed=document.getElementById('l');
 var b={{submitter_name:document.getElementById('n').value.trim(),
 submitter_email:document.getElementById('e').value.trim()||null,
 relationship:[document.getElementById('t').value.trim(),document.getElementById('o').value.trim(),document.getElementById('r').value.trim(),document.getElementById('q').value].filter(Boolean).join(' | ')||null,
 years_known:document.getElementById('y').value?parseFloat(document.getElementById('y').value):null,
 letter_text:ed.innerHTML.trim()}};
 if(!b.submitter_name||!ed.textContent.trim()){{alert('Name and letter are required.');return;}}
 var r=await fetch(location.pathname,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(b)}});
 if(r.ok){{document.getElementById('ok').style.display='block';document.querySelector('button.go').disabled=true;}}
 else{{alert('Submission failed: '+(await r.text()).slice(0,200));}}
}}
</script></body></html>"""
    return HTMLResponse(html)


@router.post("/recommend/{token}")
async def rec_form_submit(request: Request, token: str, body: RecSubmitBody):
    """Public tokenized submit — no auth; token is the credential."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT token, tenant_id, student_id, recommender_id, role, expires_at, used_at "
            "FROM recommendation_request_tokens WHERE token=$1", token)
        if not row or row["used_at"]:
            raise HTTPException(status_code=404, detail="invalid or used link")
        import datetime as _dt
        if row["expires_at"] < _dt.datetime.now(_dt.timezone.utc):
            raise HTTPException(status_code=404, detail="link expired")
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{row['tenant_id']}'")
            await conn.execute(
                "INSERT INTO recommendation_letters (tenant_id, student_id, recommender_id, "
                "letter_text, source, submitter_name, submitter_email, relationship, years_known, status) "
                "VALUES ($1::uuid,$2::uuid,$3::uuid,$4,'direct_form',$5,$6,$7,$8,'received')",
                row["tenant_id"], row["student_id"], row["recommender_id"], body.letter_text,
                body.submitter_name, body.submitter_email, body.relationship, body.years_known)
            await conn.execute("UPDATE recommendation_request_tokens SET used_at=now() WHERE token=$1", token)
    return {"ok": True}


@router.get("/catalogs/courses")
async def get_course_catalog(request: Request, subject: Optional[str] = None, q: Optional[str] = None):
    """SCED v13 course codes. Filter by subject (2-digit area) and/or title search q."""
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    pool: asyncpg.Pool = request.app.state.pool
    sql = "SELECT code, subject_code, title FROM sced_courses WHERE is_active"
    args = []
    if subject:
        args.append(subject); sql += f" AND subject_code=${len(args)}"
    if q:
        args.append("%" + q + "%"); sql += f" AND title ILIKE ${len(args)}"
    sql += " ORDER BY code LIMIT 3000"
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(sql, *args)
    return {"courses": [dict(r) for r in rows]}


# ---------------- v0.12.117: cascading K-12 school picker ----------------
# NCES names are abbreviated and uppercase ("ISBELL EL", "FRISCO H S"), so free-text
# search fails for the names families actually use. Instead: pick country -> state
# -> district -> school. Backed by k12_schools (ncessch, leaid, name, district_name).

def _pretty_school(name: str) -> str:
    """ISBELL EL -> Isbell Elementary; FRISCO H S -> Frisco High School."""
    if not name:
        return ""
    s = " ".join(str(name).split())
    up = s.upper()
    for pat, rep in (
        (" H S", " High School"), (" HS", " High School"),
        (" J H", " Junior High"), (" JH", " Junior High"),
        (" EL", " Elementary"), (" ELEM", " Elementary"),
        (" MIDDLE", " Middle School"), (" MS", " Middle School"),
        (" ACAD", " Academy"), (" INT", " Intermediate"),
        (" PRI", " Primary"),
    ):
        if up.endswith(pat):
            s = s[: len(s) - len(pat)] + rep
            break
    out = s.title()
    for k, v in (("Isd", "ISD"), ("Cisd", "CISD"), ("Stem", "STEM"), ("Jjaep", "JJAEP")):
        out = out.replace(k, v)
    return out


@router.get("/catalogs/k12/states")
async def get_k12_states(request: Request, country: str = "US"):
    await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT state FROM k12_schools "
            "WHERE state IS NOT NULL AND coalesce(country,'US')=$1 ORDER BY state",
            (country or "US").upper()[:2])
    return {"country": (country or "US").upper()[:2],
            "states": [r["state"] for r in rows]}


@router.get("/catalogs/k12/districts")
async def get_k12_districts(request: Request, state: str, q: Optional[str] = None):
    await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    st = (state or "").upper()[:2]
    sql = ("SELECT leaid, min(district_name) AS district_name, count(*) AS schools "
           "FROM k12_schools WHERE state=$1 AND district_name IS NOT NULL ")
    args: list = [st]
    if q and q.strip():
        args.append("%" + q.strip() + "%")
        sql += f"AND district_name ILIKE ${len(args)} "
    sql += "GROUP BY leaid ORDER BY 2"
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return {"state": st, "districts": [
        {"leaid": r["leaid"],
         "district_name": _pretty_school(r["district_name"]),
         "schools": r["schools"]} for r in rows]}


@router.get("/catalogs/k12/schools")
async def get_k12_schools_in_district(request: Request, leaid: str):
    await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ncessch, leaid, name, address_line1, city, state, zip, phone, district_name "
            "FROM k12_schools WHERE leaid=$1 ORDER BY name", (leaid or "").strip())
    return {"leaid": leaid, "schools": [
        {"ncessch": r["ncessch"], "leaid": r["leaid"],
         "name": _pretty_school(r["name"]), "name_raw": r["name"],
         "street": r["address_line1"], "city": (r["city"] or "").title(),
         "state": r["state"], "zip": r["zip"], "phone": r["phone"],
         "district_name": _pretty_school(r["district_name"])} for r in rows]}


@router.get("/catalogs/subjects")
async def get_subject_catalog(request: Request):
    """SCED (NCES) subject areas — canonical US K-12 subject taxonomy."""
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT code, title FROM sced_subject_areas WHERE is_active ORDER BY sort_order")
    return {"subjects": [dict(r) for r in rows]}


@router.post("/student/{student_id}/courses")
async def post_student_courses(request: Request, student_id: str, body: CoursesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = 0
    updated = 0
    deleted = 0
    import traceback as _tb
    pool: asyncpg.Pool = request.app.state.pool
    try:
      async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            default_school = await _pp_current_school_name(conn, student_id)
            # v0.12.116 DATA-LOSS FIX. The old code always did a blanket
            # DELETE of every parent_portal course and re-inserted body.items.
            # Callers that save ONE course (the band UI) therefore wiped every
            # other course, and a delete_ids-only post wiped all of them.
            #   upsert mode  -> no blanket delete; id => UPDATE, no id => INSERT
            #   replace mode -> legacy full-list replace (the old Academics form)
            upsert = (body.mode or "").strip().lower() == "upsert" \
                     or bool(body.delete_ids) \
                     or any((it.id or "").strip() for it in body.items)
            for did in body.delete_ids or []:
                try:
                    _uuid.UUID(did)
                except Exception:
                    continue
                await conn.execute(
                    "UPDATE courses_taken SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                deleted += 1
            if not upsert:
                await conn.execute("DELETE FROM courses_taken WHERE tenant_id=$1::uuid "
                                   "AND student_id=$2::uuid AND source_system='parent_portal'",
                                   tenant_id, student_id)
            for it in body.items:
                name = (it.course_name or "").strip()
                if not name:
                    continue
                school = (it.school_name or "").strip() or default_school or "Unspecified"
                _ALLOWED_SUBJ = {"01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","other"}
                subj = (it.subject or "").strip() or None
                if subj is not None and subj not in _ALLOWED_SUBJ:
                    subj = "other"
                rigor = (it.rigor or "regular").lower()
                if rigor not in _RIGOR:
                    rigor = "regular"
                # v0.12.126: the form's AP / IB / Dual / Honors checkboxes are the
                # source of truth when they are ticked - they were being ignored.
                if it.is_ap:
                    rigor = "ap"
                elif it.is_ib:
                    rigor = "ib"
                elif it.is_dual_credit:
                    rigor = "dual"
                elif it.is_honors:
                    rigor = "honors"
                # v0.12.116: course_type is the record TYPE (regular vs private_tutoring),
                # not the rigor. Rigor lives in the is_* booleans + details.rigor.
                ctype = (it.course_type or "regular").strip().lower()
                if ctype not in ("regular", "private_tutoring"):
                    ctype = "regular"
                gp = _pp_grade_points(it.grade_received)
                gpw = (gp + _RIGOR_BONUS.get(rigor, 0.0)) if gp is not None else None
                # v0.12.116: award/certificate of completion is an alternative to a letter
                # grade (tutoring, pass/fail enrichment). Kept in details.
                _details = {
                    "rigor": rigor,
                    "course_description": (it.course_description or None),
                    "completion_award": (it.completion_award or None),
                    "is_private_tutoring": (ctype == "private_tutoring"),
                }
                _details = {k: v for k, v in _details.items() if v is not None}
                # v0.12.120: period + school link + teacher link
                _period = (it.period or "").strip() or None
                _schid = (it.school_id or "").strip()
                if _schid:
                    try:
                        _uuid.UUID(_schid)
                    except Exception:
                        _schid = ""
                _tchid = (it.teacher_id or "").strip()
                if _tchid:
                    try:
                        _uuid.UUID(_tchid)
                    except Exception:
                        _tchid = ""
                _cid = (it.id or "").strip()
                if _cid:
                    try:
                        _uuid.UUID(_cid)
                    except Exception:
                        _cid = ""
                if _cid:
                    # v0.12.116: in-place UPDATE (upsert mode)
                    await conn.execute(
                        "UPDATE courses_taken SET course_name=$3, course_code=$4, sced_code=$5, "
                        "school_name=$6, course_type=$7, subject=$8, subject_other=$9, "
                        "grade_level=$10, school_year=$11, term=$12, credit_hours=$13, "
                        "grade_received=$14, is_honors=$15, is_ap=$16, is_ib=$17, is_dual_credit=$18, "
                        "ap_exam_score=$19, teacher_name=$20, teacher_email=$21, "
                        "course_description=$22, details=$23::jsonb, grade_points_4_0=$24, "
                        "grade_points_weighted=$25, notes=$26, admission_traits_developed=$27::jsonb, "
                        "evidence_artifact_ids=$28::text[]::uuid[], "
                        "period=$30, school_id=NULLIF($31,'')::uuid, teacher_id=NULLIF($32,'')::uuid, "
                        "updated_at=now(), updated_by=$29::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        _cid, student_id, name, (it.course_code or None), (it.sced_code or None),
                        school, ctype, subj, (it.subject_other or None),
                        _pp_int(it.grade_level), (it.school_year or None), (it.term or None),
                        _pp_num(it.credit_hours), (it.grade_received or None),
                        rigor == "honors", rigor == "ap", rigor == "ib", rigor == "dual",
                        _pp_int(it.ap_exam_score), (it.teacher_name or None), (it.teacher_email or None),
                        (it.course_description or None), json.dumps(_details), gp, gpw,
                        (it.notes or None), json.dumps(_pp_skills(it.skills)),
                        _pp_artifacts(it.artifact_ids), user_id,
                        _period, _schid, _tchid)
                    if it.skills:
                        await _course_skills_to_inventory(conn, tenant_id, student_id, user_id,
                                                          _cid, it.skills)
                    updated += 1
                    continue
                await conn.execute(
                    "INSERT INTO courses_taken (tenant_id, student_id, course_name, course_code, sced_code, school_name, "
                    "course_type, subject, subject_other, grade_level, school_year, term, credit_hours, grade_received, "
                    "is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, teacher_name, teacher_email, "
                    "course_description, details, "
                    "period, school_id, teacher_id, "
                    "grade_points_4_0, grade_points_weighted, notes, admission_traits_developed, "
                    "evidence_artifact_ids, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$25,$26,$4,$5,$6,$24,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$27,"
                    "$28,$29::jsonb,"
                    "$30,NULLIF($31,'')::uuid,NULLIF($32,'')::uuid,"
                    "$18,$19,$20,$21::jsonb,$22::text[]::uuid[],'parent_portal',$23::uuid,$23::uuid)",
                    tenant_id, student_id, name, school, ctype, subj,
                    _pp_int(it.grade_level), (it.school_year or None), (it.term or None),
                    _pp_num(it.credit_hours), (it.grade_received or None),
                    rigor == "honors", rigor == "ap", rigor == "ib", rigor == "dual",
                    _pp_int(it.ap_exam_score), (it.teacher_name or None),
                    gp, gpw, (it.notes or None), json.dumps(_pp_skills(it.skills)),
                    _pp_artifacts(it.artifact_ids), user_id, (it.subject_other or None),
                    (it.course_code or None), (it.sced_code or None),
                    (it.teacher_email or None), (it.course_description or None),
                    json.dumps(_details),
                    _period, _schid, _tchid)
                # v0.12.116: universal skills-gained - mirror course skills into
                # student_skills so EVERY course (academic or tutoring) feeds the
                # skill inventory and the meta-skill inference engine.
                if it.skills:
                    _cid = await conn.fetchval(
                        "SELECT id FROM courses_taken WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                        "AND source_system='parent_portal' AND course_name=$3 "
                        "ORDER BY created_at DESC LIMIT 1", tenant_id, student_id, name)
                    if _cid:
                        await _course_skills_to_inventory(conn, tenant_id, student_id, user_id,
                                                          str(_cid), it.skills)
                saved += 1
    except Exception as _e:
        raise HTTPException(status_code=400, detail="course_insert_error: " + str(_e)[:300])
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# --------------------------- Standardized Tests ----------------------------

_TEST_NAMES = {"SAT": "SAT", "ACT": "ACT", "PSAT": "PSAT/NMSQT", "AP": "AP Exam", "IB": "IB Exam"}


class TestItem(BaseModel):
    test_code: Optional[str] = None
    sitting_date: Optional[str] = None
    score_overall: Optional[float] = None
    percentile: Optional[float] = None
    notes: Optional[str] = None
    skills: list[str] = Field(default_factory=list)
    artifact_ids: list[str] = Field(default_factory=list)


class TestsRequest(BaseModel):
    items: list[TestItem] = Field(default_factory=list)


@router.get("/student/{student_id}/tests")
async def get_student_tests(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT test_code, sitting_date, score_overall, percentile, notes, "
            "admission_traits_developed, evidence_artifact_ids FROM standardized_test_scores "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL AND source_system='parent_portal' "
            "ORDER BY sitting_date DESC NULLS LAST", student_id)
    return {"student_id": student_id, "items": [
        {"test_code": r["test_code"],
         "sitting_date": r["sitting_date"].isoformat() if r["sitting_date"] else None,
         "score_overall": float(r["score_overall"]) if r["score_overall"] is not None else None,
         "percentile": float(r["percentile"]) if r["percentile"] is not None else None,
         "notes": r["notes"], "skills": _pp_skills(r["admission_traits_developed"]),
         "artifact_ids": _pp_artifacts(r["evidence_artifact_ids"])} for r in rows]}


@router.post("/student/{student_id}/tests")
async def post_student_tests(request: Request, student_id: str, body: TestsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute("DELETE FROM standardized_test_scores WHERE tenant_id=$1::uuid "
                               "AND student_id=$2::uuid AND source_system='parent_portal'",
                               tenant_id, student_id)
            for it in body.items:
                code = (it.test_code or "").strip().upper()
                d = _pp_parse_date(it.sitting_date)
                if code not in _TEST_NAMES or d is None:
                    continue
                await conn.execute(
                    "INSERT INTO standardized_test_scores (tenant_id, student_id, test_code, test_name, "
                    "sitting_date, score_overall, percentile, is_official, reporting_status, notes, "
                    "admission_traits_developed, evidence_artifact_ids, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,false,'self_reported',$8,$9::jsonb,"
                    "$10::text[]::uuid[],'parent_portal',$11::uuid,$11::uuid)",
                    tenant_id, student_id, code, _TEST_NAMES[code], d,
                    _pp_num(it.score_overall), _pp_num(it.percentile), (it.notes or None),
                    json.dumps(_pp_skills(it.skills)), _pp_artifacts(it.artifact_ids), user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved}


# -------------------------------- Family -----------------------------------
# Names + email are envelope-encrypted (focms_encrypt_pii / _decrypt_pii).
# Per-field public choice stored in details->'public_fields'. A parent's email,
# phone, DOB, legal sex, and marital status are LOCKED (forced non-public).

import os as _pp_os
_PP_KEK = _pp_os.environ.get("FOCMS_KEK_MASTER")
_FAMILY_LOCKED = {"email", "phone", "date_of_birth", "legal_sex", "marital_relationship",
                  "street_address", "street_address_line_2", "city_town", "state_province",
                  "zip_postal_code", "country", "mailing_address"}


class FamilyMember(BaseModel):
    prefix: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    suffix: Optional[str] = None
    legal_sex: Optional[str] = None
    date_of_birth: Optional[str] = None
    is_living: Optional[bool] = True
    email: Optional[str] = None
    phone: Optional[str] = None
    phone_home: Optional[str] = None
    phone_work: Optional[str] = None
    profession: Optional[str] = None
    position_title: Optional[str] = None
    employer: Optional[str] = None
    undergrad_institution: Optional[str] = None
    undergrad_degree: Optional[str] = None
    undergrad_year: Optional[int] = None
    grad_institution: Optional[str] = None
    grad_degree: Optional[str] = None
    grad_year: Optional[int] = None
    marital_relationship: Optional[str] = None
    resides_with_student: Optional[bool] = None
    is_legal_guardian: Optional[bool] = True
    notes: Optional[str] = None
    # v0.12.85: physical address (family_members columns, free-text
    # international) + mailing address (details jsonb). Never public.
    street_address: Optional[str] = None
    street_address_line_2: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    country: Optional[str] = None
    mailing_same_as_physical: Optional[bool] = True
    mailing_address: Optional[dict] = None   # {street_address, street_address_line_2, city_town, state_province, zip_postal_code, country}
    education_level: Optional[str] = None    # v0.12.87: high_school_diploma | ged | other
    county: Optional[str] = None             # v0.12.88: county/district of residence
    public: dict = Field(default_factory=dict)


class FamilyRequest(BaseModel):
    father: FamilyMember = Field(default_factory=FamilyMember)
    mother: FamilyMember = Field(default_factory=FamilyMember)


def _fm_has_data(m: FamilyMember) -> bool:
    return bool((m.first_name or "").strip() or (m.last_name or "").strip())


async def _insert_family_member(conn, tenant_id, student_id, user_id, relationship, order, m: FamilyMember):
    # v0.12.89: family_members_relationship_check has no father/mother values.
    # Store relationship='parent' and keep the side in details->>'parent_role'.
    parent_role = relationship
    relationship = "parent"
    public = {k: (False if k in _FAMILY_LOCKED else bool(v)) for k, v in (m.public or {}).items()}
    pub_json = json.dumps(public)
    await conn.execute(
        """
        INSERT INTO family_members
            (tenant_id, student_id, relationship, guardian_order, is_legal_guardian,
             prefix, first_name_ciphertext, middle_name_ciphertext, last_name_ciphertext, suffix,
             legal_sex, date_of_birth, is_living, email_ciphertext, phone,
             profession, position_title, employer,
             undergrad_institution, undergrad_degree, undergrad_year,
             grad_institution, grad_degree, grad_year,
             marital_relationship, resides_with_student, notes, details,
             street_address, street_address_line_2, city_town, state_province,
             zip_postal_code, country,
             source_system, created_by, updated_by)
        VALUES
            ($1::uuid,$2::uuid,$3,$4,$5,
             $6, focms_encrypt_pii($1::uuid,$7,$29), focms_encrypt_pii($1::uuid,$8,$29),
             focms_encrypt_pii($1::uuid,$9,$29), $10,
             $11,$12,$13, focms_encrypt_pii($1::uuid,$14,$29), $15,
             $16,$17,$18,
             $19,$20,$21,
             $22,$23,$24,
             $25,$26,$27,
             jsonb_build_object('public_fields', $30::jsonb,
                                'mailing_same_as_physical', $31::boolean,
                                'mailing_address', $32::jsonb,
                                'education_level', $39::text,
                                'county', $40::text),
             $33,$34,$35,$36,$37,$38,
             'parent_portal',$28::uuid,$28::uuid)
        """,
        tenant_id, student_id, relationship, order,
        (m.is_legal_guardian if m.is_legal_guardian is not None else True),
        (m.prefix or None), (m.first_name or None), (m.middle_name or None),
        (m.last_name or None), (m.suffix or None),
        (m.legal_sex or None), _pp_parse_date(m.date_of_birth),
        (m.is_living if m.is_living is not None else True),
        (m.email or None), (m.phone or None),
        (m.profession or None), (m.position_title or None), (m.employer or None),
        (m.undergrad_institution or None), (m.undergrad_degree or None), _pp_int(m.undergrad_year),
        (m.grad_institution or None), (m.grad_degree or None), _pp_int(m.grad_year),
        (m.marital_relationship or None), m.resides_with_student, (m.notes or None),
        user_id, _PP_KEK, pub_json,
        (m.mailing_same_as_physical if m.mailing_same_as_physical is not None else True),
        json.dumps(m.mailing_address or {}),
        (m.street_address or None), (m.street_address_line_2 or None), (m.city_town or None),
        (m.state_province or None), (m.zip_postal_code or None), (m.country or None),
        (m.education_level or None), (m.county or None), parent_role,
        json.dumps({"home": m.phone_home or None, "work": m.phone_work or None}),
    )


@router.get("/student/{student_id}/family")
async def get_student_family(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            """
            SELECT relationship, is_legal_guardian, prefix,
                   focms_decrypt_pii(tenant_id, first_name_ciphertext, $2)  AS first_name,
                   focms_decrypt_pii(tenant_id, middle_name_ciphertext, $2) AS middle_name,
                   focms_decrypt_pii(tenant_id, last_name_ciphertext, $2)   AS last_name,
                   suffix, legal_sex, date_of_birth, is_living,
                   focms_decrypt_pii(tenant_id, email_ciphertext, $2)       AS email,
                   phone, profession, position_title, employer,
                   undergrad_institution, undergrad_degree, undergrad_year,
                   grad_institution, grad_degree, grad_year,
                   marital_relationship, resides_with_student, notes,
                   street_address, street_address_line_2, city_town, state_province,
                   zip_postal_code, country,
                   details->'public_fields' AS public_fields,
                   COALESCE((details->>'mailing_same_as_physical')::boolean, true) AS mailing_same_as_physical,
                   details->'mailing_address' AS mailing_address,
                   details->>'education_level' AS education_level,
                   details->>'county' AS county,
                   details->>'parent_role' AS parent_role,
                   details->'phones' AS fam_phones
              FROM family_members
             WHERE student_id=$1::uuid AND deleted_at IS NULL AND source_system='parent_portal'
             ORDER BY guardian_order NULLS LAST
            """,
            student_id, _PP_KEK,
        )
    out = {"father": {}, "mother": {}}
    for r in rows:
        pf = r["public_fields"]
        if isinstance(pf, str):
            try:
                pf = json.loads(pf)
            except Exception:
                pf = {}
        d = {
            "prefix": r["prefix"], "first_name": r["first_name"], "middle_name": r["middle_name"],
            "last_name": r["last_name"], "suffix": r["suffix"], "legal_sex": r["legal_sex"],
            "date_of_birth": r["date_of_birth"].isoformat() if r["date_of_birth"] else None,
            "is_living": r["is_living"], "email": r["email"], "phone": r["phone"],
            "profession": r["profession"], "position_title": r["position_title"], "employer": r["employer"],
            "undergrad_institution": r["undergrad_institution"], "undergrad_degree": r["undergrad_degree"],
            "undergrad_year": r["undergrad_year"], "grad_institution": r["grad_institution"],
            "grad_degree": r["grad_degree"], "grad_year": r["grad_year"],
            "marital_relationship": r["marital_relationship"], "resides_with_student": r["resides_with_student"],
            "is_legal_guardian": r["is_legal_guardian"], "notes": r["notes"],
            "street_address": r["street_address"], "street_address_line_2": r["street_address_line_2"],
            "city_town": r["city_town"], "state_province": r["state_province"],
            "zip_postal_code": r["zip_postal_code"], "country": r["country"],
            "mailing_same_as_physical": r["mailing_same_as_physical"],
            "education_level": r["education_level"], "county": r["county"],
            **(lambda ph: {"phone_home": ph.get("home"), "phone_work": ph.get("work")})(
                (json.loads(r["fam_phones"]) if isinstance(r["fam_phones"], str) else (r["fam_phones"] or {}))),
            "mailing_address": (json.loads(r["mailing_address"]) if isinstance(r["mailing_address"], str) else (r["mailing_address"] or {})),
            "public": pf or {},
        }
        rel = (r["parent_role"] or r["relationship"] or "").lower()
        if rel in ("father", "mother"):
            out[rel] = d
    return {"student_id": student_id, "father": out["father"], "mother": out["mother"], "locked": sorted(_FAMILY_LOCKED)}


@router.post("/student/{student_id}/family")
async def post_student_family(request: Request, student_id: str, body: FamilyRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    if not _PP_KEK:
        raise HTTPException(status_code=503, detail="pii_encryption_unavailable")
    written = []
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "DELETE FROM family_members WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                "AND source_system='parent_portal' AND COALESCE(details->>'parent_role','') IN ('father','mother')",
                tenant_id, student_id)
            if _fm_has_data(body.father):
                await _insert_family_member(conn, tenant_id, student_id, user_id, "father", 1, body.father)
                written.append("father")
            if _fm_has_data(body.mother):
                await _insert_family_member(conn, tenant_id, student_id, user_id, "mother", 2, body.mother)
                written.append("mother")
    return {"student_id": student_id, "written": written}


# -------------------------------- Religion ---------------------------------
# Stored in student_personal_details.details->'religion' (no schema change).
# Structured on Pew's three dimensions: affiliation, behavior, belief.
# Per-field public choices live in religion.public {field_key: bool}; the row's
# own `visibility` stays private (it also holds SSN/race), so the public site
# must honor the per-field map rather than the row flag.

class ReligionRequest(BaseModel):
    affiliation: Optional[str] = None
    affiliation_other: Optional[str] = None
    attendance: Optional[str] = None
    observance_needs: Optional[str] = None
    importance: Optional[str] = None
    public: dict = Field(default_factory=dict)


@router.get("/student/{student_id}/religion")
async def get_student_religion(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT details->'religion' AS religion FROM student_personal_details "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    rel = row["religion"] if row and row["religion"] is not None else {}
    if isinstance(rel, str):
        try:
            rel = json.loads(rel)
        except Exception:
            rel = {}
    return {"student_id": student_id, "religion": rel or {}}


@router.post("/student/{student_id}/religion")
async def post_student_religion(request: Request, student_id: str, body: ReligionRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    rel = {
        "affiliation": (body.affiliation or None),
        "affiliation_other": (body.affiliation_other or None),
        "attendance": (body.attendance or None),
        "observance_needs": (body.observance_needs or None),
        "importance": (body.importance or None),
        "public": {k: bool(v) for k, v in (body.public or {}).items()},
    }
    rel_json = json.dumps(rel)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            # v0.12.83: legal name on students row (display_name kept in sync)
            fn = (body.first_name or "").strip()
            ln = (body.last_name or "").strip()
            if fn and ln:
                mn = (body.middle_name or "").strip() or None
                await conn.execute(
                    "UPDATE students SET first_name=$2, middle_name=$5, last_name=$3, "
                    "display_name=$2||' '||$3, updated_by=$4::uuid, updated_at=now() "
                    "WHERE id=$1::uuid AND deleted_at IS NULL",
                    student_id, fn, ln, user_id, mn)
            exists = await conn.fetchrow(
                "SELECT 1 FROM student_personal_details WHERE student_id=$1::uuid AND deleted_at IS NULL",
                student_id)
            if exists:
                await conn.execute(
                    "UPDATE student_personal_details SET "
                    "details = COALESCE(details,'{}'::jsonb) || jsonb_build_object('religion', $2::jsonb), "
                    "updated_by=$3::uuid, updated_at=now() "
                    "WHERE student_id=$1::uuid AND deleted_at IS NULL",
                    student_id, rel_json, user_id)
            else:
                await conn.execute(
                    "INSERT INTO student_personal_details "
                    "(tenant_id, student_id, details, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid, jsonb_build_object('religion', $3::jsonb), "
                    "'parent_portal', $4::uuid, $4::uuid)",
                    tenant_id, student_id, rel_json, user_id)
    return {"student_id": student_id, "saved": True}


# ---------------------------- Personal details -----------------------------
# Identity + demographics on student_personal_details (direct columns).
# Per-field public choice in details->'public_fields'. Fields that cannot be
# public for a minor under privacy / anti-discrimination / child-safety law are
# LOCKED: their public flag is forced false server-side regardless of input,
# and the client renders them non-toggleable.

_PERSONAL_LOCKED = {
    "gender_identity", "legal_sex_at_birth", "email_primary", "phone_primary",
    "citizenship_status", "place_of_birth_country", "is_hispanic_or_latino", "racial_background",
}


class PersonalDetailsRequest(BaseModel):
    first_name: Optional[str] = None   # v0.12.83/84: legal name lives on students
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    chosen_name: Optional[str] = None
    pronouns: list[str] = Field(default_factory=list)
    gender_identity: list[str] = Field(default_factory=list)
    legal_sex_at_birth: Optional[str] = None
    email_primary: Optional[str] = None
    phone_primary: Optional[str] = None
    phone_home: Optional[str] = None
    phone_mobile: Optional[str] = None
    phone_work: Optional[str] = None
    citizenship_status: Optional[str] = None
    place_of_birth_country: Optional[str] = None
    is_hispanic_or_latino: Optional[bool] = None
    racial_background: list[str] = Field(default_factory=list)
    language_spoken_at_home: Optional[str] = None
    first_language_native: Optional[str] = None
    public: dict = Field(default_factory=dict)


@router.get("/student/{student_id}/personal-details")
async def get_student_personal_details(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        names = await conn.fetchrow(
            "SELECT first_name, middle_name, last_name FROM students WHERE id=$1::uuid AND deleted_at IS NULL",
            student_id)
        row = await conn.fetchrow(
            "SELECT chosen_name, pronouns, gender_identity, legal_sex_at_birth, email_primary, "
            "phone_primary, citizenship_status, place_of_birth_country, is_hispanic_or_latino, "
            "racial_background, language_spoken_at_home, first_language_native, "
            "details->'public_fields' AS public_fields, details->'phones' AS phones "
            "FROM student_personal_details WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    locked = sorted(_PERSONAL_LOCKED)
    base = {"first_name": names["first_name"] if names else None,
            "middle_name": names["middle_name"] if names else None,
            "last_name": names["last_name"] if names else None}
    if not row:
        return {"student_id": student_id, "personal": base, "locked": locked}
    pf = row["public_fields"]
    if isinstance(pf, str):
        try:
            pf = json.loads(pf)
        except Exception:
            pf = {}
    return {"student_id": student_id, "locked": locked, "personal": {
        **base,
        "chosen_name": row["chosen_name"],
        "pronouns": list(row["pronouns"] or []),
        "gender_identity": list(row["gender_identity"] or []),
        "legal_sex_at_birth": row["legal_sex_at_birth"],
        "email_primary": row["email_primary"],
        "phone_primary": row["phone_primary"],
        **(lambda ph: {"phone_home": ph.get("home"), "phone_mobile": ph.get("mobile"), "phone_work": ph.get("work")})(
            (json.loads(row["phones"]) if isinstance(row["phones"], str) else (row["phones"] or {}))),
        "citizenship_status": row["citizenship_status"],
        "place_of_birth_country": row["place_of_birth_country"],
        "is_hispanic_or_latino": row["is_hispanic_or_latino"],
        "racial_background": list(row["racial_background"] or []),
        "language_spoken_at_home": row["language_spoken_at_home"],
        "first_language_native": row["first_language_native"],
        "public": pf or {},
    }}


@router.post("/student/{student_id}/personal-details")
async def post_student_personal_details(request: Request, student_id: str, body: PersonalDetailsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    public = {k: (False if k in _PERSONAL_LOCKED else bool(v)) for k, v in (body.public or {}).items()}
    pf_json = json.dumps(public)

    def arr(x):
        return [s.strip() for s in (x or []) if str(s).strip()]

    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            # v0.12.83: legal name on students row (display_name kept in sync)
            fn = (body.first_name or "").strip()
            ln = (body.last_name or "").strip()
            if fn and ln:
                mn = (body.middle_name or "").strip() or None
                await conn.execute(
                    "UPDATE students SET first_name=$2, middle_name=$5, last_name=$3, "
                    "display_name=$2||' '||$3, updated_by=$4::uuid, updated_at=now() "
                    "WHERE id=$1::uuid AND deleted_at IS NULL",
                    student_id, fn, ln, user_id, mn)
            exists = await conn.fetchrow(
                "SELECT 1 FROM student_personal_details WHERE student_id=$1::uuid AND deleted_at IS NULL",
                student_id)
            args = (
                student_id,
                (body.chosen_name or None), arr(body.pronouns), arr(body.gender_identity),
                (body.legal_sex_at_birth or None), (body.email_primary or None), (body.phone_primary or None),
                (body.citizenship_status or None), (body.place_of_birth_country or None),
                body.is_hispanic_or_latino, arr(body.racial_background),
                (body.language_spoken_at_home or None), (body.first_language_native or None),
                pf_json, user_id,
                json.dumps({"home": body.phone_home or None,
                            "mobile": body.phone_mobile or None,
                            "work": body.phone_work or None}),
            )
            if exists:
                await conn.execute(
                    "UPDATE student_personal_details SET "
                    "chosen_name=$2, pronouns=$3::text[], gender_identity=$4::text[], legal_sex_at_birth=$5, "
                    "email_primary=$6, phone_primary=$7, citizenship_status=$8, place_of_birth_country=$9, "
                    "is_hispanic_or_latino=$10, racial_background=$11::text[], language_spoken_at_home=$12, "
                    "first_language_native=$13, "
                    "details = COALESCE(details,'{}'::jsonb) || jsonb_build_object('public_fields', $14::jsonb, 'phones', $16::jsonb), "
                    "updated_by=$15::uuid, updated_at=now() "
                    "WHERE student_id=$1::uuid AND deleted_at IS NULL",
                    *args)
            else:
                await conn.execute(
                    "INSERT INTO student_personal_details "
                    "(student_id, chosen_name, pronouns, gender_identity, legal_sex_at_birth, email_primary, "
                    "phone_primary, citizenship_status, place_of_birth_country, is_hispanic_or_latino, "
                    "racial_background, language_spoken_at_home, first_language_native, details, "
                    "tenant_id, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2,$3::text[],$4::text[],$5,$6,$7,$8,$9,$10,$11::text[],$12,$13, "
                    "jsonb_build_object('public_fields', $14::jsonb, 'phones', $16::jsonb), "
                    "$17::uuid, 'parent_portal', "
                    "COALESCE($15::uuid,'019ed384-56d8-77fb-bfe6-00b1d064da18'::uuid), "
                    "COALESCE($15::uuid,'019ed384-56d8-77fb-bfe6-00b1d064da18'::uuid))",
                    *args, tenant_id)
    return {"student_id": student_id, "saved": True}


# ---------------- Zip geography + occupations (v0.12.90) -------------------
# us_zip_geo.json.gz: GeoNames US postal data (city, state, counties per zip).
# occupations.json.gz: O*NET / DOL SOC occupation titles (867 detailed).
# Both live beside this module in the repo and are lazy-loaded into memory.

import gzip as _gz
import pathlib as _pl
_GEO_CACHE: dict | None = None
_OCC_CACHE: list | None = None
_DATA_DIR = _pl.Path(__file__).resolve().parent


def _load_gz_json(name):
    with _gz.open(_DATA_DIR / name, "rt", encoding="utf-8") as f:
        return json.load(f)


@router.get("/catalogs/zip/{zip_code}")
async def get_zip_geo(request: Request, zip_code: str):
    await _resolve_context(request)
    global _GEO_CACHE
    if _GEO_CACHE is None:
        try:
            _GEO_CACHE = _load_gz_json("us_zip_geo.json.gz")
        except Exception:
            _GEO_CACHE = {}
    z = _GEO_CACHE.get(zip_code.strip()[:5])
    if not z:
        raise HTTPException(404, {"error": "zip_not_found"})
    return {"zip": zip_code.strip()[:5], "city": z["city"], "state": z["state"],
            "state_iso": f"US-{z['state']}", "counties": z["counties"]}


@router.get("/catalogs/occupations")
async def get_occupations(request: Request):
    await _resolve_context(request)
    global _OCC_CACHE
    if _OCC_CACHE is None:
        try:
            _OCC_CACHE = _load_gz_json("occupations.json.gz")
        except Exception:
            _OCC_CACHE = []
    return {"occupation_count": len(_OCC_CACHE), "occupations": _OCC_CACHE}


# ------------------ Subdivisions catalog (v0.12.88, ISO 3166-2) ------------
# Canonical geographic divisions from pycountry (ISO 3166-2). The portal
# renders these as the State/Province dropdown after a country is chosen and
# stores the ISO code (e.g. US-TX), never the localized display name.
# Countries without formal subdivisions return an empty list; the portal
# falls back to a free-text Region / City input.

@router.get("/catalogs/subdivisions")
async def get_subdivisions(request: Request, country: str):
    await _resolve_context(request)
    iso2 = (country or "").strip().upper()[:2]
    try:
        import pycountry
        subs = pycountry.subdivisions.get(country_code=iso2) or []
        out = sorted(({"code": x.code, "name": x.name} for x in subs),
                     key=lambda d: d["name"])
    except Exception:
        out = []
    return {"country": iso2, "subdivision_count": len(out), "subdivisions": out}


# ---------------------- Student addresses (v0.12.85) -----------------------
# Physical ('permanent') + 'mailing' rows in student_addresses. Free-text
# fields accommodate every country; iso/e164 normalization is the i18n
# validate endpoint's job. Addresses are NEVER site-eligible (no public map).

_ADDR_FIELDS = ("street_address", "street_address_line_2", "city_town",
                "state_province", "zip_postal_code", "country", "phone_at_address")
_ADDR_DETAIL_FIELDS = ("county",)


class StudentAddressIn(BaseModel):
    county: Optional[str] = None
    street_address: Optional[str] = None
    street_address_line_2: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    country: Optional[str] = None
    phone_at_address: Optional[str] = None


class StudentAddressesRequest(BaseModel):
    physical: StudentAddressIn = Field(default_factory=StudentAddressIn)
    mailing_same_as_physical: bool = True
    mailing: StudentAddressIn = Field(default_factory=StudentAddressIn)


@router.get("/student/{student_id}/addresses")
async def get_student_addresses(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id, address_kind, street_address, street_address_line_2, city_town, "
            "state_province, zip_postal_code, country, phone_at_address, "
            "details->>'county' AS county, "
            "COALESCE((details->>'mailing_same_as_physical')::boolean, true) AS same_flag "
            "FROM student_addresses WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "AND address_kind IN ('permanent','current_mailing')", student_id)
    out = {"physical": {}, "mailing": {}, "mailing_same_as_physical": True,
           "physical_id": None, "mailing_id": None}
    for r in rows:
        d = {k: r[k] for k in _ADDR_FIELDS}
        d["county"] = r["county"]
        if r["address_kind"] == "permanent":
            out["physical"] = d
            out["physical_id"] = str(r["id"])
            out["mailing_same_as_physical"] = r["same_flag"]
        else:
            out["mailing"] = d
            out["mailing_id"] = str(r["id"])
    return {"student_id": student_id, **out}


@router.post("/student/{student_id}/addresses")
async def post_student_addresses(request: Request, student_id: str, body: StudentAddressesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    mailing = body.physical if body.mailing_same_as_physical else body.mailing
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            ids = {}
            for kind, a, extra in (("permanent", body.physical,
                                    {"mailing_same_as_physical": body.mailing_same_as_physical,
                                     "county": body.physical.county}),
                                   ("current_mailing", mailing, {"county": mailing.county})):
                row = await conn.fetchrow(
                    """INSERT INTO student_addresses
                         (student_id, tenant_id, address_kind, is_current,
                          street_address, street_address_line_2, city_town, state_province,
                          zip_postal_code, country, phone_at_address, details,
                          visibility, source_system, created_by, updated_by)
                       VALUES ($1::uuid,$2::uuid,$3,true,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,
                               'private','parent_portal',$12::uuid,$12::uuid)
                       ON CONFLICT (student_id, address_kind) DO UPDATE SET
                         street_address=EXCLUDED.street_address,
                         street_address_line_2=EXCLUDED.street_address_line_2,
                         city_town=EXCLUDED.city_town, state_province=EXCLUDED.state_province,
                         zip_postal_code=EXCLUDED.zip_postal_code, country=EXCLUDED.country,
                         phone_at_address=EXCLUDED.phone_at_address,
                         details = COALESCE(student_addresses.details,'{}'::jsonb) || EXCLUDED.details,
                         updated_by=$12::uuid, updated_at=now(), deleted_at=NULL
                       RETURNING id""",
                    student_id, tenant_id, kind,
                    (a.street_address or None), (a.street_address_line_2 or None),
                    (a.city_town or None), (a.state_province or None),
                    (a.zip_postal_code or None), (a.country or None),
                    (a.phone_at_address or None), json.dumps(extra), user_id)
                ids["physical_id" if kind == "permanent" else "mailing_id"] = str(row["id"])
    return {"student_id": student_id, "saved": True,
            "mailing_same_as_physical": body.mailing_same_as_physical, **ids}


# --------------------------------- Skills ----------------------------------
# skills_catalog (500-skill taxonomy) + student_skills (per-student, RLS).
# Three provenance tiers:
#   presumed  - typical_age_max below the child's age; NOT stored (default-on),
#               only explicit parent overrides (acquired=false) are stored
#   attested  - parent-marked (source_system='parent_portal')
#   evidenced - attached from activities via source_activity (future inference)

_PROF = {"emerging", "developing", "proficient", "mastered"}


async def _pp_student_age(conn, student_id: str):
    """Age in years from the encrypted DOB (plaintext fallback), or None."""
    row = await conn.fetchrow(
        "SELECT focms_decrypt_pii(tenant_id, birth_date_ciphertext, $2) AS dob_enc, "
        "       birth_date "
        "FROM students WHERE id=$1::uuid AND deleted_at IS NULL",
        student_id, _PP_KEK)
    if not row:
        return None
    dob = None
    if row["dob_enc"]:
        dob = _pp_parse_date(str(row["dob_enc"]))
    if dob is None and row["birth_date"]:
        dob = row["birth_date"]
    if dob is None:
        return None
    from datetime import date as _d
    return round((_d.today() - dob).days / 365.25, 2)


@router.get("/skills-catalog")
async def get_skills_catalog(request: Request):
    await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT code, title, stage, domain, typical_age_min, typical_age_max, sort_order "
            "FROM skills_catalog WHERE is_active ORDER BY sort_order")
    return {"skills": [
        {"code": r["code"], "title": r["title"], "stage": r["stage"], "domain": r["domain"],
         "age_min": float(r["typical_age_min"]) if r["typical_age_min"] is not None else None,
         "age_max": float(r["typical_age_max"]) if r["typical_age_max"] is not None else None,
         "sort_order": r["sort_order"]} for r in rows]}


class SkillItem(BaseModel):
    skill_code: Optional[str] = None
    custom_title: Optional[str] = None
    custom_domain: Optional[str] = None
    acquired: bool = True
    acquired_date: Optional[str] = None
    proficiency: Optional[str] = None
    notes: Optional[str] = None
    artifact_url: Optional[str] = None


class SkillsRequest(BaseModel):
    items: list[SkillItem] = Field(default_factory=list)


@router.get("/student/{student_id}/skills")
async def get_student_skills(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        age = await _pp_student_age(conn, student_id)
        rows = await conn.fetch(
            "SELECT skill_code, custom_title, custom_domain, acquired, acquired_date, proficiency, "
            "notes, artifact_url, source_activity, source_system "
            "FROM student_skills WHERE student_id=$1::uuid AND deleted_at IS NULL",
            student_id)
    catalog, custom = [], []
    for r in rows:
        d = {"skill_code": r["skill_code"], "custom_title": r["custom_title"],
             "custom_domain": r["custom_domain"], "acquired": r["acquired"],
             "acquired_date": r["acquired_date"].isoformat() if r["acquired_date"] else None,
             "proficiency": r["proficiency"], "notes": r["notes"], "artifact_url": r["artifact_url"],
             "source_activity": r["source_activity"], "source_system": r["source_system"]}
        (catalog if r["skill_code"] else custom).append(d)
    return {"student_id": student_id, "student_age": age, "skills": catalog, "custom": custom}


@router.post("/student/{student_id}/skills")
async def post_student_skills(request: Request, student_id: str, body: SkillsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = overrides = cleared = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        age = await _pp_student_age(conn, student_id)
        age_max_by_code = {
            r["code"]: (float(r["typical_age_max"]) if r["typical_age_max"] is not None else None)
            for r in await conn.fetch("SELECT code, typical_age_max FROM skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "DELETE FROM student_skills WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                "AND source_system='parent_portal' AND skill_code IS NULL", tenant_id, student_id)
            for it in body.items:
                code = (it.skill_code or "").strip()
                prof = (it.proficiency or "").strip().lower()
                if prof not in _PROF:
                    prof = None
                d = _pp_parse_date(it.acquired_date)
                notes = (it.notes or "").strip() or None
                art = (it.artifact_url or "").strip() or None
                if code:
                    if code not in age_max_by_code:
                        continue
                    amax = age_max_by_code[code]
                    presumed = age is not None and amax is not None and age >= amax
                    await conn.execute(
                        "DELETE FROM student_skills WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                        "AND skill_code=$3 AND source_system='parent_portal'",
                        tenant_id, student_id, code)
                    if it.acquired:
                        if presumed and not prof and not d and not notes and not art:
                            cleared += 1  # matches the presumption default; no row needed
                            continue
                        await conn.execute(
                            "INSERT INTO student_skills (tenant_id, student_id, skill_code, acquired, "
                            "acquired_date, proficiency, notes, artifact_url, source_system, created_by, updated_by) "
                            "VALUES ($1::uuid,$2::uuid,$3,true,$4,$5,$6,$7,'parent_portal',$8::uuid,$8::uuid)",
                            tenant_id, student_id, code, d, prof, notes, art, user_id)
                        saved += 1
                    else:
                        if presumed:
                            await conn.execute(
                                "INSERT INTO student_skills (tenant_id, student_id, skill_code, acquired, "
                                "notes, source_system, created_by, updated_by) "
                                "VALUES ($1::uuid,$2::uuid,$3,false,$4,'parent_portal',$5::uuid,$5::uuid)",
                                tenant_id, student_id, code, notes, user_id)
                            overrides += 1
                        else:
                            cleared += 1  # non-presumed + not acquired = default; delete was enough
                else:
                    title = (it.custom_title or "").strip()
                    if not title:
                        continue
                    await conn.execute(
                        "INSERT INTO student_skills (tenant_id, student_id, skill_code, custom_title, "
                        "custom_domain, acquired, acquired_date, proficiency, notes, artifact_url, "
                        "source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,NULL,$3,$4,$5,$6,$7,$8,$9,'parent_portal',$10::uuid,$10::uuid)",
                        tenant_id, student_id, title, (it.custom_domain or None), bool(it.acquired),
                        d, prof, notes, art, user_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "presumption_overrides": overrides, "cleared": cleared}


# --------------------------- Identity documents -----------------------------
# Proof of age (birth_certificate | passport | government_id) and SS card.
# Soft requirement: the child record exists without documents, but
#   - age_verified is true only when an age-proof document is VERIFIED
#   - free access (age 10 and under) applies only when age is verified
# Documents upload through /media; this registers type + artifact + status.

_AGE_PROOF = {"birth_certificate", "passport", "government_id"}
_DOC_TYPES = _AGE_PROOF | {"ss_card"}


class IdentityDocItem(BaseModel):
    doc_type: str
    artifact_id: Optional[str] = None
    notes: Optional[str] = None


class IdentityDocsRequest(BaseModel):
    items: list[IdentityDocItem] = Field(default_factory=list)


@router.get("/student/{student_id}/identity-documents")
async def get_identity_documents(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT doc_type, artifact_id, status, verified_at, notes "
            "FROM student_identity_documents WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY created_at", student_id)
        age = await _pp_student_age(conn, student_id)
    docs = [{"doc_type": r["doc_type"],
             "artifact_id": str(r["artifact_id"]) if r["artifact_id"] else None,
             "status": r["status"],
             "verified_at": r["verified_at"].isoformat() if r["verified_at"] else None,
             "notes": r["notes"]} for r in rows]
    age_verified = any(d["doc_type"] in _AGE_PROOF and d["status"] == "verified" for d in docs)
    age_submitted = any(d["doc_type"] in _AGE_PROOF and d["status"] in ("submitted", "verified") for d in docs)
    ssn_documented = any(d["doc_type"] == "ss_card" and d["status"] in ("submitted", "verified") for d in docs)
    return {
        "student_id": student_id,
        "documents": docs,
        "student_age": age,
        "age_proof_submitted": age_submitted,
        "age_verified": age_verified,
        "ssn_documented": ssn_documented,
        "free_access_eligible": bool(age_verified and age is not None and age <= 10),
    }


@router.post("/student/{student_id}/identity-documents")
async def post_identity_documents(request: Request, student_id: str, body: IdentityDocsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = 0
    _verify_targets = []  # (row_id, artifact_id) for age-proof docs
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for it in body.items:
                dt = (it.doc_type or "").strip().lower()
                if dt not in _DOC_TYPES or not (it.artifact_id or "").strip():
                    continue
                # one live row per doc_type; re-upload replaces (and resets to submitted)
                await conn.execute(
                    "DELETE FROM student_identity_documents WHERE tenant_id=$1::uuid "
                    "AND student_id=$2::uuid AND doc_type=$3", tenant_id, student_id, dt)
                _row_id = await conn.fetchval(
                    "INSERT INTO student_identity_documents (tenant_id, student_id, doc_type, "
                    "artifact_id, status, notes, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4::uuid,'submitted',$5,'parent_portal',$6::uuid,$6::uuid) "
                    "RETURNING id",
                    tenant_id, student_id, dt, it.artifact_id.strip(), (it.notes or None), user_id)
                saved += 1
                if dt == "birth_certificate":
                    _verify_targets.append((str(_row_id), it.artifact_id.strip()))
    # v0.12.111: automated verification on upload - verified or rejected, no queue.
    result = {"student_id": student_id, "saved": saved}
    for _row_id, _art_id in _verify_targets:
        try:
            outcome = await _auto_verify_birth_certificate(pool, tenant_id, student_id, _row_id, _art_id)
            if outcome:
                result["verification"] = outcome
        except Exception as exc:
            log.warning("identity-doc auto-verify errored (non-fatal): %r", exc)
    return result


async def _auto_verify_birth_certificate(pool, tenant_id: str, student_id: str,
                                         row_id: str, artifact_id: str):
    """v0.12.111: fetch the uploaded artifact bytes (bytea or R2), run the same
    automated check as signup, and set verified/rejected. Returns a dict for
    the portal to display, or None when the check is unavailable (stays
    submitted; re-checked on next upload)."""
    from focms_cohort_signup import _ai_verify_birth_certificate, _bc_rejection_reasons, _send_email
    import focms_storage as _st
    async with _tenant_conn(pool, tenant_id) as conn:
        media = await conn.fetchrow(
            "SELECT mime_type, content, storage_kind, storage_uri, bucket FROM media_files "
            "WHERE id=$1::uuid AND tenant_id=$2::uuid AND deleted_at IS NULL",
            artifact_id, tenant_id)
        student = await conn.fetchrow(
            "SELECT first_name, last_name, birth_date FROM students WHERE id=$1::uuid", student_id)
        parent_email = await conn.fetchval(
            "SELECT primary_email FROM tenants WHERE id=$1::uuid", tenant_id)
    if not media or not student:
        return None
    doc_bytes = None
    if media["content"]:
        doc_bytes = bytes(media["content"])
    elif media["storage_kind"] == "r2" and media["storage_uri"]:
        try:
            import asyncio as _aio
            def _fetch():
                client = _st.get_r2_client()
                bucket = _st._resolve_bucket_name(media["bucket"] or "private")
                return client.get_object(Bucket=bucket, Key=media["storage_uri"])["Body"].read()
            doc_bytes = await _aio.to_thread(_fetch)
        except Exception as exc:
            log.warning("bc auto-verify: R2 fetch failed: %r", exc)
            return None
    if not doc_bytes:
        return None
    verdict = await _ai_verify_birth_certificate(
        doc_bytes, media["mime_type"] or "image/jpeg",
        student["first_name"], student["last_name"],
        student["birth_date"].isoformat() if student["birth_date"] else None)
    if not verdict:
        return None
    ok = bool(verdict.get("is_birth_certificate")) and bool(verdict.get("name_matches")) \
         and bool(verdict.get("birth_date_matches")) and bool(verdict.get("registrar_seal_visible")) \
         and not bool(verdict.get("tamper_signs")) \
         and (verdict.get("confidence") or "").lower() == "high"
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    _now = _dt.now(_tz.utc)
    async with _tenant_conn(pool, tenant_id) as conn:
        if ok:
            await conn.execute(
                "UPDATE student_identity_documents SET status='verified', verified_at=now(), "
                "notes=$2, updated_at=now() WHERE id=$1::uuid",
                row_id, "Automated document check passed: " + json.dumps(verdict)[:1500])
            await conn.execute(
                """INSERT INTO tenant_settings (tenant_id, feature_flags)
                   VALUES ($1::uuid, jsonb_build_object('age_verification', $2::jsonb))
                   ON CONFLICT (tenant_id) DO UPDATE SET
                   feature_flags = coalesce(tenant_settings.feature_flags,'{}'::jsonb)
                                   || jsonb_build_object('age_verification', $2::jsonb),
                   updated_at = now()""",
                tenant_id,
                json.dumps({"status": "verified", "method": "ai_birth_certificate",
                            "verified_at": _now.isoformat(),
                            "valid_until": (_now + _td(days=3653)).isoformat()}))
            return {"status": "verified"}
        reasons = _bc_rejection_reasons(verdict)
        await conn.execute(
            "UPDATE student_identity_documents SET status='rejected', "
            "notes=$2, updated_at=now() WHERE id=$1::uuid",
            row_id, "Automated document check failed: " + json.dumps(verdict)[:1500])
    if parent_email:
        try:
            await _send_email(
                parent_email, "outcomestar - birth certificate could not be verified",
                "<p>The automated review could not verify the birth certificate you uploaded:</p><ul>"
                + "".join(f"<li>{x}</li>" for x in reasons)
                + "</ul><p>Upload a clear, complete photo or scan of the official certified "
                  "birth certificate in Personal &rarr; Identity Documents - it is re-checked "
                  "automatically the moment you upload.</p>")
        except Exception as exc:
            log.warning("bc rejection email failed (non-fatal): %r", exc)
    return {"status": "rejected", "reasons": reasons}


# ======================================================================
# v0.12.15 - Meta-skills tracking + Major-gap report engine
# ----------------------------------------------------------------------
# Meta-skills (meta_skills_catalog) are practiced-over-time capabilities
# with a 0-100 proficiency that moves; unlike the 500-skill catalog they
# are never age-presumed (no typical_age_max). Captured in
# student_meta_skills (current_level / target_level) with a dated
# meta_skill_practice_log for the trajectory.
#
# Major-gap report: given a student and a CIP major, compares the
# student's skill inventory (acquired + age-presumed from the 500 catalog,
# plus meta-skill levels) against major_skill_requirements, and returns
# the differential weighted by importance. Cited to IPEDS/CIP.
# ======================================================================

_META_PROF_MIN, _META_PROF_MAX = 0, 100


def _clamp_level(v):
    """Coerce an incoming level to an int in 0..100, or None."""
    if v is None:
        return None
    try:
        n = int(round(float(v)))
    except (TypeError, ValueError):
        return None
    return max(_META_PROF_MIN, min(_META_PROF_MAX, n))


# ----------------------------- Meta-skills catalog -----------------------------

@router.get("/meta-skills-catalog")
async def get_meta_skills_catalog(request: Request):
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal":
        raise HTTPException(status_code=403, detail="internal_only")
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT code, title, framework, description, daily_practice, protocol, sort_order "
            "FROM meta_skills_catalog WHERE is_active ORDER BY sort_order")
    return {"meta_skills": [
        {"code": r["code"], "title": r["title"], "framework": r["framework"],
         "description": r["description"], "daily_practice": r["daily_practice"],
         "protocol": r["protocol"], "sort_order": r["sort_order"]} for r in rows]}


# ----------------------------- Student meta-skills -----------------------------

class MetaSkillItem(BaseModel):
    meta_skill_code: str
    current_level: Optional[int] = None
    target_level: Optional[int] = None
    notes: Optional[str] = None


class MetaSkillsRequest(BaseModel):
    items: list[MetaSkillItem] = Field(default_factory=list)


@router.get("/student/{student_id}/meta-skills")
async def get_student_meta_skills(request: Request, student_id: str):
    tenant_id, _ = await _pp_internal_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT meta_skill_code, current_level, target_level, notes, updated_at "
            "FROM student_meta_skills WHERE student_id=$1::uuid AND deleted_at IS NULL",
            student_id)
        # last practice date per meta-skill, for the trajectory hint
        practice = await conn.fetch(
            "SELECT meta_skill_code, max(practice_date) AS last_date, count(*) AS sessions "
            "FROM meta_skill_practice_log WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "GROUP BY meta_skill_code", student_id)
    pmap = {r["meta_skill_code"]: r for r in practice}
    out = []
    for r in rows:
        p = pmap.get(r["meta_skill_code"])
        out.append({
            "meta_skill_code": r["meta_skill_code"],
            "current_level": r["current_level"], "target_level": r["target_level"],
            "notes": r["notes"],
            "last_practice_date": p["last_date"].isoformat() if p and p["last_date"] else None,
            "practice_sessions": (p["sessions"] if p else 0),
        })
    return {"student_id": student_id, "meta_skills": out}


@router.post("/student/{student_id}/meta-skills")
async def post_student_meta_skills(request: Request, student_id: str, body: MetaSkillsRequest):
    tenant_id, user_id = await _pp_internal_context(request, student_id)
    saved = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        valid = {r["code"] for r in await conn.fetch(
            "SELECT code FROM meta_skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for it in body.items:
                code = (it.meta_skill_code or "").strip()
                if code not in valid:
                    continue
                cur = _clamp_level(it.current_level)
                tgt = _clamp_level(it.target_level)
                notes = (it.notes or "").strip() or None
                # upsert one live row per (student, meta_skill)
                await conn.execute(
                    "DELETE FROM student_meta_skills WHERE tenant_id=$1::uuid "
                    "AND student_id=$2::uuid AND meta_skill_code=$3", tenant_id, student_id, code)
                await conn.execute(
                    "INSERT INTO student_meta_skills (tenant_id, student_id, meta_skill_code, "
                    "current_level, target_level, notes, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,'parent_portal',$7::uuid,$7::uuid)",
                    tenant_id, student_id, code, cur, tgt, notes, user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved}


# ----------------------------- Practice log -----------------------------

class PracticeItem(BaseModel):
    meta_skill_code: str
    practice_date: str
    duration_minutes: Optional[int] = None
    practice_type: Optional[str] = None
    reflection: Optional[str] = None
    level_after: Optional[int] = None


class PracticeRequest(BaseModel):
    items: list[PracticeItem] = Field(default_factory=list)


@router.get("/student/{student_id}/meta-skills/practice")
async def get_meta_skill_practice(request: Request, student_id: str, meta_skill_code: Optional[str] = None):
    tenant_id, _ = await _pp_internal_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        if meta_skill_code:
            rows = await conn.fetch(
                "SELECT meta_skill_code, practice_date, duration_minutes, practice_type, "
                "reflection, level_after FROM meta_skill_practice_log "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL AND meta_skill_code=$2 "
                "ORDER BY practice_date DESC, created_at DESC", student_id, meta_skill_code.strip())
        else:
            rows = await conn.fetch(
                "SELECT meta_skill_code, practice_date, duration_minutes, practice_type, "
                "reflection, level_after FROM meta_skill_practice_log "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "ORDER BY practice_date DESC, created_at DESC LIMIT 500", student_id)
    return {"student_id": student_id, "sessions": [
        {"meta_skill_code": r["meta_skill_code"],
         "practice_date": r["practice_date"].isoformat() if r["practice_date"] else None,
         "duration_minutes": r["duration_minutes"], "practice_type": r["practice_type"],
         "reflection": r["reflection"], "level_after": r["level_after"]} for r in rows]}


@router.post("/student/{student_id}/meta-skills/practice")
async def post_meta_skill_practice(request: Request, student_id: str, body: PracticeRequest):
    """Append-only practice log. Each item is a dated session; optionally
    updates the meta-skill's current_level when level_after is supplied."""
    tenant_id, user_id = await _pp_internal_context(request, student_id)
    logged = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        valid = {r["code"] for r in await conn.fetch(
            "SELECT code FROM meta_skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for it in body.items:
                code = (it.meta_skill_code or "").strip()
                pdate = _pp_parse_date(it.practice_date)
                if code not in valid or pdate is None:
                    continue
                lvl = _clamp_level(it.level_after)
                dur = it.duration_minutes if isinstance(it.duration_minutes, int) else None
                await conn.execute(
                    "INSERT INTO meta_skill_practice_log (tenant_id, student_id, meta_skill_code, "
                    "practice_date, duration_minutes, practice_type, reflection, level_after, "
                    "source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,'parent_portal',$9::uuid,$9::uuid)",
                    tenant_id, student_id, code, pdate, dur,
                    ((it.practice_type or "").strip() or None),
                    ((it.reflection or "").strip() or None), lvl, user_id)
                logged += 1
                # if a level_after was recorded, advance the current_level snapshot
                if lvl is not None:
                    await conn.execute(
                        "UPDATE student_meta_skills SET current_level=$4, updated_at=now(), updated_by=$5::uuid "
                        "WHERE tenant_id=$1::uuid AND student_id=$2::uuid AND meta_skill_code=$3",
                        tenant_id, student_id, code, lvl, user_id)
    return {"student_id": student_id, "logged": logged}


# ----------------------------- CIP majors -----------------------------

@router.get("/cip-majors")
async def get_cip_majors(request: Request, q: Optional[str] = None):
    await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if q and q.strip():
            like = "%" + q.strip().lower() + "%"
            rows = await conn.fetch(
                "SELECT cip_code, title, cip_family, keywords FROM cip_majors "
                "WHERE is_active AND (lower(title) LIKE $1 OR lower(keywords) LIKE $1 OR cip_code LIKE $2) "
                "ORDER BY title", like, (q.strip() + "%"))
        else:
            rows = await conn.fetch(
                "SELECT cip_code, title, cip_family, keywords FROM cip_majors "
                "WHERE is_active ORDER BY title")
    return {"majors": [
        {"cip_code": r["cip_code"], "title": r["title"], "cip_family": r["cip_family"],
         "keywords": r["keywords"]} for r in rows]}


# ----------------------------- Major-gap report -----------------------------
# The skill-cluster -> CIP-major engine. Deterministic comparison first;
# every number traces to a stored value (skills_catalog age presumption,
# student_skills attestations, student_meta_skills levels,
# major_skill_requirements weights). Cited to IPEDS/CIP.

# A required meta-skill is considered "met" at/above this level unless the
# student set a personal target that is higher.
_META_TARGET_DEFAULT = 70

# Map requirement importance (1-5) to a coverage weight.
_IMP_WEIGHT = {1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0, 5: 5.0}


@router.get("/student/{student_id}/major-gap")
async def get_major_gap(request: Request, student_id: str, cip_code: str, audience: str = None):
    """Differential of a student's capability inventory against a major's
    required skill cluster. Returns per-skill status, weighted coverage,
    strengths, and gaps ranked by importance.

    v0.12.17: meta-skills are INTERNAL-ONLY. Parent-portal tokens (and any
    caller passing audience=parent) get a hard-skills-only report - meta
    requirements are excluded from items, coverage, and next actions."""
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal" and student_id not in (ctx.get("student_ids") or []):
        raise HTTPException(status_code=403, detail="student_not_authorized")
    tenant_id = str(ctx["tenant_id"])
    parent_view = (ctx.get("scope") == "parent_portal") or ((audience or "").strip().lower() == "parent")
    cip = (cip_code or "").strip()
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:

        major = await conn.fetchrow(
            "SELECT cip_code, title, cip_family FROM cip_majors WHERE cip_code=$1 AND is_active", cip)
        if not major:
            raise HTTPException(status_code=404, detail="cip_major_not_found")

        reqs = await conn.fetch(
            "SELECT skill_code, meta_skill_code, importance, rationale "
            "FROM major_skill_requirements WHERE cip_code=$1 AND is_active", cip)
        if parent_view:
            reqs = [r for r in reqs if r["skill_code"] is not None]
        if not reqs:
            raise HTTPException(status_code=404, detail="no_requirements_for_major")

        age = await _pp_student_age(conn, student_id)

        # student's hard-skill inventory: explicit rows + age presumption
        srows = await conn.fetch(
            "SELECT skill_code, acquired, proficiency FROM student_skills "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL AND skill_code IS NOT NULL", student_id)
        explicit = {r["skill_code"]: r for r in srows}
        amax = {r["code"]: (float(r["typical_age_max"]) if r["typical_age_max"] is not None else None)
                for r in await conn.fetch("SELECT code, typical_age_max FROM skills_catalog WHERE is_active")}
        titles = {r["code"]: r["title"]
                  for r in await conn.fetch("SELECT code, title FROM skills_catalog WHERE is_active")}

        # student's inferred meta-skill read (evidence-based; parents do not self-rate)
        await _ensure_inferences(conn, tenant_id, student_id, ctx.get("user_id") and str(ctx["user_id"]))
        mrows = await conn.fetch(
            "SELECT meta_skill_code, score, confidence FROM meta_skill_inferences "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
        mlevel = {r["meta_skill_code"]: r for r in mrows}
        mtitles = {r["code"]: (r["title"], r["framework"])
                   for r in await conn.fetch("SELECT code, title, framework FROM meta_skills_catalog WHERE is_active")}

    def hard_status(code):
        """(status, detail) for a required hard skill."""
        row = explicit.get(code)
        if row is not None:
            if row["acquired"]:
                return "have", (row["proficiency"] or "attested")
            return "gap", "marked not yet acquired"
        m = amax.get(code)
        if age is not None and m is not None and age >= m:
            return "presumed", "presumed by age"
        return "gap", "not yet acquired"

    def meta_status(code, imp):
        """Evidence-based: reads the inference engine's output (1-5 + confidence).
        No inference row means not enough life evidence yet - never a deficiency."""
        row = mlevel.get(code)
        if row is None:
            return "gap", None, None, "no evidence captured yet"
        score = row["score"]
        conf = row["confidence"]
        if score >= 4:
            return "have", score, conf, "evidenced (" + str(conf) + " confidence)"
        if score == 3:
            return "developing", score, conf, "emerging in the activity record"
        return "gap", score, conf, "early evidence only"

    have_w = total_w = 0.0
    strengths, gaps, developing = [], [], []
    hard_items, meta_items = [], []

    for r in reqs:
        imp = int(r["importance"] or 3)
        w = _IMP_WEIGHT.get(imp, 3.0)
        total_w += w
        if r["skill_code"]:
            status, detail = hard_status(r["skill_code"])
            item = {"kind": "skill", "code": r["skill_code"],
                    "title": titles.get(r["skill_code"], r["skill_code"]),
                    "importance": imp, "status": status, "detail": detail,
                    "rationale": r["rationale"]}
            hard_items.append(item)
            if status in ("have", "presumed"):
                have_w += w
                strengths.append(item)
            else:
                gaps.append(item)
        else:
            status, score, conf, detail = meta_status(r["meta_skill_code"], imp)
            t, fw = mtitles.get(r["meta_skill_code"], (r["meta_skill_code"], None))
            item = {"kind": "meta_skill", "code": r["meta_skill_code"], "title": t,
                    "framework": fw, "importance": imp, "status": status,
                    "score": score, "confidence": conf, "detail": detail,
                    "rationale": r["rationale"]}
            meta_items.append(item)
            if status == "have":
                have_w += w
                strengths.append(item)
            elif status == "developing":
                have_w += w * 0.5
                developing.append(item)
            else:
                gaps.append(item)

    coverage = round((have_w / total_w) * 100, 1) if total_w else 0.0
    gaps.sort(key=lambda x: -x["importance"])
    strengths.sort(key=lambda x: -x["importance"])

    # top next actions: the highest-importance gaps, framed as evidence to build
    next_actions = []
    for g in gaps[:5]:
        if g["kind"] == "meta_skill":
            if g.get("score") is not None:
                next_actions.append(
                    f"Grow the evidence for {g['title']} (early signals at {g['score']}/5). {g['rationale']}")
            else:
                next_actions.append(
                    f"Create opportunities that demonstrate {g['title']} - no life evidence captured yet. {g['rationale']}")
        else:
            next_actions.append(f"Develop: {g['title']} ({g['rationale']})")

    return {
        "student_id": student_id,
        "student_age": age,
        "basis": ("hard_skills_only" if parent_view else "full"),
        "major": {"cip_code": major["cip_code"], "title": major["title"],
                  "cip_family": major["cip_family"]},
        "coverage_pct": coverage,
        "counts": {
            "required_total": len(reqs),
            "have": len(strengths),
            "developing": len(developing),
            "gaps": len(gaps),
        },
        "strengths": strengths,
        "developing": developing,
        "gaps": gaps,
        "hard_skills": hard_items,
        "meta_skills": meta_items,
        "next_actions": next_actions,
        "citation": {
            "taxonomy": "U.S. Dept. of Education IPEDS / CIP 2020",
            "cip_code": major["cip_code"],
            "note": "Requirement weights are FOCMS curated; major identity and code follow the "
                    "federal CIP taxonomy. Skill presumption uses catalog typical-age bands.",
        },
    }


# ======================================================================
# v0.12.16 - Meta-skill INFERENCE ENGINE (evidence-based, not self-rated)
# ----------------------------------------------------------------------
# Principle: do not ask the family to name the child's meta-skills.
# Examine the life they have built - the activity record - and infer.
# Deterministic rules read events, personal_records, and logs; each
# finding carries a strength score (1-5), a confidence (low/medium/high),
# and cited evidence. Patterns across time, never single events.
# Positive evidence only: absence of a finding means "not enough
# evidence yet", never a deficiency.
#
# Parent slider endpoints from v0.12.15 remain for API compatibility but
# are DEPRECATED - nothing reads student_meta_skills any more. The
# major-gap report and the Capability Read both consume
# meta_skill_inferences written by this engine.
# ======================================================================

_CONF_RANK = {"low": 1, "medium": 2, "high": 3}


def _infer_add(findings, code, score, confidence, rule, evidence_line):
    """Merge a finding: keep the max score, max confidence, union evidence."""
    f = findings.get(code)
    if f is None:
        findings[code] = {"score": score, "confidence": confidence,
                          "rules": [rule], "evidence": [evidence_line]}
        return
    if score > f["score"]:
        f["score"] = score
    if _CONF_RANK.get(confidence, 0) > _CONF_RANK.get(f["confidence"], 0):
        f["confidence"] = confidence
    if rule not in f["rules"]:
        f["rules"].append(rule)
    if evidence_line not in f["evidence"]:
        f["evidence"].append(evidence_line)


async def _run_meta_inference(conn, tenant_id: str, student_id: str):
    """Compute the evidence-based meta-skill read from the activity record."""
    findings = {}

    # ---------- Signal A: activity events grouped by type ----------
    ev = await conn.fetch(
        "SELECT event_type, count(*) AS n, min(event_date) AS first, max(event_date) AS last, "
        "count(DISTINCT date_part('year', event_date)) AS years, "
        "array_agg(DISTINCT source_system) AS sources "
        "FROM events WHERE student_id=$1::uuid AND deleted_at IS NULL AND event_date IS NOT NULL "
        "GROUP BY event_type", student_id)
    by_type = {r["event_type"]: r for r in ev}

    def span_months(r):
        if not r or not r["first"] or not r["last"]:
            return 0
        return round((r["last"] - r["first"]).days / 30.44, 1)

    # competition-style activities (extendable as new activity types arrive)
    COMPETITION_TYPES = {"swim_race", "meet", "match", "tournament", "competition", "race"}
    PERFORMANCE_TYPES = {"music_performance", "recital", "concert", "theater_performance"}

    # ---------- Rule R1: sustained practice in one activity ----------
    for etype, r in by_type.items():
        n, months, years = r["n"], span_months(r), int(r["years"])
        src = ", ".join(s for s in (r["sources"] or []) if s)
        label = etype.replace("_", " ")
        if n >= 100 and months >= 24:
            ev_line = f"{n} logged {label} events over {months} months across {years} calendar years (sources: {src})"
            for code, sc in [("wd_consistency", 5), ("wd_discipline", 5), ("wd_sustained_effort", 5),
                             ("la_practice_discipline", 5), ("wd_habit_formation", 4),
                             ("es_persistence", 5), ("es_grit", 4)]:
                _infer_add(findings, code, sc, "high", "sustained_practice", ev_line)
            _infer_add(findings, "sm_long_term_growth_orientation", 4, "medium", "sustained_practice", ev_line)
        elif n >= 30 and months >= 12:
            ev_line = f"{n} logged {label} events over {months} months (sources: {src})"
            for code, sc in [("wd_consistency", 4), ("wd_discipline", 4), ("wd_sustained_effort", 4),
                             ("la_practice_discipline", 4), ("es_persistence", 4)]:
                _infer_add(findings, code, sc, "medium", "sustained_practice", ev_line)
        elif n >= 10 and months >= 6:
            ev_line = f"{n} logged {label} events over {months} months (sources: {src})"
            for code, sc in [("wd_consistency", 3), ("la_practice_discipline", 3)]:
                _infer_add(findings, code, sc, "medium", "sustained_practice", ev_line)

    # ---------- Rule R3: repeated voluntary competition ----------
    comp_n = sum(r["n"] for t, r in by_type.items() if t in COMPETITION_TYPES)
    comp_years = max((int(r["years"]) for t, r in by_type.items() if t in COMPETITION_TYPES), default=0)
    if comp_n >= 50 and comp_years >= 3:
        ev_line = (f"{comp_n} timed, officiated competition entries across {comp_years} calendar years - "
                   "repeatedly returning to judged competition and continuing to improve")
        for code, sc in [("es_calmness_under_pressure", 4), ("es_stress_tolerance", 4),
                         ("es_mental_toughness", 4), ("es_confidence", 3)]:
            _infer_add(findings, code, sc, "medium", "competition_exposure", ev_line)
        _infer_add(findings, "tj_judgment_under_pressure", 3, "medium", "competition_exposure", ev_line)
    elif comp_n >= 15:
        ev_line = f"{comp_n} competition entries logged"
        for code, sc in [("es_stress_tolerance", 3), ("es_confidence", 3)]:
            _infer_add(findings, code, sc, "medium", "competition_exposure", ev_line)

    # ---------- Signals from event titles/details (competition detail) ----------
    trows = await conn.fetch(
        "SELECT title, details->>'meet' AS meet FROM events "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL AND event_type::text = ANY($2::text[])",
        student_id, list(COMPETITION_TYPES))
    import re as _re
    disciplines = set()
    relay_n = 0
    champ_n = 0
    for t in trows:
        title = t["title"] or ""
        m = _re.match(r"^(\d+\s+[A-Za-z]+(?:\s+[A-Za-z]+)?)\s", title)
        if m:
            disciplines.add(m.group(1).lower())
        if "relay" in title.lower():
            relay_n += 1
        meet = (t["meet"] or "").lower()
        if "championship" in meet or "champs" in meet or "sectional" in meet or "state" in meet:
            champ_n += 1

    # ---------- Rule R4: high-stakes scheduled events ----------
    if champ_n >= 5:
        ev_line = f"{champ_n} races swum at championship-level meets - qualifying for and performing on the scheduled day"
        for code, sc in [("es_composure", 3), ("wd_preparation", 3)]:
            _infer_add(findings, code, sc, "medium", "high_stakes_events", ev_line)
        _infer_add(findings, "wd_reliability_under_deadlines", 3, "low", "high_stakes_events", ev_line)

    # ---------- Rule R5: team events ----------
    if relay_n >= 3:
        ev_line = f"{relay_n} relay entries - performing as one leg of a team where others depend on the result"
        for code, sc in [("rs_collaboration", 3), ("rs_reliability", 3)]:
            _infer_add(findings, code, sc, "medium", "team_events", ev_line)
        _infer_add(findings, "li_team_alignment", 2, "low", "team_events", ev_line)

    # ---------- Rule R6: versatility across disciplines ----------
    if len(disciplines) >= 8:
        ev_line = f"{len(disciplines)} distinct race disciplines (stroke/distance combinations) competed in"
        _infer_add(findings, "la_adaptability", 3, "medium", "versatility", ev_line)
        _infer_add(findings, "tj_mental_flexibility", 3, "low", "versatility", ev_line)
        _infer_add(findings, "la_skill_transfer", 3, "low", "versatility", ev_line)

    # ---------- Rule R2: measured improvement over time ----------
    pr = await conn.fetchrow(
        "SELECT count(*) AS bests, "
        "count(*) FILTER (WHERE total_drop_numeric IS NOT NULL AND total_drop_numeric > 0) AS drops, "
        "min(achieved_date) AS first, max(achieved_date) AS last "
        "FROM personal_records WHERE student_id=$1::uuid AND deleted_at IS NULL "
        "AND record_kind='swim_best'", student_id)
    if pr and pr["bests"]:
        drops = int(pr["drops"] or 0)
        pmonths = 0
        if pr["first"] and pr["last"]:
            pmonths = round((pr["last"] - pr["first"]).days / 30.44, 1)
        if drops >= 15 and pmonths >= 18:
            ev_line = (f"{drops} measured personal-best improvements over {pmonths} months "
                       f"({int(pr['bests'])} tracked bests) - objective, repeated time drops under coaching")
            for code, sc, cf in [("la_iterative_improvement", 5, "high"), ("la_coachability", 4, "high"),
                                 ("la_feedback_application", 4, "medium"), ("sm_self_correction", 4, "medium"),
                                 ("la_growth_mindset", 4, "medium"), ("la_learning_agility", 3, "medium")]:
                _infer_add(findings, code, sc, cf, "measured_improvement", ev_line)
        elif drops >= 5:
            ev_line = f"{drops} measured personal-best improvements ({int(pr['bests'])} tracked bests)"
            for code, sc in [("la_iterative_improvement", 4), ("la_coachability", 3), ("sm_self_correction", 3)]:
                _infer_add(findings, code, sc, "medium", "measured_improvement", ev_line)

    # ---------- Rule R8: public performance ----------
    perf_n = sum(r["n"] for t, r in by_type.items() if t in PERFORMANCE_TYPES)
    if perf_n >= 3:
        ev_line = f"{perf_n} public performances logged"
        for code, sc in [("cm_public_speaking_presence", 3), ("rs_social_confidence", 3), ("es_courage", 3)]:
            _infer_add(findings, code, sc, "medium", "public_performance", ev_line)
    elif perf_n >= 1:
        ev_line = f"{perf_n} public performance logged - early evidence"
        for code, sc in [("cm_public_speaking_presence", 2), ("rs_social_confidence", 2), ("es_courage", 2)]:
            _infer_add(findings, code, sc, "low", "public_performance", ev_line)

    # ---------- Rule R9: reflection practice ----------
    dl = await conn.fetchrow(
        "SELECT count(*) AS n FROM personal_records WHERE student_id=$1::uuid "
        "AND deleted_at IS NULL AND record_kind='daily_log'", student_id)
    if dl and int(dl["n"]) >= 10:
        ev_line = f"{int(dl['n'])} daily log entries - a maintained reflection habit"
        _infer_add(findings, "sm_self_reflection", 3, "medium", "reflection_practice", ev_line)
        _infer_add(findings, "sm_attention_to_personal_habits", 3, "low", "reflection_practice", ev_line)
        _infer_add(findings, "sm_self_awareness", 2, "low", "reflection_practice", ev_line)

    # ---------- Rule R7: cross-domain engagement ----------
    domains = set()
    for t in by_type:
        if t in COMPETITION_TYPES:
            domains.add("athletics")
        elif t in PERFORMANCE_TYPES:
            domains.add("performing arts")
        elif t == "summer_experience":
            domains.add("exploration")
        else:
            domains.add(t)
    mr = await conn.fetchrow(
        "SELECT count(*) AS n FROM personal_records WHERE student_id=$1::uuid "
        "AND deleted_at IS NULL AND record_kind='music_repertoire'", student_id)
    if mr and int(mr["n"]) >= 1:
        domains.add("performing arts")
    if dl and int(dl["n"]) >= 10:
        domains.add("reflection")
    if len(domains) >= 3:
        ev_line = "active across " + str(len(domains)) + " distinct life domains: " + ", ".join(sorted(domains))
        _infer_add(findings, "la_curiosity", 3, "medium", "cross_domain", ev_line)
        _infer_add(findings, "ci_curiosity_driven_exploration", 3, "low", "cross_domain", ev_line)
        _infer_add(findings, "sm_life_balance", 3, "low", "cross_domain", ev_line)

    # ---------- Rule R10: work experience -> meta-skills ----------
    jobs = await conn.fetch(
        "SELECT job_title, job_description, duties, skills_gained, is_paid, "
        "start_date, end_date, is_current FROM job_experiences "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    if jobs:
        n_jobs = len(jobs)
        # Any job at all signals responsibility/work ethic.
        titles = ", ".join([j["job_title"] for j in jobs if j["job_title"]][:4])
        ev0 = f"{n_jobs} work experience record(s)" + (f": {titles}" if titles else "")
        _infer_add(findings, "wd_responsibility", 3, "medium", "work_experience", ev0)
        _infer_add(findings, "wd_work_ethic", 3, "medium", "work_experience", ev0)
        _infer_add(findings, "rs_reliability", 3, "low", "work_experience", ev0)
        if n_jobs >= 2:
            _infer_add(findings, "wd_sustained_effort", 3, "low", "work_experience", ev0)

        # Keyword map: token -> list of (meta_skill_code, score, confidence)
        KW = {
            "lead": [("rs_leadership", 4, "medium"), ("rs_delegation", 3, "low")],
            "manage": [("rs_leadership", 4, "medium"), ("wd_organization", 3, "low")],
            "supervis": [("rs_leadership", 4, "medium")],
            "train": [("cm_teaching_others", 3, "medium"), ("rs_leadership", 3, "low")],
            "mentor": [("cm_teaching_others", 3, "medium")],
            "custom": [("rs_customer_orientation", 4, "medium"), ("cm_interpersonal_communication", 3, "low")],
            "client": [("rs_customer_orientation", 4, "medium")],
            "serv": [("rs_customer_orientation", 3, "low")],
            "cash": [("wd_accountability", 3, "medium"), ("wd_attention_to_detail", 3, "low")],
            "sale": [("cm_persuasion", 3, "medium"), ("rs_customer_orientation", 3, "low")],
            "team": [("rs_collaboration", 4, "medium")],
            "cook": [("wd_attention_to_detail", 3, "low"), ("es_stress_tolerance", 3, "low")],
            "tutor": [("cm_teaching_others", 4, "medium"), ("la_knowledge_sharing", 3, "low")],
            "coach": [("rs_leadership", 3, "medium"), ("cm_teaching_others", 3, "low")],
            "volunteer": [("ci_service_orientation", 4, "medium")],
            "intern": [("ci_professional_curiosity", 3, "low"), ("la_curiosity", 3, "low")],
            "research": [("la_analytical_thinking", 3, "medium"), ("ci_curiosity_driven_exploration", 3, "low")],
            "code": [("la_analytical_thinking", 3, "medium"), ("sm_self_direction", 3, "low")],
            "design": [("ci_creativity", 3, "medium")],
            "write": [("cm_written_communication", 3, "medium")],
            "organiz": [("wd_organization", 3, "medium")],
            "schedul": [("wd_time_management", 3, "medium")],
            "deadline": [("wd_time_management", 3, "medium"), ("es_stress_tolerance", 3, "low")],
            "budget": [("la_analytical_thinking", 3, "low"), ("wd_accountability", 3, "low")],
            "safe": [("wd_conscientiousness", 3, "low")],
            "detail": [("wd_attention_to_detail", 4, "medium")],
        }
        for j in jobs:
            blob = " ".join(filter(None, [
                (j["job_title"] or "").lower(),
                (j["job_description"] or "").lower(),
                (j["duties"] or "").lower(),
                " ".join(j["skills_gained"] or []).lower(),
            ]))
            if not blob.strip():
                continue
            label = (j["job_title"] or "job").strip()
            for token, metas in KW.items():
                if token in blob:
                    ev = f"work as '{label}' (matched '{token}' in role/description/skills)"
                    for code, sc, cf in metas:
                        _infer_add(findings, code, sc, cf, "work_experience", ev)

    # ---------- Rule R11: coursework (incl. private tutoring) -> meta-skills ----------
    # Reads courses_taken subject / course name / description / parent-entered
    # skills. Realizes "meta-skills based on subject, course, and skills entered
    # by the parent" as EVIDENCE-BASED inference. Meta-skills remain INTERNAL-ONLY;
    # parents never enter or see them (decision of record 2026-07-02).
    # Every meta-skill code below is verified against meta_skills_catalog.
    crows = await conn.fetch(
        "SELECT course_name, subject, course_description, details, "
        "admission_traits_developed, is_ap, is_ib, is_dual_credit, is_honors "
        "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    if crows:
        n_courses = len(crows)
        n_tutoring = 0
        _infer_add(findings, "la_self_directed_learning", 3, "medium", "coursework",
                   f"{n_courses} course record(s) logged")
        n_rigor = sum(1 for c in crows if c["is_ap"] or c["is_ib"] or c["is_dual_credit"])
        if n_rigor >= 1:
            _infer_add(findings, "tj_critical_thinking", 4, "medium", "coursework",
                       f"{n_rigor} advanced course(s) (AP/IB/dual-credit)")
            _infer_add(findings, "wd_discipline", 4, "medium", "coursework",
                       f"{n_rigor} advanced course(s)")
        # SCED 2-digit subject area -> meta-skills
        SUBJ_META = {
            "02": [("tj_critical_thinking", 4, "medium"), ("tj_pattern_recognition", 3, "low")],
            "03": [("ci_experiment_design", 4, "medium"), ("tj_evidence_evaluation", 3, "low")],
            "01": [("cm_clear_writing", 3, "medium"), ("tj_critical_thinking", 3, "low")],
            "04": [("tj_big_picture_thinking", 3, "low"), ("tj_critical_thinking", 3, "low")],
            "05": [("rs_cultural_awareness", 3, "medium"), ("la_skill_transfer", 3, "low")],
            "06": [("ci_creative_thinking", 3, "medium")],
            "10": [("tj_systems_thinking", 3, "medium"), ("la_self_directed_learning", 3, "low")],
            "21": [("ci_design_thinking", 3, "low")],
            "22": [("tj_evidence_evaluation", 3, "low")],
        }
        CKW = {
            "code": [("tj_systems_thinking", 3, "medium"), ("la_self_directed_learning", 3, "low")],
            "program": [("tj_systems_thinking", 3, "medium")],
            "robot": [("ci_design_thinking", 3, "medium"), ("tj_systems_thinking", 3, "low")],
            "debate": [("cm_persuasive_communication", 4, "medium"), ("cm_public_speaking_presence", 3, "low")],
            "writ": [("cm_clear_writing", 3, "medium")],
            "research": [("la_research_ability", 3, "medium"), ("ci_curiosity_driven_exploration", 3, "low")],
            "calculus": [("tj_critical_thinking", 4, "medium")],
            "algebra": [("tj_critical_thinking", 3, "medium")],
            "chemistry": [("ci_experiment_design", 3, "medium")],
            "physics": [("ci_experiment_design", 4, "medium"), ("tj_critical_thinking", 3, "low")],
            "biolog": [("tj_evidence_evaluation", 3, "medium")],
            "econ": [("tj_critical_thinking", 3, "low")],
            "music": [("ci_creative_thinking", 3, "medium")],
            "art": [("ci_creative_thinking", 3, "medium")],
            "language": [("rs_cultural_awareness", 3, "medium")],
            "leadership": [("li_leadership_presence", 3, "medium")],
        }
        for c in crows:
            det = c["details"]
            if isinstance(det, str):
                try:
                    det = json.loads(det)
                except Exception:
                    det = {}
            det = det or {}
            cname = (c["course_name"] or "course").strip()
            if det.get("is_private_tutoring") or "tutor" in cname.lower():
                n_tutoring += 1
            subj = (c["subject"] or "").strip()
            if subj in SUBJ_META:
                for code, sc, cf in SUBJ_META[subj]:
                    _infer_add(findings, code, sc, cf, "coursework_subject",
                               f"coursework in subject {subj}: {cname}")
            blob = " ".join(filter(None, [
                cname.lower(),
                (c["course_description"] or "").lower(),
                " ".join(_pp_skills(c["admission_traits_developed"])).lower(),
            ]))
            if blob.strip():
                for token, metas in CKW.items():
                    if token in blob:
                        for code, sc, cf in metas:
                            _infer_add(findings, code, sc, cf, "coursework_keyword",
                                       f"course '{cname}' (matched '{token}')")
        # Private tutoring = actively sought additional instruction.
        if n_tutoring >= 1:
            _infer_add(findings, "la_self_directed_learning", 4, "medium", "private_tutoring",
                       f"{n_tutoring} private tutoring engagement(s) - sought additional instruction")
            _infer_add(findings, "la_growth_mindset", 4, "medium", "private_tutoring",
                       f"{n_tutoring} private tutoring engagement(s)")
            _infer_add(findings, "wd_task_completion", 3, "low", "private_tutoring",
                       f"{n_tutoring} private tutoring engagement(s)")

    return findings


# v0.12.19: shared engine writer + lazy auto-run
async def _write_inferences(conn, tenant_id: str, student_id: str, user_id):
    """Replace stored inferences from a fresh engine run. Returns count written."""
    valid = {r["code"] for r in await conn.fetch("SELECT code FROM meta_skills_catalog WHERE is_active")}
    findings = await _run_meta_inference(conn, tenant_id, student_id)
    async with conn.transaction():
        await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
        await conn.execute(
            "DELETE FROM meta_skill_inferences WHERE tenant_id=$1::uuid AND student_id=$2::uuid",
            tenant_id, student_id)
        written = 0
        for code, f in findings.items():
            if code not in valid:
                continue
            await conn.execute(
                "INSERT INTO meta_skill_inferences (tenant_id, student_id, meta_skill_code, "
                "score, confidence, evidence, rule_code, engine_version, created_by, updated_by) "
                "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6::jsonb,$7,'v1',$8::uuid,$8::uuid)",
                tenant_id, student_id, code, int(f["score"]), f["confidence"],
                json.dumps(f["evidence"]), ",".join(f["rules"]), user_id)
            written += 1
    return written


async def _ensure_inferences(conn, tenant_id: str, student_id: str, user_id):
    """Lazy internal compute: if no inference rows exist for the student,
    run the engine now. Internal tracking only - never surfaced to parents."""
    n = await conn.fetchval(
        "SELECT count(*) FROM meta_skill_inferences WHERE student_id=$1::uuid AND deleted_at IS NULL",
        student_id)
    if not n:
        await _write_inferences(conn, tenant_id, student_id, user_id)


@router.post("/student/{student_id}/meta-skills/infer")
async def run_meta_skill_inference(request: Request, student_id: str):
    """Run the inference engine over the student's activity record and
    replace the stored capability read."""
    tenant_id, user_id = await _pp_internal_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        written = await _write_inferences(conn, tenant_id, student_id, user_id)
    return {"student_id": student_id, "inferred": written}


@router.get("/student/{student_id}/meta-skills/inferred")
async def get_inferred_meta_skills(request: Request, student_id: str):
    """The capability read: inferred meta-skills grouped by category, with
    evidence. Skills without findings are listed as 'awaiting evidence'."""
    tenant_id, _ = await _pp_internal_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        cat = await conn.fetch(
            "SELECT code, title, framework, sort_order FROM meta_skills_catalog "
            "WHERE is_active ORDER BY sort_order")
        inf = await conn.fetch(
            "SELECT meta_skill_code, score, confidence, evidence, rule_code, computed_at "
            "FROM meta_skill_inferences WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    imap = {r["meta_skill_code"]: r for r in inf}
    out = []
    computed_at = None
    for c in cat:
        r = imap.get(c["code"])
        if r and (computed_at is None or r["computed_at"] > computed_at):
            computed_at = r["computed_at"]
        out.append({
            "code": c["code"], "title": c["title"], "category": c["framework"],
            "score": (r["score"] if r else None),
            "confidence": (r["confidence"] if r else None),
            "evidence": (json.loads(r["evidence"]) if r and isinstance(r["evidence"], str)
                         else (r["evidence"] if r else [])),
            "rules": (r["rule_code"].split(",") if r and r["rule_code"] else []),
        })
    return {"student_id": student_id,
            "computed_at": computed_at.isoformat() if computed_at else None,
            "found": len(inf), "total": len(cat), "skills": out}


# ======================================================================
# v0.12.18 - Success Predictor Score (SPS)
# ----------------------------------------------------------------------
# SPS = (w1*A) + (w2*E) + (w3*S) + (w4*M) + meta-alignment boost (<=5%)
#   A Academic Foundation  - latest percentile per assessment subject
#   E Engagement & Grit    - activity volume, span, and breadth
#   S Skills               - hard-skill coverage vs the major (gap engine)
#   M Context & Milestones - milestones, repertoire, championship record
# Weights are per-major (cip_majors.weight_*), family-calibrated.
# Meta boost reads meta_skill_inferences (internal engine). Parent
# audiences receive the boost folded into the score with no meta-skill
# names or itemization (meta-skills are internal-only).
# Buckets with no data are excluded and weights renormalized - the
# score never punishes a family for data not yet captured.
# ======================================================================

async def _sps_buckets(conn, student_id: str):
    """Compute A/E/M vectors (0-1) with cited evidence. S comes from the gap engine."""
    out = {}

    # A - Academic Foundation: latest percentile per subject, averaged
    arows = await conn.fetch(
        "SELECT DISTINCT ON (subject) subject, percentile, test_date FROM assessments "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL AND percentile IS NOT NULL "
        "AND subject IS NOT NULL ORDER BY subject, test_date DESC", student_id)
    if arows:
        vals = [float(r["percentile"]) / 100.0 for r in arows]
        out["academics"] = {
            "score": round(sum(vals) / len(vals), 3),
            "evidence": ["Latest percentile per subject: " + ", ".join(
                f"{r['subject']} P{int(r['percentile'])} ({r['test_date']})" for r in arows)]}

    # E - Engagement & Grit: volume x span x breadth of the activity record
    ev = await conn.fetchrow(
        "SELECT count(*) AS n, min(event_date) AS first, max(event_date) AS last, "
        "count(DISTINCT event_type) AS types FROM events "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL AND event_date IS NOT NULL", student_id)
    if ev and int(ev["n"] or 0) > 0:
        n = int(ev["n"])
        months = round((ev["last"] - ev["first"]).days / 30.44, 1) if ev["first"] and ev["last"] else 0
        breadth = int(ev["types"])
        e = min(1.0, 0.5 * min(n / 150.0, 1.0) + 0.3 * min(months / 36.0, 1.0) + 0.2 * min(breadth / 4.0, 1.0))
        out["engagement"] = {
            "score": round(e, 3),
            "evidence": [f"{n} logged activity events over {months} months across {breadth} activity types"]}

    # M - Context & Milestones: milestones + repertoire + championship record
    mrow = await conn.fetchrow(
        "SELECT (SELECT count(*) FROM student_life_milestones WHERE student_id=$1::uuid AND deleted_at IS NULL) AS miles, "
        "(SELECT count(*) FROM personal_records WHERE student_id=$1::uuid AND deleted_at IS NULL AND record_kind='music_repertoire') AS rep, "
        "(SELECT count(*) FROM events WHERE student_id=$1::uuid AND deleted_at IS NULL "
        " AND (lower(coalesce(details->>'meet','')) LIKE '%championship%' OR lower(coalesce(details->>'meet','')) LIKE '%champs%')) AS champ",
        student_id)
    miles, rep, champ = int(mrow["miles"] or 0), int(mrow["rep"] or 0), int(mrow["champ"] or 0)
    if miles + rep + champ > 0:
        m = min(1.0, (miles * 2.0 + rep + min(champ, 10) * 0.3) / 10.0)
        out["milestones"] = {
            "score": round(m, 3),
            "evidence": [f"{miles} recorded life milestones, {rep} performance repertoire pieces, "
                         f"{champ} championship-level competition entries"]}
    return out


@router.get("/student/{student_id}/major-sps")
async def get_major_sps(request: Request, student_id: str, cip_code: str, audience: str = None):
    """Success Predictor Score for a student against a major."""
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal" and student_id not in (ctx.get("student_ids") or []):
        raise HTTPException(status_code=403, detail="student_not_authorized")
    tenant_id = str(ctx["tenant_id"])
    parent_view = (ctx.get("scope") == "parent_portal") or ((audience or "").strip().lower() == "parent")
    cip = (cip_code or "").strip()
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        major = await conn.fetchrow(
            "SELECT cip_code, title, weight_academics, weight_engagement, weight_skills, weight_milestones "
            "FROM cip_majors WHERE cip_code=$1 AND is_active", cip)
        if not major:
            raise HTTPException(status_code=404, detail="cip_major_not_found")

        buckets = await _sps_buckets(conn, student_id)

        # S - hard-skill coverage vs this major, AGE-AWARE (v0.12.22).
        # Denominator = requirements answerable now: typical_age_min at or
        # below the student's age, plus anything explicitly evidenced (skills
        # earned ahead of age count fully). A young student is never penalized
        # for skills that are not age-appropriate yet - the denominator grows
        # as they age. Trajectory over point-in-time.
        reqs = await conn.fetch(
            "SELECT skill_code, importance FROM major_skill_requirements "
            "WHERE cip_code=$1 AND is_active AND skill_code IS NOT NULL", cip)
        if reqs:
            age = await _pp_student_age(conn, student_id)
            srows = await conn.fetch(
                "SELECT skill_code, acquired FROM student_skills "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL AND skill_code IS NOT NULL", student_id)
            explicit = {r["skill_code"]: r["acquired"] for r in srows}
            bands = {r["code"]: (
                        (float(r["typical_age_min"]) if r["typical_age_min"] is not None else None),
                        (float(r["typical_age_max"]) if r["typical_age_max"] is not None else None))
                     for r in await conn.fetch(
                        "SELECT code, typical_age_min, typical_age_max FROM skills_catalog WHERE is_active")}
            have_w = due_w = 0.0
            eligible = 0
            for r in reqs:
                acq = explicit.get(r["skill_code"])
                amin, amax_v = bands.get(r["skill_code"], (None, None))
                presumed = (acq is None and age is not None and amax_v is not None and age > amax_v)
                age_due = (age is None or amin is None or amin <= age)
                if acq is not None or presumed or age_due:
                    eligible += 1
                    due_w += r["importance"]
                    if acq is True or presumed:
                        have_w += r["importance"]
            if eligible:
                later = len(reqs) - eligible
                ev = ("Coverage of " + str(eligible) + " age-appropriate requirements for " + major["title"]
                      + ((" (" + str(later) + " more unlock with age)") if later else ""))
                buckets["skills"] = {"score": round(have_w / due_w, 3), "evidence": [ev]}

        # meta-alignment boost: share of the major's meta requirements evidenced at 4+
        boost = 0.0
        matched_codes = []
        await _ensure_inferences(conn, tenant_id, student_id, ctx.get("user_id") and str(ctx["user_id"]))
        mreqs = await conn.fetch(
            "SELECT meta_skill_code FROM major_skill_requirements "
            "WHERE cip_code=$1 AND is_active AND meta_skill_code IS NOT NULL", cip)
        if mreqs:
            inf = await conn.fetch(
                "SELECT meta_skill_code, score FROM meta_skill_inferences "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
            imap = {r["meta_skill_code"]: r["score"] for r in inf}
            need = [r["meta_skill_code"] for r in mreqs]
            matched_codes = [c for c in need if imap.get(c, 0) >= 4]
            boost = (len(matched_codes) / len(need)) * 0.05

    # weighted score over buckets that have data; weights renormalized
    wmap = {"academics": float(major["weight_academics"] or 0.40),
            "engagement": float(major["weight_engagement"] or 0.20),
            "skills": float(major["weight_skills"] or 0.30),
            "milestones": float(major["weight_milestones"] or 0.10)}
    active_w = sum(w for k, w in wmap.items() if k in buckets)
    base = 0.0
    comp = []
    for k, w in wmap.items():
        b = buckets.get(k)
        wn = round(w / active_w, 3) if active_w else 0.0
        comp.append({"bucket": k, "weight": w, "weight_normalized": (wn if b else None),
                     "score": (b["score"] if b else None),
                     "evidence": (b["evidence"] if b else ["No data captured yet - excluded from the score"])})
        if b and active_w:
            base += (w / active_w) * b["score"]
    sps = round(min((base + boost) * 100.0, 100.0), 1)

    resp = {"student_id": student_id, "major": {"cip_code": major["cip_code"], "title": major["title"]},
            "sps": sps, "base_pct": round(base * 100.0, 1),
            "alignment_bonus_pct": round(boost * 100.0, 1),
            "basis": ("parent" if parent_view else "full"),
            "components": comp,
            "note": "Buckets without data are excluded and weights renormalized; the score reflects captured evidence only."}
    if not parent_view:
        resp["alignment_matched_meta_skills"] = matched_codes
    return resp


# ======================================================================
# v0.12.23 - Extra Curricular pillar (affiliations)
# ----------------------------------------------------------------------
# Parent captures programs, activities, service organizations, and coach
# relationships. Feeds SPS engagement bucket + inference engine (breadth,
# leadership, sustained involvement).
# ======================================================================

_AFFIL_TYPES = {"program", "activity", "service_org", "coach_relationship"}


class AffiliationItem(BaseModel):
    id: Optional[str] = None
    skills_gained: List[str] = []
    show_on_showcase: Optional[bool] = None
    program_code: Optional[str] = None  # v0.12.98: catalog link (details->>'program_code')
    affiliation_type: str
    organization_name: str
    organization_url: Optional[str] = None
    organization_city: Optional[str] = None
    organization_state: Optional[str] = None
    role: Optional[str] = None
    role_start_date: Optional[str] = None
    role_end_date: Optional[str] = None
    weekly_hours: Optional[float] = None
    total_hours: Optional[float] = None
    coach_name: Optional[str] = None
    coach_email: Optional[str] = None
    coach_role: Optional[str] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None


class AffiliationsRequest(BaseModel):
    items: List[AffiliationItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/affiliations")
async def get_student_affiliations(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT a.id, a.affiliation_type::text AS affiliation_type, a.organization_name, "
            "a.details->>'program_code' AS program_code, "
            "a.organization_url, a.organization_city, a.organization_state, a.role, "
            "a.role_start_date, a.role_end_date, a.weekly_hours, a.total_hours, "
            "a.coach_name, a.coach_email, a.coach_role, a.notes, a.public_description, "
            "a.is_verified, a.source_system, (a.visibility='public') AS show_on_showcase, "
            "COALESCE((SELECT array_agg(coalesce(ss.skill_code, ss.custom_title)) "
            " FROM student_skills ss WHERE ss.source_activity = 'affiliations:'||a.id::text "
            " AND ss.deleted_at IS NULL), ARRAY[]::text[]) AS skills_gained "
            "FROM affiliations a WHERE a.student_id=$1::uuid AND a.deleted_at IS NULL "
            "ORDER BY a.affiliation_type, coalesce(a.role_start_date, a.created_at::date) DESC",
            student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["id"] = str(d["id"])
        for k in ("role_start_date", "role_end_date"):
            d[k] = d[k].isoformat() if d[k] else None
        for k in ("weekly_hours", "total_hours"):
            d[k] = float(d[k]) if d[k] is not None else None
        out.append(d)
    return {"student_id": student_id, "affiliations": out}


@router.post("/student/{student_id}/affiliations")
async def post_student_affiliations(request: Request, student_id: str, body: AffiliationsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try:
                    _ = _uuid.UUID(did)
                except Exception:
                    continue
                r = await conn.execute(
                    "UPDATE affiliations SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"):
                    deleted += 1
            for it in body.items or []:
                atype = (it.affiliation_type or "").strip()
                if atype not in _AFFIL_TYPES:
                    continue
                name = (it.organization_name or "").strip()
                if not name:
                    continue
                if it.id:
                    try:
                        _ = _uuid.UUID(it.id)
                    except Exception:
                        continue
                    r = await conn.execute(
                        "UPDATE affiliations SET affiliation_type=$3::affiliation_type_enum, "
                        "organization_name=$4, organization_url=$5, organization_city=$6, "
                        "organization_state=$7, role=$8, role_start_date=NULLIF($9,'')::date, "
                        "role_end_date=NULLIF($10,'')::date, weekly_hours=$11, total_hours=$12, "
                        "coach_name=$13, coach_email=$14, coach_role=$15, notes=$16, "
                        "public_description=$17, "
                        "details = CASE WHEN $19::text IS NULL THEN details "
                        "ELSE details || jsonb_build_object('program_code', $19::text) END, "
                        "updated_at=now(), updated_by=$18::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, atype, name, it.organization_url,
                        it.organization_city, it.organization_state, it.role,
                        it.role_start_date, it.role_end_date, it.weekly_hours,
                        it.total_hours, it.coach_name, it.coach_email, it.coach_role,
                        it.notes, it.public_description, user_id, it.program_code)
                    if r and r.endswith(" 1"):
                        await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                            "affiliations", it.id, it.skills_gained, it.show_on_showcase)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO affiliations (tenant_id, student_id, affiliation_type, "
                        "organization_name, organization_url, organization_city, organization_state, "
                        "role, role_start_date, role_end_date, weekly_hours, total_hours, "
                        "coach_name, coach_email, coach_role, notes, public_description, "
                        "details, visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3::affiliation_type_enum,$4,$5,$6,$7,$8,"
                        "NULLIF($9,'')::date,NULLIF($10,'')::date,$11,$12,$13,$14,$15,$16,$17,"
                        "CASE WHEN $19::text IS NULL THEN '{}'::jsonb "
                        "ELSE jsonb_build_object('program_code', $19::text) END,"
                        "'private','parent_portal',"
                        "$18::uuid,$18::uuid) RETURNING id",
                        tenant_id, student_id, atype, name, it.organization_url,
                        it.organization_city, it.organization_state, it.role,
                        it.role_start_date, it.role_end_date, it.weekly_hours,
                        it.total_hours, it.coach_name, it.coach_email, it.coach_role,
                        it.notes, it.public_description, user_id, it.program_code)
                    await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                        "affiliations", rid, it.skills_gained, it.show_on_showcase)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}




# v0.12.116: course skills -> student_skills inventory. courses_taken has no
# visibility_locked column, so this is a trimmed writer without the showcase
# toggle. Makes EVERY course (academic or private tutoring) contribute to the
# skill inventory and the meta-skill inference engine, exactly like
# affiliations / awards / ec-sessions already do.
async def _course_skills_to_inventory(conn, tenant_id, student_id, user_id,
                                      record_id, skills_gained):
    if not skills_gained:
        return
    valid = {r["code"] for r in await conn.fetch("SELECT code FROM skills_catalog WHERE is_active")}
    src = f"courses_taken:{record_id}"
    for entry in skills_gained:
        code = (entry or "").strip()
        if not code:
            continue
        if code in valid:
            existing = await conn.fetchval(
                "SELECT id FROM student_skills WHERE student_id=$1::uuid AND skill_code=$2 "
                "AND deleted_at IS NULL LIMIT 1", student_id, code)
            if existing:
                await conn.execute(
                    "UPDATE student_skills SET acquired=true, source_activity=$3, "
                    "updated_at=now(), updated_by=$4::uuid WHERE id=$1::uuid AND student_id=$2::uuid",
                    existing, student_id, src, user_id)
            else:
                await conn.execute(
                    "INSERT INTO student_skills (tenant_id, student_id, skill_code, acquired, "
                    "acquired_date, source_activity, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,true,now()::date,$4,'parent_portal',$5::uuid,$5::uuid)",
                    tenant_id, student_id, code, src, user_id)
        else:
            await conn.execute(
                "INSERT INTO student_skills (tenant_id, student_id, custom_title, custom_domain, "
                "acquired, acquired_date, source_activity, source_system, created_by, updated_by) "
                "VALUES ($1::uuid,$2::uuid,$3,'custom',true,now()::date,$4,'parent_portal',$5::uuid,$5::uuid)",
                tenant_id, student_id, code, src, user_id)


# v0.12.25: universal activity helpers
async def _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                                     table, record_id, skills_gained, show_on_showcase):
    if show_on_showcase is True:
        await conn.execute(
            f"UPDATE {table} SET visibility='public' "
            f"WHERE id=$1::uuid AND student_id=$2::uuid AND visibility_locked=false",
            record_id, student_id)
    elif show_on_showcase is False:
        await conn.execute(
            f"UPDATE {table} SET visibility='private' "
            f"WHERE id=$1::uuid AND student_id=$2::uuid AND visibility_locked=false",
            record_id, student_id)
    if not skills_gained:
        return
    valid = {r["code"] for r in await conn.fetch("SELECT code FROM skills_catalog WHERE is_active")}
    src = f"{table}:{record_id}"
    for entry in skills_gained:
        code = (entry or "").strip()
        if not code:
            continue
        if code in valid:
            existing = await conn.fetchval(
                "SELECT id FROM student_skills WHERE student_id=$1::uuid AND skill_code=$2 "
                "AND deleted_at IS NULL LIMIT 1", student_id, code)
            if existing:
                await conn.execute(
                    "UPDATE student_skills SET acquired=true, source_activity=$3, "
                    "updated_at=now(), updated_by=$4::uuid WHERE id=$1::uuid AND student_id=$2::uuid",
                    existing, student_id, src, user_id)
            else:
                await conn.execute(
                    "INSERT INTO student_skills (tenant_id, student_id, skill_code, acquired, "
                    "acquired_date, source_activity, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,true,now()::date,$4,'parent_portal',$5::uuid,$5::uuid)",
                    tenant_id, student_id, code, src, user_id)
        else:
            await conn.execute(
                "INSERT INTO student_skills (tenant_id, student_id, custom_title, custom_domain, "
                "acquired, acquired_date, source_activity, source_system, created_by, updated_by) "
                "VALUES ($1::uuid,$2::uuid,$3,'custom',true,now()::date,$4,'parent_portal',$5::uuid,$5::uuid)",
                tenant_id, student_id, code, src, user_id)

# ======================================================================
# v0.12.24 - Extracurricular expansion
# picker: programs catalog; sub-domains: awards, sessions, milestones
# ======================================================================


@router.get("/catalogs/affiliation-programs")
async def get_affiliation_programs(request: Request):
    _ = await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT code, title, category, capstone_award FROM affiliation_programs_catalog "
            "WHERE is_active ORDER BY sort_order, title")
    return {"programs": [dict(r) for r in rows]}


@router.get("/catalogs/named-awards")
async def get_named_awards(request: Request):
    _ = await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, title, category, granting_organization "
            "FROM named_awards_catalog WHERE is_active ORDER BY category, title")
    return {"named_awards": [dict(r) for r in rows]}


@router.get("/catalogs/ec-milestones")
async def get_ec_milestones(request: Request):
    _ = await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT code, title, sub_pillar, category, typical_age_min, typical_age_max "
            "FROM life_milestones_catalog WHERE is_active AND pillar='Extra Curricular' "
            "ORDER BY sub_pillar, sort_order, title")
    return {"milestones": [dict(r) for r in rows]}


class AwardItem(BaseModel):
    id: Optional[str] = None
    skills_gained: List[str] = []
    show_on_showcase: Optional[bool] = None
    award_name: str
    granting_organization: Optional[str] = None
    awarded_date: Optional[str] = None
    level: Optional[str] = None
    category: Optional[str] = None
    rank_or_placement: Optional[str] = None
    competing_pool_size: Optional[int] = None
    monetary_value_usd: Optional[float] = None
    named_award_catalog_id: Optional[str] = None
    related_affiliation_id: Optional[str] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None


class AwardsRequest(BaseModel):
    items: List[AwardItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/awards")
async def get_student_awards(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT a.id::text AS id, a.award_name, a.granting_organization, a.awarded_date, a.level, "
            "a.category, a.rank_or_placement, a.competing_pool_size, a.monetary_value_usd, "
            "a.named_award_catalog_id::text AS named_award_catalog_id, "
            "a.related_affiliation_id::text AS related_affiliation_id, "
            "a.notes, a.public_description, a.source_system, (a.visibility='public') AS show_on_showcase, "
            "COALESCE((SELECT array_agg(coalesce(ss.skill_code, ss.custom_title)) "
            " FROM student_skills ss WHERE ss.source_activity = 'awards_honors:'||a.id::text "
            " AND ss.deleted_at IS NULL), ARRAY[]::text[]) AS skills_gained "
            "FROM awards_honors a WHERE a.student_id=$1::uuid AND a.deleted_at IS NULL "
            "ORDER BY a.awarded_date DESC NULLS LAST, a.created_at DESC", student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["awarded_date"] = d["awarded_date"].isoformat() if d["awarded_date"] else None
        d["monetary_value_usd"] = float(d["monetary_value_usd"]) if d["monetary_value_usd"] is not None else None
        out.append(d)
    return {"student_id": student_id, "awards": out}


@router.post("/student/{student_id}/awards")
async def post_student_awards(request: Request, student_id: str, body: AwardsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE awards_honors SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                name = (it.award_name or "").strip()
                if not name: continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE awards_honors SET award_name=$3, granting_organization=$4, "
                        "awarded_date=$5::date, level=$6, category=$7, rank_or_placement=$8, "
                        "competing_pool_size=$9, monetary_value_usd=$10, "
                        "named_award_catalog_id=NULLIF($11,'')::uuid, "
                        "related_affiliation_id=NULLIF($12,'')::uuid, notes=$13, "
                        "public_description=$14, updated_at=now(), updated_by=$15::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, name, it.granting_organization, it.awarded_date,
                        it.level, it.category, it.rank_or_placement, it.competing_pool_size,
                        it.monetary_value_usd, it.named_award_catalog_id or '',
                        it.related_affiliation_id or '', it.notes, it.public_description, user_id)
                    if r and r.endswith(" 1"):
                        await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                            "awards_honors", it.id, it.skills_gained, it.show_on_showcase)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO awards_honors (tenant_id, student_id, award_name, "
                        "granting_organization, awarded_date, level, category, rank_or_placement, "
                        "competing_pool_size, monetary_value_usd, "
                        "named_award_catalog_id, related_affiliation_id, notes, "
                        "public_description, visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5::date,$6,$7,$8,$9,$10,"
                        "NULLIF($11,'')::uuid,NULLIF($12,'')::uuid,$13,$14,'private',"
                        "'parent_portal',$15::uuid,$15::uuid) RETURNING id",
                        tenant_id, student_id, name, it.granting_organization, it.awarded_date,
                        it.level, it.category, it.rank_or_placement, it.competing_pool_size,
                        it.monetary_value_usd, it.named_award_catalog_id or '',
                        it.related_affiliation_id or '', it.notes, it.public_description, user_id)
                    await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                        "awards_honors", rid, it.skills_gained, it.show_on_showcase)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


_EC_EVENT_TYPES = {"service_session", "summer_experience", "leadership_milestone",
                   "competition", "stem_event", "music_performance"}


class EcSessionItem(BaseModel):
    id: Optional[str] = None
    skills_gained: List[str] = []
    show_on_showcase: Optional[bool] = None
    event_type: str
    title: str
    event_date: str
    duration_hours: Optional[float] = None
    location: Optional[str] = None
    related_affiliation_id: Optional[str] = None
    notes: Optional[str] = None
    instrument: Optional[str] = None      # v0.12.99: music_performance detail
    music_played: Optional[str] = None    # v0.12.99: piece(s) performed
    composer: Optional[str] = None        # v0.12.99: composer(s)
    milestone_kind: Optional[str] = None  # v0.12.100: rank | merit_badge | award | training


class EcSessionsRequest(BaseModel):
    items: List[EcSessionItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/ec-sessions")
async def get_ec_sessions(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT e.id::text AS id, e.event_type::text AS event_type, e.title, e.event_date, "
            "e.duration_minutes, e.location_name AS location, e.affiliation_id::text AS related_affiliation_id, "
            "e.details->>'instrument' AS instrument, e.details->>'music_played' AS music_played, "
            "e.details->>'composer' AS composer, e.details->>'milestone_kind' AS milestone_kind, "
            "e.notes, e.source_system, (e.visibility='public') AS show_on_showcase, "
            "COALESCE((SELECT array_agg(coalesce(ss.skill_code, ss.custom_title)) "
            " FROM student_skills ss WHERE ss.source_activity = 'events:'||e.id::text "
            " AND ss.deleted_at IS NULL), ARRAY[]::text[]) AS skills_gained "
            "FROM events e WHERE e.student_id=$1::uuid AND e.deleted_at IS NULL "
            "AND e.event_type::text = ANY($2::text[]) "
            "ORDER BY e.event_date DESC NULLS LAST",
            student_id, list(_EC_EVENT_TYPES))
    out = []
    for r in rows:
        d = dict(r)
        d["event_date"] = d["event_date"].isoformat() if d["event_date"] else None
        d["duration_hours"] = round(d["duration_minutes"]/60.0, 2) if d["duration_minutes"] is not None else None
        d.pop("duration_minutes", None)
        out.append(d)
    return {"student_id": student_id, "sessions": out}


@router.post("/student/{student_id}/ec-sessions")
async def post_ec_sessions(request: Request, student_id: str, body: EcSessionsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE events SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                etype = (it.event_type or "").strip()
                if etype not in _EC_EVENT_TYPES: continue
                title = (it.title or "").strip()
                edate = (it.event_date or "").strip()
                if not title or not edate: continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE events SET event_type=$3::event_type_enum, title=$4, "
                        "event_date=NULLIF($5,'')::date, duration_minutes=$6, location_name=$7, "
                        "affiliation_id=NULLIF($8,'')::uuid, notes=$9, "
                        "details = details || jsonb_strip_nulls(jsonb_build_object("
                        "'instrument', $11::text, 'music_played', $12::text, 'composer', $13::text, "
                        "'milestone_kind', $14::text)), "
                        "updated_at=now(), updated_by=$10::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, etype, title, edate,
                        int(it.duration_hours*60) if it.duration_hours is not None else None,
                        it.location, it.related_affiliation_id or '', it.notes, user_id,
                        it.instrument, it.music_played, it.composer, it.milestone_kind)
                    if r and r.endswith(" 1"):
                        await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                            "events", it.id, it.skills_gained, it.show_on_showcase)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO events (tenant_id, student_id, event_type, title, "
                        "event_date, duration_minutes, location_name, affiliation_id, notes, "
                        "details, visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3::event_type_enum,$4,NULLIF($5,'')::date,$6,$7,"
                        "NULLIF($8,'')::uuid,$9,"
                        "jsonb_strip_nulls(jsonb_build_object("
                        "'instrument', $11::text, 'music_played', $12::text, 'composer', $13::text, "
                        "'milestone_kind', $14::text)),"
                        "'private','parent_portal',$10::uuid,$10::uuid) RETURNING id",
                        tenant_id, student_id, etype, title, edate,
                        int(it.duration_hours*60) if it.duration_hours is not None else None,
                        it.location, it.related_affiliation_id or '', it.notes, user_id,
                        it.instrument, it.music_played, it.composer, it.milestone_kind)
                    await _apply_skills_and_showcase(conn, tenant_id, student_id, user_id,
                        "events", rid, it.skills_gained, it.show_on_showcase)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.26 - Higher Education pillar: Target Universities
# ======================================================================


@router.get("/catalogs/universities")
async def get_universities_catalog(request: Request, q: Optional[str] = None, limit: int = 50):
    _ = await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if q:
            rows = await conn.fetch(
                "SELECT leaid, name, common_name, city, state, us_news_rank, "
                "admit_rate, has_rotc, has_d1_swim, is_service_academy, common_app_member "
                "FROM universities WHERE name ILIKE $1 OR common_name ILIKE $1 "
                "ORDER BY us_news_rank NULLS LAST, name LIMIT $2",
                f"%{q}%", limit)
        else:
            rows = await conn.fetch(
                "SELECT leaid, name, common_name, city, state, us_news_rank, "
                "admit_rate, has_rotc, has_d1_swim, is_service_academy, common_app_member "
                "FROM universities "
                "ORDER BY us_news_rank NULLS LAST, name LIMIT $1", limit)
    out = []
    for r in rows:
        d = dict(r)
        d["admit_rate"] = float(d["admit_rate"]) if d["admit_rate"] is not None else None
        out.append(d)
    return {"universities": out}


class TargetSchoolItem(BaseModel):
    id: Optional[str] = None
    university_leaid: str
    priority: Optional[int] = None
    pathways_pursuing: List[str] = []
    fit_category: Optional[str] = None
    interest_level: Optional[int] = None
    program_of_interest: Optional[str] = None
    why_interested: Optional[str] = None
    advantages: Optional[str] = None
    blockers: Optional[str] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class TargetsRequest(BaseModel):
    items: List[TargetSchoolItem] = []
    delete_ids: List[str] = []


_FIT_CATS = {"reach", "target", "likely", "safety"}
_PATHWAYS = {"service_academy", "rotc", "academic_merit", "athletic", "regular"}


@router.get("/student/{student_id}/target-schools")
async def get_target_schools(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT t.id::text AS id, t.university_leaid, t.priority, t.pathways_pursuing, "
            "t.fit_category, t.interest_level, t.program_of_interest, t.why_interested, "
            "t.advantages, t.blockers, t.notes, t.public_description, "
            "(t.visibility='public') AS show_on_showcase, "
            "u.name AS university_name, u.common_name, u.city AS university_city, "
            "u.state AS university_state, u.us_news_rank, u.admit_rate, "
            "u.has_rotc, u.has_d1_swim, u.is_service_academy, u.common_app_member "
            "FROM target_universities t "
            "LEFT JOIN universities u ON u.leaid = t.university_leaid "
            "WHERE t.student_id=$1::uuid AND t.deleted_at IS NULL AND t.is_active "
            "ORDER BY t.priority NULLS LAST, u.us_news_rank NULLS LAST",
            student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["admit_rate"] = float(d["admit_rate"]) if d["admit_rate"] is not None else None
        d["pathways_pursuing"] = list(d["pathways_pursuing"] or [])
        out.append(d)
    return {"student_id": student_id, "targets": out}


@router.post("/student/{student_id}/target-schools")
async def post_target_schools(request: Request, student_id: str, body: TargetsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE target_universities SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                leaid = (it.university_leaid or "").strip()
                if not leaid: continue
                fit = it.fit_category if it.fit_category in _FIT_CATS else None
                pathways = [p for p in (it.pathways_pursuing or []) if p in _PATHWAYS]
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE target_universities SET university_leaid=$3, priority=$4, "
                        "pathways_pursuing=$5, fit_category=$6, interest_level=$7, "
                        "program_of_interest=$8, why_interested=$9, advantages=$10, "
                        "blockers=$11, notes=$12, public_description=$13, "
                        "updated_at=now(), updated_by=$14::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, leaid, it.priority, pathways, fit,
                        it.interest_level, it.program_of_interest, it.why_interested,
                        it.advantages, it.blockers, it.notes, it.public_description, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute(
                                "UPDATE target_universities SET visibility='public' "
                                "WHERE id=$1::uuid AND student_id=$2::uuid AND visibility_locked=false",
                                it.id, student_id)
                        elif it.show_on_showcase is False:
                            await conn.execute(
                                "UPDATE target_universities SET visibility='private' "
                                "WHERE id=$1::uuid AND student_id=$2::uuid AND visibility_locked=false",
                                it.id, student_id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO target_universities (tenant_id, student_id, university_leaid, "
                        "priority, pathways_pursuing, fit_category, interest_level, "
                        "program_of_interest, why_interested, advantages, blockers, notes, "
                        "public_description, is_active, visibility, source_system, "
                        "created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,"
                        "true,'private','parent_portal',$14::uuid,$14::uuid) RETURNING id",
                        tenant_id, student_id, leaid, it.priority, pathways, fit,
                        it.interest_level, it.program_of_interest, it.why_interested,
                        it.advantages, it.blockers, it.notes, it.public_description, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute(
                            "UPDATE target_universities SET visibility='public' "
                            "WHERE id=$1::uuid AND visibility_locked=false", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.27 - Higher Education: Applications & Deadlines
# ======================================================================

_DECISION_PLANS = {"ed1", "ed2", "ea", "rea", "rd", "rd2", "rolling", "priority"}
_APP_PLATFORMS  = {"common_app", "coalition", "uca", "institutional", "questbridge", "scoir", "other"}


class ApplicationItem(BaseModel):
    id: Optional[str] = None
    university_leaid: str
    target_university_id: Optional[str] = None
    application_year: Optional[int] = None
    term_starting: Optional[str] = None   # stored in details.term_starting
    possible_major_cip: Optional[str] = None
    possible_career: Optional[str] = None
    decision_plan: Optional[str] = None
    application_platform: Optional[str] = None
    pathway_track: Optional[str] = None
    status: Optional[str] = None
    deadline: Optional[str] = None
    decision_release_date: Optional[str] = None
    submitted_at: Optional[str] = None
    portal_url: Optional[str] = None       # stored in details.portal_url
    portal_username: Optional[str] = None  # stored in details.portal_username
    fee_paid_usd: Optional[float] = None
    fee_waiver_used: Optional[bool] = None
    ed_signature_date: Optional[str] = None  # details.ed_signature_date
    notes: Optional[str] = None
    public_description: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class ApplicationsRequest(BaseModel):
    items: List[ApplicationItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/applications")
async def get_student_applications(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT a.id::text AS id, a.university_leaid, "
            "a.target_university_id::text AS target_university_id, "
            "a.application_year, a.decision_plan, a.application_platform, "
            "a.status::text AS status, a.pathway_track::text AS pathway_track, "
            "a.deadline, a.decision_release_date, a.submitted_at, "
            "a.fee_paid_usd, a.fee_waiver_used, a.details, "
            "a.notes, a.public_description, "
            "(a.visibility='public') AS show_on_showcase, "
            "u.name AS university_name, u.common_name, u.city AS university_city, "
            "u.state AS university_state, u.us_news_rank "
            "FROM applications a "
            "LEFT JOIN universities u ON u.leaid = a.university_leaid "
            "WHERE a.student_id=$1::uuid AND a.deleted_at IS NULL "
            "ORDER BY a.deadline NULLS LAST, u.us_news_rank NULLS LAST",
            student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["fee_paid_usd"] = float(d["fee_paid_usd"]) if d["fee_paid_usd"] is not None else None
        for k in ("deadline", "decision_release_date", "submitted_at"):
            d[k] = d[k].isoformat() if d[k] else None
        import json as _json
        det = d.pop("details", None) or {}
        if isinstance(det, str):
            try: det = _json.loads(det)
            except Exception: det = {}
        if not isinstance(det, dict): det = {}
        d["term_starting"] = det.get("term_starting")
        d["possible_major_cip"] = det.get("possible_major_cip")
        d["possible_career"] = det.get("possible_career")
        d["portal_url"] = det.get("portal_url")
        d["portal_username"] = det.get("portal_username")
        d["ed_signature_date"] = det.get("ed_signature_date")
        out.append(d)
    return {"student_id": student_id, "applications": out}


@router.post("/student/{student_id}/applications")
async def post_student_applications(request: Request, student_id: str, body: ApplicationsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE applications SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                leaid = (it.university_leaid or "").strip()
                if not leaid: continue
                plan = it.decision_plan if (it.decision_plan or "").strip() in _DECISION_PLANS else None
                plat = it.application_platform if (it.application_platform or "").strip() in _APP_PLATFORMS else None
                details = {}
                for k in ("term_starting", "possible_major_cip", "possible_career",
                          "portal_url", "portal_username", "ed_signature_date"):
                    v = getattr(it, k, None)
                    if v: details[k] = v
                import json as _json
                det_json = _json.dumps(details or {})
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE applications SET university_leaid=$3, "
                        "application_year=$4, decision_plan=$5, application_platform=$6, "
                        "pathway_track=NULLIF($7,'')::pathway_enum, deadline=NULLIF($8::text,'')::date, "
                        "decision_release_date=NULLIF($9::text,'')::date, submitted_at=NULLIF($10::text,'')::date, "
                        "fee_paid_usd=$11, fee_waiver_used=$12, "
                        "details = COALESCE(details, '{}'::jsonb) || COALESCE($13::jsonb, '{}'::jsonb), "
                        "notes=$14, public_description=$15, "
                        "updated_at=now(), updated_by=$16::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, leaid, (it.application_year or __import__('datetime').date.today().year), plan, plat,
                        it.pathway_track, it.deadline, it.decision_release_date,
                        it.submitted_at, it.fee_paid_usd, it.fee_waiver_used,
                        det_json, it.notes, it.public_description, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute(
                                "UPDATE applications SET visibility='public' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute(
                                "UPDATE applications SET visibility='private' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO applications (tenant_id, student_id, university_leaid, "
                        "application_year, decision_plan, application_platform, "
                        "pathway_track, deadline, decision_release_date, submitted_at, "
                        "fee_paid_usd, fee_waiver_used, details, notes, public_description, "
                        "status, visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,"
                        "NULLIF($7,'')::pathway_enum,NULLIF($8::text,'')::date,NULLIF($9::text,'')::date,NULLIF($10::text,'')::date,"
                        "$11,$12,$13::jsonb,$14,$15,'planned','private','parent_portal',"
                        "$16::uuid,$16::uuid) RETURNING id",
                        tenant_id, student_id, leaid, (it.application_year or __import__('datetime').date.today().year), plan, plat,
                        it.pathway_track, it.deadline, it.decision_release_date,
                        it.submitted_at, it.fee_paid_usd, it.fee_waiver_used,
                        det_json, it.notes, it.public_description, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute(
                            "UPDATE applications SET visibility='public' "
                            "WHERE id=$1::uuid AND visibility_locked=false", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


@router.get("/catalogs/cip-majors")
async def get_cip_majors_catalog(request: Request, q: Optional[str] = None):
    _ = await _resolve_context(request)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if q:
            rows = await conn.fetch(
                "SELECT cip_code, title FROM cip_majors "
                "WHERE is_active AND title ILIKE $1 ORDER BY title LIMIT 100",
                f"%{q}%")
        else:
            rows = await conn.fetch(
                "SELECT cip_code, title FROM cip_majors WHERE is_active ORDER BY title")
    return {"majors": [dict(r) for r in rows]}


# ======================================================================
# v0.12.28 - Academics grade-band scoping
# ======================================================================
# Bands: preschool (0-K), elementary (1-5), middle (6-8), high (9-12)
# Derived from grade_level integer stored on each row.

BAND_RANGES = {
    "preschool":  (0, 0),
    "elementary": (1, 5),
    "middle":     (6, 8),
    "high":       (9, 12),
}


def _band_bounds(band: Optional[str]) -> Optional[tuple[int, int]]:
    return BAND_RANGES.get((band or "").lower())


@router.get("/student/{student_id}/academics-summary")
async def get_academics_summary(request: Request, student_id: str):
    """Counts by grade band. Cheap dashboard query."""
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        crows = await conn.fetch(
            "SELECT grade_level, count(*) n FROM courses_taken "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "GROUP BY grade_level", student_id)
        arows = await conn.fetch(
            "SELECT grade_at_test, count(*) n FROM assessments "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "GROUP BY grade_at_test", student_id)
    bands = {b: {"courses": 0, "assessments": 0} for b in BAND_RANGES}
    def _coerce(v):
        if v is None: return None
        if isinstance(v, int): return v
        s = str(v).strip().lower()
        if not s: return None
        if s in ("pk","prek","pre-k","preschool","p"): return 0
        if s in ("k","kg","kindergarten"): return 0
        try: return int(s)
        except Exception:
            import re as _re
            m = _re.search(r"\d+", s)
            return int(m.group(0)) if m else None
    for r in crows:
        g = _coerce(r["grade_level"])
        if g is None: continue
        for b, (lo, hi) in BAND_RANGES.items():
            if lo <= g <= hi: bands[b]["courses"] += r["n"]; break
    for r in arows:
        g = _coerce(r["grade_at_test"])
        if g is None: continue
        for b, (lo, hi) in BAND_RANGES.items():
            if lo <= g <= hi: bands[b]["assessments"] += r["n"]; break
    return {"student_id": student_id, "bands": bands}


@router.get("/student/{student_id}/courses")
async def get_student_courses(request: Request, student_id: str,
                              band: Optional[str] = None):
    tenant_id, _ = await _pp_context(request, student_id)
    bounds = _band_bounds(band) if band else None
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        if bounds:
            rows = await conn.fetch(
                "SELECT id::text AS id, course_name, school_name, course_type, "
                "subject, grade_level, school_year, term, credit_hours, "
                "grade_received, grade_points_4_0, grade_points_weighted, "
                "is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, "
                "teacher_name, notes, "
                "(visibility='public') AS show_on_showcase "
                "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "AND grade_level BETWEEN $2 AND $3 "
                "ORDER BY grade_level, school_year, term, course_name",
                student_id, bounds[0], bounds[1])
        else:
            rows = await conn.fetch(
                "SELECT id::text AS id, course_name, school_name, course_type, "
                "subject, grade_level, school_year, term, credit_hours, "
                "grade_received, grade_points_4_0, grade_points_weighted, "
                "is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, "
                "teacher_name, notes, "
                "(visibility='public') AS show_on_showcase "
                "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "ORDER BY grade_level, school_year, term, course_name", student_id)
    out = []
    for r in rows:
        d = dict(r)
        for k in ("credit_hours", "grade_points_4_0", "grade_points_weighted"):
            d[k] = float(d[k]) if d[k] is not None else None
        out.append(d)
    return {"student_id": student_id, "band": band, "courses": out}


class CourseItemLegacy(BaseModel):
    id: Optional[str] = None
    course_name: str
    school_name: Optional[str] = None
    course_type: Optional[str] = None
    subject: Optional[str] = None
    grade_level: int
    school_year: Optional[str] = None
    term: Optional[str] = None
    credit_hours: Optional[float] = None
    grade_received: Optional[str] = None
    grade_points_4_0: Optional[float] = None
    grade_points_weighted: Optional[float] = None
    is_honors: Optional[bool] = None
    is_ap: Optional[bool] = None
    is_ib: Optional[bool] = None
    is_dual_credit: Optional[bool] = None
    ap_exam_score: Optional[int] = None
    teacher_name: Optional[str] = None
    notes: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class CoursesRequestLegacy(BaseModel):
    items: List[CourseItemLegacy] = []
    delete_ids: List[str] = []


@router.post("/student/{student_id}/courses")
async def post_student_courses_legacy(request: Request, student_id: str, body: CoursesRequestLegacy):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE courses_taken SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                name = (it.course_name or "").strip()
                if not name: continue
                if it.grade_level is None or it.grade_level < 0 or it.grade_level > 13:
                    continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE courses_taken SET course_name=$3, school_name=$4, "
                        "course_type=$5, subject=$6, grade_level=$7, school_year=$8, "
                        "term=$9, credit_hours=$10, grade_received=$11, "
                        "grade_points_4_0=$12, grade_points_weighted=$13, "
                        "is_honors=$14, is_ap=$15, is_ib=$16, is_dual_credit=$17, "
                        "ap_exam_score=$18, teacher_name=$19, notes=$20, "
                        "updated_at=now(), updated_by=$21::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, name, it.school_name, it.course_type,
                        it.subject, it.grade_level, it.school_year, it.term,
                        it.credit_hours, it.grade_received, it.grade_points_4_0,
                        it.grade_points_weighted, it.is_honors, it.is_ap, it.is_ib,
                        it.is_dual_credit, it.ap_exam_score, it.teacher_name,
                        it.notes, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute(
                                "UPDATE courses_taken SET visibility='public' "
                                "WHERE id=$1::uuid", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute(
                                "UPDATE courses_taken SET visibility='private' "
                                "WHERE id=$1::uuid", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO courses_taken (tenant_id, student_id, course_name, "
                        "school_name, course_type, subject, grade_level, school_year, "
                        "term, credit_hours, grade_received, grade_points_4_0, "
                        "grade_points_weighted, is_honors, is_ap, is_ib, is_dual_credit, "
                        "ap_exam_score, teacher_name, notes, visibility, source_system, "
                        "created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,"
                        "$14,$15,$16,$17,$18,$19,$20,'private','parent_portal',"
                        "$21::uuid,$21::uuid) RETURNING id",
                        tenant_id, student_id, name, it.school_name, it.course_type,
                        it.subject, it.grade_level, it.school_year, it.term,
                        it.credit_hours, it.grade_received, it.grade_points_4_0,
                        it.grade_points_weighted, it.is_honors, it.is_ap, it.is_ib,
                        it.is_dual_credit, it.ap_exam_score, it.teacher_name,
                        it.notes, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute(
                            "UPDATE courses_taken SET visibility='public' "
                            "WHERE id=$1::uuid", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.29 - Current school context helper
# ======================================================================


@router.get("/student/{student_id}/current-school")
async def get_current_school(request: Request, student_id: str):
    """Best-effort current-school context for prefilling course/school fields."""
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT school_name, school_ceeb_code, street_address, city_town, "
            "state_province, zip_postal_code, formatted_address, "
            "counselor_phone, counselor_phone_e164, "
            "grade_levels_attended, is_current_school "
            "FROM student_school_enrollments "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY is_current_school DESC NULLS LAST, "
            "coalesce(end_date, current_date + 3650) DESC "
            "LIMIT 1", student_id)
    if not row:
        return {"student_id": student_id, "school": None}
    d = dict(row)
    d["grade_levels_attended"] = list(d["grade_levels_attended"] or [])
    return {"student_id": student_id, "school": d}


# ======================================================================
# v0.12.30 - School profile (School Report data) + Report Cards
# ======================================================================


class SchoolProfileItem(BaseModel):
    id: Optional[str] = None
    school_name: str
    school_leaid: Optional[str] = None
    district_leaid: Optional[str] = None           # v0.12.122 (NCES district)
    district_name: Optional[str] = None            # v0.12.122
    student_school_id: Optional[str] = None        # v0.12.122 district-assigned student ID
    state_student_id: Optional[str] = None         # v0.12.125 state ID (TX TSDS/STAAR). INTERNAL - never shown.
    school_ceeb_code: Optional[str] = None
    ceeb_code: Optional[str] = None
    school_type: Optional[str] = None
    street_address: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    country: Optional[str] = None
    counselor_name: Optional[str] = None
    counselor_position: Optional[str] = None
    counselor_phone: Optional[str] = None
    counselor_email: Optional[str] = None
    counselor_fax: Optional[str] = None
    is_current_school: Optional[bool] = False
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    grade_levels_attended: List[str] = []
    grading_scale: Optional[str] = None
    max_grade_offered: Optional[str] = None
    schedule_type: Optional[str] = None
    courses_available_flags: Optional[dict] = None
    courses_available_notes: Optional[str] = None
    graduating_class_size: Optional[int] = None
    boarding_students: Optional[int] = None
    curriculum_notes: Optional[str] = None
    notes: Optional[str] = None


class SchoolProfilesRequest(BaseModel):
    items: List[SchoolProfileItem] = []
    delete_ids: List[str] = []


@router.post("/student/{student_id}/report-card-parse")
async def report_card_parse(request: Request, student_id: str, body: dict):
    """v0.12.123: paste a report card (text copied from the school portal, or an
    email) and get back structured rows to review. NOTHING is saved here - the
    parent sees the parsed rows in the form and presses Save themselves."""
    await _pp_context(request, student_id)
    text = (body or {}).get("text") or ""
    text = str(text).strip()[:20000]
    if len(text) < 20:
        raise HTTPException(400, {"error": "empty", "message": "Paste the report card text first."})
    system = (
        "You extract report-card data from text a parent pasted from a school portal, "
        "or from a district report card. Respond ONLY with JSON - no prose, no code fences. "
        "Schema: "
        '{"school_year": str|null, "term": str|null, "grade_level": int|null, '
        '"school_name": str|null, "counselor": str|null, '
        '"student_school_id": str|null, "state_student_id": str|null, '
        '"gpa_unweighted": number|null, "gpa_weighted": number|null, '
        '"days_present": int|null, "days_absent": int|null, "days_tardy": int|null, '
        '"teacher_comments": str|null, '
        '"subjects": [{"subject": str, "course_code": str|null, "period": str|null, '
        '"teacher": str|null, "q1": str|null, "q2": str|null, "sem1": str|null, '
        '"q3": str|null, "q4": str|null, "sem2": str|null, "fin": str|null, "grade": str|null}]}. '
        "Column mapping: many district cards print a table with columns "
        "Pd | Course | Sect | Description | Teacher | Q1 | Q2 | SEM1 | Q3 | Q4 | SEM2 | FIN | ABS | TDY. "
        "Map Pd->period, Course->course_code, Description->subject (the readable course name), "
        "Teacher->teacher, and each grading column to its matching field. "
        "If a card shows all four quarters and a FIN, set term to 'Full year'. "
        "If it shows only Q1/Q2/SEM1, set term to 'Semester 1'; only Q3/Q4/SEM2 -> 'Semester 2'. "
        "Otherwise term is one of: Quarter 1..4, Trimester 1..3, Summer, Summer I, Summer II, else null. "
        "Keep grades exactly as written (numeric like 94, or letter like A-). "
        "student_school_id is the district's ID for the student (often in parentheses after the name); "
        "state_student_id is a separate 'Unique State ID' if present. "
        "Never invent a course, grade, teacher, or ID that is not in the text."
    )
    res = await _llm_complete(system, "REPORT CARD TEXT:\n" + text, max_tokens=2200, want_json=True)
    if res.get("unavailable"):
        raise HTTPException(503, {"error": "llm_unavailable",
                                  "message": "The reader is unavailable right now - enter the grades manually."})
    data = _extract_json(res.get("text", "")) or {}
    subs = []
    for s in (data.get("subjects") or [])[:30]:
        if not isinstance(s, dict):
            continue
        name = str(s.get("subject") or "").strip()
        if not name:
            continue
        def _v(k):
            v = s.get(k)
            v = "" if v is None else str(v).strip()
            return v or None
        subs.append({"subject": name[:200], "course_code": _v("course_code"),
                     "period": _v("period"), "teacher": _v("teacher"),
                     "q1": _v("q1"), "q2": _v("q2"), "sem1": _v("sem1"),
                     "q3": _v("q3"), "q4": _v("q4"), "sem2": _v("sem2"),
                     "fin": _v("fin"), "grade": _v("grade")})
    if not subs:
        raise HTTPException(422, {"error": "no_subjects",
                                  "message": "No courses were found in that text. Check the paste, or enter the grades manually."})
    def _num(k):
        v = data.get(k)
        return v if isinstance(v, (int, float)) else None
    return {
        "school_year": (str(data.get("school_year")).strip() if data.get("school_year") else None),
        "term": (str(data.get("term")).strip() if data.get("term") else None),
        "grade_level": data.get("grade_level") if isinstance(data.get("grade_level"), int) else None,
        "school_name": (str(data.get("school_name")).strip() if data.get("school_name") else None),
        "counselor": (str(data.get("counselor")).strip() if data.get("counselor") else None),
        "student_school_id": (str(data.get("student_school_id")).strip() if data.get("student_school_id") else None),
        "state_student_id": (str(data.get("state_student_id")).strip() if data.get("state_student_id") else None),
        "gpa_unweighted": _num("gpa_unweighted"),
        "gpa_weighted": _num("gpa_weighted"),
        "days_present": _num("days_present"),
        "days_absent": _num("days_absent"),
        "days_tardy": _num("days_tardy"),
        "teacher_comments": (str(data.get("teacher_comments")).strip()[:2000]
                             if data.get("teacher_comments") else None),
        "subjects": subs,
    }


@router.get("/student/{student_id}/school-profiles")
async def get_school_profiles(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, school_name, school_leaid, district_leaid, district_name, "
            "student_school_id, school_ceeb_code, ceeb_code, "
            "school_type, street_address, city_town, state_province, "
            "zip_postal_code, country, counselor_name, counselor_position, "
            "counselor_phone, counselor_email, counselor_fax, "
            "is_current_school, start_date, end_date, grade_levels_attended, "
            "grading_scale, max_grade_offered, schedule_type, "
            "courses_available_flags, courses_available_notes, "
            "graduating_class_size, boarding_students, curriculum_notes, notes "
            "FROM student_school_enrollments "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY is_current_school DESC NULLS LAST, start_date DESC NULLS LAST",
            student_id)
    import json as _json
    out = []
    for r in rows:
        d = dict(r)
        for k in ("start_date", "end_date"):
            d[k] = d[k].isoformat() if d[k] else None
        d["grade_levels_attended"] = list(d["grade_levels_attended"] or [])
        caf = d.get("courses_available_flags")
        if isinstance(caf, str):
            try: d["courses_available_flags"] = _json.loads(caf)
            except Exception: d["courses_available_flags"] = {}
        out.append(d)
    return {"student_id": student_id, "schools": out}


@router.post("/student/{student_id}/school-profiles")
async def post_school_profiles(request: Request, student_id: str, body: SchoolProfilesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    import json as _json
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE student_school_enrollments SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                name = (it.school_name or "").strip()
                if not name: continue
                caf = _json.dumps(it.courses_available_flags) if it.courses_available_flags else None
                # v0.12.118 fix: asyncpg infers $n::date as a date parameter, so a
                # plain 'YYYY-MM-DD' string raises "'str' object has no attribute
                # 'toordinal'" (500). Parse to real date objects before binding.
                _sd = _pp_parse_date(it.start_date)
                _ed = _pp_parse_date(it.end_date)
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    if it.is_current_school:
                        await conn.execute(
                            "UPDATE student_school_enrollments SET is_current_school=false "
                            "WHERE student_id=$1::uuid AND id != $2::uuid AND deleted_at IS NULL",
                            student_id, it.id)
                    r = await conn.execute(
                        "UPDATE student_school_enrollments SET school_name=$3, "
                        "school_ceeb_code=$4, ceeb_code=$5, school_type=$6, "
                        "street_address=$7, city_town=$8, state_province=$9, "
                        "zip_postal_code=$10, country=$11, counselor_name=$12, "
                        "counselor_position=$13, counselor_phone=$14, counselor_email=$15, "
                        "counselor_fax=$16, is_current_school=$17, "
                        "start_date=$18::date, end_date=$19::date, grade_levels_attended=$20, "
                        "grading_scale=$21, max_grade_offered=$22, schedule_type=$23, "
                        "courses_available_flags=$24::jsonb, courses_available_notes=$25, "
                        "graduating_class_size=$26, boarding_students=$27, "
                        "curriculum_notes=$28, notes=$29, "
                        "school_leaid=COALESCE($31, school_leaid), "
                        "district_leaid=COALESCE($32, district_leaid), "
                        "district_name=COALESCE($33, district_name), "
                        "student_school_id=COALESCE($34, student_school_id), "
                        "state_student_id=COALESCE($35, state_student_id), "
                        "updated_at=now(), updated_by=$30::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, name, it.school_ceeb_code, it.ceeb_code,
                        it.school_type, it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.counselor_name, it.counselor_position,
                        it.counselor_phone, it.counselor_email, it.counselor_fax,
                        (it.is_current_school or False), _sd, _ed, it.grade_levels_attended,
                        it.grading_scale, it.max_grade_offered, it.schedule_type,
                        caf, it.courses_available_notes, it.graduating_class_size,
                        it.boarding_students, it.curriculum_notes, it.notes, user_id,
                        it.school_leaid, it.district_leaid, it.district_name,
                        it.student_school_id, it.state_student_id)
                    if r and r.endswith(" 1"): updated += 1
                else:
                    if it.is_current_school:
                        await conn.execute(
                            "UPDATE student_school_enrollments SET is_current_school=false "
                            "WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
                    await conn.execute(
                        "INSERT INTO student_school_enrollments (tenant_id, student_id, "
                        "school_name, school_leaid, district_leaid, district_name, "
                        "student_school_id, "
                        "state_student_id, "
                        "school_ceeb_code, ceeb_code, school_type, "
                        "street_address, city_town, state_province, zip_postal_code, "
                        "country, counselor_name, counselor_position, counselor_phone, "
                        "counselor_email, counselor_fax, is_current_school, "
                        "start_date, end_date, grade_levels_attended, "
                        "grading_scale, max_grade_offered, schedule_type, "
                        "courses_available_flags, courses_available_notes, "
                        "graduating_class_size, boarding_students, curriculum_notes, notes, "
                        "visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$31,$32,$33,$34,$35,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,"
                        "$15,$16,$17,$18::date,$19::date,$20,$21,$22,$23,$24::jsonb,$25,"
                        "$26,$27,$28,$29,'private','parent_portal',$30::uuid,$30::uuid)",
                        tenant_id, student_id, name, it.school_ceeb_code, it.ceeb_code,
                        it.school_type, it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.counselor_name, it.counselor_position,
                        it.counselor_phone, it.counselor_email, it.counselor_fax,
                        (it.is_current_school or False), _sd, _ed, it.grade_levels_attended,
                        it.grading_scale, it.max_grade_offered, it.schedule_type,
                        caf, it.courses_available_notes, it.graduating_class_size,
                        it.boarding_students, it.curriculum_notes, it.notes, user_id,
                        it.school_leaid, it.district_leaid, it.district_name,
                        it.student_school_id, it.state_student_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ---- Report cards ----

class ReportCardSubject(BaseModel):
    model_config = ConfigDict(extra="forbid")   # v0.12.126: never silently drop again

    subject: str
    grade: Optional[str] = None
    numeric_grade: Optional[float] = None
    comment: Optional[str] = None
    # v0.12.124: the real district report-card shape. These were being silently
    # dropped by Pydantic, so a saved card came back with course names only.
    course_code: Optional[str] = None
    period: Optional[str] = None
    teacher: Optional[str] = None
    q1: Optional[str] = None
    q2: Optional[str] = None
    sem1: Optional[str] = None
    q3: Optional[str] = None
    q4: Optional[str] = None
    sem2: Optional[str] = None
    fin: Optional[str] = None


class ReportCardItem(BaseModel):
    model_config = ConfigDict(extra="forbid")   # v0.12.126

    id: Optional[str] = None
    school_name: Optional[str] = None
    school_id: Optional[str] = None             # v0.12.126 (school picker)
    school_year: str
    grade_level: int
    period_kind: str
    period_label: Optional[str] = None
    period_end_date: Optional[str] = None
    gpa_unweighted: Optional[float] = None
    gpa_weighted: Optional[float] = None
    class_rank: Optional[int] = None
    class_size: Optional[int] = None
    days_present: Optional[int] = None
    days_absent: Optional[int] = None
    days_tardy: Optional[int] = None
    subjects: List[ReportCardSubject] = []
    teacher_comments: Optional[str] = None
    evidence_urls: List[str] = []
    notes: Optional[str] = None
    public_description: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class ReportCardsRequest(BaseModel):
    items: List[ReportCardItem] = []
    delete_ids: List[str] = []


_RC_PERIODS = {"quarter", "trimester", "semester", "year_end", "mid_year", "final",
               "first_marking", "second_marking", "third_marking", "fourth_marking",
               "progress", "other"}


@router.get("/student/{student_id}/report-cards")
async def get_report_cards(request: Request, student_id: str,
                           grade_level: Optional[int] = None):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        if grade_level is not None:
            rows = await conn.fetch(
                "SELECT id::text AS id, school_name, school_year, grade_level, "
                "period_kind, period_label, period_end_date, gpa_unweighted, "
                "gpa_weighted, class_rank, class_size, days_present, days_absent, "
                "days_tardy, subjects, teacher_comments, evidence_urls, notes, "
                "public_description, (visibility='public') AS show_on_showcase "
                "FROM report_cards WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "AND grade_level=$2 "
                "ORDER BY period_end_date DESC NULLS LAST",
                student_id, grade_level)
        else:
            rows = await conn.fetch(
                "SELECT id::text AS id, school_name, school_year, grade_level, "
                "period_kind, period_label, period_end_date, gpa_unweighted, "
                "gpa_weighted, class_rank, class_size, days_present, days_absent, "
                "days_tardy, subjects, teacher_comments, evidence_urls, notes, "
                "public_description, (visibility='public') AS show_on_showcase "
                "FROM report_cards WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "ORDER BY grade_level, period_end_date DESC NULLS LAST",
                student_id)
    import json as _json
    out = []
    for r in rows:
        d = dict(r)
        d["period_end_date"] = d["period_end_date"].isoformat() if d["period_end_date"] else None
        for k in ("gpa_unweighted", "gpa_weighted"):
            d[k] = float(d[k]) if d[k] is not None else None
        for k in ("subjects", "evidence_urls"):
            v = d.get(k)
            if isinstance(v, str):
                try: d[k] = _json.loads(v)
                except Exception: d[k] = []
        out.append(d)
    return {"student_id": student_id, "report_cards": out}


@router.post("/student/{student_id}/report-cards")
async def post_report_cards(request: Request, student_id: str, body: ReportCardsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    import json as _json
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE report_cards SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if not it.school_year or it.grade_level is None: continue
                if it.grade_level < 0 or it.grade_level > 12: continue
                pk = it.period_kind if it.period_kind in _RC_PERIODS else "other"
                subs = _json.dumps([s.model_dump() for s in it.subjects]) if it.subjects else "[]"
                urls = _json.dumps(list(it.evidence_urls or []))
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE report_cards SET school_name=$3, school_year=$4, "
                        "grade_level=$5, period_kind=$6, period_label=$7, "
                        "period_end_date=$8::date, gpa_unweighted=$9, gpa_weighted=$10, "
                        "class_rank=$11, class_size=$12, days_present=$13, days_absent=$14, "
                        "days_tardy=$15, subjects=$16::jsonb, teacher_comments=$17, "
                        "evidence_urls=$18::jsonb, notes=$19, public_description=$20, "
                        "updated_at=now(), updated_by=$21::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.school_name, it.school_year, it.grade_level,
                        pk, it.period_label, it.period_end_date, it.gpa_unweighted,
                        it.gpa_weighted, it.class_rank, it.class_size, it.days_present,
                        it.days_absent, it.days_tardy, subs, it.teacher_comments,
                        urls, it.notes, it.public_description, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute(
                                "UPDATE report_cards SET visibility='public' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute(
                                "UPDATE report_cards SET visibility='private' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO report_cards (tenant_id, student_id, school_name, "
                        "school_year, grade_level, period_kind, period_label, period_end_date, "
                        "gpa_unweighted, gpa_weighted, class_rank, class_size, days_present, "
                        "days_absent, days_tardy, subjects, teacher_comments, evidence_urls, "
                        "notes, public_description, visibility, source_system, "
                        "created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8::date,$9,$10,$11,$12,$13,"
                        "$14,$15,$16::jsonb,$17,$18::jsonb,$19,$20,'private','parent_portal',"
                        "$21::uuid,$21::uuid) RETURNING id",
                        tenant_id, student_id, it.school_name, it.school_year, it.grade_level,
                        pk, it.period_label, it.period_end_date, it.gpa_unweighted,
                        it.gpa_weighted, it.class_rank, it.class_size, it.days_present,
                        it.days_absent, it.days_tardy, subs, it.teacher_comments,
                        urls, it.notes, it.public_description, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute(
                            "UPDATE report_cards SET visibility='public' "
                            "WHERE id=$1::uuid AND visibility_locked=false", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.31 - Student year records (per-grade, multi-school-year, mid-year transfer)
# ======================================================================


class YearTeacherItem(BaseModel):
    """v0.12.119: a teacher for one grade year. Elementary students commonly have
    two or more (e.g. one for ELA/Social Studies, one for Math/Science)."""
    teacher_id: Optional[str] = None
    teacher_name: Optional[str] = None
    subject_taught: Optional[str] = None
    is_homeroom: Optional[bool] = False


class YearRecordItem(BaseModel):
    id: Optional[str] = None
    grade_level: int
    school_year: str
    school_id: Optional[str] = None
    school_name: Optional[str] = None
    teachers: List[YearTeacherItem] = []            # v0.12.119
    homeroom_teacher_id: Optional[str] = None      # v0.12.119
    homeroom_teacher_name: Optional[str] = None    # v0.12.119
    is_full_year: Optional[bool] = None
    attendance_from: Optional[str] = None
    attendance_to: Optional[str] = None
    gpa_unweighted: Optional[float] = None
    gpa_weighted: Optional[float] = None
    days_present: Optional[int] = None
    days_absent: Optional[int] = None
    days_tardy: Optional[int] = None
    notes: Optional[str] = None
    public_description: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class YearRecordsRequest(BaseModel):
    items: List[YearRecordItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/year-records")
async def get_year_records(request: Request, student_id: str,
                           grade_level: Optional[int] = None):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        if grade_level is not None:
            rows = await conn.fetch(
                "SELECT id::text AS id, grade_level, school_year, "
                "school_id::text AS school_id, school_name, "
                "homeroom_teacher_id::text AS homeroom_teacher_id, homeroom_teacher_name, "
                "is_full_year, "
                "attendance_from, attendance_to, gpa_unweighted, gpa_weighted, "
                "days_present, days_absent, days_tardy, notes, public_description, "
                "(visibility='public') AS show_on_showcase "
                "FROM student_year_records WHERE student_id=$1::uuid "
                "AND deleted_at IS NULL AND grade_level=$2 "
                "ORDER BY school_year DESC, attendance_from DESC NULLS LAST",
                student_id, grade_level)
        else:
            rows = await conn.fetch(
                "SELECT id::text AS id, grade_level, school_year, "
                "school_id::text AS school_id, school_name, "
                "homeroom_teacher_id::text AS homeroom_teacher_id, homeroom_teacher_name, "
                "is_full_year, "
                "attendance_from, attendance_to, gpa_unweighted, gpa_weighted, "
                "days_present, days_absent, days_tardy, notes, public_description, "
                "(visibility='public') AS show_on_showcase "
                "FROM student_year_records WHERE student_id=$1::uuid "
                "AND deleted_at IS NULL "
                "ORDER BY grade_level, school_year DESC, attendance_from DESC NULLS LAST",
                student_id)
    out = []
    for r in rows:
        d = dict(r)
        for k in ("attendance_from", "attendance_to"):
            d[k] = d[k].isoformat() if d[k] else None
        for k in ("gpa_unweighted", "gpa_weighted"):
            d[k] = float(d[k]) if d[k] is not None else None
        out.append(d)
    # v0.12.119: attach the teacher list for each year (multi-teacher years are
    # the norm in elementary - e.g. one teacher for ELA, another for Math).
    if out:
        async with _tenant_conn(pool, tenant_id) as conn2:
            trows = await conn2.fetch(
                "SELECT year_record_id::text AS year_record_id, id::text AS id, "
                "teacher_id::text AS teacher_id, teacher_name, subject_taught, is_homeroom "
                "FROM student_year_teachers "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL "
                "ORDER BY is_homeroom DESC, subject_taught NULLS LAST", student_id)
        by_year: dict = {}
        for t in trows:
            by_year.setdefault(t["year_record_id"], []).append({
                "id": t["id"], "teacher_id": t["teacher_id"],
                "teacher_name": t["teacher_name"],
                "subject_taught": t["subject_taught"],
                "is_homeroom": t["is_homeroom"]})
        for d in out:
            d["teachers"] = by_year.get(d["id"], [])
    return {"student_id": student_id, "years": out}


async def _save_year_teachers(conn, tenant_id, student_id, user_id, year_record_id, teachers):
    """v0.12.119: replace the teacher list for one grade year. Elementary years
    routinely have two or more teachers (ELA/Social Studies + Math/Science), so
    this is a child list rather than a single column. The teacher's NAME is
    denormalized alongside the id, so the year still reads correctly if the
    teacher is later removed from the registry."""
    if teachers is None:
        return
    await conn.execute(
        "UPDATE student_year_teachers SET deleted_at=now(), deleted_by=$3::uuid "
        "WHERE year_record_id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
        year_record_id, student_id, user_id)
    for t in teachers:
        tid = (t.teacher_id or "").strip()
        if tid:
            try:
                _uuid.UUID(tid)
            except Exception:
                tid = ""
        name = (t.teacher_name or "").strip()
        subject = (t.subject_taught or "").strip()
        if not tid and not name:
            continue
        await conn.execute(
            "INSERT INTO student_year_teachers (tenant_id, student_id, year_record_id, "
            "teacher_id, teacher_name, subject_taught, is_homeroom, "
            "source_system, created_by, updated_by) "
            "VALUES ($1::uuid,$2::uuid,$3::uuid,NULLIF($4,'')::uuid,$5,$6,$7,"
            "'parent_portal',$8::uuid,$8::uuid)",
            tenant_id, student_id, year_record_id, tid, (name or None),
            (subject or None), bool(t.is_homeroom), user_id)


@router.post("/student/{student_id}/year-records")
async def post_year_records(request: Request, student_id: str, body: YearRecordsRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE student_year_records SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if it.grade_level is None or it.grade_level < 0 or it.grade_level > 12:
                    continue
                sy = (it.school_year or "").strip()
                if not sy: continue
                # v0.12.119: parse dates (asyncpg treats $n::date as a date param,
                # so a raw string 500s), and carry the homeroom teacher.
                _af = _pp_parse_date(it.attendance_from)
                _at = _pp_parse_date(it.attendance_to)
                _htid = (it.homeroom_teacher_id or "").strip()
                if _htid:
                    try:
                        _uuid.UUID(_htid)
                    except Exception:
                        _htid = ""
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE student_year_records SET grade_level=$3, school_year=$4, "
                        "school_id=NULLIF($5,'')::uuid, school_name=$6, is_full_year=$7, "
                        "attendance_from=$8, attendance_to=$9, "
                        "gpa_unweighted=$10, gpa_weighted=$11, days_present=$12, "
                        "days_absent=$13, days_tardy=$14, notes=$15, "
                        "public_description=$16, "
                        "homeroom_teacher_id=NULLIF($18,'')::uuid, homeroom_teacher_name=$19, "
                        "updated_at=now(), updated_by=$17::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.grade_level, sy, it.school_id or '',
                        it.school_name, it.is_full_year, _af,
                        _at, it.gpa_unweighted, it.gpa_weighted,
                        it.days_present, it.days_absent, it.days_tardy, it.notes,
                        it.public_description, user_id, _htid, it.homeroom_teacher_name)
                    if r and r.endswith(" 1"):
                        await _save_year_teachers(conn, tenant_id, student_id, user_id, it.id, it.teachers)
                        if it.show_on_showcase is True:
                            await conn.execute(
                                "UPDATE student_year_records SET visibility='public' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute(
                                "UPDATE student_year_records SET visibility='private' "
                                "WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO student_year_records (tenant_id, student_id, "
                        "grade_level, school_year, school_id, school_name, is_full_year, "
                        "attendance_from, attendance_to, gpa_unweighted, gpa_weighted, "
                        "days_present, days_absent, days_tardy, notes, public_description, "
                        "homeroom_teacher_id, homeroom_teacher_name, "
                        "visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,NULLIF($5,'')::uuid,$6,$7,"
                        "$8,$9,$10,$11,$12,$13,$14,$15,$16,"
                        "NULLIF($18,'')::uuid,$19,"
                        "'private','parent_portal',$17::uuid,$17::uuid) RETURNING id",
                        tenant_id, student_id, it.grade_level, sy, it.school_id or '',
                        it.school_name, it.is_full_year, _af,
                        _at, it.gpa_unweighted, it.gpa_weighted,
                        it.days_present, it.days_absent, it.days_tardy, it.notes,
                        it.public_description, user_id, _htid, it.homeroom_teacher_name)
                    await _save_year_teachers(conn, tenant_id, student_id, user_id, str(rid), it.teachers)
                    if it.show_on_showcase is True:
                        await conn.execute(
                            "UPDATE student_year_records SET visibility='public' "
                            "WHERE id=$1::uuid AND visibility_locked=false", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.32 - Teacher registry + universal school search proxy
# ======================================================================


class TeacherItem(BaseModel):
    id: Optional[str] = None
    teacher_name: Optional[str] = None             # v0.12.127: derived from first+last
    first_name: Optional[str] = None               # v0.12.127
    last_name: Optional[str] = None                # v0.12.127
    role: Optional[str] = None                     # v0.12.121 'teacher' | 'counselor'
    school_name: Optional[str] = None
    school_leaid: Optional[str] = None
    street_address: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    school_phone: Optional[str] = None
    teacher_email: Optional[str] = None
    subject_taught: Optional[str] = None
    title_position: Optional[str] = None
    notes: Optional[str] = None


class TeachersRequest(BaseModel):
    items: List[TeacherItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/teachers")
async def get_teachers(request: Request, student_id: str, role: Optional[str] = None):
    """v0.12.121: same registry serves teachers and counselors (role column).
    Pass ?role=counselor to list counselors only; omit for everyone."""
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    sql = ("SELECT id::text AS id, teacher_name, first_name, last_name, role, "
           "school_name, school_leaid, "
           "street_address, city_town, state_province, zip_postal_code, "
           "school_phone, teacher_email, subject_taught, title_position, notes "
           "FROM teachers WHERE (student_id=$1::uuid OR student_id IS NULL) "
           "AND deleted_at IS NULL ")
    args: list = [student_id]
    if role in ("teacher", "counselor"):
        args.append(role)
        sql += f"AND coalesce(role,'teacher')=${len(args)} "
    # v0.12.127: alphabetical by last name (fall back to the last word of the
    # full name for any record that predates the split).
    sql += ("ORDER BY lower(coalesce(nullif(last_name,''), "
            "split_part(teacher_name, ' ', array_length(string_to_array(teacher_name,' '),1)))), "
            "lower(coalesce(first_name, teacher_name))")
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(sql, *args)
    return {"student_id": student_id, "teachers": [dict(r) for r in rows]}


@router.post("/student/{student_id}/teachers")
async def post_teachers(request: Request, student_id: str, body: TeachersRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                # v0.12.125 fix: $2 was bound but never referenced, so Postgres
                # could not infer its type and every delete threw a 500.
                r = await conn.execute(
                    "UPDATE teachers SET deleted_at=now(), deleted_by=$2::uuid "
                    "WHERE id=$1::uuid AND deleted_at IS NULL",
                    did, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                # v0.12.127: first + last are the source of truth; teacher_name is
                # the derived display value (kept because courses reference it).
                _fn = (it.first_name or "").strip()
                _ln = (it.last_name or "").strip()
                nm = (" ".join(p for p in (_fn, _ln) if p)).strip() or (it.teacher_name or "").strip()
                if not nm: continue
                if not _fn and not _ln and nm:
                    parts = nm.split()
                    _fn = parts[0]
                    _ln = " ".join(parts[1:]) if len(parts) > 1 else ""
                _role = (it.role or "teacher").strip().lower()
                if _role not in ("teacher", "counselor"):
                    _role = "teacher"
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE teachers SET teacher_name=$2, school_name=$3, "
                        "school_leaid=$4, street_address=$5, city_town=$6, "
                        "state_province=$7, zip_postal_code=$8, school_phone=$9, "
                        "teacher_email=$10, subject_taught=$11, title_position=$12, "
                        "notes=$13, role=$15, first_name=$16, last_name=$17, "
                        "updated_at=now(), updated_by=$14::uuid "
                        "WHERE id=$1::uuid AND deleted_at IS NULL",
                        it.id, nm, it.school_name, it.school_leaid, it.street_address,
                        it.city_town, it.state_province, it.zip_postal_code,
                        it.school_phone, it.teacher_email, it.subject_taught,
                        it.title_position, it.notes, user_id, _role,
                        (_fn or None), (_ln or None))
                    if r and r.endswith(" 1"): updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO teachers (tenant_id, student_id, teacher_name, "
                        "first_name, last_name, "
                        "school_name, school_leaid, street_address, city_town, "
                        "state_province, zip_postal_code, school_phone, teacher_email, "
                        "subject_taught, title_position, notes, role, source_system, "
                        "created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$17,$18,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,"
                        "$14,$16,'parent_portal',$15::uuid,$15::uuid)",
                        tenant_id, student_id, nm, it.school_name, it.school_leaid,
                        it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.school_phone, it.teacher_email,
                        it.subject_taught, it.title_position, it.notes, user_id, _role,
                        (_fn or None), (_ln or None))
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


@router.get("/catalogs/school-search")
async def school_search(request: Request, q: str, level: str = "k12",
                        state: Optional[str] = None, limit: int = 12):
    """Band-aware school search. level=k12 -> DOE CCD; level=college -> IPEDS universities."""
    _ = await _resolve_context(request)
    q = (q or "").strip()
    if len(q) < 3:
        return {"results": []}
    if level == "college":
        pool: asyncpg.Pool = request.app.state.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT leaid, name, common_name, city, state, us_news_rank "
                "FROM universities WHERE name ILIKE $1 OR common_name ILIKE $1 "
                "ORDER BY us_news_rank NULLS LAST, name LIMIT $2",
                f"%{q}%", limit)
        return {"results": [
            {"name": r["name"], "common_name": r["common_name"],
             "city": r["city"], "state": r["state"], "leaid": r["leaid"],
             "us_news_rank": r["us_news_rank"], "street": None,
             "zip": None, "phone": None} for r in rows]}
    # k12 -> query the locally loaded CCD directory in `schools` (pg_trgm).
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if state:
            rows = await conn.fetch(
                "SELECT ncessch, name, address_line1, city, state, zip, phone, "
                "district_name FROM k12_schools "
                "WHERE state = $1 AND lower(name) LIKE '%' || lower($2) || '%' "
                "ORDER BY name LIMIT $3",
                state.upper(), q, limit)
        else:
            rows = await conn.fetch(
                "SELECT ncessch, name, address_line1, city, state, zip, phone, "
                "district_name FROM k12_schools "
                "WHERE lower(name) LIKE '%' || lower($1) || '%' "
                "ORDER BY name LIMIT $2",
                q, limit)
    out = [{
        "name": r["name"],
        "city": r["city"] or "",
        "state": r["state"] or "",
        "street": r["address_line1"] or "",
        "zip": r["zip"] or "",
        "leaid": r["ncessch"] or "",
        "phone": r["phone"] or "",
        "district": r["district_name"] or "",
    } for r in rows]
    return {"results": out}


# ======================================================================
# v0.12.35 - Tenant locale (UI language)
# ======================================================================

_SUPPORTED_LOCALES = {"en-US", "es-ES"}


@router.get("/tenant/locale")
async def get_tenant_locale(request: Request):
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        loc = await conn.fetchval(
            "SELECT locale FROM tenants WHERE id=$1::uuid", tenant_id)
    return {"tenant_id": tenant_id, "locale": loc or "en-US"}


class LocaleBody(BaseModel):
    locale: str


@router.post("/tenant/locale")
async def set_tenant_locale(request: Request, body: LocaleBody):
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    loc = body.locale if body.locale in _SUPPORTED_LOCALES else "en-US"
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "UPDATE tenants SET locale=$2, updated_at=now() WHERE id=$1::uuid",
                tenant_id, loc)
    return {"tenant_id": tenant_id, "locale": loc}


# ======================================================================
# v0.12.36 - Google-supported languages catalog (for demographic dropdowns)
# ======================================================================

_LANG_CACHE: dict = {"ts": 0, "data": None}


@router.get("/catalogs/languages")
async def get_languages_catalog(request: Request, target: str = "en"):
    """Return Google Cloud Translation supported languages. Cached 24h.
    Falls back to a built-in seed list if GOOGLE_TRANSLATE_API_KEY is unset
    or the call fails."""
    import time as _t
    _ = await _resolve_context(request)
    now = _t.time()
    if _LANG_CACHE["data"] and now - _LANG_CACHE["ts"] < 86400:
        return {"languages": _LANG_CACHE["data"], "cached": True}
    key = _pp_os.environ.get("GOOGLE_TRANSLATE_API_KEY") or _pp_os.environ.get("GOOGLE_PLACES_API_KEY")
    langs = None
    if key:
        import urllib.request as _u, urllib.parse as _up, json as _json
        try:
            url = ("https://translation.googleapis.com/language/translate/v2/languages?"
                   + _up.urlencode({"key": key, "target": target}))
            req = _u.Request(url, headers={"Accept": "application/json"})
            with _u.urlopen(req, timeout=15) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            rows = data.get("data", {}).get("languages", [])
            langs = [{"code": r.get("language"), "name": r.get("name") or r.get("language")}
                     for r in rows if r.get("language")]
            langs.sort(key=lambda x: x["name"].lower())
        except Exception:
            langs = None
    if not langs:
        seed = [("en","English"),("es","Spanish"),("zh","Chinese"),("hi","Hindi"),
                ("ar","Arabic"),("fr","French"),("ko","Korean"),("vi","Vietnamese"),
                ("tl","Tagalog"),("pt","Portuguese"),("ru","Russian"),("de","German"),
                ("ja","Japanese"),("ur","Urdu"),("bn","Bengali"),("it","Italian"),
                ("pl","Polish"),("tr","Turkish"),("uk","Ukrainian"),("he","Hebrew"),
                ("th","Thai"),("sw","Swahili")]
        langs = [{"code": c, "name": n} for c, n in seed]
        # Do NOT cache the seed fallback, so a later valid key returns the full list.
        return {"languages": langs, "cached": False, "seed": True}
    _LANG_CACHE["data"] = langs
    _LANG_CACHE["ts"] = now
    return {"languages": langs, "cached": False}


# ======================================================================
# v0.12.37 - Runtime UI-string translation (Google Translate, DB-cached)
# ======================================================================


class UiStringsBody(BaseModel):
    locale: str
    strings: List[str] = []


@router.post("/catalogs/ui-strings")
async def translate_ui_strings(request: Request, body: UiStringsBody):
    """Translate a batch of English UI strings into the requested locale.
    Cached per-locale in ui_string_translations (jsonb map en->translated).
    Only missing keys are sent to Google; the merged map is returned."""
    _ = await _resolve_context(request)
    import json as _json
    locale = (body.locale or "en-US").strip()
    lang = locale.split("-")[0].lower()
    if lang == "en" or not body.strings:
        return {"locale": locale, "strings": {}}

    pool: asyncpg.Pool = request.app.state.pool
    cached: dict = {}
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT strings FROM ui_string_translations WHERE locale=$1", locale)
        if row:
            cached = row if isinstance(row, dict) else _json.loads(row)

    missing = [s for s in body.strings if s and s not in cached]
    if not missing:
        return {"locale": locale, "strings": {k: cached[k] for k in body.strings if k in cached}}

    key = _pp_os.environ.get("GOOGLE_TRANSLATE_API_KEY") or _pp_os.environ.get("GOOGLE_PLACES_API_KEY")
    if not key:
        return {"locale": locale, "strings": cached, "error": "no_api_key"}

    import urllib.request as _u, urllib.parse as _up
    newly: dict = {}
    # Google Translate v2 accepts multiple q params per call; batch in chunks.
    CHUNK = 100
    try:
        for i in range(0, len(missing), CHUNK):
            batch = missing[i:i + CHUNK]
            params = [("key", key), ("target", lang), ("source", "en"), ("format", "text")]
            params += [("q", b) for b in batch]
            url = "https://translation.googleapis.com/language/translate/v2?" + _up.urlencode(params)
            req = _u.Request(url, headers={"Accept": "application/json"})
            with _u.urlopen(req, timeout=30) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            trans = data.get("data", {}).get("translations", [])
            for src, tr in zip(batch, trans):
                newly[src] = tr.get("translatedText", src)
    except Exception as e:
        # Return whatever we have cached; client falls back to English for the rest.
        return {"locale": locale, "strings": cached, "error": "translate_failed"}

    merged = dict(cached)
    merged.update(newly)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO ui_string_translations (locale, strings, updated_at) "
                "VALUES ($1,$2::jsonb, now()) "
                "ON CONFLICT (locale) DO UPDATE SET strings=$2::jsonb, updated_at=now()",
                locale, _json.dumps(merged))

    return {"locale": locale, "strings": {k: merged[k] for k in body.strings if k in merged}}


# ======================================================================
# v0.12.38 - Career pillar: profile, job experiences, references
# ======================================================================


class CareerProfileBody(BaseModel):
    authorized_us_work: Optional[bool] = None
    work_auth_basis: Optional[str] = None
    has_hs_diploma_or_ged: Optional[bool] = None
    convicted_felony: Optional[bool] = None
    felony_explanation: Optional[str] = None
    special_skills: Optional[str] = None
    possible_career: Optional[str] = None
    salary_desired: Optional[str] = None
    earliest_start_date: Optional[str] = None
    willing_full_time: Optional[bool] = None
    willing_part_time: Optional[bool] = None
    willing_days: Optional[bool] = None
    willing_evenings: Optional[bool] = None
    willing_swing: Optional[bool] = None
    willing_graveyard: Optional[bool] = None
    willing_weekends: Optional[bool] = None
    willing_regular: Optional[bool] = None
    willing_temporary: Optional[bool] = None
    notes: Optional[str] = None


@router.get("/student/{student_id}/career-profile")
async def get_career_profile(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT authorized_us_work, work_auth_basis, has_hs_diploma_or_ged, "
            "convicted_felony, felony_explanation, special_skills, possible_career, "
            "salary_desired, earliest_start_date, willing_full_time, willing_part_time, "
            "willing_days, willing_evenings, willing_swing, willing_graveyard, "
            "willing_weekends, willing_regular, willing_temporary, notes "
            "FROM career_profile WHERE student_id=$1::uuid", student_id)
    if not row:
        return {"student_id": student_id, "profile": {}}
    d = dict(row)
    if d.get("earliest_start_date"):
        d["earliest_start_date"] = d["earliest_start_date"].isoformat()
    return {"student_id": student_id, "profile": d}


@router.post("/student/{student_id}/career-profile")
async def post_career_profile(request: Request, student_id: str, body: CareerProfileBody):
    tenant_id, user_id = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "INSERT INTO career_profile (student_id, tenant_id, authorized_us_work, "
                "work_auth_basis, has_hs_diploma_or_ged, convicted_felony, felony_explanation, "
                "special_skills, possible_career, salary_desired, earliest_start_date, "
                "willing_full_time, willing_part_time, willing_days, willing_evenings, "
                "willing_swing, willing_graveyard, willing_weekends, willing_regular, "
                "willing_temporary, notes, updated_by) "
                "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12,$13,$14,"
                "$15,$16,$17,$18,$19,$20,$21,$22::uuid) "
                "ON CONFLICT (student_id) DO UPDATE SET "
                "authorized_us_work=$3, work_auth_basis=$4, has_hs_diploma_or_ged=$5, "
                "convicted_felony=$6, felony_explanation=$7, special_skills=$8, "
                "possible_career=$9, salary_desired=$10, earliest_start_date=$11::date, "
                "willing_full_time=$12, willing_part_time=$13, willing_days=$14, "
                "willing_evenings=$15, willing_swing=$16, willing_graveyard=$17, "
                "willing_weekends=$18, willing_regular=$19, willing_temporary=$20, "
                "notes=$21, updated_at=now(), updated_by=$22::uuid",
                student_id, tenant_id, body.authorized_us_work, body.work_auth_basis,
                body.has_hs_diploma_or_ged, body.convicted_felony, body.felony_explanation,
                body.special_skills, body.possible_career, body.salary_desired,
                body.earliest_start_date, body.willing_full_time, body.willing_part_time,
                body.willing_days, body.willing_evenings, body.willing_swing,
                body.willing_graveyard, body.willing_weekends, body.willing_regular,
                body.willing_temporary, body.notes, user_id)
    return {"student_id": student_id, "saved": True}


class JobExperienceItem(BaseModel):
    id: Optional[str] = None
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    supervisor_name: Optional[str] = None
    supervisor_phone: Optional[str] = None
    supervisor_email: Optional[str] = None
    street_address: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    country: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    is_current: Optional[bool] = None
    is_paid: Optional[bool] = None
    job_description: Optional[str] = None
    duties: Optional[str] = None
    reason_for_leaving: Optional[str] = None
    starting_salary: Optional[str] = None
    ending_salary: Optional[str] = None
    may_contact: Optional[bool] = None
    hours_type: Optional[str] = None
    employment_status: Optional[str] = None
    skills_gained: List[str] = []
    notes: Optional[str] = None
    public_description: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class JobExperiencesRequest(BaseModel):
    items: List[JobExperienceItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/job-experiences")
async def get_job_experiences(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, job_title, company_name, supervisor_name, "
            "supervisor_phone, supervisor_email, street_address, city_town, state_province, "
            "zip_postal_code, country, start_date, end_date, is_current, is_paid, "
            "job_description, duties, reason_for_leaving, starting_salary, ending_salary, may_contact, "
            "hours_type, employment_status, skills_gained, notes, public_description, "
            "(visibility='public') AS show_on_showcase "
            "FROM job_experiences WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY is_current DESC, start_date DESC NULLS LAST", student_id)
    out = []
    for r in rows:
        d = dict(r)
        for k in ("start_date", "end_date"):
            d[k] = d[k].isoformat() if d[k] else None
        d["skills_gained"] = list(d["skills_gained"] or [])
        out.append(d)
    return {"student_id": student_id, "jobs": out}


@router.post("/student/{student_id}/job-experiences")
async def post_job_experiences(request: Request, student_id: str, body: JobExperiencesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE job_experiences SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if not (it.job_title or it.company_name): continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE job_experiences SET job_title=$3, company_name=$4, "
                        "supervisor_name=$5, supervisor_phone=$6, supervisor_email=$28, street_address=$7, "
                        "city_town=$8, state_province=$9, zip_postal_code=$10, country=$11, "
                        "start_date=$12::date, end_date=$13::date, is_current=$14, is_paid=$15, "
                        "job_description=$27, duties=$16, reason_for_leaving=$17, starting_salary=$18, "
                        "ending_salary=$19, may_contact=$20, hours_type=$21, "
                        "employment_status=$22, skills_gained=$23, notes=$24, "
                        "public_description=$25, updated_at=now(), updated_by=$26::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.job_title, it.company_name, it.supervisor_name,
                        it.supervisor_phone, it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.start_date, it.end_date,
                        it.is_current, it.is_paid, it.duties, it.reason_for_leaving,
                        it.starting_salary, it.ending_salary, it.may_contact, it.hours_type,
                        it.employment_status, list(it.skills_gained or []), it.notes,
                        it.public_description, user_id, it.supervisor_email)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute("UPDATE job_experiences SET visibility='public' WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute("UPDATE job_experiences SET visibility='private' WHERE id=$1::uuid AND visibility_locked=false", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO job_experiences (tenant_id, student_id, job_title, "
                        "company_name, supervisor_name, supervisor_phone, supervisor_email, street_address, "
                        "city_town, state_province, zip_postal_code, country, start_date, "
                        "end_date, is_current, is_paid, duties, reason_for_leaving, "
                        "starting_salary, ending_salary, may_contact, hours_type, "
                        "employment_status, skills_gained, notes, public_description, "
                        "job_description, visibility, source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$28,$7,$8,$9,$10,$11,$12::date,"
                        "$13::date,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$27,"
                        "'private','parent_portal',$26::uuid,$26::uuid) RETURNING id",
                        tenant_id, student_id, it.job_title, it.company_name, it.supervisor_name,
                        it.supervisor_phone, it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.start_date, it.end_date,
                        it.is_current, it.is_paid, it.duties, it.reason_for_leaving,
                        it.starting_salary, it.ending_salary, it.may_contact, it.hours_type,
                        it.employment_status, list(it.skills_gained or []), it.notes,
                        it.public_description, user_id, it.job_description, it.supervisor_email)
                    if it.show_on_showcase is True:
                        await conn.execute("UPDATE job_experiences SET visibility='public' WHERE id=$1::uuid AND visibility_locked=false", rid)
                    saved += 1
        try:
            await _write_inferences(conn, tenant_id, student_id, user_id)
        except Exception:
            pass
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


class ReferenceItem(BaseModel):
    id: Optional[str] = None
    ref_name: str
    relationship: Optional[str] = None
    is_professional: Optional[bool] = None
    street_address: Optional[str] = None
    city_town: Optional[str] = None
    state_province: Optional[str] = None
    zip_postal_code: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    notes: Optional[str] = None


class ReferencesRequest(BaseModel):
    items: List[ReferenceItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/references")
async def get_references(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, ref_name, relationship, is_professional, "
            "street_address, city_town, state_province, zip_postal_code, country, "
            "phone, email, notes FROM career_references "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL ORDER BY ref_name",
            student_id)
    return {"student_id": student_id, "references": [dict(r) for r in rows]}


@router.post("/student/{student_id}/references")
async def post_references(request: Request, student_id: str, body: ReferencesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE career_references SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                nm = (it.ref_name or "").strip()
                if not nm: continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE career_references SET ref_name=$3, relationship=$4, "
                        "is_professional=$5, street_address=$6, city_town=$7, "
                        "state_province=$8, zip_postal_code=$9, country=$10, phone=$11, "
                        "email=$12, notes=$13, updated_at=now(), updated_by=$14::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, nm, it.relationship, it.is_professional,
                        it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.phone, it.email, it.notes, user_id)
                    if r and r.endswith(" 1"): updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO career_references (tenant_id, student_id, ref_name, "
                        "relationship, is_professional, street_address, city_town, "
                        "state_province, zip_postal_code, country, phone, email, notes, "
                        "source_system, created_by, updated_by) "
                        "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,"
                        "'parent_portal',$14::uuid,$14::uuid)",
                        tenant_id, student_id, nm, it.relationship, it.is_professional,
                        it.street_address, it.city_town, it.state_province,
                        it.zip_postal_code, it.country, it.phone, it.email, it.notes, user_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.40 - Higher-Ed: essays, recommenders, financial aid, testing
# ======================================================================


class EssayItem(BaseModel):
    id: Optional[str] = None
    essay_title: Optional[str] = None
    prompt_text: Optional[str] = None
    application_type: Optional[str] = None
    target_schools: List[str] = []
    status: Optional[str] = None
    word_count: Optional[int] = None
    word_limit: Optional[int] = None
    topic_themes: List[str] = []
    body_content: Optional[str] = None
    notes: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class EssaysRequest(BaseModel):
    items: List[EssayItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/essays")
async def get_essays(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, essay_title, prompt_text, application_type, "
            "target_schools, status, word_count, word_limit, topic_themes, "
            "body_content, notes, (visibility='public') AS show_on_showcase "
            "FROM essays WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY updated_at DESC", student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["target_schools"] = list(d["target_schools"] or [])
        tt = d.get("topic_themes")
        if isinstance(tt, str):
            try: tt = json.loads(tt)
            except Exception: tt = []
        d["topic_themes"] = tt or []
        out.append(d)
    return {"student_id": student_id, "essays": out}


@router.post("/student/{student_id}/essays")
async def post_essays(request: Request, student_id: str, body: EssaysRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE essays SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if not (it.essay_title or it.prompt_text): continue
                wc = it.word_count
                if wc is None and it.body_content:
                    wc = len([w for w in it.body_content.split() if w])
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE essays SET essay_title=$3, prompt_text=$4, application_type=$5, "
                        "target_schools=$6, status=$7, word_count=$8, word_limit=$9, "
                        "topic_themes=$10::jsonb, body_content=$11, notes=$12, updated_at=now(), "
                        "updated_by=$13::uuid WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.essay_title, it.prompt_text, it.application_type,
                        list(it.target_schools or []), it.status, wc, it.word_limit,
                        json.dumps(list(it.topic_themes or [])), it.body_content, it.notes, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute("UPDATE essays SET visibility='public' WHERE id=$1::uuid", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute("UPDATE essays SET visibility='private' WHERE id=$1::uuid", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO essays (tenant_id, student_id, essay_title, prompt_text, "
                        "application_type, target_schools, status, word_count, word_limit, "
                        "topic_themes, body_content, notes, visibility, source_system, "
                        "created_by, updated_by) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,"
                        "$10::jsonb,$11,$12,'private','parent_portal',$13::uuid,$13::uuid) RETURNING id",
                        tenant_id, student_id, it.essay_title, it.prompt_text,
                        (it.application_type or "common_app"),
                        list(it.target_schools or []), (it.status or "outlining"), wc, it.word_limit,
                        json.dumps(list(it.topic_themes or [])), it.body_content, it.notes, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute("UPDATE essays SET visibility='public' WHERE id=$1::uuid", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


class RecommenderItem(BaseModel):
    id: Optional[str] = None
    recommender_name: str
    recommender_role: Optional[str] = None
    organization_name: Optional[str] = None
    subject_or_specialty: Optional[str] = None
    relationship_quality: Optional[str] = None
    years_known: Optional[float] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    preferred_contact_method: Optional[str] = None
    notes: Optional[str] = None


class RecommendersRequest(BaseModel):
    items: List[RecommenderItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/recommenders")
async def get_recommenders(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, recommender_name, recommender_role, organization_name, "
            "subject_or_specialty, relationship_quality, years_known, contact_email, "
            "contact_phone, preferred_contact_method, notes FROM recommenders "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL ORDER BY recommender_name",
            student_id)
    out = []
    for r in rows:
        d = dict(r)
        d["years_known"] = float(d["years_known"]) if d["years_known"] is not None else None
        out.append(d)
    return {"student_id": student_id, "recommenders": out}


@router.post("/student/{student_id}/recommenders")
async def post_recommenders(request: Request, student_id: str, body: RecommendersRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE recommenders SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                nm = (it.recommender_name or "").strip()
                if not nm: continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE recommenders SET recommender_name=$3, recommender_role=$4, "
                        "organization_name=$5, subject_or_specialty=$6, relationship_quality=$7, "
                        "years_known=$8, contact_email=$9, contact_phone=$10, "
                        "preferred_contact_method=$11, notes=$12, updated_at=now(), "
                        "updated_by=$13::uuid WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, nm, it.recommender_role, it.organization_name,
                        it.subject_or_specialty, it.relationship_quality, it.years_known,
                        it.contact_email, it.contact_phone, it.preferred_contact_method,
                        it.notes, user_id)
                    if r and r.endswith(" 1"): updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO recommenders (tenant_id, student_id, recommender_name, "
                        "recommender_role, organization_name, subject_or_specialty, "
                        "relationship_quality, years_known, contact_email, contact_phone, "
                        "preferred_contact_method, notes, visibility, source_system, "
                        "created_by, updated_by) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,"
                        "$9,$10,$11,$12,'private','parent_portal',$13::uuid,$13::uuid)",
                        tenant_id, student_id, nm, it.recommender_role, it.organization_name,
                        it.subject_or_specialty, it.relationship_quality, it.years_known,
                        it.contact_email, it.contact_phone, it.preferred_contact_method,
                        it.notes, user_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


class FinAidItem(BaseModel):
    id: Optional[str] = None
    aid_type: str
    school_name: Optional[str] = None
    university_leaid: Optional[str] = None
    form_or_program: Optional[str] = None
    status: Optional[str] = None
    submitted_date: Optional[str] = None
    deadline: Optional[str] = None
    priority_deadline: Optional[str] = None
    award_amount: Optional[float] = None
    is_renewable: Optional[bool] = None
    renewal_terms: Optional[str] = None
    css_code: Optional[str] = None
    requires_css: Optional[bool] = None
    requires_fafsa: Optional[bool] = None
    requires_idoc: Optional[bool] = None
    fee_waiver_used: Optional[bool] = None
    notes: Optional[str] = None


class FinAidRequest(BaseModel):
    items: List[FinAidItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/financial-aid")
async def get_financial_aid(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, aid_type, school_name, university_leaid, form_or_program, "
            "status, submitted_date, deadline, priority_deadline, award_amount, is_renewable, "
            "renewal_terms, css_code, requires_css, requires_fafsa, requires_idoc, "
            "fee_waiver_used, notes FROM financial_aid_items "
            "WHERE student_id=$1::uuid AND deleted_at IS NULL ORDER BY deadline NULLS LAST",
            student_id)
    out = []
    for r in rows:
        d = dict(r)
        for k in ("submitted_date", "deadline", "priority_deadline"):
            d[k] = d[k].isoformat() if d[k] else None
        d["award_amount"] = float(d["award_amount"]) if d["award_amount"] is not None else None
        out.append(d)
    return {"student_id": student_id, "financial_aid": out}


@router.post("/student/{student_id}/financial-aid")
async def post_financial_aid(request: Request, student_id: str, body: FinAidRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE financial_aid_items SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if not it.aid_type: continue
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE financial_aid_items SET aid_type=$3, school_name=$4, "
                        "university_leaid=$5, form_or_program=$6, status=$7, "
                        "submitted_date=$8::date, deadline=$9::date, priority_deadline=$10::date, "
                        "award_amount=$11, is_renewable=$12, renewal_terms=$13, css_code=$14, "
                        "requires_css=$15, requires_fafsa=$16, requires_idoc=$17, "
                        "fee_waiver_used=$18, notes=$19, updated_at=now(), updated_by=$20::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.aid_type, it.school_name, it.university_leaid,
                        it.form_or_program, it.status, it.submitted_date, it.deadline,
                        it.priority_deadline, it.award_amount, it.is_renewable, it.renewal_terms,
                        it.css_code, it.requires_css, it.requires_fafsa, it.requires_idoc,
                        it.fee_waiver_used, it.notes, user_id)
                    if r and r.endswith(" 1"): updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO financial_aid_items (tenant_id, student_id, aid_type, "
                        "school_name, university_leaid, form_or_program, status, submitted_date, "
                        "deadline, priority_deadline, award_amount, is_renewable, renewal_terms, "
                        "css_code, requires_css, requires_fafsa, requires_idoc, fee_waiver_used, "
                        "notes, source_system, created_by, updated_by) VALUES ($1::uuid,$2::uuid,"
                        "$3,$4,$5,$6,$7,$8::date,$9::date,$10::date,$11,$12,$13,$14,$15,$16,$17,"
                        "$18,$19,'parent_portal',$20::uuid,$20::uuid)",
                        tenant_id, student_id, it.aid_type, it.school_name, it.university_leaid,
                        it.form_or_program, it.status, it.submitted_date, it.deadline,
                        it.priority_deadline, it.award_amount, it.is_renewable, it.renewal_terms,
                        it.css_code, it.requires_css, it.requires_fafsa, it.requires_idoc,
                        it.fee_waiver_used, it.notes, user_id)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


class ColTestItem(BaseModel):
    id: Optional[str] = None
    test_type: str
    test_date: Optional[str] = None
    registration_deadline: Optional[str] = None
    is_planned: Optional[bool] = None
    is_superscore: Optional[bool] = None
    composite_score: Optional[int] = None
    section_scores: Optional[dict] = None
    percentile: Optional[int] = None
    superscore_composite: Optional[int] = None
    registration_status: Optional[str] = None
    fee_waiver_used: Optional[bool] = None
    notes: Optional[str] = None
    show_on_showcase: Optional[bool] = None


class ColTestRequest(BaseModel):
    items: List[ColTestItem] = []
    delete_ids: List[str] = []


@router.get("/student/{student_id}/college-tests")
async def get_college_tests(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT id::text AS id, test_type, test_date, registration_deadline, is_planned, "
            "is_superscore, composite_score, section_scores, percentile, superscore_composite, "
            "registration_status, fee_waiver_used, notes, (visibility='public') AS show_on_showcase "
            "FROM college_test_scores WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "ORDER BY test_date DESC NULLS LAST", student_id)
    import json as _json
    out = []
    for r in rows:
        d = dict(r)
        for k in ("test_date", "registration_deadline"):
            d[k] = d[k].isoformat() if d[k] else None
        ss = d.get("section_scores")
        if isinstance(ss, str):
            try: d["section_scores"] = _json.loads(ss)
            except Exception: d["section_scores"] = {}
        out.append(d)
    return {"student_id": student_id, "tests": out}


@router.post("/student/{student_id}/college-tests")
async def post_college_tests(request: Request, student_id: str, body: ColTestRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    import json as _json
    saved = updated = deleted = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            for did in body.delete_ids or []:
                try: _uuid.UUID(did)
                except Exception: continue
                r = await conn.execute(
                    "UPDATE college_test_scores SET deleted_at=now(), deleted_by=$3::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                    did, student_id, user_id)
                if r and r.endswith(" 1"): deleted += 1
            for it in body.items or []:
                if not it.test_type: continue
                ss = _json.dumps(it.section_scores) if it.section_scores else "{}"
                if it.id:
                    try: _uuid.UUID(it.id)
                    except Exception: continue
                    r = await conn.execute(
                        "UPDATE college_test_scores SET test_type=$3, test_date=$4::date, "
                        "registration_deadline=$5::date, is_planned=$6, is_superscore=$7, "
                        "composite_score=$8, section_scores=$9::jsonb, percentile=$10, "
                        "superscore_composite=$11, registration_status=$12, fee_waiver_used=$13, "
                        "notes=$14, updated_at=now(), updated_by=$15::uuid "
                        "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                        it.id, student_id, it.test_type, it.test_date, it.registration_deadline,
                        it.is_planned, it.is_superscore, it.composite_score, ss, it.percentile,
                        it.superscore_composite, it.registration_status, it.fee_waiver_used,
                        it.notes, user_id)
                    if r and r.endswith(" 1"):
                        if it.show_on_showcase is True:
                            await conn.execute("UPDATE college_test_scores SET visibility='public' WHERE id=$1::uuid", it.id)
                        elif it.show_on_showcase is False:
                            await conn.execute("UPDATE college_test_scores SET visibility='private' WHERE id=$1::uuid", it.id)
                        updated += 1
                else:
                    rid = await conn.fetchval(
                        "INSERT INTO college_test_scores (tenant_id, student_id, test_type, "
                        "test_date, registration_deadline, is_planned, is_superscore, "
                        "composite_score, section_scores, percentile, superscore_composite, "
                        "registration_status, fee_waiver_used, notes, visibility, source_system, "
                        "created_by, updated_by) VALUES ($1::uuid,$2::uuid,$3,$4::date,$5::date,"
                        "$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,'private','parent_portal',"
                        "$15::uuid,$15::uuid) RETURNING id",
                        tenant_id, student_id, it.test_type, it.test_date, it.registration_deadline,
                        it.is_planned, it.is_superscore, it.composite_score, ss, it.percentile,
                        it.superscore_composite, it.registration_status, it.fee_waiver_used,
                        it.notes, user_id)
                    if it.show_on_showcase is True:
                        await conn.execute("UPDATE college_test_scores SET visibility='public' WHERE id=$1::uuid", rid)
                    saved += 1
    return {"student_id": student_id, "saved": saved, "updated": updated, "deleted": deleted}


# ======================================================================
# v0.12.41 - Essay Studio: prompt guidance, AI sample, versioned autosave
# ======================================================================

import os as _es_os
try:
    import httpx as _es_httpx
except Exception:
    _es_httpx = None


@router.get("/catalogs/essay-guidance")
async def get_essay_guidance(request: Request):
    """Public reference catalog: the 7 Common App prompts with strategy notes."""
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(
            "SELECT code, prompt_number, title, prompt_text, hidden_question, best_for, "
            "common_trap, strategy, route FROM essay_prompt_guidance "
            "WHERE is_active ORDER BY sort_order")
    return {"prompts": [dict(r) for r in rows]}


async def _gather_student_essay_context(conn, student_id: str) -> dict:
    """Pull the strongest narrative signals from the student's record."""
    ctx = {}
    srow = await conn.fetchrow(
        "SELECT first_name, current_grade FROM students WHERE id=$1::uuid", student_id)
    if srow:
        ctx["first_name"] = srow["first_name"]
        ctx["grade_level"] = srow["current_grade"]
    # top awards
    aw = await conn.fetch(
        "SELECT award_name, description, granting_organization, level, rank_or_placement, awarded_date "
        "FROM awards_honors WHERE student_id=$1::uuid AND deleted_at IS NULL "
        "ORDER BY awarded_date DESC NULLS LAST LIMIT 8", student_id)
    ctx["awards"] = [
        {"award_name": r["award_name"], "description": r["description"],
         "organization": r["granting_organization"], "level": r["level"],
         "placement": r["rank_or_placement"],
         "date": r["awarded_date"].isoformat() if r["awarded_date"] else None}
        for r in aw]
    # affiliations / activities with sustained involvement
    af = await conn.fetch(
        "SELECT organization_name, role, affiliation_type, notes, weekly_hours, total_hours, "
        "role_start_date, role_end_date FROM affiliations "
        "WHERE student_id=$1::uuid AND deleted_at IS NULL "
        "ORDER BY role_start_date DESC NULLS LAST LIMIT 10", student_id)
    ctx["affiliations"] = [
        {"organization_name": r["organization_name"], "role": r["role"],
         "type": r["affiliation_type"], "notes": r["notes"],
         "weekly_hours": float(r["weekly_hours"]) if r["weekly_hours"] is not None else None,
         "total_hours": float(r["total_hours"]) if r["total_hours"] is not None else None,
         "start": r["role_start_date"].isoformat() if r["role_start_date"] else None,
         "end": r["role_end_date"].isoformat() if r["role_end_date"] else None}
        for r in af]
    # event volume by type (shows sustained practice)
    ev = await conn.fetch(
        "SELECT event_type, count(*) AS n, min(event_date) AS first, max(event_date) AS last "
        "FROM events WHERE student_id=$1::uuid AND deleted_at IS NULL AND event_date IS NOT NULL "
        "GROUP BY event_type ORDER BY count(*) DESC LIMIT 6", student_id)
    ctx["activity_volume"] = [
        {"event_type": r["event_type"], "count": int(r["n"]),
         "first": r["first"].isoformat() if r["first"] else None,
         "last": r["last"].isoformat() if r["last"] else None} for r in ev]
    # inferred meta-skills (top signals)
    ms = await conn.fetch(
        "SELECT meta_skill_code, score, evidence FROM meta_skill_inferences "
        "WHERE student_id=$1::uuid ORDER BY score DESC LIMIT 12", student_id)
    ctx["meta_skills"] = [
        {"code": r["meta_skill_code"], "score": r["score"]} for r in ms]
    return ctx


class EssaySampleRequest(BaseModel):
    prompt_code: Optional[str] = None
    prompt_text: Optional[str] = None
    word_limit: Optional[int] = 650


@router.post("/student/{student_id}/essays/sample")
async def generate_essay_sample(request: Request, student_id: str, body: EssaySampleRequest):
    """Generate a first-person sample essay grounded in the student's real record.
    Model provider is swappable via env (ANTHROPIC_API_KEY / FOCMS_LLM_*)."""
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        sctx = await _gather_student_essay_context(conn, student_id)
        guide = None
        if body.prompt_code:
            g = await conn.fetchrow(
                "SELECT title, prompt_text, hidden_question, common_trap, strategy "
                "FROM essay_prompt_guidance WHERE code=$1", body.prompt_code)
            guide = dict(g) if g else None

    prompt_text = body.prompt_text or (guide or {}).get("prompt_text") or "Share a meaningful story about yourself."
    strategy = (guide or {}).get("strategy", "")
    trap = (guide or {}).get("common_trap", "")
    hidden = (guide or {}).get("hidden_question", "")
    wl = body.word_limit or 650

    sys = (
        "You are helping a high-school student see what a strong Common App essay could look like, "
        "written in their own authentic first-person voice. Use ONLY the facts provided about the student. "
        "Do not invent achievements, awards, or events not in the record. If the record is thin, write a "
        "smaller, honest, sensory story rather than fabricating accomplishments. This is a SAMPLE to inspire "
        "their own writing, not a final essay. Aim for about " + str(wl) + " words."
    )
    user = (
        "PROMPT:\n" + prompt_text + "\n\n"
        + ("WHAT ADMISSIONS IS REALLY ASKING: " + hidden + "\n" if hidden else "")
        + ("STRATEGY TO FOLLOW: " + strategy + "\n" if strategy else "")
        + ("AVOID THIS TRAP: " + trap + "\n" if trap else "")
        + "\nSTUDENT RECORD (facts you may draw from):\n"
        + json.dumps(sctx, default=str, indent=2)
        + "\n\nWrite the sample essay now, first person, no title, no preamble."
    )
    res = await _llm_complete(sys, user, max_tokens=1500)
    if res.get("unavailable"):
        return {"sample": None, "unavailable": True, "reason": res["reason"], "context_used": sctx}
    return {"sample": res.get("text", ""), "context_used": sctx, "model": res.get("model")}


class EssayAutosaveRequest(BaseModel):
    body_content: str = ""
    essay_title: Optional[str] = None
    snapshot: bool = False   # True = commit a numbered version into draft_history


@router.post("/student/{student_id}/essays/{essay_id}/autosave")
async def autosave_essay(request: Request, student_id: str, essay_id: str, body: EssayAutosaveRequest):
    """Debounced autosave. Always updates body_content + word_count.
    When snapshot=True, also appends the prior body to draft_history and bumps current_version."""
    tenant_id, user_id = await _pp_context(request, student_id)
    try:
        _uuid.UUID(essay_id)
    except Exception:
        raise HTTPException(status_code=400, detail="bad essay id")
    wc = len([w for w in (body.body_content or "").split() if w])
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            cur = await conn.fetchrow(
                "SELECT body_content, current_version, draft_history FROM essays "
                "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
                essay_id, student_id)
            if not cur:
                raise HTTPException(status_code=404, detail="essay not found")
            new_version = cur["current_version"] or 1
            if body.snapshot:
                hist = cur["draft_history"]
                if isinstance(hist, str):
                    try: hist = json.loads(hist)
                    except Exception: hist = []
                hist = hist or []
                prev = cur["body_content"] or ""
                prev_wc = len([w for w in prev.split() if w])
                hist.append({
                    "version": cur["current_version"] or 1,
                    "saved_at": datetime.utcnow().isoformat() + "Z",
                    "word_count": prev_wc,
                    "body_content": prev,
                })
                hist = hist[-30:]  # cap stored versions
                new_version = (cur["current_version"] or 1) + 1
                await conn.execute(
                    "UPDATE essays SET body_content=$3, word_count=$4, essay_title=COALESCE($5, essay_title), "
                    "draft_history=$6::jsonb, current_version=$7, updated_at=now(), updated_by=$8::uuid "
                    "WHERE id=$1::uuid AND student_id=$2::uuid",
                    essay_id, student_id, body.body_content, wc, body.essay_title,
                    json.dumps(hist), new_version, user_id)
            else:
                await conn.execute(
                    "UPDATE essays SET body_content=$3, word_count=$4, essay_title=COALESCE($5, essay_title), "
                    "updated_at=now(), updated_by=$6::uuid WHERE id=$1::uuid AND student_id=$2::uuid",
                    essay_id, student_id, body.body_content, wc, body.essay_title, user_id)
    return {"saved": True, "word_count": wc, "current_version": new_version, "snapshot": body.snapshot}


@router.get("/student/{student_id}/essays/{essay_id}/versions")
async def get_essay_versions(request: Request, student_id: str, essay_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT current_version, draft_history FROM essays "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
            essay_id, student_id)
    if not row:
        raise HTTPException(status_code=404, detail="essay not found")
    hist = row["draft_history"]
    if isinstance(hist, str):
        try: hist = json.loads(hist)
        except Exception: hist = []
    return {"current_version": row["current_version"], "versions": hist or []}


# ======================================================================
# v0.12.42 - Essay analysis + exemplar corpus; shared LLM adapter shim
# ======================================================================

async def _llm_complete(system: str, user: str, max_tokens: int = 1500, want_json: bool = False) -> dict:
    """Provider-swappable completion. Set FOCMS_LLM_PROVIDER=anthropic|openai_compatible.
    Returns {"text": str} or {"unavailable": True, "reason": str}.
    Interim shim until focms_llm_adapter.py lands; keeps all call sites uniform."""
    if _es_httpx is None:
        return {"unavailable": True, "reason": "httpx not installed"}
    provider = _es_os.environ.get("FOCMS_LLM_PROVIDER", "anthropic").lower()
    api_key = _es_os.environ.get("FOCMS_LLM_API_KEY") or _es_os.environ.get("ANTHROPIC_API_KEY")
    model = _es_os.environ.get("FOCMS_LLM_MODEL", "claude-sonnet-4-6")
    if not api_key:
        return {"unavailable": True, "reason": "No LLM provider configured. Set FOCMS_LLM_API_KEY (or ANTHROPIC_API_KEY)."}
    try:
        if provider == "openai_compatible":
            base = _es_os.environ.get("FOCMS_LLM_BASE_URL", "").rstrip("/")
            if not base:
                return {"unavailable": True, "reason": "FOCMS_LLM_BASE_URL required for openai_compatible."}
            payload = {"model": model, "max_tokens": max_tokens,
                       "messages": [{"role": "system", "content": system},
                                    {"role": "user", "content": user}]}
            if want_json:
                payload["response_format"] = {"type": "json_object"}
            async with _es_httpx.AsyncClient(timeout=120) as client:
                r = await client.post(base + "/chat/completions",
                    headers={"Authorization": "Bearer " + api_key, "content-type": "application/json"},
                    json=payload)
            if r.status_code != 200:
                return {"unavailable": True, "reason": f"LLM error {r.status_code}: {r.text[:200]}"}
            d = r.json()
            msg = d["choices"][0]["message"]
            txt = (msg.get("content") or "").strip()
            if not txt:
                txt = (msg.get("reasoning") or msg.get("reasoning_content") or "").strip()
            return {"text": txt, "model": model}
        else:  # anthropic
            base = _es_os.environ.get("FOCMS_LLM_BASE_URL", "https://api.anthropic.com").rstrip("/")
            async with _es_httpx.AsyncClient(timeout=90) as client:
                r = await client.post(base + "/v1/messages",
                    headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": model, "max_tokens": max_tokens, "system": system,
                          "messages": [{"role": "user", "content": user}]})
            if r.status_code != 200:
                return {"unavailable": True, "reason": f"LLM error {r.status_code}: {r.text[:200]}"}
            d = r.json()
            txt = "".join([b.get("text", "") for b in d.get("content", []) if b.get("type") == "text"])
            return {"text": txt.strip(), "model": model}
    except Exception as e:
        return {"unavailable": True, "reason": str(e)}


def _extract_json(text: str):
    """Pull a JSON object from an LLM reply, tolerating thinking models,
    markdown code fences, and <think> blocks around the JSON."""
    import re as _re
    if not text:
        return None
    t = text
    # drop <think>...</think> reasoning blocks
    t = _re.sub(r"<think>.*?</think>", " ", t, flags=_re.S | _re.I)
    # strip stray HTML tags some models inject inside values
    t = _re.sub(r"</?span[^>]*>", "", t, flags=_re.I)
    # deepseek pollution: `"key": word: 7` -> `"key": 7`
    t = _re.sub(r'(:\s*)[A-Za-z][\w\- ]*:\s*(-?\d)', r'\1\2', t)
    # em-dash / en-dash / bare dash used as a value -> null
    t = _re.sub(r':\s*[\u2014\u2013\-]+\s*([,}])', r': null\1', t)
    # prefer a fenced ```json ... ``` block if present
    fence = _re.search(r"```(?:json)?\s*(\{.*?\})\s*```", t, _re.S)
    candidates = []
    if fence:
        candidates.append(fence.group(1))
    # also scan for balanced top-level {...} objects (last one wins)
    depth = 0; start = None
    for i, ch in enumerate(t):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidates.append(t[start:i + 1])
    # try longest/last candidates first (most complete)
    for cand in sorted(set(candidates), key=len, reverse=True):
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


@router.post("/student/{student_id}/essays/{essay_id}/analyze")
async def analyze_essay(request: Request, student_id: str, essay_id: str):
    """Score the essay against an admissions rubric and return concrete recommendations.
    Learns the rubric partly from the exemplar corpus (what worked before)."""
    tenant_id, user_id = await _pp_context(request, student_id)
    try:
        _uuid.UUID(essay_id)
    except Exception:
        raise HTTPException(status_code=400, detail="bad essay id")
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT essay_title, prompt_text, word_limit, body_content FROM essays "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
            essay_id, student_id)
        if not row:
            raise HTTPException(status_code=404, detail="essay not found")
        # pull a few exemplar trait sets for calibration
        ex = await conn.fetch(
            "SELECT strengths, techniques, why_it_worked FROM essay_exemplars "
            "WHERE is_active ORDER BY created_at DESC LIMIT 5")
    body = (row["body_content"] or "").replace("\n", " ")
    # strip html tags the rich editor may have added
    import re as _re2
    body = _re2.sub(r"<[^>]+>", " ", body)
    body = _re2.sub(r"\s+", " ", body).strip()
    if len(body) < 40:
        return {"unavailable": True, "reason": "Essay is too short to analyze yet. Write a draft first."}
    calib = [dict(e) for e in ex]

    system = (
        "You are a veteran US college admissions reader. Evaluate a student's application essay and "
        "return STRICT JSON only, no prose outside the JSON. Be specific and honest but constructive; "
        "cite phrases from the essay. Schema: {\"overall_score\": int 1-10, \"scores\": {\"hook\": int 1-5, "
        "\"authentic_voice\": int 1-5, \"specificity\": int 1-5, \"internal_growth\": int 1-5, "
        "\"structure\": int 1-5, \"prompt_fit\": int 1-5}, \"strengths\": [str], \"priority_fixes\": "
        "[{\"issue\": str, \"why\": str, \"how\": str}], \"line_edits\": [{\"quote\": str, \"suggestion\": str}], "
        "\"one_thing\": str}. line_edits max 5, priority_fixes max 4."
    )
    user = (
        "PROMPT:\n" + (row["prompt_text"] or "(none given)") + "\n"
        + "WORD LIMIT: " + str(row["word_limit"] or "n/a") + "\n\n"
        + "ESSAY:\n" + body + "\n\n"
        + ("WHAT HAS WORKED IN STRONG ESSAYS (for calibration):\n" + json.dumps(calib, default=str)[:1500] + "\n\n" if calib else "")
        + "Return the JSON now."
    )
    res = await _llm_complete(system, user, max_tokens=1800, want_json=True)
    if res.get("unavailable"):
        return {"unavailable": True, "reason": res["reason"]}
    parsed = _extract_json(res.get("text", ""))
    if not parsed:
        return {"unavailable": True, "reason": "Could not parse analysis. Try again."}
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            await conn.execute(
                "UPDATE essays SET analysis=$3::jsonb, analysis_at=now(), updated_by=$4::uuid "
                "WHERE id=$1::uuid AND student_id=$2::uuid",
                essay_id, student_id, json.dumps(parsed), user_id)
    return {"analysis": parsed, "model": res.get("model"), "analyzed_at": datetime.utcnow().isoformat() + "Z"}


@router.get("/student/{student_id}/essays/{essay_id}/analysis")
async def get_essay_analysis(request: Request, student_id: str, essay_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT analysis, analysis_at FROM essays "
            "WHERE id=$1::uuid AND student_id=$2::uuid AND deleted_at IS NULL",
            essay_id, student_id)
    if not row or not row["analysis"]:
        return {"analysis": None}
    a = row["analysis"]
    if isinstance(a, str):
        try: a = json.loads(a)
        except Exception: a = None
    return {"analysis": a, "analyzed_at": row["analysis_at"].isoformat() if row["analysis_at"] else None}


# ---- Exemplar corpus (framework IP layer: what a winning essay looks like) ----

@router.get("/catalogs/essay-exemplars")
async def list_essay_exemplars(request: Request, prompt_code: Optional[str] = None, route: Optional[str] = None):
    ctx = await _resolve_context(request)
    tenant_id = ctx["tenant_id"]
    pool: asyncpg.Pool = request.app.state.pool
    q = ("SELECT id::text AS id, title, prompt_code, route, word_count, outcome_school, "
         "outcome_scholarship, admit_cycle, selectivity_tier, strengths, techniques, "
         "why_it_worked, essay_text FROM essay_exemplars WHERE is_active")
    args = []
    if prompt_code:
        args.append(prompt_code); q += f" AND prompt_code=${len(args)}"
    if route:
        args.append(route); q += f" AND route=${len(args)}"
    q += " ORDER BY created_at DESC LIMIT 25"
    async with _tenant_conn(pool, tenant_id) as conn:
        rows = await conn.fetch(q, *args)
    out = []
    for r in rows:
        d = dict(r)
        for k in ("strengths", "techniques"):
            v = d.get(k)
            if isinstance(v, str):
                try: d[k] = json.loads(v)
                except Exception: d[k] = []
        out.append(d)
    return {"exemplars": out}


class ExemplarIn(BaseModel):
    title: Optional[str] = None
    prompt_code: Optional[str] = None
    route: Optional[str] = None
    essay_text: str
    outcome_school: Optional[str] = None
    outcome_scholarship: Optional[str] = None
    admit_cycle: Optional[str] = None
    selectivity_tier: Optional[str] = None
    source_note: Optional[str] = None
    auto_extract: bool = True   # run LLM to extract traits/techniques/why


@router.post("/admin/essay-exemplars")
async def add_essay_exemplar(request: Request, body: ExemplarIn):
    """Internal-only. Adds a successful essay to the corpus and (optionally) extracts
    the traits/techniques that made it work, so future students learn from it."""
    ctx = await _resolve_context(request)
    if ctx.get("scope") == "parent_portal":
        raise HTTPException(status_code=403, detail="internal_only")
    tenant_id = ctx["tenant_id"]
    uid = ctx.get("user_id")
    text = (body.essay_text or "").strip()
    if len(text) < 100:
        raise HTTPException(status_code=400, detail="essay_text too short")
    wc = len([w for w in text.split() if w])
    strengths, techniques, why = [], [], None
    if body.auto_extract:
        system = ("You analyze a successful US college admissions essay and return STRICT JSON only: "
                  "{\"strengths\":[str],\"techniques\":[str],\"why_it_worked\":str,\"route\":str}. "
                  "route is one of: Nerdy/Passionate, Growth/Resilience, Empathy/Intellectual, Open. "
                  "techniques = concrete craft moves (e.g., 'opens in medias res', 'uses a recurring motif').")
        user = "ESSAY:\n" + text + "\n\nReturn the JSON."
        res = await _llm_complete(system, user, max_tokens=900, want_json=True)
        if not res.get("unavailable"):
            parsed = _extract_json(res.get("text", "")) or {}
            strengths = parsed.get("strengths", []) or []
            techniques = parsed.get("techniques", []) or []
            why = parsed.get("why_it_worked")
            if not body.route and parsed.get("route"):
                body.route = parsed["route"]
    pool: asyncpg.Pool = request.app.state.pool
    async with _tenant_conn(pool, tenant_id) as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")
            rid = await conn.fetchval(
                "INSERT INTO essay_exemplars (title, prompt_code, route, essay_text, word_count, "
                "outcome_school, outcome_scholarship, admit_cycle, selectivity_tier, strengths, "
                "techniques, why_it_worked, source_note, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,"
                "$9,$10::jsonb,$11::jsonb,$12,$13,$14::uuid) RETURNING id",
                body.title, body.prompt_code, body.route, text, wc, body.outcome_school,
                body.outcome_scholarship, body.admit_cycle, body.selectivity_tier,
                json.dumps(strengths), json.dumps(techniques), why, body.source_note, uid)
    return {"id": str(rid), "word_count": wc, "strengths": strengths,
            "techniques": techniques, "why_it_worked": why}