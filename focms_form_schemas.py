"""
focms_form_schemas.py — Schema-driven form definitions + entry writer.

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
from typing import Any, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

log = logging.getLogger("focms-form-schemas")
router = APIRouter(prefix="/focms/v1", tags=["form-schemas"])

# ---------------------------------------------------------------------------
# Auth dependency — reuse focms_api.authenticate (validates against
# FOCMS_API_TOKENS_JSON env var, the same mechanism every other endpoint uses)
# ---------------------------------------------------------------------------

async def _resolve_context(request: Request) -> dict:
    """
    Delegate to focms_api.authenticate. Returns dict with tenant_id, user_id,
    role from the FOCMS_API_TOKENS_JSON mapping.
    """
    from focms_api import authenticate as _authenticate
    authorization = request.headers.get("authorization")
    ctx = _authenticate(authorization=authorization)
    # authenticate returns {"tenant_id", "user_id", "role"} plus maybe more.
    # Normalize to the shape the endpoints expect.
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
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant_id', $1, true)",
            str(tenant_id),
        )

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
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant_id', $1, true)",
            str(tenant_id),
        )

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
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant_id', $1, true)",
            str(tenant_id),
        )

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
# Parent-portal capture endpoints (v0.12.20)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    course_name: Optional[str] = None
    school_name: Optional[str] = None
    subject: Optional[str] = None
    school_year: Optional[str] = None
    grade_level: Optional[int] = None
    term: Optional[str] = None
    grade_received: Optional[str] = None
    credit_hours: Optional[float] = None
    rigor: Optional[str] = None
    ap_exam_score: Optional[int] = None
    teacher_name: Optional[str] = None
    notes: Optional[str] = None
    skills: list[str] = Field(default_factory=list)
    artifact_ids: list[str] = Field(default_factory=list)


class CoursesRequest(BaseModel):
    items: list[CourseItem] = Field(default_factory=list)


@router.get("/student/{student_id}/courses")
async def get_student_courses(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        rows = await conn.fetch(
            "SELECT course_name, school_name, subject, school_year, grade_level, term, grade_received, "
            "credit_hours, course_type, is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, "
            "teacher_name, notes, admission_traits_developed, evidence_artifact_ids "
            "FROM courses_taken WHERE student_id=$1::uuid AND deleted_at IS NULL "
            "AND source_system='parent_portal' ORDER BY grade_level NULLS LAST, course_name", student_id)

    def rigor_of(r):
        if r["is_ap"]: return "ap"
        if r["is_ib"]: return "ib"
        if r["is_dual_credit"]: return "dual"
        if r["is_honors"]: return "honors"
        return r["course_type"] or "regular"

    return {"student_id": student_id, "items": [
        {"course_name": r["course_name"], "school_name": r["school_name"], "subject": r["subject"],
         "school_year": r["school_year"], "grade_level": r["grade_level"], "term": r["term"],
         "grade_received": r["grade_received"],
         "credit_hours": float(r["credit_hours"]) if r["credit_hours"] is not None else None,
         "rigor": rigor_of(r), "ap_exam_score": r["ap_exam_score"], "teacher_name": r["teacher_name"],
         "notes": r["notes"], "skills": _pp_skills(r["admission_traits_developed"]),
         "artifact_ids": _pp_artifacts(r["evidence_artifact_ids"])} for r in rows]}


@router.post("/student/{student_id}/courses")
async def post_student_courses(request: Request, student_id: str, body: CoursesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
            default_school = await _pp_current_school_name(conn, student_id)
            await conn.execute("DELETE FROM courses_taken WHERE tenant_id=$1::uuid "
                               "AND student_id=$2::uuid AND source_system='parent_portal'",
                               tenant_id, student_id)
            for it in body.items:
                name = (it.course_name or "").strip()
                if not name:
                    continue
                school = (it.school_name or "").strip() or default_school or "Unspecified"
                rigor = (it.rigor or "regular").lower()
                if rigor not in _RIGOR:
                    rigor = "regular"
                gp = _pp_grade_points(it.grade_received)
                gpw = (gp + _RIGOR_BONUS.get(rigor, 0.0)) if gp is not None else None
                await conn.execute(
                    "INSERT INTO courses_taken (tenant_id, student_id, course_name, school_name, "
                    "course_type, subject, grade_level, school_year, term, credit_hours, grade_received, "
                    "is_honors, is_ap, is_ib, is_dual_credit, ap_exam_score, teacher_name, "
                    "grade_points_4_0, grade_points_weighted, notes, admission_traits_developed, "
                    "evidence_artifact_ids, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,"
                    "$18,$19,$20,$21::jsonb,$22::text[]::uuid[],'parent_portal',$23::uuid,$23::uuid)",
                    tenant_id, student_id, name, school, rigor, (it.subject or None),
                    _pp_int(it.grade_level), (it.school_year or None), (it.term or None),
                    _pp_num(it.credit_hours), (it.grade_received or None),
                    rigor == "honors", rigor == "ap", rigor == "ib", rigor == "dual",
                    _pp_int(it.ap_exam_score), (it.teacher_name or None),
                    gp, gpw, (it.notes or None), json.dumps(_pp_skills(it.skills)),
                    _pp_artifacts(it.artifact_ids), user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved}


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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
_FAMILY_LOCKED = {"email", "phone", "date_of_birth", "legal_sex", "marital_relationship"}


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
    public: dict = Field(default_factory=dict)


class FamilyRequest(BaseModel):
    father: FamilyMember = Field(default_factory=FamilyMember)
    mother: FamilyMember = Field(default_factory=FamilyMember)


def _fm_has_data(m: FamilyMember) -> bool:
    return bool((m.first_name or "").strip() or (m.last_name or "").strip())


async def _insert_family_member(conn, tenant_id, student_id, user_id, relationship, order, m: FamilyMember):
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
             source_system, created_by, updated_by)
        VALUES
            ($1::uuid,$2::uuid,$3,$4,$5,
             $6, focms_encrypt_pii($1::uuid,$7,$29), focms_encrypt_pii($1::uuid,$8,$29),
             focms_encrypt_pii($1::uuid,$9,$29), $10,
             $11,$12,$13, focms_encrypt_pii($1::uuid,$14,$29), $15,
             $16,$17,$18,
             $19,$20,$21,
             $22,$23,$24,
             $25,$26,$27, jsonb_build_object('public_fields', $30::jsonb),
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
    )


@router.get("/student/{student_id}/family")
async def get_student_family(request: Request, student_id: str):
    tenant_id, _ = await _pp_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
                   details->'public_fields' AS public_fields
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
            "is_legal_guardian": r["is_legal_guardian"], "notes": r["notes"], "public": pf or {},
        }
        rel = (r["relationship"] or "").lower()
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
            await conn.execute(
                "DELETE FROM family_members WHERE tenant_id=$1::uuid AND student_id=$2::uuid "
                "AND source_system='parent_portal' AND relationship IN ('father','mother')",
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    chosen_name: Optional[str] = None
    pronouns: list[str] = Field(default_factory=list)
    gender_identity: list[str] = Field(default_factory=list)
    legal_sex_at_birth: Optional[str] = None
    email_primary: Optional[str] = None
    phone_primary: Optional[str] = None
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        row = await conn.fetchrow(
            "SELECT chosen_name, pronouns, gender_identity, legal_sex_at_birth, email_primary, "
            "phone_primary, citizenship_status, place_of_birth_country, is_hispanic_or_latino, "
            "racial_background, language_spoken_at_home, first_language_native, "
            "details->'public_fields' AS public_fields "
            "FROM student_personal_details WHERE student_id=$1::uuid AND deleted_at IS NULL", student_id)
    locked = sorted(_PERSONAL_LOCKED)
    if not row:
        return {"student_id": student_id, "personal": {}, "locked": locked}
    pf = row["public_fields"]
    if isinstance(pf, str):
        try:
            pf = json.loads(pf)
        except Exception:
            pf = {}
    return {"student_id": student_id, "locked": locked, "personal": {
        "chosen_name": row["chosen_name"],
        "pronouns": list(row["pronouns"] or []),
        "gender_identity": list(row["gender_identity"] or []),
        "legal_sex_at_birth": row["legal_sex_at_birth"],
        "email_primary": row["email_primary"],
        "phone_primary": row["phone_primary"],
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
            )
            if exists:
                await conn.execute(
                    "UPDATE student_personal_details SET "
                    "chosen_name=$2, pronouns=$3::text[], gender_identity=$4::text[], legal_sex_at_birth=$5, "
                    "email_primary=$6, phone_primary=$7, citizenship_status=$8, place_of_birth_country=$9, "
                    "is_hispanic_or_latino=$10, racial_background=$11::text[], language_spoken_at_home=$12, "
                    "first_language_native=$13, "
                    "details = COALESCE(details,'{}'::jsonb) || jsonb_build_object('public_fields', $14::jsonb), "
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
                    "jsonb_build_object('public_fields', $14::jsonb), "
                    "$16::uuid, 'parent_portal', "
                    "COALESCE($15::uuid,'019ed384-56d8-77fb-bfe6-00b1d064da18'::uuid), "
                    "COALESCE($15::uuid,'019ed384-56d8-77fb-bfe6-00b1d064da18'::uuid))",
                    *args, tenant_id)
    return {"student_id": student_id, "saved": True}


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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        age = await _pp_student_age(conn, student_id)
        age_max_by_code = {
            r["code"]: (float(r["typical_age_max"]) if r["typical_age_max"] is not None else None)
            for r in await conn.fetch("SELECT code, typical_age_max FROM skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
            for it in body.items:
                dt = (it.doc_type or "").strip().lower()
                if dt not in _DOC_TYPES or not (it.artifact_id or "").strip():
                    continue
                # one live row per doc_type; re-upload replaces (and resets to submitted)
                await conn.execute(
                    "DELETE FROM student_identity_documents WHERE tenant_id=$1::uuid "
                    "AND student_id=$2::uuid AND doc_type=$3", tenant_id, student_id, dt)
                await conn.execute(
                    "INSERT INTO student_identity_documents (tenant_id, student_id, doc_type, "
                    "artifact_id, status, notes, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4::uuid,'submitted',$5,'parent_portal',$6::uuid,$6::uuid)",
                    tenant_id, student_id, dt, it.artifact_id.strip(), (it.notes or None), user_id)
                saved += 1
    return {"student_id": student_id, "saved": saved}


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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        valid = {r["code"] for r in await conn.fetch(
            "SELECT code FROM meta_skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        valid = {r["code"] for r in await conn.fetch(
            "SELECT code FROM meta_skills_catalog WHERE is_active")}
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)

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

    return findings


# v0.12.19: shared engine writer + lazy auto-run
async def _write_inferences(conn, tenant_id: str, student_id: str, user_id):
    """Replace stored inferences from a fresh engine run. Returns count written."""
    valid = {r["code"] for r in await conn.fetch("SELECT code FROM meta_skills_catalog WHERE is_active")}
    findings = await _run_meta_inference(conn, tenant_id, student_id)
    async with conn.transaction():
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        written = await _write_inferences(conn, tenant_id, student_id, user_id)
    return {"student_id": student_id, "inferred": written}


@router.get("/student/{student_id}/meta-skills/inferred")
async def get_inferred_meta_skills(request: Request, student_id: str):
    """The capability read: inferred meta-skills grouped by category, with
    evidence. Skills without findings are listed as 'awaiting evidence'."""
    tenant_id, _ = await _pp_internal_context(request, student_id)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
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
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        major = await conn.fetchrow(
            "SELECT cip_code, title, weight_academics, weight_engagement, weight_skills, weight_milestones "
            "FROM cip_majors WHERE cip_code=$1 AND is_active", cip)
        if not major:
            raise HTTPException(status_code=404, detail="cip_major_not_found")

        buckets = await _sps_buckets(conn, student_id)

        # S - hard-skill coverage vs this major (same math as the gap report)
        reqs = await conn.fetch(
            "SELECT skill_code, importance FROM major_skill_requirements "
            "WHERE cip_code=$1 AND is_active AND skill_code IS NOT NULL", cip)
        if reqs:
            age = await _pp_student_age(conn, student_id)
            srows = await conn.fetch(
                "SELECT skill_code, acquired FROM student_skills "
                "WHERE student_id=$1::uuid AND deleted_at IS NULL AND skill_code IS NOT NULL", student_id)
            explicit = {r["skill_code"]: r["acquired"] for r in srows}
            amax = {r["code"]: (float(r["typical_age_max"]) if r["typical_age_max"] is not None else None)
                    for r in await conn.fetch("SELECT code, typical_age_max FROM skills_catalog WHERE is_active")}
            have_w = total_w = 0.0
            signal_hits = 0
            for r in reqs:
                total_w += r["importance"]
                acq = explicit.get(r["skill_code"])
                presumed = (acq is None and age is not None and amax.get(r["skill_code"]) is not None
                            and age > amax[r["skill_code"]])
                if acq is not None or presumed:
                    signal_hits += 1
                if acq is True or presumed:
                    have_w += r["importance"]
            # no explicit rows and no presumable requirements = no signal at all:
            # exclude the bucket (renormalize) instead of scoring a false zero
            if total_w and (explicit or signal_hits):
                buckets["skills"] = {
                    "score": round(have_w / total_w, 3),
                    "evidence": [f"Hard-skill coverage of {len(reqs)} requirements for {major['title']}"]}

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
