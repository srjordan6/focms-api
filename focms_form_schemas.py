"""
focms_form_schemas.py — Schema-driven form definitions + entry writer.

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
# Parent-portal capture endpoints (v0.12.6)
#   milestones (+ media) . academics (school/GPA/rank + est GPA) . courses . tests
#   GPA auto-calc; notes/skills/evidence on course & test rows.
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
    milestone_code: str
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
            "SELECT milestone_code, happened, event_date, event_notes, artifact_url "
            "FROM student_life_milestones WHERE student_id=$1::uuid AND deleted_at IS NULL",
            student_id,
        )
    return {"student_id": student_id, "milestones": [
        {"milestone_code": r["milestone_code"], "happened": r["happened"],
         "event_date": r["event_date"].isoformat() if r["event_date"] else None,
         "event_notes": r["event_notes"], "artifact_url": r["artifact_url"]} for r in rows]}


@router.post("/student/{student_id}/milestones")
async def post_student_milestones(request: Request, student_id: str, body: MilestonesRequest):
    tenant_id, user_id = await _pp_context(request, student_id)
    saved = cleared = 0
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.current_tenant_id', $1, true)", tenant_id)
            for item in body.items:
                code = (item.milestone_code or "").strip()
                if not code:
                    continue
                await conn.execute(
                    "DELETE FROM student_life_milestones WHERE tenant_id=$1::uuid "
                    "AND student_id=$2::uuid AND milestone_code=$3",
                    tenant_id, student_id, code)
                notes = item.event_notes.strip() if item.event_notes and item.event_notes.strip() else None
                art = (item.artifact_url or "").strip() or None
                if not item.happened and not item.event_date and not notes and not art:
                    cleared += 1
                    continue
                await conn.execute(
                    "INSERT INTO student_life_milestones (tenant_id, student_id, milestone_code, "
                    "happened, event_date, event_notes, artifact_url, source_system, created_by, updated_by) "
                    "VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,'parent_portal',$8::uuid,$8::uuid)",
                    tenant_id, student_id, code, bool(item.happened),
                    _pp_parse_date(item.event_date), notes, art, user_id)
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
