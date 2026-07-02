"""
focms_form_schemas.py — Schema-driven form definitions + entry writer.

v0.12.0 · Session 1 of the schema-driven parent portal build.

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
# Context dependency — resolved from focms_api at import wire-up time
# ---------------------------------------------------------------------------
# focms_api.py exposes an async dependency `get_context(request)` that returns
# a dict with keys: user_id, tenant_id, scope, student_ids, token_id.
# We import it lazily in the endpoint to avoid a circular import at module load.

async def _resolve_context(request: Request) -> dict:
    from focms_api import get_context  # local import to break cycle
    return await get_context(request)


# ---------------------------------------------------------------------------
# Pool accessor — same lazy pattern
# ---------------------------------------------------------------------------

async def _get_pool() -> asyncpg.Pool:
    from focms_api import DB_POOL  # module-level global in focms_api
    if DB_POOL is None:
        raise HTTPException(status_code=503, detail="database_not_ready")
    return DB_POOL


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

    pool = await _get_pool()
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
        "version": "0.12.0",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "field_count": len(fields),
        "fields": fields,
        "catalogs": catalogs if include_catalogs else None,
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

    pool = await _get_pool()
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
                col_list = ", ".join(cols)
                placeholders = ", ".join(
                    f"${i + 3}" for i in range(len(cols))
                )
                update_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in cols
                )
                # Non-null defaults for required cols not being written
                # is_veteran, is_active_us_military, is_dependent_of_us_veteran,
                # is_national_guard_or_active_reserve default to false.
                # service_branches defaults to empty array.
                required_defaults = {
                    "is_veteran": False,
                    "is_active_us_military": False,
                    "is_dependent_of_us_veteran": False,
                    "is_national_guard_or_active_reserve": False,
                    "service_branches": [],
                }
                defaults_cols = [c for c in required_defaults if c not in cols]
                defaults_vals = [required_defaults[c] for c in defaults_cols]
                if defaults_cols:
                    col_list = col_list + ", " + ", ".join(defaults_cols)
                    extra_placeholders = ", ".join(
                        f"${len(cols) + 4 + i}" for i in range(len(defaults_cols))
                    )
                    placeholders = placeholders + ", " + extra_placeholders

                sql = f"""
                    INSERT INTO veteran_military_status
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
                all_vals = list(vals) + defaults_vals
                row = await conn.fetchrow(
                    sql, body.student_id, tenant_id, *all_vals, user_id
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