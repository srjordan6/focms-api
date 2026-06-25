from fastapi.middleware.cors import CORSMiddleware
"""focms_api.py - FOCMS Data Provider REST API v0.7.0

Read + write API in front of FOCMS Postgres. Enforces per-request tenant
context via RLS (SET LOCAL app.current_tenant_id). Runs as a Render Web
Service behind PgBouncer in transaction mode.

v0.7.0 (2026-06-24):
- Parent portal endpoints. New module focms_parent_portal.py defines:
    POST  /focms/v1/parent/auth/verify-token        (URL-token auth)
    GET   /focms/v1/parent/students/{id}/form       (localized form + values)
    POST  /focms/v1/parent/students/{id}/save       (save field updates)
    POST  /focms/v1/parent/admin/generate-token     (admin: mint parent URL)
    GET   /focms/v1/parent/admin/tokens             (admin: list tokens)
    POST  /focms/v1/parent/admin/tokens/{id}/revoke (admin: revoke)
  Parents authenticate via URL-embedded token. Save dispatch is config-driven
  from SAVE_TABLE_CONFIG (4 patterns) across 28 source tables. Every parent
  edit writes to audit_log for COPPA/FERPA compliance.

v0.6.0 (2026-06-24):
- Internationalization (i18n) endpoints. New module focms_i18n.py defines:
    GET   /focms/v1/i18n/strings          (batch UI string retrieval)
    POST  /focms/v1/i18n/translate        (single text translation/transliteration/romanization)
    POST  /focms/v1/i18n/translate-batch  (bulk translation for UI string seeding)
  Powered by Google Cloud Translation API v2 (same API key as Places) with a
  translation_cache table for memoization. Supports 29 locales out of the box;
  any new locale added to the `locales` table works automatically. Free-text
  parent input gets cached translations on first save; UI strings get cached
  per (namespace, locale) pair forever (text meaning is stable). Names in
  non-Latin scripts (Korean Hangul, Chinese Han, Japanese Kana, Arabic, Hindi,
  Russian, Hebrew, Greek, Thai) get romanized via translation_kind='romanize'
  so the same parent_supplied name flows through to UCA / Common App in Latin
  script with the original native-script form preserved for parent display.

v0.5.0 (2026-06-24):
- Address autocomplete + validation + international phone validation.
  New endpoints (defined in focms_addresses.py):
    POST   /focms/v1/addresses/autocomplete
    POST   /focms/v1/addresses/{address_id}/validate
    POST   /focms/v1/phones/validate
  Powered by Google Places API (autocomplete + place details) with a 7-day
  Postgres-backed suggestion cache. Address validation persists every
  attempt to address_validations (full audit) and updates the
  student_addresses row with standardized + verified fields.
  Phone validation backed by SQL fn_validate_phone() across 21 countries;
  libphonenumber Python upgrade comes in v0.6.0 with same JSON contract.
  USPS v3 deferred (vendor portal mid-relaunch until 2026-07-12).

v0.4.3 (2026-06-23):
- PII encryption at rest. Per-tenant envelope encryption using pgcrypto's
  OpenPGP symmetric primitives. Master KEK lives in FOCMS_KEK_MASTER env
  var (never touches the DB). Per-tenant DEKs are random 256-bit keys
  wrapped with the KEK and stored in `tenant_data_keys`. PII columns
  `first_name_ciphertext`, `middle_name_ciphertext`, `last_name_ciphertext`,
  `birth_date_ciphertext` added to `students`. Existing tenant DEKs are
  provisioned and existing student PII is encrypted on startup (idempotent
  Ã¢â‚¬â€ only encrypts where ciphertext column is NULL). Plaintext columns
  preserved during transition; v0.4.4 will route reads/writes through the
  encrypted columns; v0.4.5 drops the plaintext columns once stable.
- Helper SQL functions exposed: focms_encrypt_pii(tenant, plaintext, kek),
  focms_decrypt_pii(tenant, ciphertext, kek), focms_tenant_dek(tenant, kek),
  focms_provision_tenant_dek(tenant, kek, created_by). All SECURITY DEFINER.
- /focms/v1/health now reports crypto status (kek_set, dek_count).

v0.4.2 (2026-06-23):
- WP CPT porting Phase A (jrj_swim_race cutover): new GET
  /focms/v1/student/{id}/computed/swim-race-log endpoint returns the full
  swim race history sorted by date desc, with parsed details (distance,
  stroke, course, time, meet, team, points, time standard, relay flag).
  Replaces the WP jrj_swim_race CPT as the canonical race log data source
  for johnrjordan.com. WP CPT will be deprecated in a subsequent release
  once the WP page templates are updated to consume this endpoint.

v0.4.1 (2026-06-22):
- Self-contained student data: new GET /focms/v1/student/{id}/computed/swim-bests
  endpoint returns the swim feed schema (bests dict, power_index, standards)
  computed server-side from personal_records. Replaces the per-student bleed
  from the johnrjordan.com hardcoded WP feed.
- Student CRUD: POST/PATCH/DELETE /focms/v1/students endpoints. Lets a parent
  portal create and manage students without writing SQL directly.
- Media storage: media_files table + POST/GET/DELETE /focms/v1/media endpoints.
  Stores photos and small documents as bytea in Postgres with appropriate MIME
  types. Returns URLs that the public showcase pages can reference directly.
  Server-side size cap: 10 MB per upload.

v0.4.0 (2026-06-22):
- Self-migration on startup: creates public_showcases table, grants focms_app
  schema CREATE privilege (one-time bootstrap that lets the MCP role do DDL
  going forward), seeds John's showcase row. All idempotent (CREATE TABLE IF
  NOT EXISTS, ON CONFLICT DO NOTHING) so safe to re-run on every deploy.
- New endpoints:
    GET    /focms/v1/showcase/{slug}            (public showcase lookup; no
                                                 tenant context required since
                                                 slug is global lookup key)
    POST   /focms/v1/public_showcases           (create showcase config row)
    PATCH  /focms/v1/public_showcases/{id}      (modify showcase config)
- This is the architectural change that decouples adding a new student from a
  GitHub push + Render rebuild. Adding tenant N+1 is now just an INSERT into
  public_showcases; the outcomestar Next.js app reads the row at request time.

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
    GET    /focms/v1/student/{student_id}/computed/swim-bests
    GET    /focms/v1/student/{student_id}/computed/swim-race-log

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
from focms_addresses import router as addresses_router
from focms_i18n import router as i18n_router
from focms_parent_portal import router as parent_portal_router
from focms_milestones import router as milestones_router

DATABASE_URL = os.environ.get("DATABASE_URL_POOLED") or os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL_POOLED or DATABASE_URL must be set")
TOKENS = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
LOG_LEVEL = os.environ.get("FOCMS_API_LOG_LEVEL", "INFO")
# v0.4.3: master key encryption key for envelope-encrypted PII. If unset,
# crypto operations (DEK provisioning, PII encrypt/decrypt) are skipped at
# startup and the helper functions return errors when called. Set this on
# Render env vars; the value is a 64-char lowercase hex string (256 bits).
FOCMS_KEK_MASTER = os.environ.get("FOCMS_KEK_MASTER")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("focms-api")


# ---------------------------------------------------------------------------
# Startup migrations (idempotent)
# ---------------------------------------------------------------------------

MIGRATIONS: list[tuple[str, str]] = [
    # Grant focms_app schema CREATE privilege so Claude can do DDL via MCP.
    # This is the one-time bootstrap that ends the "Stephen runs DDL via Render
    # Postgres console" pain. Idempotent Ã¢â‚¬â€ re-running is a no-op.
    (
        "grant_schema_create_to_focms_app",
        "GRANT CREATE, USAGE ON SCHEMA public TO focms_app",
    ),
    # Default privileges: focms_app gets DML on any future table created by
    # focms_user. Means MCP can immediately read/write new tables.
    (
        "default_privileges_tables_to_focms_app",
        "ALTER DEFAULT PRIVILEGES FOR ROLE focms_user IN SCHEMA public "
        "GRANT REFERENCES, SELECT, INSERT, UPDATE, DELETE, TRIGGER ON TABLES TO focms_app",
    ),
    (
        "default_privileges_sequences_to_focms_app",
        "ALTER DEFAULT PRIVILEGES FOR ROLE focms_user IN SCHEMA public "
        "GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO focms_app",
    ),
    # public_showcases Ã¢â‚¬â€ per-student public showcase configuration.
    # Each row maps a URL slug (e.g. "john") to a tenant + student plus
    # display config (theme, photo, tagline). Adding a new student becomes
    # a one-row INSERT instead of a code change + deploy.
    (
        "create_public_showcases",
        """CREATE TABLE IF NOT EXISTS public_showcases (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid_v7(),
            tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            student_id uuid NOT NULL REFERENCES students(id) ON DELETE CASCADE,
            slug text NOT NULL,
            theme_key text NOT NULL DEFAULT 'mission-control',
            display_name text,
            tagline text,
            photo_url text,
            visibility text NOT NULL DEFAULT 'private'
              CHECK (visibility IN ('public','unlisted','private')),
            created_by uuid REFERENCES users(id),
            updated_by uuid REFERENCES users(id),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            deleted_at timestamptz
        )""",
    ),
    (
        "public_showcases_slug_unique",
        "CREATE UNIQUE INDEX IF NOT EXISTS public_showcases_slug_unique "
        "ON public_showcases (slug) WHERE deleted_at IS NULL",
    ),
    (
        "public_showcases_tenant_idx",
        "CREATE INDEX IF NOT EXISTS public_showcases_tenant_idx ON public_showcases (tenant_id)",
    ),
    (
        "public_showcases_student_idx",
        "CREATE INDEX IF NOT EXISTS public_showcases_student_idx ON public_showcases (student_id)",
    ),
    (
        "public_showcases_grant_dml",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON public_showcases TO focms_app",
    ),
    (
        "public_showcases_enable_rls",
        "ALTER TABLE public_showcases ENABLE ROW LEVEL SECURITY",
    ),
    # RLS policy: tenant isolation for writes, but allow lookup by anyone when
    # no tenant context is set (so the public showcase page can resolve slugs
    # across tenants).
    (
        "public_showcases_policy",
        """DO $do$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies
                WHERE tablename = 'public_showcases'
                  AND policyname = 'public_showcases_isolation'
            ) THEN
                CREATE POLICY public_showcases_isolation ON public_showcases
                    USING (
                        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
                        OR NULLIF(current_setting('app.current_tenant_id', true), '') IS NULL
                    );
            END IF;
        END $do$""",
    ),
    # Seed John's showcase row (idempotent Ã¢â‚¬â€ ON CONFLICT DO NOTHING).
    # This is what makes outcomestar.app/john keep working after page.tsx
    # drops its hardcoded TENANTS constant.
    # Media storage: bytea blobs with MIME type. Sized for portraits and small
    # PDFs (Mindprint reports, transcripts, etc). Larger artifacts later move
    # to S3/R2 via a separate code path.
    (
        "create_media_files",
        """CREATE TABLE IF NOT EXISTS media_files (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid_v7(),
            tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            student_id uuid REFERENCES students(id) ON DELETE SET NULL,
            kind text NOT NULL DEFAULT 'image'
              CHECK (kind IN ('image','document','video','other')),
            mime_type text NOT NULL,
            original_filename text,
            byte_size integer NOT NULL,
            content bytea NOT NULL,
            visibility text NOT NULL DEFAULT 'private'
              CHECK (visibility IN ('public','unlisted','private')),
            sha256_hex text,
            created_by uuid REFERENCES users(id),
            created_at timestamptz NOT NULL DEFAULT now(),
            deleted_at timestamptz
        )""",
    ),
    (
        "media_files_tenant_idx",
        "CREATE INDEX IF NOT EXISTS media_files_tenant_idx ON media_files (tenant_id)",
    ),
    (
        "media_files_student_idx",
        "CREATE INDEX IF NOT EXISTS media_files_student_idx ON media_files (student_id)",
    ),
    (
        "media_files_grant_dml",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON media_files TO focms_app",
    ),
    (
        "media_files_enable_rls",
        "ALTER TABLE media_files ENABLE ROW LEVEL SECURITY",
    ),
    (
        "media_files_policy",
        """DO $do$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies
                WHERE tablename = 'media_files'
                  AND policyname = 'media_files_isolation'
            ) THEN
                CREATE POLICY media_files_isolation ON media_files
                    USING (
                        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid
                        OR NULLIF(current_setting('app.current_tenant_id', true), '') IS NULL
                    );
            END IF;
        END $do$""",
    ),
    (
        "seed_john_showcase",
        """INSERT INTO public_showcases (
            tenant_id, student_id, slug, theme_key, display_name,
            tagline, photo_url, visibility
        ) VALUES (
            '019ed384-56fc-7516-bfbf-efaa5231e281'::uuid,
            '019ed384-5769-72ca-864a-28e40c4e5d30'::uuid,
            'john',
            'mission-control',
            'John Ray Jordan',
            'Future astronaut Ã‚Â· breaststroke specialist',
            'https://johnrjordan.com/wp-content/uploads/2026/05/john-at-the-cotillion-Ball-03292026-2-2-scaled.jpg',
            'public'
        ) ON CONFLICT DO NOTHING""",
    ),
    # ----------------------------------------------------------------------
    # v0.4.3 Ã¢â‚¬â€ per-tenant envelope encryption for PII at rest
    # ----------------------------------------------------------------------
    # tenant_data_keys: stores per-tenant DEKs (data encryption keys) wrapped
    # with the master KEK (key encryption key). One row per tenant. The
    # wrapped DEK is useless without the KEK which lives only in env vars.
    (
        "crypto_pgcrypto_extension",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto",
    ),
    (
        "crypto_create_tenant_data_keys",
        """CREATE TABLE IF NOT EXISTS tenant_data_keys (
            tenant_id       uuid PRIMARY KEY,
            dek_wrapped     bytea NOT NULL,
            algorithm       text NOT NULL DEFAULT 'aes256/openpgp-symmetric',
            kek_version     integer NOT NULL DEFAULT 1,
            key_version     integer NOT NULL DEFAULT 1,
            created_at      timestamptz NOT NULL DEFAULT now(),
            created_by      uuid NOT NULL,
            rotated_at      timestamptz,
            rotated_by      uuid,
            notes           text
        )""",
    ),
    (
        "crypto_grant_app_dml_tenant_data_keys",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_data_keys TO focms_app",
    ),
    # Helper functions. All SECURITY DEFINER so the calling role doesn't need
    # direct grants on tenant_data_keys Ã¢â‚¬â€ only EXECUTE on the function.
    # KEK is passed explicitly on every call (no session GUC dependency) so
    # the helpers work identically from API, MCP, or psql.
    (
        "crypto_fn_tenant_dek",
        """CREATE OR REPLACE FUNCTION focms_tenant_dek(p_tenant_id uuid, p_kek text)
        RETURNS text
        LANGUAGE sql STABLE
        SECURITY DEFINER
        AS $crypto_fn$
          SELECT pgp_sym_decrypt(
            (SELECT dek_wrapped FROM tenant_data_keys WHERE tenant_id = p_tenant_id),
            p_kek
          )
        $crypto_fn$""",
    ),
    (
        "crypto_fn_encrypt_pii",
        """CREATE OR REPLACE FUNCTION focms_encrypt_pii(p_tenant_id uuid, p_plaintext text, p_kek text)
        RETURNS bytea
        LANGUAGE sql STABLE
        SECURITY DEFINER
        AS $crypto_fn$
          SELECT CASE
            WHEN p_plaintext IS NULL THEN NULL
            ELSE pgp_sym_encrypt(p_plaintext, focms_tenant_dek(p_tenant_id, p_kek))
          END
        $crypto_fn$""",
    ),
    (
        "crypto_fn_decrypt_pii",
        """CREATE OR REPLACE FUNCTION focms_decrypt_pii(p_tenant_id uuid, p_ciphertext bytea, p_kek text)
        RETURNS text
        LANGUAGE sql STABLE
        SECURITY DEFINER
        AS $crypto_fn$
          SELECT CASE
            WHEN p_ciphertext IS NULL THEN NULL
            ELSE pgp_sym_decrypt(p_ciphertext, focms_tenant_dek(p_tenant_id, p_kek))
          END
        $crypto_fn$""",
    ),
    (
        "crypto_fn_provision_tenant_dek",
        """CREATE OR REPLACE FUNCTION focms_provision_tenant_dek(p_tenant_id uuid, p_kek text, p_created_by uuid)
        RETURNS uuid
        LANGUAGE sql
        SECURITY DEFINER
        AS $crypto_fn$
          WITH new_dek AS (SELECT encode(gen_random_bytes(32), 'hex') AS dek_text)
          INSERT INTO tenant_data_keys (tenant_id, dek_wrapped, created_by)
          SELECT p_tenant_id, pgp_sym_encrypt(dek_text, p_kek), p_created_by FROM new_dek
          ON CONFLICT (tenant_id) DO NOTHING
          RETURNING tenant_id
        $crypto_fn$""",
    ),
    # Students PII ciphertext columns. Plaintext columns kept during transition.
    # v0.4.4 will route API reads/writes through the ciphertext columns;
    # v0.4.5 drops the plaintext columns once stable.
    (
        "crypto_alter_students_add_first_name_ct",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS first_name_ciphertext bytea",
    ),
    (
        "crypto_alter_students_add_middle_name_ct",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS middle_name_ciphertext bytea",
    ),
    (
        "crypto_alter_students_add_last_name_ct",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS last_name_ciphertext bytea",
    ),
    (
        "crypto_alter_students_add_birth_date_ct",
        "ALTER TABLE students ADD COLUMN IF NOT EXISTS birth_date_ciphertext bytea",
    ),
]


async def run_migrations(pool: asyncpg.Pool) -> None:
    """Run idempotent startup migrations.

    Each statement is wrapped in its own try/except so one failure does not
    block the rest from running. Failures are logged at WARNING level. The
    service continues to start even if migrations partially fail Ã¢â‚¬â€ the
    existing endpoints still work against the existing schema.
    """
    async with pool.acquire() as conn:
        # Detect what user we are so we know whether DDL is expected to work.
        try:
            current = await conn.fetchval("SELECT current_user")
            log.info("migrations: running as user=%s", current)
        except Exception as exc:
            log.warning("migrations: could not determine current_user: %s", exc)
            current = "unknown"

        success_count = 0
        skip_count = 0
        for name, sql in MIGRATIONS:
            try:
                await conn.execute(sql)
                log.info("migration ok: %s", name)
                success_count += 1
            except Exception as exc:
                # Common skip causes: insufficient privilege (when running as
                # a non-owner role), or object already exists in a non-IF-NOT-EXISTS
                # form. Log and continue.
                log.warning("migration skipped: %s: %s", name, exc)
                skip_count += 1
        log.info("migrations complete: %d ok, %d skipped", success_count, skip_count)


async def run_crypto_setup(pool: asyncpg.Pool) -> None:
    """v0.4.3: provision per-tenant DEKs and backfill students PII ciphertext.

    Idempotent. Only does work where ciphertext is NULL or the tenant has no
    DEK yet. Safe to re-run on every deploy. If FOCMS_KEK_MASTER is not set,
    this skips entirely (and logs a warning); the rest of the API still
    starts and serves traffic against the plaintext columns.
    """
    if not FOCMS_KEK_MASTER:
        log.warning("crypto: FOCMS_KEK_MASTER not set; skipping DEK provisioning + PII backfill")
        return

    async with pool.acquire() as conn:
        # Provision DEKs for any tenants without one. created_by is Stephen
        # for now (the platform admin); when a self-serve onboarding flow
        # exists, that user_id will replace this.
        try:
            stephen_uuid = "019ed384-56d8-77fb-bfe6-00b1d064da18"
            tenants_needing = await conn.fetch("""
                SELECT t.id FROM tenants t
                LEFT JOIN tenant_data_keys k ON k.tenant_id = t.id
                WHERE k.tenant_id IS NULL
            """)
            provisioned = 0
            for r in tenants_needing:
                # Use the provision function so the algorithm + key length
                # stay consistent with any future call sites.
                res = await conn.fetchval(
                    "SELECT focms_provision_tenant_dek($1, $2, $3::uuid)",
                    r["id"], FOCMS_KEK_MASTER, stephen_uuid,
                )
                if res is not None:
                    provisioned += 1
            log.info("crypto: provisioned %d new tenant DEK(s)", provisioned)
        except Exception as exc:
            log.warning("crypto: DEK provisioning failed: %s", exc)
            return

        # Backfill students PII ciphertext for any rows where the ciphertext
        # column is NULL. One UPDATE handles all four columns; cast the date
        # to text before encrypting so we can round-trip through pgp_sym.
        # The WHERE clause makes this idempotent Ã¢â‚¬â€ re-runs are no-ops once
        # ciphertext is populated.
        try:
            result = await conn.execute("""
                UPDATE students SET
                    first_name_ciphertext  = focms_encrypt_pii(tenant_id, first_name, $1),
                    middle_name_ciphertext = focms_encrypt_pii(tenant_id, middle_name, $1),
                    last_name_ciphertext   = focms_encrypt_pii(tenant_id, last_name, $1),
                    birth_date_ciphertext  = focms_encrypt_pii(tenant_id, birth_date::text, $1),
                    updated_at = now()
                WHERE deleted_at IS NULL
                  AND first_name_ciphertext IS NULL
            """, FOCMS_KEK_MASTER)
            log.info("crypto: students PII backfill - %s", result)
        except Exception as exc:
            log.warning("crypto: students backfill failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """asyncpg pool. PgBouncer transaction mode requires statement_cache_size=0."""
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, statement_cache_size=0,
    )
    log.info("DB pool ready")
    await run_migrations(app.state.pool)
    await run_crypto_setup(app.state.pool)
    try:
        yield
    finally:
        await app.state.pool.close()
        log.info("DB pool closed")


app = FastAPI(title="FOCMS Data Provider API", version="0.7.0", lifespan=lifespan)
# CORS - parent portal frontend at outcomestar.app
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://outcomestar.app",
        "https://www.outcomestar.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(addresses_router)
app.include_router(i18n_router)
app.include_router(parent_portal_router)
app.include_router(milestones_router)

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
async def health(request: Request) -> dict[str, Any]:
    """Service health + crypto wiring sanity check.

    The crypto subsection lets ops verify after deploy that the KEK env var
    is configured and per-tenant DEKs are in place. Returns counts only Ã¢â‚¬â€
    never any key material.
    """
    payload: dict[str, Any] = {"status": "ok", "version": "0.4.3"}
    crypto: dict[str, Any] = {"kek_set": FOCMS_KEK_MASTER is not None}
    try:
        async with request.app.state.pool.acquire() as conn:
            crypto["dek_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM tenant_data_keys"
            )
            crypto["students_encrypted"] = await conn.fetchval(
                "SELECT COUNT(*) FROM students "
                "WHERE deleted_at IS NULL AND first_name_ciphertext IS NOT NULL"
            )
            crypto["students_unencrypted"] = await conn.fetchval(
                "SELECT COUNT(*) FROM students "
                "WHERE deleted_at IS NULL AND first_name_ciphertext IS NULL"
            )
    except Exception as exc:
        crypto["error"] = f"{type(exc).__name__}: {exc}"
    payload["crypto"] = crypto
    return payload


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
# v0.3.0 Ã¢â‚¬â€ Write surface
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




# ===========================================================================
# v0.4.0 Ã¢â‚¬â€ public_showcases endpoints
# ===========================================================================


class PublicShowcaseCreate(BaseModel):
    """Body for POST /focms/v1/public_showcases.

    `slug` becomes the URL segment in outcomestar.app/{slug}.
    Constrained to lowercase alphanumeric + hyphen to keep URLs clean.
    """
    model_config = ConfigDict(extra="forbid")

    student_id: UUID
    slug: str = Field(..., min_length=1, max_length=64, pattern=r"^[a-z0-9-]+$")
    theme_key: Optional[str] = None
    display_name: Optional[str] = None
    tagline: Optional[str] = None
    photo_url: Optional[str] = None
    visibility: Optional[str] = None


class PublicShowcasePatch(BaseModel):
    """Body for PATCH /focms/v1/public_showcases/{id}. All fields optional."""
    model_config = ConfigDict(extra="forbid")

    slug: Optional[str] = Field(None, min_length=1, max_length=64, pattern=r"^[a-z0-9-]+$")
    theme_key: Optional[str] = None
    display_name: Optional[str] = None
    tagline: Optional[str] = None
    photo_url: Optional[str] = None
    visibility: Optional[str] = None


@app.get("/focms/v1/public_showcases")
async def list_public_showcases(
    request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """List all showcase configs for the authenticated tenant. Used by the
    admin UI to show what's currently published.

    Includes private+unlisted+public, since the admin needs full visibility.
    """
    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch(
            """
            SELECT s.id, s.slug, s.student_id, s.theme_key, s.display_name,
                   s.tagline, s.photo_url, s.visibility, s.created_at, s.updated_at,
                   st.first_name, st.last_name, st.current_grade
            FROM public_showcases s
            LEFT JOIN students st ON st.id = s.student_id
            WHERE s.deleted_at IS NULL
            ORDER BY s.created_at DESC
            """
        )
    return {
        "count": len(rows),
        "showcases": [
            {
                "id": str(r["id"]),
                "slug": r["slug"],
                "student_id": str(r["student_id"]),
                "student_name": f'{r["first_name"]} {r["last_name"]}' if r["first_name"] else None,
                "current_grade": r["current_grade"],
                "theme_key": r["theme_key"],
                "display_name": r["display_name"],
                "tagline": r["tagline"],
                "photo_url": r["photo_url"],
                "visibility": r["visibility"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ],
    }


@app.get("/focms/v1/students")
async def list_students(
    request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """List all students for the authenticated tenant. Used by the admin UI."""
    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch(
            """
            SELECT id, first_name, last_name, display_name, preferred_name,
                   current_grade, residence_state, expected_hs_graduation_year,
                   birth_date, headline, created_at
            FROM students
            WHERE deleted_at IS NULL
            ORDER BY created_at DESC
            """
        )
    return {
        "count": len(rows),
        "students": [
            {
                "id": str(r["id"]),
                "first_name": r["first_name"],
                "last_name": r["last_name"],
                "display_name": r["display_name"],
                "preferred_name": r["preferred_name"],
                "current_grade": r["current_grade"],
                "residence_state": r["residence_state"],
                "expected_hs_graduation_year": r["expected_hs_graduation_year"],
                "birth_date": r["birth_date"].isoformat() if r["birth_date"] else None,
                "headline": r["headline"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


@app.get("/focms/v1/showcase/{slug}")
async def get_showcase_by_slug(
    slug: str, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Look up a public showcase config by URL slug.

    Used by the outcomestar Next.js page router. Slug lookup is the entry
    point for the public showcase Ã¢â‚¬â€ the returned tenant_id + student_id is
    then used to fetch the rest of the student data via tenant-scoped
    endpoints.

    Unlike most endpoints this does not set the tenant context (slug is the
    lookup key, not tenant). RLS policy allows lookup when no context is
    set. Returns 404 when the showcase is private or missing.
    """
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, slug, tenant_id, student_id, theme_key,
                   display_name, tagline, photo_url, visibility
            FROM public_showcases
            WHERE slug = $1
              AND deleted_at IS NULL
              AND visibility != 'private'
            """,
            slug,
        )
        if not row:
            raise HTTPException(404, f"Showcase '{slug}' not found")
        return {
            "id": str(row["id"]),
            "slug": row["slug"],
            "tenant_id": str(row["tenant_id"]),
            "student_id": str(row["student_id"]),
            "theme_key": row["theme_key"],
            "display_name": row["display_name"],
            "tagline": row["tagline"],
            "photo_url": row["photo_url"],
            "visibility": row["visibility"],
        }


@app.post("/focms/v1/public_showcases", status_code=201)
async def create_public_showcase(
    body: PublicShowcaseCreate,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Create a public showcase row for a student.

    The slug must be globally unique (URL keys cannot collide across
    tenants). Returns 409 on conflict.
    """
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO public_showcases (
                    tenant_id, student_id, slug, theme_key, display_name,
                    tagline, photo_url, visibility, created_by
                ) VALUES (
                    $1, $2, $3, COALESCE($4, 'mission-control'), $5,
                    $6, $7, COALESCE($8, 'private'), $9
                )
                RETURNING *
                """,
                UUID(tenant_id), body.student_id, body.slug,
                body.theme_key, body.display_name,
                body.tagline, body.photo_url, body.visibility,
                UUID(user_id),
            )
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(
                409, f"Slug '{body.slug}' is already taken"
            ) from exc
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        except asyncpg.ForeignKeyViolationError as exc:
            raise HTTPException(400, f"Referenced row missing: {exc.detail or exc}") from exc
        result = _row_to_dict(row)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="create",
            target_table="public_showcases",
            target_id=result["id"],
            target_label=body.slug,
            new_value={
                "slug": body.slug,
                "theme_key": body.theme_key,
                "visibility": body.visibility,
            },
        )
    return result


@app.patch("/focms/v1/public_showcases/{showcase_id}")
async def patch_public_showcase(
    showcase_id: UUID,
    body: PublicShowcasePatch,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Update fields on a showcase. Only fields present in the body are
    written. Returns the updated row."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(400, "No fields to update")

    set_parts = []
    params: list[Any] = []
    for i, (col, val) in enumerate(fields.items(), start=1):
        set_parts.append(f"{col} = ${i}")
        params.append(val)
    set_parts.append(f"updated_by = ${len(params) + 1}")
    params.append(UUID(user_id))
    set_parts.append("updated_at = now()")
    params.append(showcase_id)
    where_param = f"${len(params)}"

    sql = f"""
        UPDATE public_showcases
        SET {", ".join(set_parts)}
        WHERE id = {where_param} AND deleted_at IS NULL
        RETURNING *
    """

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            raise HTTPException(409, "Slug already taken") from exc
        if not row:
            raise HTTPException(404, "Showcase not found")
        result = _row_to_dict(row)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="update",
            target_table="public_showcases",
            target_id=str(showcase_id),
            new_value={k: (str(v) if isinstance(v, UUID) else v) for k, v in fields.items()},
        )
    return result


@app.delete("/focms/v1/public_showcases/{showcase_id}", status_code=204)
async def delete_public_showcase(
    showcase_id: UUID, request: Request,
    principal: dict = Depends(require_write),
):
    """Soft-delete a showcase. Returns 204."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            UPDATE public_showcases
            SET deleted_at = now(), updated_by = $1, updated_at = now()
            WHERE id = $2 AND deleted_at IS NULL
            RETURNING id
            """,
            UUID(user_id), showcase_id,
        )
        if not row:
            raise HTTPException(404, "Showcase not found or already deleted")
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action="delete",
            target_table="public_showcases", target_id=str(showcase_id),
        )
    return JSONResponse(status_code=204, content=None)



# ===========================================================================
# v0.4.1 Ã¢â‚¬â€ students CRUD
# ===========================================================================


class StudentCreate(BaseModel):
    """Body for POST /focms/v1/students. Mirrors the students table's required
    columns plus the common optional ones."""
    model_config = ConfigDict(extra="forbid")

    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    birth_date: date
    preferred_name: Optional[str] = None
    middle_name: Optional[str] = None
    pronouns: Optional[str] = None
    current_grade: Optional[str] = None
    expected_hs_graduation_year: Optional[int] = None
    residence_state: Optional[str] = None
    residence_country: Optional[str] = None
    current_school_leaid: Optional[str] = None
    birth_country: Optional[str] = None
    primary_citizenship: Optional[str] = None
    secondary_citizenship: Optional[str] = None
    headline: Optional[str] = None
    bio: Optional[str] = None


class StudentPatch(BaseModel):
    """Body for PATCH /focms/v1/students/{id}. All fields optional."""
    model_config = ConfigDict(extra="forbid")

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    preferred_name: Optional[str] = None
    middle_name: Optional[str] = None
    pronouns: Optional[str] = None
    birth_date: Optional[date] = None
    current_grade: Optional[str] = None
    expected_hs_graduation_year: Optional[int] = None
    residence_state: Optional[str] = None
    residence_country: Optional[str] = None
    current_school_leaid: Optional[str] = None
    headline: Optional[str] = None
    bio: Optional[str] = None


@app.post("/focms/v1/students", status_code=201)
async def create_student(
    body: StudentCreate,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Create a new student record under the authenticated tenant."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO students (
                    tenant_id, first_name, last_name, display_name, preferred_name,
                    middle_name, pronouns, birth_date, current_grade,
                    expected_hs_graduation_year, residence_state, residence_country,
                    current_school_leaid, birth_country, primary_citizenship,
                    secondary_citizenship, headline, bio, created_by
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14, $15, $16, $17, $18, $19
                )
                RETURNING *
                """,
                UUID(tenant_id), body.first_name, body.last_name, body.display_name,
                body.preferred_name, body.middle_name, body.pronouns, body.birth_date,
                body.current_grade, body.expected_hs_graduation_year,
                body.residence_state, body.residence_country,
                body.current_school_leaid, body.birth_country,
                body.primary_citizenship, body.secondary_citizenship,
                body.headline, body.bio, UUID(user_id),
            )
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        result = _row_to_dict(row)
        await write_audit(
            conn,
            actor_user_id=user_id,
            actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id,
            action="create",
            target_table="students",
            target_id=result["id"],
            target_label=body.display_name,
            new_value={"display_name": body.display_name, "grade": body.current_grade},
        )
    return result


@app.patch("/focms/v1/students/{student_id}")
async def patch_student(
    student_id: UUID,
    body: StudentPatch,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Update fields on a student. Only fields present in the body are written."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(400, "No fields to update")

    set_parts = []
    params: list[Any] = []
    for i, (col, val) in enumerate(fields.items(), start=1):
        set_parts.append(f"{col} = ${i}")
        params.append(val)
    set_parts.append(f"updated_by = ${len(params) + 1}")
    params.append(UUID(user_id))
    set_parts.append("updated_at = now()")
    params.append(student_id)
    where_param = f"${len(params)}"

    sql = f"""
        UPDATE students
        SET {", ".join(set_parts)}
        WHERE id = {where_param} AND deleted_at IS NULL
        RETURNING *
    """

    async with tx(request, tenant_id) as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.exceptions.InvalidTextRepresentationError as exc:
            raise HTTPException(400, f"Invalid value: {exc}") from exc
        if not row:
            raise HTTPException(404, "Student not found")
        result = _row_to_dict(row)
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action="update", target_table="students",
            target_id=str(student_id),
            new_value={k: (str(v) if isinstance(v, (UUID, date, datetime)) else v) for k, v in fields.items()},
        )
    return result


@app.delete("/focms/v1/students/{student_id}", status_code=204)
async def delete_student(
    student_id: UUID, request: Request,
    principal: dict = Depends(require_write),
):
    """Soft-delete a student. Also soft-deletes any public_showcases pointing
    at the student so URLs go to 404 immediately."""
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            UPDATE students
            SET deleted_at = now(), updated_by = $1, updated_at = now()
            WHERE id = $2 AND deleted_at IS NULL
            RETURNING id
            """,
            UUID(user_id), student_id,
        )
        if not row:
            raise HTTPException(404, "Student not found or already deleted")
        # Cascade to showcases
        await conn.execute(
            "UPDATE public_showcases SET deleted_at = now() WHERE student_id = $1 AND deleted_at IS NULL",
            student_id,
        )
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action="delete", target_table="students",
            target_id=str(student_id),
        )
    return JSONResponse(status_code=204, content=None)


# ===========================================================================
# v0.4.1 Ã¢â‚¬â€ student-scoped swim bests feed (replaces hardcoded WP feed)
# ===========================================================================

USA_SWIMMING_TIERS_2024_2028_BOYS_11_12 = {
    # Time standards in seconds. SCY = Short Course Yards, LCM = Long Course Meters.
    # Tiers: AAAA (elite) > AAA > AA > A > BB > B (slowest)
    # Used by Mission Control performance log and the recompute() logic in the
    # WP-era hydrator. Embedded here so the feed is self-contained.
    "SCY": {
        "50 Free":   {"AAAA": 25.69, "AAA": 26.39, "AA": 27.39, "A": 28.49, "BB": 30.09, "B": 32.49},
        "100 Free":  {"AAAA": 56.49, "AAA": 58.09, "AA": 60.39, "A": 62.69, "BB": 66.29, "B": 71.69},
        "200 Free":  {"AAAA": 122.49,"AAA": 125.99,"AA": 130.79,"A": 135.69,"BB": 143.49,"B": 154.99},
        "500 Free":  {"AAAA": 326.99,"AAA": 336.29,"AA": 349.09,"A": 361.99,"BB": 382.69,"B": 413.59},
        "100 Back":  {"AAAA": 64.69, "AAA": 66.59, "AA": 69.19, "A": 71.79, "BB": 75.99, "B": 82.09},
        "200 Back":  {"AAAA": 140.39,"AAA": 144.49,"AA": 150.09,"A": 155.69,"BB": 164.69,"B": 177.99},
        "100 Breast":{"AAAA": 72.99, "AAA": 75.09, "AA": 78.09, "A": 81.09, "BB": 85.79, "B": 92.69},
        "200 Breast":{"AAAA": 156.49,"AAA": 161.09,"AA": 167.29,"A": 173.59,"BB": 183.59,"B": 198.39},
        "100 Fly":   {"AAAA": 63.09, "AAA": 64.99, "AA": 67.49, "A": 70.09, "BB": 74.19, "B": 80.19},
        "200 Fly":   {"AAAA": 142.59,"AAA": 146.69,"AA": 152.39,"A": 158.19,"BB": 167.39,"B": 180.79},
        "100 IM":    {"AAAA": 65.49, "AAA": 67.49, "AA": 70.09, "A": 72.69, "BB": 76.99, "B": 83.19},
        "200 IM":    {"AAAA": 141.79,"AAA": 145.89,"AA": 151.59,"A": 157.29,"BB": 166.49,"B": 179.79},
        "400 IM":    {"AAAA": 304.49,"AAA": 313.29,"AA": 325.39,"A": 337.59,"BB": 357.09,"B": 385.79},
    },
    "LCM": {
        "50 Free":   {"AAAA": 28.99, "AAA": 29.79, "AA": 30.99, "A": 32.19, "BB": 33.99, "B": 36.69},
        "100 Free":  {"AAAA": 63.79, "AAA": 65.69, "AA": 68.19, "A": 70.79, "BB": 74.79, "B": 80.99},
        "200 Free":  {"AAAA": 138.29,"AAA": 142.29,"AA": 147.69,"A": 153.19,"BB": 162.09,"B": 174.99},
        "400 Free":  {"AAAA": 291.89,"AAA": 300.19,"AA": 311.49,"A": 322.99,"BB": 341.59,"B": 369.09},
        "100 Back":  {"AAAA": 73.59, "AAA": 75.79, "AA": 78.69, "A": 81.59, "BB": 86.29, "B": 93.29},
        "200 Back":  {"AAAA": 159.69,"AAA": 164.39,"AA": 170.69,"A": 177.09,"BB": 187.39,"B": 202.49},
        "100 Breast":{"AAAA": 82.49, "AAA": 84.89, "AA": 88.19, "A": 91.59, "BB": 96.89, "B": 104.69},
        "200 Breast":{"AAAA": 177.49,"AAA": 182.69,"AA": 189.69,"A": 196.79,"BB": 208.19,"B": 224.99},
        "100 Fly":   {"AAAA": 71.59, "AAA": 73.69, "AA": 76.49, "A": 79.39, "BB": 84.09, "B": 90.79},
        "200 Fly":   {"AAAA": 162.89,"AAA": 167.59,"AA": 174.09,"A": 180.69,"BB": 191.19,"B": 206.49},
        "200 IM":    {"AAAA": 161.49,"AAA": 166.19,"AA": 172.69,"A": 179.19,"BB": 189.69,"B": 204.79},
        "400 IM":    {"AAAA": 346.79,"AAA": 356.79,"AA": 370.49,"A": 384.39,"BB": 406.59,"B": 439.19},
    },
}


def _next_standard(seconds: float, standards: dict[str, float]) -> tuple[Optional[str], Optional[float]]:
    """Return the next-faster tier above the current achieved tier."""
    tier_order = ["B", "BB", "A", "AA", "AAA", "AAAA"]
    # Sort standards from slowest to fastest (highest seconds to lowest)
    sorted_tiers = [(t, standards.get(t)) for t in tier_order if standards.get(t)]
    sorted_tiers.sort(key=lambda x: -x[1])  # slowest first
    achieved = None
    next_std = None
    next_time = None
    for tier, std_time in sorted_tiers:
        if seconds <= std_time:
            achieved = tier
        else:
            # Faster than current; next standard up
            if achieved is None:
                next_std = tier
                next_time = std_time
                break
    if achieved:
        # Find next tier faster than achieved
        for tier, std_time in [(t, standards.get(t)) for t in tier_order if standards.get(t)]:
            if std_time < (standards.get(achieved) or float("inf")):
                if next_std is None or std_time > (next_time or 0):
                    next_std = tier
                    next_time = std_time
    return achieved, next_time


@app.get("/focms/v1/student/{student_id}/computed/swim-bests")
async def get_swim_bests_feed(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    """Return the swim feed payload for the student (replaces the WP feed).

    Same JSON schema as johnrjordan.com/focms-feed-swim-bests/ so the outcomestar
    theme code doesn't need to change: a `bests` dict keyed by "DIST STROKE COURSE",
    optional `power_index`, and `standards` reference. Computed from
    `personal_records` where record_kind='swim_best'.
    """
    async with tx(request, principal["tenant_id"]) as conn:
        records = await conn.fetch("""
            SELECT title, value_numeric, achieved_date,
                   prior_value_numeric, total_drop_numeric, details
            FROM personal_records
            WHERE student_id = $1 AND record_kind = 'swim_best'
              AND deleted_at IS NULL AND value_numeric IS NOT NULL
            ORDER BY title
        """, student_id)

    standards_scy = USA_SWIMMING_TIERS_2024_2028_BOYS_11_12["SCY"]
    standards_lcm = USA_SWIMMING_TIERS_2024_2028_BOYS_11_12["LCM"]
    bests: dict[str, Any] = {}
    for r in records:
        title = r["title"]
        seconds = float(r["value_numeric"])
        # Parse title like "100 Breast SCY"
        parts = title.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        event, course = parts
        std_table = standards_scy if course == "SCY" else (standards_lcm if course == "LCM" else None)
        if std_table is None:
            continue
        std_for_event = std_table.get(event, {})
        achieved_std, next_time = _next_standard(seconds, std_for_event)
        bests[title] = {
            "time": _format_time(seconds),
            "seconds": seconds,
            "date": r["achieved_date"].isoformat() if r["achieved_date"] else None,
            "usa_standard": achieved_std,
            "next_std": _tier_above(achieved_std, std_for_event),
            "next_time_seconds": next_time,
        }

    # Also call into the existing power-index logic for the same student
    pi_payload: Optional[dict] = None
    try:
        # Re-use the same logic the dedicated endpoint uses
        async with tx(request, principal["tenant_id"]) as conn:
            base_row = await conn.fetchrow("""
                SELECT detail, source_url, archive_date, source_id
                FROM archive_entries
                WHERE archive_type = 'reference_data'
                  AND source_id = 'ncaa_d1_men_2026_qualifying_standards_scy'
                LIMIT 1
            """)
            if base_row and base_row["detail"]:
                base_payload = json.loads(base_row["detail"])
                base_scy = base_payload["events"]
                lcm_factor = base_payload["conversion_to_lcm_factor"]
                base_year = base_payload["effective_year"]
                bests_rows = await conn.fetch("""
                    SELECT title, value_numeric AS seconds
                    FROM personal_records
                    WHERE student_id = $1 AND record_kind = 'swim_best'
                      AND deleted_at IS NULL AND value_numeric IS NOT NULL
                """, student_id)
                pps = []
                for r in bests_rows:
                    event, course = _split_event_course(r["title"])
                    pp = _compute_pp(float(r["seconds"]), event, course, base_scy, lcm_factor)
                    if pp is not None:
                        pps.append((r["title"], pp))
                pps.sort(key=lambda x: x[1])
                top4 = pps[:4]
                pi_value = None
                if top4:
                    weights = PI_WEIGHTS[: len(top4)]
                    total_w = sum(weights)
                    pi_value = round(sum(top4[i][1] * weights[i] for i in range(len(top4))) / total_w, 1)
                pi_payload = {
                    "value": pi_value,
                    "top_4": [{"event_course": l, "pp": p} for l, p in top4],
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
                }
    except Exception as exc:
        log.warning("PI compute failed for student %s: %s", student_id, exc)

    return {
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "focms_api",
            "student_id": str(student_id),
            "tenant_id": principal["tenant_id"],
            "row_count": len(bests),
            "schema_version": "1.1",
        },
        "bests": bests,
        "standards": {
            "usa_swimming_2024_2028_boys_11_12": USA_SWIMMING_TIERS_2024_2028_BOYS_11_12,
            "tier_order_fastest_to_slowest": ["AAAA","AAA","AA","A","BB","B"],
            "below_b_label": "Slower than B",
        },
        "power_index": pi_payload,
    }


def _format_time(seconds: float) -> str:
    """Format seconds as MM:SS.HH if >= 60, else SS.HH."""
    if seconds >= 60:
        m = int(seconds // 60)
        s = seconds - m * 60
        return f"{m}:{s:05.2f}"
    return f"{seconds:.2f}"


def _tier_above(achieved: Optional[str], standards: dict[str, float]) -> Optional[str]:
    """Given the current tier label, return the next-faster tier label that's
    actually defined in the standards table."""
    tier_order = ["B", "BB", "A", "AA", "AAA", "AAAA"]
    if not achieved:
        # Return slowest defined
        for t in tier_order:
            if standards.get(t):
                return t
        return None
    try:
        idx = tier_order.index(achieved)
    except ValueError:
        return None
    for t in tier_order[idx + 1:]:
        if standards.get(t):
            return t
    return None


# ===========================================================================
# v0.4.2 Ã¢â‚¬â€ student-scoped swim race log feed (replaces WP jrj_swim_race CPT)
@app.get("/focms/v1/student/{student_id}/computed/swim-race-log")
async def get_swim_race_log_feed(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
    limit: int = 1000,
    course: Optional[str] = None,
    stroke: Optional[str] = None,
    since: Optional[str] = None,
) -> dict[str, Any]:
    """Return the swim race log feed for the student (replaces WP jrj_swim_race CPT).

    Returns all swim_race events sorted by event_date desc, with parsed details
    (distance, stroke, course, time, meet, team, points, time standard, relay flag).

    Optional query params:
      - limit: max rows (default 1000)
      - course: filter by SCY / SCM / LCM
      - stroke: filter by FR / BK / BR / FL / IM
      - since: ISO date (YYYY-MM-DD), only races on or after this date
    """
    where_clauses = [
        "student_id = $1",
        "event_type = 'swim_race'",
        "deleted_at IS NULL",
    ]
    params: list[Any] = [student_id]

    if course:
        params.append(course.upper())
        where_clauses.append(f"details->>'course' = ${len(params)}")
    if stroke:
        params.append(stroke.upper())
        where_clauses.append(f"details->>'stroke' = ${len(params)}")
    if since:
        try:
            since_d = date.fromisoformat(since)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"invalid since date: {since!r}")
        params.append(since_d)
        where_clauses.append(f"event_date >= ${len(params)}")

    params.append(int(limit))
    limit_param = f"${len(params)}"

    sql = f"""
        SELECT source_id, title, event_date,
               details::text AS details_json,
               source_system, visibility
        FROM events
        WHERE {' AND '.join(where_clauses)}
        ORDER BY event_date DESC, source_id DESC
        LIMIT {limit_param}
    """

    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch(sql, *params)

    races = []
    for r in rows:
        d = json.loads(r["details_json"]) if r["details_json"] else {}
        time_str = d.get("swim_time") or ""
        is_relay = isinstance(time_str, str) and time_str.endswith("r")
        time_clean = time_str.rstrip("r") if isinstance(time_str, str) else time_str
        races.append({
            "source_id": r["source_id"],
            "date": r["event_date"].isoformat() if r["event_date"] else None,
            "title": r["title"],
            "distance_m": d.get("distance_m"),
            "stroke": d.get("stroke"),
            "course": d.get("course"),
            "time": time_clean,
            "is_relay_leg": is_relay,
            "meet": d.get("meet"),
            "team": d.get("team"),
            "lsc": d.get("lsc"),
            "age": d.get("age"),
            "points": d.get("points"),
            "time_standard": d.get("time_standard"),
            "source_system": r["source_system"],
            "visibility": r["visibility"],
        })

    return {
        "student_id": str(student_id),
        "total": len(races),
        "races": races,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "source_system": "focms_api/events",
        "filters": {
            "course": course,
            "stroke": stroke,
            "since": since,
            "limit": limit,
        },
    }


# v0.4.1 Ã¢â‚¬â€ media storage (binary blobs in Postgres)
# ===========================================================================

import base64 as _b64
import hashlib as _hashlib
from fastapi.responses import Response

MAX_MEDIA_BYTES = 10 * 1024 * 1024  # 10 MB cap


class MediaUpload(BaseModel):
    """Body for POST /focms/v1/media.

    Binary content is base64-encoded in JSON to avoid the python-multipart
    dependency. JS clients use FileReader.readAsDataURL() and strip the
    "data:...;base64," prefix; PowerShell clients use
    [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($path)).
    """
    model_config = ConfigDict(extra="forbid")

    filename: str = Field(..., min_length=1, max_length=512)
    mime_type: str = Field(..., min_length=1)
    content_base64: str = Field(..., min_length=1)
    kind: Optional[str] = None
    visibility: Optional[str] = None
    student_id: Optional[UUID] = None


@app.post("/focms/v1/media", status_code=201)
async def upload_media(
    body: MediaUpload,
    request: Request,
    principal: dict = Depends(require_write),
) -> dict[str, Any]:
    """Upload a binary file (image, document, etc).

    JSON body: { filename, mime_type, content_base64, kind?, visibility?, student_id? }
    Returns the media id and a URL that can be used in image src etc.
    Size cap: 10 MB after base64 decode. Kinds: image, document, video, other.
    """
    try:
        content = _b64.b64decode(body.content_base64, validate=True)
    except Exception as exc:
        raise HTTPException(400, f"Invalid base64: {exc}") from exc
    if len(content) > MAX_MEDIA_BYTES:
        raise HTTPException(413, f"File too large: {len(content)} bytes (max {MAX_MEDIA_BYTES})")
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    sha = _hashlib.sha256(content).hexdigest()

    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO media_files (
                tenant_id, student_id, kind, mime_type, original_filename,
                byte_size, content, visibility, sha256_hex, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id, byte_size, mime_type, kind, visibility, sha256_hex
            """,
            UUID(tenant_id),
            body.student_id,
            body.kind or "image",
            body.mime_type,
            body.filename,
            len(content),
            content,
            body.visibility or "public",
            sha,
            UUID(user_id),
        )
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action="create", target_table="media_files",
            target_id=str(row["id"]), target_label=body.filename,
            new_value={"mime_type": row["mime_type"], "bytes": row["byte_size"]},
        )
    return {
        "id": str(row["id"]),
        "url": f"/focms/v1/media/{row['id']}",
        "mime_type": row["mime_type"],
        "byte_size": row["byte_size"],
        "kind": row["kind"],
        "visibility": row["visibility"],
        "sha256": row["sha256_hex"],
    }


@app.get("/focms/v1/media/{media_id}")
async def serve_media(media_id: UUID, request: Request):
    """Return the raw binary content of a media file with appropriate MIME type.

    No auth required for public files; private/unlisted return 404 to
    unauthenticated callers. (For simplicity, treat unlisted same as public.)
    """
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT content, mime_type, byte_size, visibility
            FROM media_files
            WHERE id = $1 AND deleted_at IS NULL
            """,
            media_id,
        )
        if not row:
            raise HTTPException(404, "Media not found")
        if row["visibility"] == "private":
            # In a future iteration: check bearer, allow if tenant matches
            raise HTTPException(404, "Media not found")
    return Response(
        content=bytes(row["content"]),
        media_type=row["mime_type"],
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.delete("/focms/v1/media/{media_id}", status_code=204)
async def delete_media(
    media_id: UUID, request: Request,
    principal: dict = Depends(require_write),
):
    tenant_id = principal["tenant_id"]
    user_id = principal["user_id"]
    async with tx(request, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            UPDATE media_files SET deleted_at = now()
            WHERE id = $1 AND deleted_at IS NULL
            RETURNING id
            """,
            media_id,
        )
        if not row:
            raise HTTPException(404, "Media not found or already deleted")
        await write_audit(
            conn, actor_user_id=user_id, actor_role=principal.get("role", "admin"),
            tenant_id=tenant_id, action="delete", target_table="media_files",
            target_id=str(media_id),
        )
    return JSONResponse(status_code=204, content=None)


# Helper exposed for tests: list media for a student
@app.get("/focms/v1/student/{student_id}/media")
async def list_student_media(
    student_id: UUID, request: Request,
    principal: dict = Depends(authenticate),
) -> dict[str, Any]:
    async with tx(request, principal["tenant_id"]) as conn:
        rows = await conn.fetch(
            """
            SELECT id, kind, mime_type, original_filename, byte_size,
                   visibility, created_at
            FROM media_files
            WHERE student_id = $1 AND deleted_at IS NULL
            ORDER BY created_at DESC
            """,
            student_id,
        )
    return {
        "student_id": str(student_id),
        "count": len(rows),
        "media": [
            {
                "id": str(r["id"]),
                "url": f"/focms/v1/media/{r['id']}",
                "kind": r["kind"],
                "mime_type": r["mime_type"],
                "original_filename": r["original_filename"],
                "byte_size": r["byte_size"],
                "visibility": r["visibility"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ],
    }

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
