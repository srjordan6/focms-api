"""
focms_cohort_signup.py - Anonymous cohort-based parent signup for
outcomestar.app. Provisions a family tenant, parent user, student
record, tenant_owner role, and API token in one atomic transaction.

Architecture: archive_entries source_id='cohort_signup_backend_design_v0_1'

v0.11.10 (2026-07-05):
- HOTFIX: v0.11.9 seed inserts poisoned the signup transaction (RLS FORCE on
  student_personal_details/family_members blocks inserts without tenant
  context; try/except cannot unpoison - the v0.11.1 lesson). Each seed insert
  now runs in its own SAVEPOINT (nested conn.transaction()) with
  SET LOCAL app.current_tenant_id (validated-UUID f-string literal per
  playbook PgBouncer rule) so a failure rolls back only the savepoint.

v0.11.9 (2026-07-05):
- Signup data now lands in the portal: provisioning (both 13+ and under-13
  webhook paths) seeds student_personal_details (residence country, primary
  student email if given) and a family_members row for the signing parent
  (relationship Parent, legal guardian, email/name encrypted via
  focms_encrypt_pii with FOCMS_KEK_MASTER; plaintext skipped if KEK unset).
  source_system='cohort_signup'.

v0.11.8 (2026-07-05):
- GET /focms/v1/auth/token-context: bearer token -> {tenant_id, tenant_name,
  student_id, student_first_name, student_last_name, student_age_band}.
  Resolves api_tokens rows (first student_ids entry) or FOCMS_API_TOKENS_JSON
  registry entries (tenant's first student). Lets the parent portal resolve
  its tenant/student at runtime instead of hardcoded constants - required
  for any tenant other than JRJ to use the portal.

v0.11.7 (2026-07-05):
- Under-13 free-plan path: storage_plan_key='free' triggers a one-time $1
  Parental Consent Verification charge (pricing_tiers plan_key='vpc_consent',
  mode=payment) instead of a subscription; family provisions on the free
  1 GB plan after the charge. Paid plans remain available at signup.

v0.11.6 (2026-07-05):
- Payment-first under-13 signup (unblocks the VPC gate per
  coppa_vpc_method_selection_v0_1): when the student is under 13, signup
  requires choosing a paid storage plan; the request is parked in
  pending_signups (24h expiry, no tenant/student rows created), a Stripe
  Checkout session is returned (402-style flow, response field
  vpc_checkout_url), and the family is provisioned ONLY by the webhook on
  checkout.session.completed - payment precedes all child data persistence.
  Welcome email (with portal token) + verification emails sent post-provision.
- Signup body gains optional storage_plan_key (required under 13).
- Core provisioning extracted to _provision_family() shared by both paths.

v0.11.5a (2026-07-05):
- /billing-session accepts tokens from the FOCMS_API_TOKENS_JSON env registry
  (the same registry focms_api uses), tenant from X-Tenant-Id header or the
  registry entry, in addition to api_tokens rows.

v0.11.4 (2026-07-05):
- GET /focms/v1/auth/pricing: anonymous read of active pricing_tiers rows
  (plan_key, display_name, storage_gb, price_usd_cents, video_allowed,
  stackable) + the locked verbatim deletion notice string, so no surface
  ever hardcodes prices or policy text.

v0.11.3 (2026-07-05):
- Storage billing (pricing decision of record 2026-07-05: signup free with code
  + verified emails; artifact storage is the only charge).
- POST /focms/v1/auth/billing-session: bearer parent-portal token -> Stripe
  Checkout Session (subscription) for a pricing_tiers plan; expansion blocks
  stackable via quantity. Needs STRIPE_SECRET_KEY env.
- POST /focms/v1/auth/stripe-webhook: signature-verified
  (STRIPE_WEBHOOK_SECRET); on checkout.session.completed updates tenants
  storage_plan/storage_quota_gb/stripe_customer_id/billing_verified_at and
  logs coppa_vpc_captured (payment_transaction) with youngest-student age -
  the durable VPC evidence per coppa_vpc_method_selection_v0_1.
- Under-13 signup gate unchanged (payment-first signup flow is the future
  unblocking path).

v0.11.2 (2026-07-05):
- Optional student_email on signup (free-tier eligibility requires BOTH
  parent and student email verified).
- email_verifications table: one row per address; token emailed via Resend
  (RESEND_API_KEY env; sender EMAIL_FROM env, default onboarding@resend.dev
  until outcomestar.app domain is verified in Resend).
- GET /focms/v1/auth/verify-email?token=... marks the address verified and
  returns a branded HTML page. Student emails not on an edu-looking domain
  are flagged needs_review=true for manual approval.
- Email send failures are non-fatal; signup still succeeds.

v0.11.0 (2026-07-01):
- New endpoint POST /focms/v1/auth/cohort-signup
- Anonymous (no bearer token). Rate limited per client IP.
- Consumes a row from the cohorts table (created in v0.11.0 migration).
- Increments redemption_count atomically inside the signup transaction.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime, date, timezone
from typing import Any, Optional
from uuid import UUID

import os

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

log = logging.getLogger("focms-cohort-signup")
router = APIRouter(prefix="/focms/v1/auth", tags=["cohort_signup"])


# ---------------------------------------------------------------------------
# Rate limiting (in-memory - fine for pilot scale, swap to Redis at 1k+ QPS)
# ---------------------------------------------------------------------------

_signup_attempts_by_ip: dict[str, deque[float]] = defaultdict(deque)
_signup_attempts_by_code: dict[str, deque[float]] = defaultdict(deque)
_rate_limit_lock = asyncio.Lock()

RATE_LIMIT_IP_PER_MIN = 10
RATE_LIMIT_CODE_PER_MIN = 100


async def check_rate_limit(client_ip: str, code: str) -> None:
    """Raise 429 if IP or code has exceeded per-minute signup attempts.

    Keeps the last 60 seconds of attempts per key in a deque. Cheap enough
    for pilot; move to Redis when we outgrow single-process focms-api.
    """
    now = time.monotonic()
    cutoff = now - 60.0
    async with _rate_limit_lock:
        for key, store, cap in (
            (client_ip, _signup_attempts_by_ip, RATE_LIMIT_IP_PER_MIN),
            (code, _signup_attempts_by_code, RATE_LIMIT_CODE_PER_MIN),
        ):
            dq = store[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= cap:
                raise HTTPException(429, {
                    "error": "rate_limit_exceeded",
                    "message": "Too many signup attempts. Try again in a minute.",
                })
        # Record this attempt (only after both checks passed)
        _signup_attempts_by_ip[client_ip].append(now)
        _signup_attempts_by_code[code].append(now)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

VALID_GRADES = {"K", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}


class CohortSignupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(..., min_length=1, max_length=64)
    parent_email: EmailStr
    parent_display_name: str = Field(..., min_length=1, max_length=200)
    parent_first_name: str = Field(..., min_length=1, max_length=100)
    parent_last_name: str = Field(..., min_length=1, max_length=100)
    student_first_name: str = Field(..., min_length=1, max_length=100)
    student_last_name: str = Field(..., min_length=1, max_length=100)
    student_grade: str = Field(..., min_length=1, max_length=2)
    student_birth_year: int = Field(..., ge=2000, le=2030)
    student_email: Optional[EmailStr] = None
    storage_plan_key: Optional[str] = None   # required when student is under 13
    accept_user_agreement: bool
    accept_privacy_policy: bool

    @field_validator("code")
    @classmethod
    def normalize_code(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("student_grade")
    @classmethod
    def validate_grade(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in VALID_GRADES:
            raise ValueError(f"student_grade must be one of {sorted(VALID_GRADES)}")
        return v


class CohortSignupResponse(BaseModel):
    family_tenant_id: str
    verification_sent_to: list[str] = []
    free_tier_pending_verification: bool = False
    vpc_checkout_url: Optional[str] = None
    family_tenant_slug: str
    student_id: str
    parent_user_id: str
    api_token: str
    welcome_url: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def compute_hs_graduation_year(birth_year: int, current_grade: str) -> int:
    """Estimate expected HS graduation year. Assumes standard K-12 progression."""
    # A typical HS graduate turns 18 in graduation year. Adjust if grade info gives us more.
    return birth_year + 18


def compute_current_age(birth_year: int) -> int:
    """Approximate current age from birth year. Uses year math (no birth month)."""
    return datetime.now(timezone.utc).year - birth_year


SLUG_STRIP = re.compile(r"[^a-z0-9-]+")


def generate_family_slug(first: str, last: str) -> str:
    """
    Generate a family tenant slug. Format: last-first-<short-random>.
    E.g. "smith-alex-a3b9c7". Never guaranteed unique; caller retries
    on collision.
    """
    base = f"{last}-{first}".lower()
    base = SLUG_STRIP.sub("-", base).strip("-")[:40]
    suffix = secrets.token_hex(3)  # 6 hex chars
    return f"{base}-{suffix}" if base else f"family-{suffix}"


def generate_short_id() -> str:
    """8-char base32-ish public tenant identifier for URLs."""
    alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"  # avoid ambiguous chars
    return "".join(secrets.choice(alphabet) for _ in range(8))


def generate_api_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex_of_token). Raw goes in response, hash goes in DB."""
    raw = secrets.token_urlsafe(32)  # ~43 char URL-safe token
    token_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return raw, token_hash


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

EDU_DOMAIN_RE = re.compile(r"\.(edu|k12\.[a-z]{2}\.us|k12\.us|ac\.[a-z]{2})$|(^|\.)(isd|schools?|school|academy|college|university)\.", re.I)


def _looks_edu(email: str) -> bool:
    domain = email.rsplit("@", 1)[-1].lower()
    return bool(EDU_DOMAIN_RE.search("." + domain))


async def _send_verification_email(to_email: str, role: str, student_name: str, token: str) -> None:
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        log.warning("RESEND_API_KEY not set; verification email to %s skipped", to_email)
        return
    sender = os.environ.get("EMAIL_FROM", "outcomestar <onboarding@resend.dev>")
    link = f"https://focms-api.onrender.com/focms/v1/auth/verify-email?token={token}"
    who = "your parent account" if role == "parent" else f"{student_name}'s student email"
    html = (
        '<div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;color:#1a1a2e">'
        '<h2 style="color:#201868">Confirm ' + who + '</h2>'
        '<p>To activate free access to outcomestar, both the parent and student '
        'email addresses must be confirmed.</p>'
        '<p><a href="' + link + '" style="background:#F07800;color:#fff;padding:12px 26px;'
        'border-radius:8px;text-decoration:none;font-weight:bold">Confirm this email</a></p>'
        '<p style="color:#7A8A9E;font-size:12px">This link expires in 7 days. '
        'If you did not sign up for outcomestar, ignore this email.</p></div>'
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"from": sender, "to": [to_email],
                  "subject": "Confirm your email — outcomestar", "html": html},
        )
        if r.status_code >= 300:
            log.warning("resend send failed %s: %s", r.status_code, r.text[:200])


async def _seed_portal_rows(conn: asyncpg.Connection, tenant_id, student_id, parent_user_id, p: dict) -> None:
    """v0.11.9/10: portal-visible seed rows. Each insert in its own SAVEPOINT
    with tenant RLS context, so a failure never poisons the outer signup
    transaction."""
    kek = os.environ.get("FOCMS_KEK_MASTER")
    tid = str(UUID(str(tenant_id)))  # validate before f-string literal (PgBouncer rule)
    try:
        async with conn.transaction():  # SAVEPOINT
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tid}'")
            await conn.execute(
                """INSERT INTO student_personal_details (student_id, tenant_id, email_primary,
                     residence_country, visibility, source_system, created_by, updated_by)
                   VALUES ($1, $2, $3, 'US', 'private', 'cohort_signup', $4, $4)
                   ON CONFLICT (student_id) DO NOTHING""",
                student_id, tenant_id, p.get("student_email"), parent_user_id)
    except Exception as exc:
        log.warning("seed personal_details failed (non-fatal): %r", exc)
    if not kek:
        log.warning("FOCMS_KEK_MASTER unset; family member seed skipped")
        return
    try:
        async with conn.transaction():  # SAVEPOINT
            await conn.execute(f"SET LOCAL app.current_tenant_id = '{tid}'")
            await conn.execute(
                """INSERT INTO family_members (tenant_id, student_id, relationship, is_legal_guardian,
                     guardian_order, first_name_ciphertext, last_name_ciphertext, email_ciphertext,
                     is_living, resides_with_student, visibility, source_system, created_by, updated_by)
                   VALUES ($1, $2, 'Parent', true, 1,
                     focms_encrypt_pii($1, $3, $6), focms_encrypt_pii($1, $4, $6),
                     focms_encrypt_pii($1, $5, $6),
                     true, true, 'private', 'cohort_signup', $7, $7)""",
                tenant_id, student_id, p["parent_first_name"], p["parent_last_name"],
                p["parent_email"], kek, parent_user_id)
    except Exception as exc:
        log.warning("seed family_members failed (non-fatal): %r", exc)


@router.post("/cohort-signup", response_model=CohortSignupResponse, status_code=201)
async def cohort_signup(body: CohortSignupRequest, request: Request) -> dict[str, Any]:
    """Anonymous cohort-based signup. See design doc cohort_signup_backend_design_v0_1."""
    client_ip = request.client.host if request.client else "unknown"
    await check_rate_limit(client_ip, body.code)

    # Consent gate
    if not (body.accept_user_agreement and body.accept_privacy_policy):
        raise HTTPException(400, {
            "error": "consent_required",
            "message": "Both User Agreement and Privacy Policy must be accepted.",
        })

    # Sanity: computed age plausible for K-12 (4-19)
    age = compute_current_age(body.student_birth_year)
    if age < 4 or age > 19:
        raise HTTPException(400, {
            "error": "invalid_request",
            "message": "student_birth_year is not plausible for a K-12 student.",
        })

    pool: asyncpg.Pool = request.app.state.pool

    # COPPA gate (v0.11.6): under-13 requires payment-first VPC. Park the
    # request, return a Stripe Checkout URL; the webhook provisions the family
    # AFTER payment - no child data is persisted before consent.
    if age < 13:
        api_key = os.environ.get("STRIPE_SECRET_KEY")
        if not api_key:
            raise HTTPException(400, {
                "error": "vpc_required",
                "message": "Students under 13 require verified parental consent; "
                           "enrollment for under-13 students is temporarily unavailable.",
            })
        requested = (body.storage_plan_key or "").strip().lower()
        one_time = requested == "free"
        async with pool.acquire() as conn:
            if one_time:
                tier = await conn.fetchrow(
                    "SELECT 'free'::text AS plan_key, stripe_price_id FROM pricing_tiers "
                    "WHERE plan_key = 'vpc_consent' AND stripe_price_id IS NOT NULL")
            else:
                tier = await conn.fetchrow(
                    "SELECT plan_key, stripe_price_id FROM pricing_tiers "
                    "WHERE plan_key = $1 AND active AND stripe_price_id IS NOT NULL AND NOT stackable",
                    requested)
            if not tier:
                raise HTTPException(400, {
                    "error": "vpc_plan_required",
                    "message": "For students under 13, federal law requires verified parental "
                               "consent. Choose the $1 one-time consent verification (free plan) "
                               "or a storage plan - the card payment is the FTC-recognized "
                               "consent method.",
                })
            existing = await conn.fetchval(
                "SELECT id FROM users WHERE email = $1 AND deactivated_at IS NULL",
                body.parent_email)
            if existing:
                raise HTTPException(409, {"error": "email_already_exists",
                                          "message": "An account with this email already exists."})
            pending_id = await conn.fetchval(
                "INSERT INTO pending_signups (payload, cohort_code) VALUES ($1::jsonb, $2) RETURNING id",
                body.model_dump_json(), body.code)
        form = {
            "mode": "payment" if one_time else "subscription",
            "line_items[0][price]": tier["stripe_price_id"],
            "line_items[0][quantity]": "1",
            "success_url": "https://outcomestar.app/signup.html?vpc=complete",
            "cancel_url": "https://outcomestar.app/signup.html?vpc=cancelled",
            "customer_email": body.parent_email,
            "metadata[pending_signup_id]": str(pending_id),
            "metadata[plan_key]": tier["plan_key"],
        }
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post("https://api.stripe.com/v1/checkout/sessions",
                                  headers={"Authorization": f"Bearer {api_key}"}, data=form)
        if r.status_code >= 300:
            log.warning("vpc checkout create failed %s: %s", r.status_code, r.text[:300])
            raise HTTPException(502, {"error": "stripe_error", "message": "Could not start consent checkout."})
        sess = r.json()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE pending_signups SET checkout_session_id=$2 WHERE id=$1",
                               pending_id, sess["id"])
        log.info("under13_vpc_checkout pending=%s plan=%s", pending_id, tier["plan_key"])
        return {
            "family_tenant_id": "", "family_tenant_slug": "", "student_id": "",
            "parent_user_id": "", "api_token": "", "welcome_url": "",
            "verification_sent_to": [], "free_tier_pending_verification": False,
            "vpc_checkout_url": sess["url"],
        }

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: lock the cohort row for the duration of the transaction
            cohort = await conn.fetchrow(
                """
                SELECT id, tenant_id, tier, max_redemptions, redemption_count,
                       expires_at, status
                FROM cohorts
                WHERE code = $1
                FOR UPDATE
                """,
                body.code,
            )

            # Step 2: validate cohort state
            if not cohort:
                raise HTTPException(404, {
                    "error": "cohort_not_found",
                    "message": "That code is not recognized.",
                })
            now_utc = datetime.now(timezone.utc)
            if cohort["status"] == "revoked":
                log.info("signup_rejected_revoked code=%s", body.code)
                raise HTTPException(409, {
                    "error": "cohort_revoked",
                    "message": "This code is no longer active.",
                })
            if cohort["status"] == "paused":
                raise HTTPException(409, {
                    "error": "cohort_paused",
                    "message": "Signup is temporarily unavailable. Please try again later.",
                })
            if cohort["status"] == "expired" or cohort["expires_at"] < now_utc:
                raise HTTPException(410, {
                    "error": "cohort_expired",
                    "message": "This code has expired.",
                })
            if cohort["redemption_count"] >= cohort["max_redemptions"]:
                raise HTTPException(409, {
                    "error": "cohort_exhausted",
                    "message": "This cohort is full.",
                })

            # Step 3: email collision check
            existing = await conn.fetchval(
                "SELECT id FROM users WHERE email = $1 AND deactivated_at IS NULL",
                body.parent_email,
            )
            if existing:
                raise HTTPException(409, {
                    "error": "email_already_exists",
                    "message": "An account with this email already exists.",
                    "hint": "Try logging in instead.",
                })

            # Step 4: create family tenant. Retry once on slug collision.
            slug = generate_family_slug(body.student_first_name, body.student_last_name)
            try:
                family_tenant_id = await conn.fetchval(
                    """
                    INSERT INTO tenants (
                        slug, short_id, display_name, primary_email,
                        country, locale, timezone, plan, status,
                        storage_used_bytes
                    ) VALUES ($1, $2, $3, $4, 'US', 'en-US', 'America/Chicago',
                              $5, 'active', 0)
                    RETURNING id
                    """,
                    slug,
                    generate_short_id(),
                    f"{body.parent_last_name} Family",
                    body.parent_email,
                    cohort["tier"],
                )
            except asyncpg.UniqueViolationError:
                # Retry once with a fresh slug
                slug = generate_family_slug(body.student_first_name, body.student_last_name)
                family_tenant_id = await conn.fetchval(
                    """
                    INSERT INTO tenants (
                        slug, short_id, display_name, primary_email,
                        country, locale, timezone, plan, status,
                        storage_used_bytes
                    ) VALUES ($1, $2, $3, $4, 'US', 'en-US', 'America/Chicago',
                              $5, 'active', 0)
                    RETURNING id
                    """,
                    slug,
                    generate_short_id(),
                    f"{body.parent_last_name} Family",
                    body.parent_email,
                    cohort["tier"],
                )

            # Step 5: create parent user (no password; auth via magic-link/token)
            parent_user_id = await conn.fetchval(
                """
                INSERT INTO users (
                    email, display_name, first_name, last_name,
                    mfa_enabled, webauthn_credentials, oauth_providers,
                    is_active, is_platform_admin, failed_login_count
                ) VALUES ($1, $2, $3, $4, false, '[]'::jsonb, '{}'::jsonb,
                          true, false, 0)
                RETURNING id
                """,
                body.parent_email,
                body.parent_display_name,
                body.parent_first_name,
                body.parent_last_name,
            )

            # Step 6: create student
            # Birth date approximated as July 1 of birth year (used only if
            # nothing better; the parent can correct it later in the portal).
            approx_birth_date = date(body.student_birth_year, 7, 1)
            hs_grad = compute_hs_graduation_year(body.student_birth_year, body.student_grade)
            display_name = f"{body.student_first_name} {body.student_last_name}"
            student_id = await conn.fetchval(
                """
                INSERT INTO students (
                    tenant_id, first_name, last_name, display_name,
                    birth_date, current_grade,
                    expected_hs_graduation_year, residence_country,
                    created_by, updated_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'US', $8, $8)
                RETURNING id
                """,
                family_tenant_id,
                body.student_first_name,
                body.student_last_name,
                display_name,
                approx_birth_date,
                body.student_grade,
                hs_grad,
                parent_user_id,
            )

            # Step 7: grant parent tenant_owner role
            await conn.execute(
                """
                INSERT INTO user_tenant_roles (
                    user_id, tenant_id, role, granted_at, granted_by,
                    invitation_email, accepted_at
                ) VALUES ($1, $2, 'tenant_owner', now(), $1, $3, now())
                """,
                parent_user_id,
                family_tenant_id,
                body.parent_email,
            )

            # Step 8: provision API token
            raw_token, token_hash = generate_api_token()
            await conn.execute(
                """
                INSERT INTO api_tokens (
                    tenant_id, token_hash, student_ids, name, scope, created_by
                ) VALUES ($1, $2, ARRAY[$3::uuid], 'parent-portal', 'parent_portal', $4)
                """,
                family_tenant_id,
                token_hash,
                student_id,
                parent_user_id,
            )

            # Step 8b (v0.11.9): seed portal-visible rows from signup data
            await _seed_portal_rows(conn, family_tenant_id, student_id, parent_user_id, {
                "parent_first_name": body.parent_first_name,
                "parent_last_name": body.parent_last_name,
                "parent_email": str(body.parent_email),
                "student_email": str(body.student_email) if body.student_email else None,
            })

            # Step 9b (v0.11.2): email verification rows
            verify_targets: list[tuple[str, str, str, bool]] = []  # (role, email, raw_token, needs_review)
            def _mk(role: str, email: str, needs_review: bool):
                raw = secrets.token_urlsafe(32)
                verify_targets.append((role, email, raw, needs_review))
                return raw, hashlib.sha256(raw.encode()).hexdigest()

            _, p_hash = _mk("parent", body.parent_email, False)
            await conn.execute(
                """
                INSERT INTO email_verifications (tenant_id, subject_role, email, token_hash, needs_review, expires_at)
                VALUES ($1, 'parent', $2, $3, false, now() + interval '7 days')
                """,
                family_tenant_id, body.parent_email, p_hash,
            )
            if body.student_email:
                review = not _looks_edu(str(body.student_email))
                _, s_hash = _mk("student", str(body.student_email), review)
                await conn.execute(
                    """
                    INSERT INTO email_verifications (tenant_id, subject_role, email, token_hash, needs_review, expires_at)
                    VALUES ($1, 'student', $2, $3, $4, now() + interval '7 days')
                    """,
                    family_tenant_id, str(body.student_email), s_hash, review,
                )

            # Step 9: increment cohort
            await conn.execute(
                """
                UPDATE cohorts
                SET redemption_count = redemption_count + 1,
                    updated_at = now()
                WHERE id = $1
                """,
                cohort["id"],
            )


    # v0.11.1: audit_log MUST run on a fresh connection outside the main
    # transaction. Any error inside a Postgres transaction poisons it, and
    # try/except at Python level cannot unpoison. Moved out means audit
    # failure does not undo the completed signup.
    try:
        async with pool.acquire() as audit_conn:
            await audit_conn.execute(
                """
                INSERT INTO audit_log (
                    tenant_id, actor_user_id, actor_role, action,
                    target_table, target_id, target_label, new_value
                ) VALUES ($1, $2, 'tenant_owner', 'create',
                          'tenants', $5, $3, $4::jsonb)
                """,
                family_tenant_id,
                parent_user_id,
                f"{body.parent_last_name} Family",
                f'{{"cohort_id": "{cohort["id"]}", "cohort_code": "{body.code}", "client_ip": "{client_ip}", "event_type": "cohort_signup"}}',
                str(family_tenant_id),
            )
    except Exception as exc:
        log.warning("audit_log insert failed (non-fatal): %r", exc)
    # v0.11.2: send verification emails outside the transaction (non-fatal)
    sent_to: list[str] = []
    for role, email, raw, _rev in verify_targets:
        try:
            await _send_verification_email(email, role, body.student_first_name, raw)
            sent_to.append(email)
        except Exception as exc:
            log.warning("verification email to %s failed (non-fatal): %r", email, exc)

    # Transaction committed. Build welcome URL.
    welcome_url = f"https://app.outcomestar.app/welcome?t={raw_token}"

    log.info(
        "cohort_signup_success family=%s student=%s parent=%s cohort=%s ip=%s",
        family_tenant_id, student_id, parent_user_id, body.code, client_ip,
    )

    return {
        "family_tenant_id": str(family_tenant_id),
        "family_tenant_slug": slug,
        "student_id": str(student_id),
        "parent_user_id": str(parent_user_id),
        "api_token": raw_token,
        "welcome_url": welcome_url,
        "verification_sent_to": sent_to,
        "free_tier_pending_verification": bool(body.student_email),
    }


# ---------------------------------------------------------------------------
# Storage billing (v0.11.3)
# ---------------------------------------------------------------------------

@router.get("/token-context")
async def token_context(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise HTTPException(401, {"error": "auth_required", "message": "Bearer token required."})
        raw = auth[7:].strip()
        tenant_id = None
        student_id = None
        try:
            registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
        except Exception:
            registry = {}
        entry = registry.get(raw)
        if entry:
            tenant_id = str(entry.get("tenant_id", "")) or None
        else:
            row = await conn.fetchrow(
                "SELECT tenant_id, student_ids FROM api_tokens "
                "WHERE token_hash = $1 AND revoked_at IS NULL",
                hashlib.sha256(raw.encode()).hexdigest())
            if row:
                tenant_id = str(row["tenant_id"])
                if row["student_ids"]:
                    student_id = str(row["student_ids"][0])
        if not tenant_id:
            raise HTTPException(401, {"error": "invalid_token", "message": "Token not recognized."})
        if not student_id:
            student_id = await conn.fetchval(
                "SELECT id FROM students WHERE tenant_id = $1::uuid ORDER BY created_at LIMIT 1",
                tenant_id)
        st = None
        if student_id:
            st = await conn.fetchrow(
                "SELECT first_name, last_name, extract(year from age(birth_date))::int AS age "
                "FROM students WHERE id = $1::uuid", student_id)
        tn = await conn.fetchval("SELECT display_name FROM tenants WHERE id = $1::uuid", tenant_id)
    age = st["age"] if st and st["age"] is not None else None
    band = None
    if age is not None:
        band = "band_1_5" if age <= 5 else ("band_6_12" if age <= 12 else "band_13_18")
    return {"tenant_id": tenant_id, "tenant_name": tn,
            "student_id": str(student_id) if student_id else None,
            "student_first_name": st["first_name"] if st else None,
            "student_last_name": st["last_name"] if st else None,
            "student_age_band": band}


DELETION_NOTICE = (
    "Your child's life record is yours forever. Files cost us money to store; "
    "if you stop, we delete the files. We never delete the record. "
    "Artifacts (photos, videos, documents) are deleted 90 days after a storage "
    "plan lapses; all structured records remain yours permanently and are "
    "exportable free at any time."
)


@router.get("/pricing")
async def get_pricing(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT plan_key, display_name, storage_gb, price_usd_cents, "
            "billing_interval, video_allowed, stackable, sort_order "
            "FROM pricing_tiers WHERE active ORDER BY sort_order")
    return {"deletion_notice": DELETION_NOTICE,
            "plans": [dict(r) for r in rows]}


class BillingSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_key: str = Field(..., min_length=1, max_length=40)
    quantity: int = Field(1, ge=1, le=50)          # >1 only for stackable plans
    success_url: str = Field("https://outcomestar.app/billing-success")
    cancel_url: str = Field("https://outcomestar.app/portal")


async def _tenant_from_bearer(request: Request, conn: asyncpg.Connection) -> dict:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, {"error": "auth_required", "message": "Bearer token required."})
    raw = auth[7:].strip()
    # Env token registry (FOCMS_API_TOKENS_JSON: {token: {tenant_id, ...}}) -
    # same registry focms_api uses for bearer auth.
    try:
        registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
    except Exception:
        registry = {}
    entry = registry.get(raw)
    if entry:
        tenant_id = (request.headers.get("x-tenant-id", "").strip()
                     or str(entry.get("tenant_id", "")))
        if not tenant_id:
            raise HTTPException(401, {"error": "tenant_required", "message": "X-Tenant-Id required with admin token."})
        row = await conn.fetchrow(
            "SELECT id AS tenant_id, primary_email, stripe_customer_id FROM tenants WHERE id = $1::uuid",
            tenant_id)
        if not row:
            raise HTTPException(401, {"error": "invalid_tenant", "message": "Tenant not found."})
        return dict(row)
    token_hash = hashlib.sha256(raw.encode()).hexdigest()
    row = await conn.fetchrow(
        """
        SELECT at.tenant_id, t.primary_email, t.stripe_customer_id
        FROM api_tokens at JOIN tenants t ON t.id = at.tenant_id
        WHERE at.token_hash = $1 AND at.revoked_at IS NULL
        """,
        token_hash,
    )
    if not row:
        raise HTTPException(401, {"error": "invalid_token", "message": "Token not recognized."})
    return dict(row)


@router.post("/billing-session")
async def billing_session(body: BillingSessionRequest, request: Request) -> dict[str, Any]:
    api_key = os.environ.get("STRIPE_SECRET_KEY")
    if not api_key:
        raise HTTPException(503, {"error": "billing_unavailable", "message": "Billing is not configured."})
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        principal = await _tenant_from_bearer(request, conn)
        tier = await conn.fetchrow(
            "SELECT plan_key, display_name, stripe_price_id, stackable FROM pricing_tiers "
            "WHERE plan_key = $1 AND active AND stripe_price_id IS NOT NULL",
            body.plan_key,
        )
    if not tier:
        raise HTTPException(404, {"error": "plan_not_found", "message": "Unknown or non-purchasable plan."})
    qty = body.quantity if tier["stackable"] else 1
    form = {
        "mode": "subscription",
        "line_items[0][price]": tier["stripe_price_id"],
        "line_items[0][quantity]": str(qty),
        "success_url": body.success_url,
        "cancel_url": body.cancel_url,
        "customer_email": principal["primary_email"],
        "metadata[tenant_id]": str(principal["tenant_id"]),
        "metadata[plan_key]": tier["plan_key"],
        "metadata[quantity]": str(qty),
        "subscription_data[metadata][tenant_id]": str(principal["tenant_id"]),
    }
    if principal.get("stripe_customer_id"):
        form["customer"] = principal["stripe_customer_id"]
        form.pop("customer_email", None)
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post("https://api.stripe.com/v1/checkout/sessions",
                              headers={"Authorization": f"Bearer {api_key}"}, data=form)
    if r.status_code >= 300:
        log.warning("stripe checkout create failed %s: %s", r.status_code, r.text[:300])
        raise HTTPException(502, {"error": "stripe_error", "message": "Could not start checkout."})
    sess = r.json()
    return {"checkout_url": sess["url"], "session_id": sess["id"]}


def _stripe_sig_ok(payload: bytes, header: str, secret: str) -> bool:
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        signed = f"{parts['t']}.".encode() + payload
        expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, parts.get("v1", ""))
    except Exception:
        return False


@router.post("/stripe-webhook")
async def stripe_webhook(request: Request) -> dict[str, Any]:
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET")
    payload = await request.body()
    if not secret or not _stripe_sig_ok(payload, request.headers.get("stripe-signature", ""), secret):
        raise HTTPException(400, {"error": "bad_signature"})
    event = json.loads(payload)
    if event.get("type") != "checkout.session.completed":
        return {"received": True, "ignored": event.get("type")}
    sess = event["data"]["object"]
    meta = sess.get("metadata") or {}
    pending_id = meta.get("pending_signup_id")
    if pending_id:
        return await _complete_pending_signup(request, pending_id, sess, meta)
    tenant_id, plan_key = meta.get("tenant_id"), meta.get("plan_key")
    qty = int(meta.get("quantity", "1") or 1)
    if not tenant_id or not plan_key:
        return {"received": True, "ignored": "no_metadata"}
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        tier = await conn.fetchrow(
            "SELECT storage_gb, stackable FROM pricing_tiers WHERE plan_key = $1", plan_key)
        if not tier:
            return {"received": True, "ignored": "unknown_plan"}
        if tier["stackable"]:
            await conn.execute(
                "UPDATE tenants SET storage_quota_gb = storage_quota_gb + $2, "
                "stripe_customer_id = COALESCE($3, stripe_customer_id), "
                "billing_verified_at = COALESCE(billing_verified_at, now()) WHERE id = $1::uuid",
                tenant_id, tier["storage_gb"] * qty, sess.get("customer"))
        else:
            await conn.execute(
                "UPDATE tenants SET storage_plan = $2, storage_quota_gb = $3, "
                "stripe_customer_id = COALESCE($4, stripe_customer_id), "
                "billing_verified_at = COALESCE(billing_verified_at, now()) WHERE id = $1::uuid",
                tenant_id, plan_key, tier["storage_gb"], sess.get("customer"))
        youngest = await conn.fetchval(
            "SELECT min(extract(year from age(birth_date)))::int FROM students "
            "WHERE tenant_id = $1::uuid AND birth_date IS NOT NULL", tenant_id)
        try:
            await conn.execute(
                """
                INSERT INTO audit_log (tenant_id, actor_role, action, target_table,
                                       target_id, target_label, new_value)
                VALUES ($1::uuid, 'tenant_owner', 'coppa_vpc_captured', 'tenants', $2, $3, $4::jsonb)
                """,
                tenant_id, str(tenant_id), f"storage purchase {plan_key} x{qty}",
                json.dumps({"method": "payment_transaction", "processor": "stripe",
                            "checkout_session": sess.get("id"),
                            "subscription": sess.get("subscription"),
                            "youngest_student_age": youngest, "plan_key": plan_key,
                            "quantity": qty}))
        except Exception as exc:
            log.warning("coppa_vpc_captured audit insert failed (non-fatal): %r", exc)
    log.info("storage_purchase tenant=%s plan=%s x%s", tenant_id, plan_key, qty)
    return {"received": True}


_VERIFY_PAGE = """<!DOCTYPE html><html><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1.0'><title>outcomestar</title>
<style>body{{font-family:Arial,sans-serif;background:#FAFAF7;color:#1a1a2e;text-align:center;padding:70px 20px}}
h1{{color:#201868}}div{{width:90px;border-bottom:3px solid #F07800;margin:14px auto}}
p{{color:#4A5563;max-width:440px;margin:0 auto}}a{{color:#F07800}}</style></head>
<body><h1>{title}</h1><div></div><p>{msg}</p><p style='margin-top:22px'><a href='https://outcomestar.app'>outcomestar.app</a></p></body></html>"""


@router.get("/verify-email", response_class=HTMLResponse)
async def verify_email(token: str, request: Request) -> HTMLResponse:
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE email_verifications
            SET verified_at = COALESCE(verified_at, now())
            WHERE token_hash = $1 AND expires_at > now()
            RETURNING tenant_id, subject_role, email, needs_review, verified_at
            """,
            token_hash,
        )
        if not row:
            return HTMLResponse(_VERIFY_PAGE.format(
                title="Link expired or invalid",
                msg="This confirmation link is no longer valid. Sign up again or contact support via the chat bubble."), status_code=404)
        remaining = await conn.fetchval(
            """
            SELECT count(*) FROM email_verifications
            WHERE tenant_id = $1 AND verified_at IS NULL
            """,
            row["tenant_id"],
        )
    if remaining == 0:
        extra = (" A team member will confirm the student email domain shortly."
                 if row["needs_review"] else "")
        msg = "Both emails are confirmed — free access is active." + extra
    else:
        msg = f"{row['email']} is confirmed. {remaining} email confirmation still pending for free access."
    log.info("email_verified tenant=%s role=%s remaining=%s", row["tenant_id"], row["subject_role"], remaining)
    return HTMLResponse(_VERIFY_PAGE.format(title="Email confirmed", msg=msg))


async def _complete_pending_signup(request: Request, pending_id: str, sess: dict, meta: dict) -> dict[str, Any]:
    """v0.11.6: provision a parked under-13 signup after Stripe payment (VPC)."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        pend = await conn.fetchrow(
            "SELECT id, payload, cohort_code FROM pending_signups "
            "WHERE id = $1::uuid AND consumed_at IS NULL AND expires_at > now()", pending_id)
    if not pend:
        log.warning("pending signup %s missing/expired/consumed", pending_id)
        return {"received": True, "ignored": "pending_missing"}
    p = json.loads(pend["payload"])
    plan_key = meta.get("plan_key", "keepsake")

    async with pool.acquire() as conn:
        async with conn.transaction():
            cohort = await conn.fetchrow(
                "SELECT id, tenant_id, tier, max_redemptions, redemption_count, expires_at, status "
                "FROM cohorts WHERE code = $1 FOR UPDATE", pend["cohort_code"])
            if not cohort or cohort["status"] != "active":
                log.warning("pending %s: cohort unavailable at completion", pending_id)
                return {"received": True, "ignored": "cohort_unavailable"}
            existing = await conn.fetchval(
                "SELECT id FROM users WHERE email = $1 AND deactivated_at IS NULL", p["parent_email"])
            if existing:
                await conn.execute("UPDATE pending_signups SET consumed_at = now() WHERE id = $1::uuid", pending_id)
                return {"received": True, "ignored": "email_exists"}

            slug = generate_family_slug(p["student_first_name"], p["student_last_name"])
            tier_row = await conn.fetchrow("SELECT storage_gb FROM pricing_tiers WHERE plan_key = $1", plan_key)
            quota = tier_row["storage_gb"] if tier_row else 1
            try:
                family_tenant_id = await conn.fetchval(
                    """INSERT INTO tenants (slug, short_id, display_name, primary_email, country, locale,
                       timezone, plan, status, storage_used_bytes, storage_plan, storage_quota_gb,
                       stripe_customer_id, billing_verified_at)
                       VALUES ($1,$2,$3,$4,'US','en-US','America/Chicago',$5,'active',0,$6,$7,$8,now())
                       RETURNING id""",
                    slug, generate_short_id(), f"{p['parent_last_name']} Family",
                    p["parent_email"], cohort["tier"], plan_key, quota, sess.get("customer"))
            except asyncpg.UniqueViolationError:
                slug = generate_family_slug(p["student_first_name"], p["student_last_name"])
                family_tenant_id = await conn.fetchval(
                    """INSERT INTO tenants (slug, short_id, display_name, primary_email, country, locale,
                       timezone, plan, status, storage_used_bytes, storage_plan, storage_quota_gb,
                       stripe_customer_id, billing_verified_at)
                       VALUES ($1,$2,$3,$4,'US','en-US','America/Chicago',$5,'active',0,$6,$7,$8,now())
                       RETURNING id""",
                    slug, generate_short_id(), f"{p['parent_last_name']} Family",
                    p["parent_email"], cohort["tier"], plan_key, quota, sess.get("customer"))

            parent_user_id = await conn.fetchval(
                """INSERT INTO users (email, display_name, first_name, last_name, mfa_enabled,
                   webauthn_credentials, oauth_providers, is_active, is_platform_admin, failed_login_count)
                   VALUES ($1,$2,$3,$4,false,'[]'::jsonb,'{}'::jsonb,true,false,0) RETURNING id""",
                p["parent_email"], p["parent_display_name"], p["parent_first_name"], p["parent_last_name"])

            approx_birth_date = date(int(p["student_birth_year"]), 7, 1)
            hs_grad = compute_hs_graduation_year(int(p["student_birth_year"]), p["student_grade"])
            student_id = await conn.fetchval(
                """INSERT INTO students (tenant_id, first_name, last_name, display_name, birth_date,
                   current_grade, expected_hs_graduation_year, residence_country, created_by, updated_by)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,'US',$8,$8) RETURNING id""",
                family_tenant_id, p["student_first_name"], p["student_last_name"],
                f"{p['student_first_name']} {p['student_last_name']}",
                approx_birth_date, p["student_grade"], hs_grad, parent_user_id)

            await conn.execute(
                """INSERT INTO user_tenant_roles (user_id, tenant_id, role, granted_at, granted_by,
                   invitation_email, accepted_at) VALUES ($1,$2,'tenant_owner',now(),$1,$3,now())""",
                parent_user_id, family_tenant_id, p["parent_email"])

            raw_token, token_hash = generate_api_token()
            await conn.execute(
                """INSERT INTO api_tokens (tenant_id, token_hash, student_ids, name, scope, created_by)
                   VALUES ($1,$2,ARRAY[$3::uuid],'parent-portal','parent_portal',$4)""",
                family_tenant_id, token_hash, student_id, parent_user_id)

            await _seed_portal_rows(conn, family_tenant_id, student_id, parent_user_id, p)

            await conn.execute(
                "UPDATE cohorts SET redemption_count = redemption_count + 1, updated_at = now() WHERE id = $1",
                cohort["id"])

            verify_rows = [("parent", p["parent_email"], False)]
            if p.get("student_email"):
                verify_rows.append(("student", p["student_email"], not _looks_edu(p["student_email"])))
            tokens_to_send = []
            for role, email, review in verify_rows:
                raw = secrets.token_urlsafe(32)
                tokens_to_send.append((role, email, raw))
                await conn.execute(
                    """INSERT INTO email_verifications (tenant_id, subject_role, email, token_hash,
                       needs_review, expires_at) VALUES ($1,$2,$3,$4,$5, now() + interval '7 days')""",
                    family_tenant_id, role, email,
                    hashlib.sha256(raw.encode()).hexdigest(), review)

            await conn.execute("UPDATE pending_signups SET consumed_at = now() WHERE id = $1::uuid", pending_id)

        try:
            await conn.execute(
                """INSERT INTO audit_log (tenant_id, actor_user_id, actor_role, action, target_table,
                   target_id, target_label, new_value)
                   VALUES ($1,$2,'tenant_owner','coppa_vpc_captured','tenants',$3,$4,$5::jsonb)""",
                family_tenant_id, parent_user_id, str(family_tenant_id),
                f"{p['parent_last_name']} Family - under-13 payment-first signup",
                json.dumps({"method": "payment_transaction", "processor": "stripe",
                            "checkout_session": sess.get("id"), "subscription": sess.get("subscription"),
                            "plan_key": plan_key, "pending_signup_id": pending_id,
                            "student_age_at_capture": compute_current_age(int(p["student_birth_year"])),
                            "cohort_code": pend["cohort_code"]}))
        except Exception as exc:
            log.warning("vpc audit insert failed (non-fatal): %r", exc)

    for role, email, raw in tokens_to_send:
        try:
            await _send_verification_email(email, role, p["student_first_name"], raw)
        except Exception as exc:
            log.warning("verification email to %s failed (non-fatal): %r", email, exc)
    try:
        await _send_welcome_email(p["parent_email"], p["parent_display_name"], raw_token)
    except Exception as exc:
        log.warning("welcome email failed (non-fatal): %r", exc)

    log.info("under13_signup_completed tenant=%s student=%s pending=%s", family_tenant_id, student_id, pending_id)
    return {"received": True, "provisioned": str(family_tenant_id)}


async def _send_welcome_email(to_email: str, display_name: str, token: str) -> None:
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        log.warning("RESEND_API_KEY not set; welcome email to %s skipped", to_email)
        return
    sender = os.environ.get("EMAIL_FROM", "outcomestar <onboarding@resend.dev>")
    html = (
        '<div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;color:#1a1a2e">'
        '<h2 style="color:#201868">Welcome to outcomestar, ' + display_name + '</h2>'
        '<p>Your payment confirmed parental consent and your family account is ready.</p>'
        '<p>Your parent portal access token (keep it private):</p>'
        '<p style="background:#F4F4F0;padding:12px;border-radius:8px;font-family:monospace;word-break:break-all">' + token + '</p>'
        '<p><a href="https://outcomestar.app/portal" style="background:#F07800;color:#fff;padding:12px 26px;'
        'border-radius:8px;text-decoration:none;font-weight:bold">Open the parent portal</a></p>'
        '<p style="color:#7A8A9E;font-size:12px">Also confirm the verification emails we just sent.</p></div>'
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"from": sender, "to": [to_email],
                  "subject": "Welcome to outcomestar - your portal access", "html": html})
        if r.status_code >= 300:
            log.warning("welcome send failed %s: %s", r.status_code, r.text[:200])
