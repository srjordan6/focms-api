"""
focms_cohort_signup.py - Anonymous cohort-based parent signup for
outcomestar.app. Provisions a family tenant, parent user, student
record, tenant_owner role, and API token in one atomic transaction.

Architecture: archive_entries source_id='cohort_signup_backend_design_v0_1'

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

    # COPPA gate: under-13 students require VPC (not implemented in v0.11.0)
    if age < 13:
        raise HTTPException(400, {
            "error": "vpc_required",
            "message": (
                "Students under 13 require verified parental consent via a "
                "process we have not yet enabled. Please contact "
                "support@outcomestar.app to enroll."
            ),
        })

    pool: asyncpg.Pool = request.app.state.pool
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
