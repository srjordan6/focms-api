"""
focms_cohort_signup.py - Anonymous cohort-based parent signup for
outcomestar.app. Provisions a family tenant, parent user, student
record, tenant_owner role, and API token in one atomic transaction.

Architecture: archive_entries source_id='cohort_signup_backend_design_v0_1'

v0.11.18 (2026-07-16):
- Demo account reset-on-login: when the login email matches DEMO_ACCOUNT_EMAIL
  (env, default demo@outcomestar.app), _demo_restore wipes the demo tenant's
  data tables and re-inserts the pristine rows stored in demo_snapshot
  (jsonb per table, MCP-captured) before the portal token is minted. Every
  demo login therefore starts from the exact same seeded state - edits made
  while demonstrating vanish on the next login. Identity tables (users,
  user_credentials, tenants, roles, api_tokens) are never reset. Restore
  failures log and never block login.

v0.11.17 (2026-07-06):
- Cloudflare Turnstile on cohort-signup, /auth/login, /auth/forgot-password.

v0.11.16a (2026-07-06):
- POST /auth/admin/test-email {to} (registry token): sends a test and returns
  the active transport + exact error string for diagnosis.

v0.11.16 (2026-07-06):
- Google Workspace email transport: unified _send_email helper. When
  GMAIL_SMTP_USER + GMAIL_SMTP_PASS are set, all product email (welcome,
  verification, password reset) sends via smtp.gmail.com:587 STARTTLS as
  support@outcomestar.app (app password; ~2,000/day/user limit). Falls back
  to Resend (RESEND_API_KEY) when Gmail env is absent, then logs-and-skips.
  SMTP runs in a thread (asyncio.to_thread) - no new dependencies.

v0.11.15 (2026-07-05):
- Password reset flow: POST /auth/forgot-password {email} (anonymous, always
  200 to avoid account enumeration; emails a 1-hour single-use link to
  outcomestar.app/reset-password) + POST /auth/reset-password
  {token, password} (consumes the token, sets the scrypt hash, clears
  lockout). New table password_resets (MCP-created).
- Change password: use POST /auth/set-password from the signed-in portal
  (portal v157 adds the Change password UI).

v0.11.14 (2026-07-05):
- Payment-first for ALL ages (Stephen decision 2026-07-05): every signup
  requires a storage plan choice ($1 one-time consent verification on the
  free plan, or a paid plan). The card payment verifies the parent and the
  card is saved on file for future billing:
  * payment mode: customer_creation=always + 
    payment_intent_data[setup_future_usage]=off_session
  * subscription mode: customer + default payment method saved by Stripe.
  The 13+ instant-provision path is removed; provisioning is webhook-only.

v0.11.13a (2026-07-05):
- login: users table uses deactivated_at (not deleted_at) - 500 fix.

v0.11.13 (2026-07-05):
- Password auth (phase 1 of auth build; passkeys are phase 2):
  * CohortSignupRequest.password (optional, min 12 chars) - hashed with
    stdlib scrypt (n=16384,r=8,p=1, per-user salt) into user_credentials
    at signup (13+ path) and at webhook provisioning (under-13 path).
  * POST /auth/set-password (bearer: registry or DB token) sets/changes
    the caller's password.
  * POST /auth/login {email,password} (anonymous, rate-limited by lockout:
    5 fails -> 15 min) verifies scrypt hash, mints a parent-portal
    api_token, returns {api_token, tenant_id, portal_url}. Email is the
    username.

v0.11.12 (2026-07-05):
- POST /auth/request-email-verification: authenticated (registry or DB
  parent-portal token) endpoint the portal calls whenever an email is entered
  or changed for the student, father, or mother. Upserts an
  email_verifications row for (tenant, subject_role, email) and sends the
  confirmation email. subject_role father/mother map to role 'parent' with
  the relationship kept in the email copy.

v0.11.11 (2026-07-05):
- Admin recovery endpoint POST /auth/admin/complete-pending/{pending_id}
  (FOCMS_API_TOKENS_JSON token required): runs _complete_pending_signup for a
  paid-but-unprovisioned under-13 signup (missed/failed webhook). Synthetic
  session metadata; plan_key from the parked payload (free -> free plan).
- Webhook handler logs event id + metadata keys on entry (diagnosis).

v0.11.10 (2026-07-05):
- Also: relationship enum value lowercase 'parent' (check constraint).
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
from datetime import datetime, date, timezone, timedelta
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

VALID_GRADES = {"NONE", "PRE-K", "K", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}

# v0.12.102: plausible age window per grade (inclusive), computed from birth year.
_GRADE_AGE_WINDOWS = {"NONE": (0, 6), "PRE-K": (2, 6), "K": (4, 7)}


def _grade_age_window(grade: str) -> tuple[int, int]:
    g = (grade or "").strip().upper()
    if g in _GRADE_AGE_WINDOWS:
        return _GRADE_AGE_WINDOWS[g]
    try:
        n = int(g)
        return (n + 4, n + 8)
    except ValueError:
        return (0, 19)


class CohortSignupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Optional[str] = Field(None, max_length=64)
    parent_email: EmailStr
    parent_display_name: str = Field(..., min_length=1, max_length=200)
    parent_first_name: str = Field(..., min_length=1, max_length=100)
    parent_last_name: str = Field(..., min_length=1, max_length=100)
    student_first_name: str = Field(..., min_length=1, max_length=100)
    student_last_name: str = Field(..., min_length=1, max_length=100)
    student_grade: str = Field(..., min_length=1, max_length=6)
    student_birth_year: Optional[int] = Field(None, ge=2000, le=2030)  # legacy; derived from birth_date when absent
    student_birth_date: Optional[date] = None  # v0.12.104: exact DOB preferred
    student_email: Optional[EmailStr] = None
    storage_plan_key: Optional[str] = None   # required when student is under 13
    birth_certificate_b64: Optional[str] = Field(None, max_length=11_500_000)  # v0.12.105: required age 0-10 (~8MB file)
    birth_certificate_mime: Optional[str] = Field(None, max_length=100)
    birth_certificate_filename: Optional[str] = Field(None, max_length=300)
    password: Optional[str] = Field(None, min_length=12, max_length=200)
    turnstile_token: Optional[str] = None
    accept_user_agreement: bool
    accept_privacy_policy: bool

    @field_validator("code")
    @classmethod
    def normalize_code(cls, v: Optional[str]) -> str:
        # v0.12.102: blank code = general-public signup cohort
        v = (v or "").strip().upper()
        return v or os.environ.get("DEFAULT_COHORT_CODE", "PUBLIC")

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


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    h = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
    return f"scrypt$16384$8$1${salt.hex()}${h.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        _, n, r, p, salt_hex, hash_hex = stored.split("$")
        h = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt_hex),
                           n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(h.hex(), hash_hex)
    except Exception:
        return False


def generate_family_slug(first: str, last: str) -> str:
    """
    Generate a family tenant slug. Format: last-first-<short-random>.
    E.g. "smith-alex-a3b9c7". Never guaranteed unique; caller retries
    on collision.
    """
    # v0.11.18: nondescript by design - never the child's name (COPPA/child-safety).
    words = ["amber","aspen","cedar","comet","coral","delta","ember","fern","flint","harbor",
             "hazel","indigo","juniper","lumen","maple","meadow","nova","onyx","orbit","pine",
             "quartz","raven","ridge","river","sierra","summit","terra","tidal","vega","willow"]
    w1 = secrets.choice(words); w2 = secrets.choice([w for w in words if w != w1])
    suffix = secrets.token_hex(3)  # 6 hex chars
    return f"{w1}-{w2}-{suffix}"


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


def _membership_key_for_age(age: int) -> str:
    """v0.12.103: age-band membership pricing. Prices live in pricing_tiers.
    0-10 free; 11-13, 14-16, 17-18 paid bands; 19+ alumni archive."""
    if age <= 10:
        return "membership_age_0_10"
    if age <= 13:
        return "membership_age_11_13"
    if age <= 16:
        return "membership_age_14_16"
    if age <= 18:
        return "membership_age_17_18"
    return "membership_age_19_plus"


async def _verify_turnstile(token, request: Request) -> None:
    """v0.11.17: enforce Turnstile when TURNSTILE_SECRET is configured.
    v0.12.113: FOCMS_TURNSTILE_MODE=soft (default) fails OPEN - a missing
    token (widget failed to load or solve: Cloudflare degradation, blocked
    challenge domains, flagged visitor IP) no longer blocks signup/login;
    a PRESENT token is still strictly verified, and a siteverify outage
    also fails open. Set FOCMS_TURNSTILE_MODE=enforce to restore the hard
    requirement."""
    secret = os.environ.get("TURNSTILE_SECRET")
    if not secret:
        return
    mode = os.environ.get("FOCMS_TURNSTILE_MODE", "soft").lower()
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        try:
            registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
            if auth[7:].strip() in registry:
                return
        except Exception:
            pass
    if not token:
        if mode == "enforce":
            raise HTTPException(400, {"error": "turnstile_required",
                                      "message": "Please complete the security check."})
        log.warning("turnstile soft-pass: no token (widget likely failed) ip=%s",
                    request.headers.get("cf-connecting-ip")
                    or (request.client.host if request.client else "?"))
        return
    ip = request.headers.get("cf-connecting-ip") or (request.client.host if request.client else None)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post("https://challenges.cloudflare.com/turnstile/v0/siteverify",
                                  data={"secret": secret, "response": token, "remoteip": ip or ""})
            ok = False
            try:
                ok = bool(r.json().get("success"))
            except Exception:
                ok = False
    except Exception as exc:
        if mode == "enforce":
            raise HTTPException(400, {"error": "turnstile_failed",
                                      "message": "Security check unavailable - reload and try again."})
        log.warning("turnstile soft-pass: siteverify unreachable (%r)", exc)
        return
    if not ok:
        raise HTTPException(400, {"error": "turnstile_failed",
                                  "message": "Security check failed - reload the page and try again."})


async def _send_email(to_email: str, subject: str, html: str) -> None:
    """v0.11.16: Gmail SMTP (Workspace) preferred, Resend fallback."""
    g_user = os.environ.get("GMAIL_SMTP_USER")
    g_pass = os.environ.get("GMAIL_SMTP_PASS")
    if g_user and g_pass:
        import smtplib
        from email.mime.text import MIMEText

        def _smtp_send():
            msg = MIMEText(html, "html", "utf-8")
            msg["Subject"] = subject
            msg["From"] = os.environ.get("EMAIL_FROM", f"outcomestar <{g_user}>")
            msg["To"] = to_email
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as srv:
                srv.starttls()
                srv.login(g_user, g_pass)
                srv.sendmail(g_user, [to_email], msg.as_string())

        import asyncio
        await asyncio.to_thread(_smtp_send)
        return
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        log.warning("no email transport configured; mail to %s skipped", to_email)
        return
    sender = os.environ.get("EMAIL_FROM", "outcomestar <onboarding@resend.dev>")
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"from": sender, "to": [to_email], "subject": subject, "html": html})
        if r.status_code >= 300:
            log.warning("resend send failed %s: %s", r.status_code, r.text[:200])


async def _send_verification_email(to_email: str, role: str, student_name: str, token: str) -> None:
    await _send_email(to_email, "Confirm your email — outcomestar", html)


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
                   VALUES ($1, $2, 'parent', true, 1,
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
    await _verify_turnstile(body.turnstile_token, request)
    # v0.12.104: exact birth date preferred; fall back to July-1 of birth year.
    if not body.student_birth_date and not body.student_birth_year:
        raise HTTPException(400, {"error": "invalid_request",
                                  "message": "student_birth_date is required."})
    bdate = body.student_birth_date or date(body.student_birth_year, 7, 1)
    if body.student_birth_year is None:
        body.student_birth_year = bdate.year
    today = date.today()
    age = today.year - bdate.year - ((today.month, today.day) < (bdate.month, bdate.day))
    if bdate > today or age > 19:
        raise HTTPException(400, {
            "error": "invalid_request",
            "message": "student_birth_date is not plausible for a Pre-K to grade-12 student.",
        })
    lo, hi = _grade_age_window(body.student_grade)
    if not (lo <= age <= hi):
        _glabel = {"NONE": "not yet in school", "PRE-K": "Pre-K", "K": "kindergarten"}.get(
            body.student_grade, f"grade {body.student_grade}")
        raise HTTPException(400, {
            "error": "grade_age_mismatch",
            "message": f"A student who is {_glabel} is usually {lo}-{hi} years old, "
                       f"but this birth date makes the student {age}. "
                       "Check the grade and birth date and try again.",
        })

    # v0.12.105: ages 0-10 require a birth certificate upload for age validation.
    _ALLOWED_DOC_MIMES = {"image/jpeg", "image/png", "image/webp", "application/pdf"}
    doc_bytes = None
    if age <= 10:
        if not body.birth_certificate_b64:
            raise HTTPException(400, {
                "error": "birth_certificate_required",
                "message": "For students aged 0-10, upload the student's birth certificate "
                           "(JPG, PNG, or PDF) so we can validate the birth date for free access.",
            })
        if (body.birth_certificate_mime or "").lower() not in _ALLOWED_DOC_MIMES:
            raise HTTPException(400, {
                "error": "birth_certificate_invalid",
                "message": "Birth certificate must be a JPG, PNG, WEBP, or PDF file.",
            })
        import base64 as _b64
        try:
            doc_bytes = _b64.b64decode(body.birth_certificate_b64, validate=True)
        except Exception:
            raise HTTPException(400, {"error": "birth_certificate_invalid",
                                      "message": "Birth certificate upload could not be read - try again."})
        if len(doc_bytes) < 10_000 or len(doc_bytes) > 8_000_000:
            raise HTTPException(400, {
                "error": "birth_certificate_invalid",
                "message": "Birth certificate file must be between 10 KB and 8 MB.",
            })

    pool: asyncpg.Pool = request.app.state.pool

    # v0.11.14: payment-first for ALL ages. The card payment verifies the
    # parent (FTC-recognized for under-13 COPPA consent; identity + billing
    # readiness for 13+) and the card is kept on file. Provisioning happens
    # only in the webhook after payment.
    if True:
        api_key = os.environ.get("STRIPE_SECRET_KEY")
        if not api_key:
            raise HTTPException(400, {
                "error": "vpc_required",
                "message": "Signup requires parent verification by card; "
                           "enrollment is temporarily unavailable.",
            })
        requested = (body.storage_plan_key or "").strip().lower()
        one_time = requested == "free"
        mem_key = _membership_key_for_age(age)
        needs_idv = age <= 10  # v0.12.106: commercial IDV for the free age band
        async with pool.acquire() as conn:
            mem = await conn.fetchrow(
                "SELECT plan_key, display_name, price_usd_cents FROM pricing_tiers "
                "WHERE plan_key = $1 AND active", mem_key)
            mem_cents = int(mem["price_usd_cents"]) if mem else 0
            idv = await conn.fetchrow(
                "SELECT display_name, price_usd_cents FROM pricing_tiers "
                "WHERE plan_key = 'idv_verification' AND active") if needs_idv else None
            if needs_idv and not idv:
                raise HTTPException(503, {
                    "error": "billing_misconfigured",
                    "message": "Age verification pricing is unavailable - try again shortly.",
                })
            if one_time:
                tier = {"plan_key": "free", "display_name": None, "price_usd_cents": 0}
            else:
                tier = await conn.fetchrow(
                    "SELECT plan_key, display_name, price_usd_cents FROM pricing_tiers "
                    "WHERE plan_key = $1 AND active AND price_usd_cents > 0 AND NOT stackable",
                    requested)
            if not tier:
                raise HTTPException(400, {
                    "error": "vpc_plan_required",
                    "message": "Choose the included storage (with the one-time age "
                               "verification for ages 0-10) or a storage plan - the card "
                               "payment verifies the parent and stays on file for billing.",
                })
            existing = await conn.fetchval(
                "SELECT id FROM users WHERE email = $1 AND deactivated_at IS NULL",
                body.parent_email)
            if existing:
                raise HTTPException(409, {"error": "email_already_exists",
                                          "message": "An account with this email already exists."})
            # v0.11.13: never park a plaintext password - hash before storing
            _pend = json.loads(body.model_dump_json())
            _pend.pop("birth_certificate_b64", None)  # v0.12.105: bytes go to columns, not payload
            if _pend.get("password"):
                _pend["password_hash"] = _pend.pop("password")
                _pend["password_hash"] = _hash_password(_pend["password_hash"])
            else:
                _pend.pop("password", None)
            pending_id = await conn.fetchval(
                "INSERT INTO pending_signups (payload, cohort_code, doc_bytes, doc_mime, doc_filename) "
                "VALUES ($1::jsonb, $2, $3, $4, $5) RETURNING id",
                json.dumps(_pend), body.code, doc_bytes,
                body.birth_certificate_mime if doc_bytes else None,
                (body.birth_certificate_filename or "birth_certificate") if doc_bytes else None)
        # v0.12.109: ALL prices are inline price_data read from pricing_tiers -
        # Stripe's product catalog is no longer referenced anywhere. Changing any
        # price is a single UPDATE on pricing_tiers.
        def _idv_item(idx: int) -> dict:
            return {
                f"line_items[{idx}][price_data][currency]": "usd",
                f"line_items[{idx}][price_data][unit_amount]": str(int(idv["price_usd_cents"])),
                f"line_items[{idx}][price_data][product_data][name]":
                    f"outcomestar {idv['display_name']}",
                f"line_items[{idx}][quantity]": "1",
            }
        def _storage_item(idx: int) -> dict:
            return {
                f"line_items[{idx}][price_data][currency]": "usd",
                f"line_items[{idx}][price_data][unit_amount]": str(int(tier["price_usd_cents"])),
                f"line_items[{idx}][price_data][recurring][interval]": "year",
                f"line_items[{idx}][price_data][product_data][name]":
                    f"outcomestar {tier['display_name']}",
                f"line_items[{idx}][quantity]": "1",
            }
        if mem_cents > 0:
            form = {
                "mode": "subscription",
                "line_items[0][price_data][currency]": "usd",
                "line_items[0][price_data][unit_amount]": str(mem_cents),
                "line_items[0][price_data][recurring][interval]": "year",
                "line_items[0][price_data][product_data][name]":
                    f"outcomestar {mem['display_name']}",
                "line_items[0][quantity]": "1",
                "success_url": "https://outcomestar.app/signup?vpc=complete",
                "cancel_url": "https://outcomestar.app/signup?vpc=cancelled",
                "customer_email": body.parent_email,
                "metadata[pending_signup_id]": str(pending_id),
                "metadata[plan_key]": tier["plan_key"],
                "metadata[membership_key]": mem_key,
            }
            if not one_time:
                form.update(_storage_item(1))
        elif needs_idv and idv:
            form = {
                "mode": "payment" if one_time else "subscription",
                "success_url": "https://outcomestar.app/signup?vpc=complete",
                "cancel_url": "https://outcomestar.app/signup?vpc=cancelled",
                "customer_email": body.parent_email,
                "metadata[pending_signup_id]": str(pending_id),
                "metadata[plan_key]": tier["plan_key"],
                "metadata[membership_key]": mem_key,
                "metadata[idv]": "1",
            }
            if one_time:
                form.update(_idv_item(0))
                form.update({"customer_creation": "always",
                             "payment_intent_data[setup_future_usage]": "off_session"})
            else:
                form.update(_storage_item(0))
                form.update(_idv_item(1))
        else:
            form = {
                "mode": "payment" if one_time else "subscription",
                **({"customer_creation": "always",
                    "payment_intent_data[setup_future_usage]": "off_session"} if one_time else {}),
                "success_url": "https://outcomestar.app/signup?vpc=complete",
                "cancel_url": "https://outcomestar.app/signup?vpc=cancelled",
                "customer_email": body.parent_email,
                "metadata[pending_signup_id]": str(pending_id),
                "metadata[plan_key]": tier["plan_key"],
                "metadata[membership_key]": mem_key,
            }
            if not one_time:
                form.update(_storage_item(0))
        form["allow_promotion_codes"] = "true"  # v0.12.128 (2026-07-15): enables friend/beta invite codes at signup checkout
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
            approx_birth_date = body.student_birth_date or date(body.student_birth_year, 7, 1)
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

            # Step 8a (v0.11.13): password credential when supplied at signup
            if body.password:
                await conn.execute(
                    """INSERT INTO user_credentials (user_id, password_hash) VALUES ($1,$2)
                       ON CONFLICT (user_id) DO UPDATE SET password_hash=EXCLUDED.password_hash, updated_at=now()""",
                    parent_user_id, _hash_password(body.password))

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
            "SELECT plan_key, display_name, price_usd_cents, stackable FROM pricing_tiers "
            "WHERE plan_key = $1 AND active AND price_usd_cents > 0",
            body.plan_key,
        )
    if not tier:
        raise HTTPException(404, {"error": "plan_not_found", "message": "Unknown or non-purchasable plan."})
    qty = body.quantity if tier["stackable"] else 1
    form = {
        "mode": "subscription",
        "line_items[0][price_data][currency]": "usd",
        "line_items[0][price_data][unit_amount]": str(int(tier["price_usd_cents"])),
        "line_items[0][price_data][recurring][interval]": "year",
        "line_items[0][price_data][product_data][name]": f"outcomestar {tier['display_name']}",
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
    form["allow_promotion_codes"] = "true"  # v0.12.128 (2026-07-15): enables promo codes on storage add-on checkouts from within portal
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post("https://api.stripe.com/v1/checkout/sessions",
                              headers={"Authorization": f"Bearer {api_key}"}, data=form)
    if r.status_code >= 300:
        log.warning("stripe checkout create failed %s: %s", r.status_code, r.text[:300])
        raise HTTPException(502, {"error": "stripe_error", "message": "Could not start checkout."})
    sess = r.json()
    return {"checkout_url": sess["url"], "session_id": sess["id"]}


class BillingPortalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    return_url: str = Field("https://outcomestar.app/portal")


@router.post("/billing-portal-session")
async def billing_portal_session(body: BillingPortalRequest, request: Request) -> dict[str, Any]:
    """v0.12.114: Stripe Customer Portal - lets the parent cancel a plan,
    change the payment method, and see invoices. Requires the tenant to
    have a stripe_customer_id (set at first checkout). The portal itself
    is Stripe-hosted; configure defaults once in Stripe Dashboard ->
    Settings -> Billing -> Customer portal."""
    api_key = os.environ.get("STRIPE_SECRET_KEY")
    if not api_key:
        raise HTTPException(503, {"error": "billing_unavailable", "message": "Billing is not configured."})
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        principal = await _tenant_from_bearer(request, conn)
    if not principal.get("stripe_customer_id"):
        raise HTTPException(404, {"error": "no_billing_account",
                                  "message": "No paid plan on file yet - there is nothing to manage or cancel."})
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post("https://api.stripe.com/v1/billing_portal/sessions",
                              headers={"Authorization": f"Bearer {api_key}"},
                              data={"customer": principal["stripe_customer_id"],
                                    "return_url": body.return_url})
    if r.status_code >= 300:
        log.warning("stripe portal create failed %s: %s", r.status_code, r.text[:300])
        raise HTTPException(502, {"error": "stripe_error", "message": "Could not open the billing portal."})
    return {"portal_url": r.json()["url"]}


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
    log.info("stripe_webhook event=%s type=%s", event.get("id"), event.get("type"))
    etype = event.get("type") or ""
    # v0.12.107: birthday-billing retry succeeded - clear the hold instantly.
    if etype == "payment_intent.succeeded":
        pi = event["data"]["object"]
        pmeta = pi.get("metadata") or {}
        if pmeta.get("birthday_billing") == "1" and pmeta.get("tenant_id"):
            _now = datetime.now(timezone.utc)
            pool: asyncpg.Pool = request.app.state.pool
            async with pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO tenant_settings (tenant_id, feature_flags)
                       VALUES ($1::uuid, jsonb_build_object('billing_hold', false, 'membership', $2::jsonb))
                       ON CONFLICT (tenant_id) DO UPDATE SET
                       feature_flags = coalesce(tenant_settings.feature_flags,'{}'::jsonb)
                                       || jsonb_build_object('billing_hold', false)
                                       || jsonb_build_object('membership',
                                          coalesce(tenant_settings.feature_flags->'membership','{}'::jsonb) || $2::jsonb),
                       updated_at = now()""",
                    pmeta["tenant_id"],
                    json.dumps({"paid_key": pmeta.get("membership_key"),
                                "paid_at": _now.date().isoformat(),
                                "paid_until": (_now + timedelta(days=365)).date().isoformat(),
                                "payment_intent": pi.get("id"), "pending_hold": None}))
            log.info("birthday_billing_paid tenant=%s pi=%s", pmeta["tenant_id"], pi.get("id"))
            return {"received": True}
        return {"received": True, "ignored": "pi_no_birthday_meta"}
    if etype != "checkout.session.completed":
        return {"received": True, "ignored": etype}
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


class EmailVerifyRequest(BaseModel):
    email: EmailStr
    subject_role: str = "parent"   # parent | father | mother | student


async def _send_reset_email(to_email: str, token: str) -> None:
    await _send_email(to_email, "Reset your password - outcomestar", html)


class ForgotPasswordRequest(BaseModel):
    turnstile_token: Optional[str] = None
    email: EmailStr


@router.post("/forgot-password")
async def forgot_password(body: ForgotPasswordRequest, request: Request) -> dict[str, Any]:
    """v0.11.15: always 200 (no account enumeration)."""
    await _verify_turnstile(body.turnstile_token, request)
    email = str(body.email).strip().lower()
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM users WHERE lower(email)=$1 AND deactivated_at IS NULL", email)
        if user:
            raw = secrets.token_urlsafe(32)
            await conn.execute(
                "INSERT INTO password_resets (user_id, token_hash, expires_at) "
                "VALUES ($1, $2, now() + interval '1 hour')",
                user["id"], hashlib.sha256(raw.encode()).hexdigest())
            try:
                await _send_reset_email(email, raw)
            except Exception as exc:
                log.warning("forgot-password send failed: %s", exc)
    return {"ok": True, "message": "If that email has an account, a reset link is on its way."}


class ResetPasswordRequest(BaseModel):
    token: str = Field(..., min_length=10, max_length=128)
    password: str = Field(..., min_length=12, max_length=200)


@router.post("/reset-password")
async def reset_password(body: ResetPasswordRequest, request: Request) -> dict[str, Any]:
    th = hashlib.sha256(body.token.encode()).hexdigest()
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, user_id FROM password_resets WHERE token_hash=$1 "
            "AND used_at IS NULL AND expires_at > now()", th)
        if not row:
            raise HTTPException(400, {"error": "invalid_or_expired",
                                      "message": "This reset link is invalid or has expired. Request a new one."})
        await conn.execute("UPDATE password_resets SET used_at=now() WHERE id=$1", row["id"])
        await conn.execute(
            """INSERT INTO user_credentials (user_id, password_hash) VALUES ($1,$2)
               ON CONFLICT (user_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,
               failed_attempts=0, locked_until=NULL, updated_at=now()""",
            row["user_id"], _hash_password(body.password))
    return {"ok": True}


class SetPasswordRequest(BaseModel):
    password: str = Field(..., min_length=12, max_length=200)


@router.post("/set-password")
async def set_password(body: SetPasswordRequest, request: Request) -> dict[str, Any]:
    """v0.11.13: set/change the caller's password (email is the username)."""
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, {"error": "auth_required"})
    token = auth[7:].strip()
    user_id = None
    try:
        registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
    except Exception:
        registry = {}
    if token in registry:
        user_id = registry[token].get("user_id")
    else:
        from focms_api import db_token_principal
        ctx = await db_token_principal(request.app.state.pool, token)
        if ctx:
            user_id = ctx.get("user_id")
    if not user_id:
        raise HTTPException(401, {"error": "invalid_token"})
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO user_credentials (user_id, password_hash) VALUES ($1,$2)
               ON CONFLICT (user_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,
               failed_attempts=0, locked_until=NULL, updated_at=now()""",
            user_id, _hash_password(body.password))
    return {"ok": True}


DEMO_ACCOUNT_EMAIL = os.environ.get("DEMO_ACCOUNT_EMAIL", "demo@outcomestar.app").strip().lower()

# v0.11.18: delete order children-first; insert order is the reverse.
_DEMO_TABLES = [
    "uca_form_instances", "applications", "essays", "student_life_milestones",
    "personal_records", "events", "affiliations", "standardized_test_scores",
    "class_rank_history", "gpa_history", "courses_taken",
    "student_school_enrollments", "family_members",
    "student_personal_details", "students",
]


async def _demo_restore(conn, tenant_id: str) -> None:
    """v0.11.18: restore the demo tenant to its pristine snapshot. Deletes all
    demo-tenant rows in _DEMO_TABLES (children first) and re-inserts the rows
    stored in demo_snapshot (parents first). Runs inside one transaction with
    tenant RLS context. Table names come from the literal list above - never
    from input."""
    tid = str(UUID(str(tenant_id)))  # validate before f-string (playbook)
    async with conn.transaction():
        await conn.execute(f"SET LOCAL app.current_tenant_id = '{tid}'")
        for t in _DEMO_TABLES:
            await conn.execute(f"DELETE FROM {t} WHERE tenant_id=$1::uuid", tid)
        for t in reversed(_DEMO_TABLES):
            await conn.execute(
                f"INSERT INTO {t} SELECT * FROM jsonb_populate_recordset(null::{t}, "
                f"(SELECT rows FROM demo_snapshot WHERE table_name=$1))", t)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1, max_length=200)
    turnstile_token: Optional[str] = None


@router.post("/login")
async def login(body: LoginRequest, request: Request) -> dict[str, Any]:
    """v0.11.13: email + password -> parent-portal api token.
    Lockout: 5 consecutive failures -> 15 minutes."""
    await _verify_turnstile(body.turnstile_token, request)
    email = str(body.email).strip().lower()
    pool: asyncpg.Pool = request.app.state.pool
    generic = HTTPException(401, {"error": "invalid_credentials",
                                  "message": "Email or password is incorrect."})
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, display_name FROM users WHERE lower(email)=$1 AND deactivated_at IS NULL", email)
        if not user:
            raise generic
        cred = await conn.fetchrow(
            "SELECT password_hash, failed_attempts, locked_until FROM user_credentials WHERE user_id=$1",
            user["id"])
        if not cred:
            raise HTTPException(401, {"error": "no_password_set",
                                      "message": "No password set for this account - use your portal link, then Set password."})
        if cred["locked_until"] and cred["locked_until"] > datetime.now(timezone.utc):
            raise HTTPException(429, {"error": "locked",
                                      "message": "Too many attempts. Try again in a few minutes."})
        if not _verify_password(body.password, cred["password_hash"]):
            await conn.execute(
                """UPDATE user_credentials SET failed_attempts = failed_attempts + 1,
                   locked_until = CASE WHEN failed_attempts + 1 >= 5
                                       THEN now() + interval '15 minutes' ELSE NULL END,
                   updated_at = now() WHERE user_id=$1""", user["id"])
            raise generic
        await conn.execute(
            "UPDATE user_credentials SET failed_attempts=0, locked_until=NULL, updated_at=now() WHERE user_id=$1",
            user["id"])
        role = await conn.fetchrow(
            "SELECT tenant_id FROM user_tenant_roles WHERE user_id=$1 ORDER BY granted_at LIMIT 1",
            user["id"])
        if not role:
            raise generic
        tenant_id = role["tenant_id"]
        if email == DEMO_ACCOUNT_EMAIL:
            try:
                await _demo_restore(conn, str(tenant_id))
            except Exception as exc:  # never block a demo login on restore
                log.warning("demo restore failed: %s", exc)
        students = await conn.fetch(
            "SELECT id FROM students WHERE tenant_id=$1 AND deleted_at IS NULL ORDER BY created_at",
            tenant_id)
        raw_token, token_hash = generate_api_token()
        await conn.execute(
            """INSERT INTO api_tokens (tenant_id, token_hash, student_ids, name, scope, created_by)
               VALUES ($1,$2,$3::uuid[],'login','parent_portal',$4)""",
            tenant_id, token_hash, [r["id"] for r in students], user["id"])
    return {"api_token": raw_token, "tenant_id": str(tenant_id),
            "display_name": user["display_name"],
            "portal_url": f"https://outcomestar.app/portal#t={raw_token}"}


@router.post("/request-email-verification")
async def request_email_verification(body: EmailVerifyRequest, request: Request) -> dict[str, Any]:
    """v0.11.12: (re)issue a verification email for an entered address."""
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, {"error": "auth_required"})
    token = auth[7:].strip()
    tenant_id = None
    try:
        registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
    except Exception:
        registry = {}
    if token in registry:
        tenant_id = registry[token].get("tenant_id")
    else:
        from focms_api import db_token_principal
        ctx = await db_token_principal(request.app.state.pool, token)
        if ctx:
            tenant_id = ctx.get("tenant_id")
    if not tenant_id:
        raise HTTPException(401, {"error": "invalid_token"})

    role_in = body.subject_role.strip().lower()
    if role_in not in ("parent", "father", "mother", "student"):
        raise HTTPException(422, {"error": "bad_subject_role"})
    db_role = "student" if role_in == "student" else "parent"
    email = str(body.email).strip().lower()
    review = (db_role == "student") and not _looks_edu(email)

    raw = secrets.token_urlsafe(32)
    th = hashlib.sha256(raw.encode()).hexdigest()
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        already = await conn.fetchrow(
            "SELECT 1 FROM email_verifications WHERE tenant_id=$1::uuid AND subject_role=$2 "
            "AND email=$3 AND verified_at IS NOT NULL", tenant_id, db_role, email)
        if already:
            return {"sent": False, "already_verified": True, "email": email}
        await conn.execute(
            "DELETE FROM email_verifications WHERE tenant_id=$1::uuid AND subject_role=$2 "
            "AND email=$3 AND verified_at IS NULL", tenant_id, db_role, email)
        await conn.execute(
            "INSERT INTO email_verifications (tenant_id, subject_role, email, token_hash, needs_review, expires_at) "
            "VALUES ($1::uuid, $2, $3, $4, $5, now() + interval '7 days')",
            tenant_id, db_role, email, th, review)
        student_name = await conn.fetchval(
            "SELECT first_name FROM students WHERE tenant_id=$1::uuid ORDER BY created_at LIMIT 1",
            tenant_id) or "your student"
    try:
        await _send_verification_email(email, role_in, student_name, raw)
        sent = True
    except Exception as exc:
        log.warning("request-email-verification send failed for %s: %s", email, exc)
        sent = False
    return {"sent": sent, "already_verified": False, "email": email, "needs_review": review}


@router.post("/admin/test-email")
async def admin_test_email(request: Request) -> dict[str, Any]:
    """v0.11.16a: send a test email and RETURN the transport + any error."""
    auth = request.headers.get("authorization", "")
    try:
        registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
    except Exception:
        registry = {}
    if not auth.lower().startswith("bearer ") or auth[7:].strip() not in registry:
        raise HTTPException(401, {"error": "admin_token_required"})
    body = await request.json()
    to = body.get("to")
    transport = "gmail" if (os.environ.get("GMAIL_SMTP_USER") and os.environ.get("GMAIL_SMTP_PASS")) else (
        "resend" if os.environ.get("RESEND_API_KEY") else "none")
    try:
        await _send_email(to, "outcomestar transport test",
                          "<p>Transport test from focms-api.</p>")
        return {"transport": transport, "ok": True}
    except Exception as exc:
        return {"transport": transport, "ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}


@router.post("/admin/complete-pending/{pending_id}")
async def admin_complete_pending(pending_id: str, request: Request) -> dict[str, Any]:
    """v0.11.11: manual recovery for paid-but-unprovisioned under-13 signups."""
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, {"error": "auth_required"})
    try:
        registry = json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))
    except Exception:
        registry = {}
    if auth[7:].strip() not in registry:
        raise HTTPException(401, {"error": "admin_token_required"})
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        pend = await conn.fetchrow(
            "SELECT payload FROM pending_signups WHERE id = $1::uuid AND consumed_at IS NULL",
            pending_id)
    if not pend:
        raise HTTPException(404, {"error": "pending_not_found_or_consumed"})
    p = json.loads(pend["payload"])
    plan_key = (p.get("storage_plan_key") or "free").strip().lower()
    sess = {"id": f"manual-recovery-{pending_id}", "customer": None, "subscription": None}
    return await _complete_pending_signup(request, pending_id, sess, {"plan_key": plan_key})


async def _complete_pending_signup(request: Request, pending_id: str, sess: dict, meta: dict) -> dict[str, Any]:
    """v0.11.6: provision a parked under-13 signup after Stripe payment (VPC)."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        pend = await conn.fetchrow(
            "SELECT id, payload, cohort_code, doc_bytes, doc_mime, doc_filename FROM pending_signups "
            "WHERE id = $1::uuid AND consumed_at IS NULL AND expires_at > now()", pending_id)
    if not pend:
        log.warning("pending signup %s missing/expired/consumed", pending_id)
        return {"received": True, "ignored": "pending_missing"}
    p = json.loads(pend["payload"])
    plan_key = meta.get("plan_key", "keepsake")
    def _pending_age() -> int:
        try:
            if p.get("student_birth_date"):
                b = date.fromisoformat(p["student_birth_date"])
                t = date.today()
                return t.year - b.year - ((t.month, t.day) < (b.month, b.day))
            return compute_current_age(int(p.get("student_birth_year") or 0))
        except Exception:
            return 0
    membership_key = meta.get("membership_key") or _membership_key_for_age(_pending_age())

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
                    p["parent_email"], membership_key or cohort["tier"], plan_key, quota, sess.get("customer"))
            except asyncpg.UniqueViolationError:
                slug = generate_family_slug(p["student_first_name"], p["student_last_name"])
                family_tenant_id = await conn.fetchval(
                    """INSERT INTO tenants (slug, short_id, display_name, primary_email, country, locale,
                       timezone, plan, status, storage_used_bytes, storage_plan, storage_quota_gb,
                       stripe_customer_id, billing_verified_at)
                       VALUES ($1,$2,$3,$4,'US','en-US','America/Chicago',$5,'active',0,$6,$7,$8,now())
                       RETURNING id""",
                    slug, generate_short_id(), f"{p['parent_last_name']} Family",
                    p["parent_email"], membership_key or cohort["tier"], plan_key, quota, sess.get("customer"))

            parent_user_id = await conn.fetchval(
                """INSERT INTO users (email, display_name, first_name, last_name, mfa_enabled,
                   webauthn_credentials, oauth_providers, is_active, is_platform_admin, failed_login_count)
                   VALUES ($1,$2,$3,$4,false,'[]'::jsonb,'{}'::jsonb,true,false,0) RETURNING id""",
                p["parent_email"], p["parent_display_name"], p["parent_first_name"], p["parent_last_name"])

            approx_birth_date = (date.fromisoformat(p["student_birth_date"])
                                 if p.get("student_birth_date") else date(int(p["student_birth_year"]), 7, 1))
            _byear = int(p.get("student_birth_year") or approx_birth_date.year)
            hs_grad = compute_hs_graduation_year(_byear, p["student_grade"])
            student_id = await conn.fetchval(
                """INSERT INTO students (tenant_id, first_name, last_name, display_name, birth_date,
                   current_grade, expected_hs_graduation_year, residence_country, created_by, updated_by)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,'US',$8,$8) RETURNING id""",
                family_tenant_id, p["student_first_name"], p["student_last_name"],
                f"{p['student_first_name']} {p['student_last_name']}",
                approx_birth_date, p["student_grade"], hs_grad, parent_user_id)

            # v0.12.105: attach the signup-staged birth certificate for age validation.
            _bc_doc_id = None
            if pend["doc_bytes"]:
                _doc_kind = "document" if (pend["doc_mime"] or "").endswith("pdf") else "image"
                _art_id = await conn.fetchval(
                    """INSERT INTO media_files (tenant_id, student_id, kind, mime_type, original_filename,
                       byte_size, content, visibility, sha256_hex, created_by, storage_kind, bucket)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,'private',$8,$9,'inline_bytea','private') RETURNING id""",
                    family_tenant_id, student_id, _doc_kind, pend["doc_mime"],
                    pend["doc_filename"] or "birth_certificate", len(pend["doc_bytes"]),
                    pend["doc_bytes"], hashlib.sha256(pend["doc_bytes"]).hexdigest(), parent_user_id)
                _bc_doc_id = await conn.fetchval(
                    """INSERT INTO student_identity_documents (tenant_id, student_id, doc_type, artifact_id,
                       status, notes, source_system, created_by, updated_by)
                       VALUES ($1,$2,'birth_certificate',$3,'pending',
                               'Uploaded at signup for age 0-10 free-tier age validation.',
                               'signup',$4,$4) RETURNING id""",
                    family_tenant_id, student_id, _art_id, parent_user_id)
                await conn.execute("UPDATE pending_signups SET doc_bytes=NULL WHERE id=$1::uuid", pending_id)

            await conn.execute(
                """INSERT INTO user_tenant_roles (user_id, tenant_id, role, granted_at, granted_by,
                   invitation_email, accepted_at) VALUES ($1,$2,'tenant_owner',now(),$1,$3,now())""",
                parent_user_id, family_tenant_id, p["parent_email"])

            raw_token, token_hash = generate_api_token()
            await conn.execute(
                """INSERT INTO api_tokens (tenant_id, token_hash, student_ids, name, scope, created_by)
                   VALUES ($1,$2,ARRAY[$3::uuid],'parent-portal','parent_portal',$4)""",
                family_tenant_id, token_hash, student_id, parent_user_id)

            if p.get("password_hash"):
                await conn.execute(
                    """INSERT INTO user_credentials (user_id, password_hash) VALUES ($1,$2)
                       ON CONFLICT (user_id) DO UPDATE SET password_hash=EXCLUDED.password_hash, updated_at=now()""",
                    parent_user_id, p["password_hash"])

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
                            "student_age_at_capture": _pending_age(),
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

    # v0.12.108: AI birth-certificate verification (replaces Stripe Identity -
    # the parent is already verified by the card payment; what we verify is the
    # CHILD's birth certificate). Vision LLM extracts the name/DOB/registrar
    # features and compares to the signup data. Full match -> verified with a
    # 10-year validity; anything else stays pending for manual review.
    if pend["doc_bytes"] and _bc_doc_id:
        try:
            verdict = await _ai_verify_birth_certificate(
                bytes(pend["doc_bytes"]), pend["doc_mime"] or "image/jpeg",
                p["student_first_name"], p["student_last_name"],
                p.get("student_birth_date"))
            _now = datetime.now(timezone.utc)
            if verdict:
                ok = bool(verdict.get("is_birth_certificate")) \
                     and bool(verdict.get("name_matches")) \
                     and bool(verdict.get("birth_date_matches")) \
                     and bool(verdict.get("registrar_seal_visible")) \
                     and not bool(verdict.get("tamper_signs")) \
                     and (verdict.get("confidence") or "").lower() == "high"
                async with pool.acquire() as conn:
                    if ok:
                        await conn.execute(
                            "UPDATE student_identity_documents SET status='verified', verified_at=now(), "
                            "notes=$2, updated_at=now() WHERE id=$1::uuid",
                            _bc_doc_id, "Automated document check passed: " + json.dumps(verdict)[:1500])
                        await conn.execute(
                            """INSERT INTO tenant_settings (tenant_id, feature_flags)
                               VALUES ($1::uuid, jsonb_build_object('age_verification', $2::jsonb))
                               ON CONFLICT (tenant_id) DO UPDATE SET
                               feature_flags = coalesce(tenant_settings.feature_flags,'{}'::jsonb)
                                               || jsonb_build_object('age_verification', $2::jsonb),
                               updated_at = now()""",
                            family_tenant_id,
                            json.dumps({"status": "verified", "method": "ai_birth_certificate",
                                        "verified_at": _now.isoformat(),
                                        "valid_until": (_now + timedelta(days=3653)).isoformat()}))
                        log.info("birth_certificate_verified tenant=%s student=%s", family_tenant_id, student_id)
                    else:
                        _reasons = _bc_rejection_reasons(verdict)
                        await conn.execute(
                            "UPDATE student_identity_documents SET status='rejected', "
                            "notes=$2, updated_at=now() WHERE id=$1::uuid",
                            _bc_doc_id, "Automated document check failed: " + json.dumps(verdict)[:1500])
                        try:
                            await _send_email(
                                p["parent_email"],
                                "outcomestar - birth certificate could not be verified",
                                f"<p>Hi {p['parent_display_name']},</p>"
                                "<p>The automated review could not verify the birth certificate "
                                f"you uploaded for {p['student_first_name']}:</p><ul>"
                                + "".join(f"<li>{x}</li>" for x in _reasons) +
                                "</ul><p>Please upload a clear, complete photo or scan of the "
                                "official certified birth certificate in your parent portal "
                                "(Personal &rarr; Identity Documents). It is re-checked "
                                "automatically the moment you upload. Your account and record "
                                "are fully usable in the meantime.</p>")
                        except Exception as exc:
                            log.warning("bc rejection email failed (non-fatal): %r", exc)
                        log.warning("birth_certificate_rejected tenant=%s verdict=%s",
                                    family_tenant_id, json.dumps(verdict)[:300])
            else:
                log.warning("birth certificate check unavailable - stays submitted, re-checked on portal re-upload")
        except Exception as exc:
            log.warning("birth certificate AI check errored (non-fatal, stays pending): %r", exc)

    log.info("under13_signup_completed tenant=%s student=%s pending=%s", family_tenant_id, student_id, pending_id)
    return {"received": True, "provisioned": str(family_tenant_id)}


def _bc_rejection_reasons(verdict: dict) -> list[str]:
    """v0.12.111: parent-facing reasons from a failed automated check."""
    r = []
    if not verdict.get("is_birth_certificate"):
        r.append("The uploaded file does not appear to be a birth certificate.")
    if not verdict.get("name_matches"):
        found = verdict.get("child_name_on_document")
        r.append(f"The child's name on the document{(' (' + found + ')') if found else ''} "
                 "does not match the name entered at signup.")
    if not verdict.get("birth_date_matches"):
        found = verdict.get("birth_date_on_document")
        r.append(f"The birth date on the document{(' (' + found + ')') if found else ''} "
                 "does not match the birth date entered at signup.")
    if not verdict.get("registrar_seal_visible"):
        r.append("The official registrar seal is not visible - upload the certified copy, "
                 "not a hospital keepsake certificate.")
    if verdict.get("tamper_signs"):
        r.append("The document shows signs of digital editing.")
    if (verdict.get("confidence") or "").lower() != "high":
        r.append("The scan is not clear enough to read reliably - retake it in good light, "
                 "flat and fully in frame.")
    return r or ["The document could not be verified."]


async def _ai_verify_birth_certificate(doc_bytes: bytes, doc_mime: str,
                                       expected_first: str, expected_last: str,
                                       expected_birth_date: Optional[str]) -> Optional[dict]:
    """v0.12.110: provider-swappable vision check of an uploaded birth
    certificate. FOCMS_LLM_PROVIDER=anthropic|openai_compatible with
    FOCMS_LLM_API_KEY / FOCMS_LLM_BASE_URL / FOCMS_LLM_MODEL - identical env
    contract to the rest of FOCMS, no vendor lock. Returns the parsed verdict
    dict, or None (-> document stays pending for manual review).
    v0.12.112: FOCMS_VISION_PROVIDER / FOCMS_VISION_MODEL / FOCMS_VISION_BASE_URL /
    FOCMS_VISION_API_KEY override the FOCMS_LLM_* values for this check only,
    because the general text model (e.g. a coder model) is often not
    vision-capable."""
    provider = (os.environ.get("FOCMS_VISION_PROVIDER")
                or os.environ.get("FOCMS_LLM_PROVIDER", "anthropic")).lower()
    api_key = (os.environ.get("FOCMS_VISION_API_KEY")
               or os.environ.get("FOCMS_LLM_API_KEY")
               or os.environ.get("ANTHROPIC_API_KEY"))
    model = (os.environ.get("FOCMS_VISION_MODEL")
             or os.environ.get("FOCMS_LLM_MODEL", "claude-sonnet-4-6"))
    if not api_key:
        return None
    import base64 as _b64
    b64 = _b64.b64encode(doc_bytes).decode()
    system = (
        "You verify scanned US birth certificates for a child-age validation system. "
        "Examine the document and return ONLY a JSON object with these keys: "
        "is_birth_certificate (bool - is this actually a birth certificate?), "
        "child_name_on_document (string or null), "
        "birth_date_on_document (ISO YYYY-MM-DD or null), "
        "name_matches (bool - does the child name match the expected name, allowing middle names, "
        "nicknames of the expected first name, and hyphenation differences?), "
        "birth_date_matches (bool - exact match to the expected birth date?), "
        "registrar_seal_visible (bool - official state/county/city registrar seal, embossed or printed), "
        "filing_or_registration_date_visible (bool), "
        "security_features_visible (bool - security paper patterns, intaglio-style borders, watermarks, or microprint), "
        "tamper_signs (bool - visible editing, font inconsistencies, misaligned or pasted text, "
        "pixel-density mismatches around data fields), "
        "confidence (high|medium|low - high only when the document is clearly legible and all "
        "determinations are certain), notes (short string). No prose outside the JSON.")
    user_text = (f"Expected child: {expected_first} {expected_last}. "
                 f"Expected birth date: {expected_birth_date or 'unknown'}. "
                 "Return ONLY the JSON object.")
    txt = ""
    if provider == "openai_compatible":
        base = (os.environ.get("FOCMS_VISION_BASE_URL")
                or os.environ.get("FOCMS_LLM_BASE_URL", "")).rstrip("/")
        if not base:
            log.warning("birth cert check: FOCMS_LLM_BASE_URL required for openai_compatible")
            return None
        if doc_mime == "application/pdf":
            # OpenAI-format vision takes images only; PDFs go to manual review.
            log.info("birth cert PDF with openai_compatible provider - manual review")
            return None
        payload = {"model": model, "max_tokens": 800,
                   "messages": [
                       {"role": "system", "content": system},
                       {"role": "user", "content": [
                           {"type": "text", "text": user_text},
                           {"type": "image_url",
                            "image_url": {"url": f"data:{doc_mime};base64,{b64}"}}]}]}
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(base + "/chat/completions",
                headers={"Authorization": "Bearer " + api_key, "content-type": "application/json"},
                json=payload)
        if r.status_code != 200:
            log.warning("birth cert LLM error %s: %s", r.status_code, r.text[:200])
            return None
        msg = r.json()["choices"][0]["message"]
        txt = (msg.get("content") or "") or (msg.get("reasoning") or "")
    else:  # anthropic-format (default; FOCMS_VISION_BASE_URL/FOCMS_LLM_BASE_URL override the host)
        base = (os.environ.get("FOCMS_VISION_BASE_URL")
                or os.environ.get("FOCMS_LLM_BASE_URL", "https://api.anthropic.com")).rstrip("/")
        block = {"type": "document" if doc_mime == "application/pdf" else "image",
                 "source": {"type": "base64", "media_type": doc_mime, "data": b64}}
        async with httpx.AsyncClient(timeout=90) as client:
            r = await client.post(base + "/v1/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"},
                json={"model": model, "max_tokens": 800, "system": system,
                      "messages": [{"role": "user",
                                    "content": [block, {"type": "text", "text": user_text}]}]})
        if r.status_code != 200:
            log.warning("birth cert LLM error %s: %s", r.status_code, r.text[:200])
            return None
        txt = "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
    start, end = txt.find("{"), txt.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(txt[start:end + 1])
    except Exception:
        return None


async def _send_welcome_email(to_email: str, display_name: str, token: str) -> None:
    """v0.12.129 (2026-07-15): route through unified _send_email so Gmail SMTP
    is used when configured (previously hard-coded Resend only, silently
    skipping when RESEND_API_KEY was absent - the reason welcome emails were
    missing while verification/reset emails worked fine)."""
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
    await _send_email(to_email, "Welcome to outcomestar - your portal access", html)
