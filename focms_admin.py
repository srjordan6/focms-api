"""
focms_admin.py  -  Platform-admin surface for admin.outcomestar.app
====================================================================
v0.12.173

A separate, cross-tenant admin API. Access is gated purely on
users.is_platform_admin (the god-flag) - NOT on a tenant role - so a
super-admin needs no tenant. Admin tokens live in their own admin_sessions
table (tenant-independent), never in api_tokens. Every admin token is checked
back to the live users row on each request, so revoking is_platform_admin or
deactivating the account instantly kills access.

Endpoints (prefix /focms/v1/admin):
  POST /login          email+password -> admin token (scrypt verify + platform-admin gate)
  POST /logout         revoke current admin token
  GET  /me             who am I
  GET  /stats          platform-wide statistics dashboard payload

Reuses the exact scrypt format from focms_cohort_signup (_verify_password),
inlined here to avoid import coupling. No new crypto.

MOUNT: in focms_api.py, after the other include_router calls, add:
    from focms_admin import router as admin_router
    app.include_router(admin_router)
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException, Request, Header
from pydantic import BaseModel, ConfigDict, Field, EmailStr

router = APIRouter(prefix="/focms/v1/admin", tags=["admin"])

ADMIN_TOKEN_TTL_HOURS = 12


# ---------------------------------------------------------------------------
# scrypt verify - identical format to focms_cohort_signup._verify_password
# ---------------------------------------------------------------------------
def _verify_password(password: str, stored: str) -> bool:
    try:
        _, n, r, p, salt_hex, hash_hex = stored.split("$")
        h = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt_hex),
                           n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(h.hex(), hash_hex)
    except Exception:
        return False


def _new_admin_token() -> tuple[str, str]:
    raw = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return raw, token_hash


def _client_ip(request: Request) -> Optional[str]:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


# ---------------------------------------------------------------------------
# Auth dependency - resolve an admin bearer token to a live platform admin
# ---------------------------------------------------------------------------
async def _admin_context(request: Request,
                         authorization: Optional[str]) -> dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, {"error": "no_token", "message": "Admin token required."})
    raw = authorization.split(" ", 1)[1].strip()
    token_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        sess = await conn.fetchrow(
            "SELECT id, user_id, expires_at FROM admin_sessions "
            "WHERE token_hash=$1 AND revoked_at IS NULL", token_hash)
        if not sess:
            raise HTTPException(401, {"error": "invalid_token", "message": "Session not found."})
        if sess["expires_at"] <= datetime.now(timezone.utc):
            raise HTTPException(401, {"error": "expired", "message": "Session expired - log in again."})
        # Re-check the live user every request: revoking admin or deactivating
        # the account kills the session immediately.
        user = await conn.fetchrow(
            "SELECT id, email, display_name, is_platform_admin "
            "FROM users WHERE id=$1 AND deactivated_at IS NULL", sess["user_id"])
        if not user or not user["is_platform_admin"]:
            raise HTTPException(403, {"error": "not_admin", "message": "Not a platform admin."})
        await conn.execute(
            "UPDATE admin_sessions SET last_used_at=now(), last_used_ip=$2 WHERE id=$1",
            sess["id"], _client_ip(request))
    return {"session_id": sess["id"], "user_id": user["id"],
            "email": user["email"], "display_name": user["display_name"]}


async def _audit(conn, admin_user_id, action, target_type=None,
                 target_id=None, detail=None, ip=None) -> None:
    import json as _json
    await conn.execute(
        "INSERT INTO admin_audit_log (admin_user_id, action, target_type, target_id, detail, ip) "
        "VALUES ($1,$2,$3,$4,$5::jsonb,$6)",
        admin_user_id, action, target_type,
        (str(target_id) if target_id is not None else None),
        (_json.dumps(detail) if detail is not None else None), ip)


# ---------------------------------------------------------------------------
# POST /login
# ---------------------------------------------------------------------------
class AdminLoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr
    password: str = Field(..., min_length=1, max_length=400)


@router.post("/login")
async def admin_login(body: AdminLoginRequest, request: Request) -> dict[str, Any]:
    """Email+password -> admin token. Gates on is_platform_admin.
    Lockout: 5 consecutive failures -> 15 minutes (shared user_credentials counter)."""
    email = str(body.email).strip().lower()
    pool: asyncpg.Pool = request.app.state.pool
    ip = _client_ip(request)
    generic = HTTPException(401, {"error": "invalid_credentials",
                                  "message": "Email or password is incorrect."})
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, display_name, is_platform_admin "
            "FROM users WHERE lower(email)=$1 AND deactivated_at IS NULL", email)
        if not user:
            raise generic
        cred = await conn.fetchrow(
            "SELECT password_hash, failed_attempts, locked_until "
            "FROM user_credentials WHERE user_id=$1", user["id"])
        if not cred:
            raise HTTPException(401, {"error": "no_password_set",
                                      "message": "No password set for this account."})
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
        # correct password - but must ALSO be a platform admin
        if not user["is_platform_admin"]:
            await _audit(conn, user["id"], "admin_login_denied_not_admin", ip=ip)
            raise HTTPException(403, {"error": "not_admin",
                                      "message": "This account is not a platform administrator."})
        await conn.execute(
            "UPDATE user_credentials SET failed_attempts=0, locked_until=NULL, updated_at=now() "
            "WHERE user_id=$1", user["id"])
        raw_token, token_hash = _new_admin_token()
        await conn.execute(
            "INSERT INTO admin_sessions (user_id, token_hash, expires_at, last_used_ip) "
            "VALUES ($1,$2, now() + ($3 || ' hours')::interval, $4)",
            user["id"], token_hash, str(ADMIN_TOKEN_TTL_HOURS), ip)
        await conn.execute(
            "UPDATE users SET last_login_at=now() WHERE id=$1", user["id"])
        await _audit(conn, user["id"], "admin_login", ip=ip)
    return {"admin_token": raw_token,
            "display_name": user["display_name"],
            "expires_in_hours": ADMIN_TOKEN_TTL_HOURS}


@router.post("/logout")
async def admin_logout(request: Request,
                       authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    ctx = await _admin_context(request, authorization)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE admin_sessions SET revoked_at=now() WHERE id=$1", ctx["session_id"])
        await _audit(conn, ctx["user_id"], "admin_logout", ip=_client_ip(request))
    return {"ok": True}


@router.get("/me")
async def admin_me(request: Request,
                   authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    ctx = await _admin_context(request, authorization)
    return {"user_id": str(ctx["user_id"]), "email": ctx["email"],
            "display_name": ctx["display_name"]}


# ---------------------------------------------------------------------------
# GET /stats  -  platform-wide dashboard payload
# ---------------------------------------------------------------------------
@router.get("/stats")
async def admin_stats(request: Request,
                      authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    ctx = await _admin_context(request, authorization)
    pool: asyncpg.Pool = request.app.state.pool

    def _num(v):
        return float(v) if v is not None else 0

    async with pool.acquire() as conn:
        # ---- GROWTH ----
        totals = await conn.fetchrow("""
            SELECT
              (SELECT count(*) FROM tenants WHERE deleted_at IS NULL) AS tenants,
              (SELECT count(*) FROM students WHERE deleted_at IS NULL) AS students,
              (SELECT count(*) FROM users WHERE deactivated_at IS NULL) AS users,
              (SELECT count(*) FROM tenants WHERE created_at >= now() - interval '1 day') AS tenants_today,
              (SELECT count(*) FROM tenants WHERE created_at >= now() - interval '7 days') AS tenants_week,
              (SELECT count(*) FROM tenants WHERE created_at >= now() - interval '30 days') AS tenants_month
        """)
        signups_series = await conn.fetch("""
            SELECT to_char(date_trunc('week', created_at), 'YYYY-MM-DD') AS wk, count(*) AS n
            FROM tenants WHERE deleted_at IS NULL AND created_at >= now() - interval '26 weeks'
            GROUP BY 1 ORDER BY 1
        """)

        # ---- ENGAGEMENT ----
        # events / personal_records / assessments are RLS-strict
        # (tenant_id = current_tenant_id(), no NULL escape), so a tenantless
        # admin connection sees zero. Aggregate per-tenant instead: set the
        # tenant GUC, count, sum in Python. Isolation is untouched.
        tenant_rows = await conn.fetch(
            "SELECT id, display_name FROM tenants WHERE deleted_at IS NULL ORDER BY created_at")
        import uuid as _uuid
        active_list = []
        records_week_map = {}
        rtype_map = {}
        last_act_map = {}
        for tr in tenant_rows:
            tid = str(tr["id"])
            _uuid.UUID(tid)  # validate before literal interpolation
            async with pool.acquire() as tconn:
                async with tconn.transaction():
                    await tconn.execute(f"SET LOCAL app.current_tenant_id = '{tid}'")
                    ev = await tconn.fetchval(
                        "SELECT count(*) FROM events WHERE deleted_at IS NULL") or 0
                    prc = await tconn.fetchval(
                        "SELECT count(*) FROM personal_records WHERE deleted_at IS NULL") or 0
                    active_list.append({"tenant_id": tid, "name": tr["display_name"],
                                        "events": ev, "records": prc})
                    for row in await tconn.fetch(
                        "SELECT to_char(date_trunc('week', created_at),'YYYY-MM-DD') wk, count(*) n "
                        "FROM events WHERE deleted_at IS NULL AND created_at >= now() - interval '26 weeks' "
                        "GROUP BY 1"):
                        records_week_map[row["wk"]] = records_week_map.get(row["wk"], 0) + row["n"]
                    for row in await tconn.fetch(
                        "SELECT event_type t, count(*) n FROM events WHERE deleted_at IS NULL GROUP BY 1"):
                        rtype_map[row["t"]] = rtype_map.get(row["t"], 0) + row["n"]
                    la = await tconn.fetchval(
                        "SELECT GREATEST("
                        " COALESCE((SELECT max(created_at) FROM events),'epoch'),"
                        " COALESCE((SELECT max(created_at) FROM personal_records),'epoch'),"
                        " COALESCE((SELECT max(created_at) FROM assessments),'epoch'))")
                    last_act_map[tid] = la
        active_tenants = sorted(active_list, key=lambda x: x["events"], reverse=True)[:10]
        records_series = [{"week": k, "count": records_week_map[k]} for k in sorted(records_week_map)]
        records_by_type_out = sorted(
            [{"type": k, "count": v} for k, v in rtype_map.items()],
            key=lambda x: x["count"], reverse=True)
        _now = datetime.now(timezone.utc)
        def _days_ago(dt):
            return (_now - dt).days if dt else 99999
        dormant_out = {
            "d30": sum(1 for v in last_act_map.values() if _days_ago(v) >= 30),
            "d60": sum(1 for v in last_act_map.values() if _days_ago(v) >= 60),
            "d90": sum(1 for v in last_act_map.values() if _days_ago(v) >= 90),
        }

        # ---- REVENUE ----
        subs_by_plan = await conn.fetch("""
            SELECT plan_code, count(*) AS n FROM subscriptions
            WHERE status IN ('active','trialing') GROUP BY 1 ORDER BY 2 DESC
        """)
        # price map for MRR (annualized plans -> monthly)
        plan_prices = await conn.fetch(
            "SELECT plan_code, amount_cents, billing_interval FROM billing_plans")

        # ---- OPERATIONAL ----
        grade_bands = await conn.fetch("""
            SELECT COALESCE(current_grade::text,'unknown') AS grade, count(*) AS n
            FROM students WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 1
        """)
        storage = await conn.fetchrow("""
            SELECT COALESCE(sum(storage_used_bytes),0) AS used_bytes,
                   COALESCE(sum(storage_quota_gb),0) AS quota_gb
            FROM tenants WHERE deleted_at IS NULL
        """)
        top_storage = await conn.fetch("""
            SELECT display_name AS name, storage_used_bytes, storage_quota_gb
            FROM tenants WHERE deleted_at IS NULL AND storage_used_bytes > 0
            ORDER BY storage_used_bytes DESC LIMIT 10
        """)

    # revenue math
    price_by_code = {r["plan_code"]: (r["amount_cents"], r["billing_interval"])
                     for r in plan_prices}
    mrr_cents = 0
    subs_out = []
    for s in subs_by_plan:
        code = s["plan_code"]; n = s["n"]
        amt, interval = price_by_code.get(code, (0, "year"))
        monthly = (amt / 12.0) if (interval or "year") == "year" else amt
        mrr_cents += monthly * n
        subs_out.append({"plan": code, "count": n,
                         "amount_cents": amt, "interval": interval})

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "growth": {
            "tenants": totals["tenants"], "students": totals["students"], "users": totals["users"],
            "new_tenants_today": totals["tenants_today"],
            "new_tenants_week": totals["tenants_week"],
            "new_tenants_month": totals["tenants_month"],
            "signups_by_week": [{"week": r["wk"], "count": r["n"]} for r in signups_series],
        },
        "engagement": {
            "records_by_week": records_series,
            "most_active_tenants": active_tenants,
            "dormant": dormant_out,
            "records_by_type": records_by_type_out,
        },
        "revenue": {
            "active_subscriptions_by_plan": subs_out,
            "mrr_cents": round(mrr_cents),
            "mrr_dollars": round(mrr_cents / 100.0, 2),
            "annual_run_rate_dollars": round(mrr_cents * 12 / 100.0, 2),
        },
        "operational": {
            "students_by_grade": [{"grade": r["grade"], "count": r["n"]} for r in grade_bands],
            "storage_used_bytes": int(storage["used_bytes"]),
            "storage_used_gb": round(int(storage["used_bytes"]) / (1024**3), 2),
            "storage_quota_gb": int(storage["quota_gb"]),
            "top_storage_tenants": [
                {"name": r["name"], "used_bytes": int(r["storage_used_bytes"]),
                 "used_gb": round(int(r["storage_used_bytes"]) / (1024**3), 3),
                 "quota_gb": r["storage_quota_gb"]} for r in top_storage],
        },
    }
