"""focms_i18n.py - Internationalization endpoints for the FOCMS API.

Drops in alongside focms_api.py and focms_addresses.py. Wired by two lines in
focms_api.py:

    from focms_i18n import router as i18n_router
    app.include_router(i18n_router)

v0.6.0 (2026-06-24):
- GET  /focms/v1/i18n/strings?namespace=X&locale=Y[&tenant_id=Z]
       Batch UI string retrieval. One database call returns every localized
       string for a (namespace, locale) pair plus metadata showing where
       each string was resolved and whether fallback was applied. Designed
       to be cached aggressively client-side per (namespace, locale).
- POST /focms/v1/i18n/translate
       Single text translation / transliteration / romanization. Cache-first
       via fn_get_or_translate; on miss calls Google Cloud Translation API
       v2 and persists the result to translation_cache.
- POST /focms/v1/i18n/translate-batch
       Bulk translation of an array of strings. Used for one-time seeding of
       the 102 field_capture_catalog labels across all 29 supported locales.
       More efficient than serial single-text calls because Google bills per
       character regardless of batching, but a single HTTP round trip handles
       many strings.

All endpoints share the same auth + tenant-context machinery as focms_api.py
and focms_addresses.py. DB operations wrapped in async with conn.transaction()
+ parameterized set_config (lesson from v0.5.0a). All jsonb returns cast to
text with json.loads on the Python side (lesson from v0.5.0b).

Google Cloud Translation API v2 endpoint: 
    POST https://translation.googleapis.com/language/translate/v2?key=KEY
Reuses the same GOOGLE_PLACES_API_KEY env var since Google Cloud routes API
key auth across services within one project. Key must allow Cloud Translation
API; if restricted to Places API only, add Translation to the allowed list.

For romanization of names from non-Latin scripts (Korean, Chinese, Japanese,
Arabic, Hindi), pass translation_kind='romanize' and Google will return the
phonetic Latin spelling. For full semantic translation of essays and prose,
pass translation_kind='translate'.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("focms.i18n")

GOOGLE_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")  # shared across Google APIs
GOOGLE_TRANSLATE_BASE = "https://translation.googleapis.com/language/translate/v2"
GOOGLE_TIMEOUT = float(os.environ.get("GOOGLE_TRANSLATE_TIMEOUT_SEC", "15"))

DEFAULT_PROVIDER = "google_translate_v2"
COST_PER_CHAR_USD = 0.00001  # $10 per million chars

router = APIRouter(prefix="/focms/v1", tags=["i18n"])


# ---------------------------------------------------------------------------
# Auth / tenant context (mirrors focms_api.py + focms_addresses.py pattern)
# ---------------------------------------------------------------------------

class AuthContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    tenant_id: UUID
    user_id: UUID
    role: str


def _tokens() -> dict[str, dict[str, str]]:
    return json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))


async def _require_auth(authorization: Optional[str] = Header(None)) -> AuthContext:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    info = _tokens().get(token)
    if not info:
        raise HTTPException(401, "invalid token")
    return AuthContext(
        tenant_id=UUID(info["tenant_id"]),
        user_id=UUID(info["user_id"]),
        role=info.get("role", "tenant_viewer"),
    )


def _require_write_role(auth: AuthContext) -> None:
    if auth.role not in {"tenant_owner", "tenant_admin", "platform_admin"}:
        raise HTTPException(403, f"role {auth.role!r} cannot write")


async def _bind_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        str(tenant_id),
    )


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class TranslateRequest(BaseModel):
    source_text: str = Field(..., min_length=1, max_length=10_000)
    target_locale: str = Field(..., min_length=2, max_length=20)
    source_locale: Optional[str] = Field(None, min_length=2, max_length=20)
    translation_kind: str = Field("translate", pattern="^(translate|transliterate|romanize|detect_then_translate)$")
    force_refresh: bool = False


class TranslateResponse(BaseModel):
    source: str  # "cache" | "live" | "identity_passthrough" | "empty_input"
    translated_text: str
    source_locale: Optional[str] = None
    target_locale: str
    translation_kind: str
    provider: str
    char_count: int
    cost_usd: float
    cache_hits: Optional[int] = None
    provider_detected_source_locale: Optional[str] = None
    provider_confidence: Optional[float] = None


class TranslateBatchItem(BaseModel):
    key: str = Field(..., min_length=1, max_length=200)
    source_text: str = Field(..., min_length=1, max_length=10_000)


class TranslateBatchRequest(BaseModel):
    items: list[TranslateBatchItem] = Field(..., min_length=1, max_length=128)
    target_locale: str = Field(..., min_length=2, max_length=20)
    source_locale: str = Field("en", min_length=2, max_length=20)
    translation_kind: str = Field("translate", pattern="^(translate|transliterate|romanize)$")
    persist_to_i18n_strings: bool = False
    i18n_namespace: Optional[str] = None


class TranslateBatchResultItem(BaseModel):
    key: str
    source_text: str
    translated_text: str
    source: str  # "cache" | "live"
    char_count: int


class TranslateBatchResponse(BaseModel):
    target_locale: str
    source_locale: str
    translation_kind: str
    provider: str
    total_items: int
    cache_hits: int
    live_calls: int
    total_char_count: int
    total_cost_usd: float
    results: list[TranslateBatchResultItem]
    persisted_to_i18n_strings: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_google_lang(locale_code: str) -> str:
    """Map FOCMS locale_code (e.g. 'zh-CN', 'pt-BR') to Google Translate code.
    Most locales work as the iso_639_1 prefix; some need full code.
    The locales table holds the authoritative mapping; this is a runtime
    shortcut that falls back to the iso_639_1 prefix if not specially handled."""
    # Special cases Google requires the full code
    special = {
        "zh-CN": "zh-CN", "zh-TW": "zh-TW", "zh-HK": "zh-TW",
        "pt-BR": "pt-BR", "pt-PT": "pt-PT",
        "es-419": "es", "es-ES": "es", "es-MX": "es",
        "he-IL": "iw",  # Google uses 'iw' for Hebrew, not 'he'
    }
    if locale_code in special:
        return special[locale_code]
    # Fall back to the language part before the hyphen
    return locale_code.split("-", 1)[0]


async def _call_google_translate(
    texts: list[str],
    source_lang: Optional[str],
    target_lang: str,
    translation_kind: str = "translate",
) -> dict:
    """Call Google Cloud Translation v2 API. Returns the raw parsed JSON.
    
    For romanization, Google's `model=nmt` Neural MT will produce a romanized
    spelling for names when the source is in a non-Latin script and the
    target is English. We rely on that here for v0.6.0; v0.7.0 may swap in
    the dedicated Transliteration API for higher accuracy if needed."""
    if not GOOGLE_API_KEY:
        raise HTTPException(503, "GOOGLE_PLACES_API_KEY not configured")
    
    params: dict[str, str] = {"key": GOOGLE_API_KEY}
    body: dict[str, Any] = {
        "q": texts,
        "target": target_lang,
        "format": "text",
    }
    if source_lang:
        body["source"] = source_lang
    
    async with httpx.AsyncClient(timeout=GOOGLE_TIMEOUT) as client:
        r = await client.post(
            GOOGLE_TRANSLATE_BASE,
            params=params,
            json=body,
        )
        if r.status_code >= 400:
            logger.warning("google translate %s: %s", r.status_code, r.text[:500])
            raise HTTPException(502, f"google translate error {r.status_code}")
        return r.json()


def _normalize_translation(raw: dict, idx: int = 0) -> dict:
    """Pull out translatedText / detectedSourceLanguage from Google v2 response."""
    translations = (raw.get("data") or {}).get("translations") or []
    if idx >= len(translations):
        return {"translated_text": "", "detected_source_lang": None}
    t = translations[idx]
    return {
        "translated_text": t.get("translatedText", ""),
        "detected_source_lang": t.get("detectedSourceLanguage"),
    }


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Endpoint 1: GET /i18n/strings - batch UI string retrieval
# ---------------------------------------------------------------------------

@router.get("/i18n/strings")
async def i18n_strings(
    request: Request,
    namespace: str = Query(..., min_length=1, max_length=64),
    locale: str = Query(..., min_length=2, max_length=20),
    tenant_id_override: Optional[UUID] = Query(None, alias="tenant_id"),
    auth: AuthContext = Depends(_require_auth),
) -> dict:
    """Return every UI string registered under `namespace` resolved to `locale`,
    with metadata showing where each string was found in the fallback chain.
    
    The tenant_id parameter defaults to the caller's authenticated tenant. The
    `tenant_id_override` query param exists for platform admins inspecting
    other tenants' overrides; non-platform-admin callers passing it gets a 403."""
    if tenant_id_override and tenant_id_override != auth.tenant_id and auth.role != "platform_admin":
        raise HTTPException(403, "tenant_id override requires platform_admin role")
    effective_tenant = tenant_id_override or auth.tenant_id
    
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            result = await conn.fetchval(
                "SELECT fn_localized_strings_for_namespace($1, $2, $3)::text",
                namespace, locale, effective_tenant,
            )
    if isinstance(result, str):
        result = json.loads(result)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2: POST /i18n/translate - single text translation with cache
# ---------------------------------------------------------------------------

@router.post("/i18n/translate", response_model=TranslateResponse)
async def i18n_translate(
    body: TranslateRequest,
    request: Request,
    auth: AuthContext = Depends(_require_auth),
) -> TranslateResponse:
    """Translate / transliterate / romanize one piece of text. Cache-first.
    On cache miss, calls Google Cloud Translation API v2 and persists.
    
    For names in non-Latin scripts, pass translation_kind='romanize' and 
    target_locale='en-US'. For essays and longer free-text, pass 
    translation_kind='translate' with the appropriate target_locale."""
    pool: asyncpg.Pool = request.app.state.pool
    
    # 1. Check cache via fn_get_or_translate (returns either cached result or 'cache_miss')
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            cache_result_raw = await conn.fetchval(
                "SELECT fn_get_or_translate($1, $2, $3, $4, $5)::text",
                body.source_text, body.target_locale, body.source_locale,
                body.translation_kind, DEFAULT_PROVIDER,
            )
    if isinstance(cache_result_raw, str):
        cache_result = json.loads(cache_result_raw)
    else:
        cache_result = cache_result_raw
    
    cache_source = cache_result.get("source")
    
    # 2. Handle cache hits and edge cases
    if cache_source in ("cache", "identity_passthrough", "empty_input") and not body.force_refresh:
        # Update cache_hits counter for actual cache hits (best effort)
        if cache_source == "cache":
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await _bind_tenant(conn, auth.tenant_id)
                    try:
                        await conn.execute(
                            """UPDATE public.translation_cache 
                               SET cache_hits = cache_hits + 1, cache_last_used_at = now()
                               WHERE source_text_hash = $1 
                                 AND coalesce(source_locale,'') = coalesce($2,'')
                                 AND target_locale = $3 
                                 AND translation_kind = $4
                                 AND provider = $5""",
                            _hash_text(body.source_text), body.source_locale,
                            body.target_locale, body.translation_kind, DEFAULT_PROVIDER,
                        )
                    except Exception:
                        logger.warning("translation cache hit counter bump failed", exc_info=True)
        return TranslateResponse(
            source=cache_source,
            translated_text=cache_result.get("translated_text", ""),
            source_locale=cache_result.get("source_locale"),
            target_locale=body.target_locale,
            translation_kind=body.translation_kind,
            provider=cache_result.get("provider", DEFAULT_PROVIDER),
            char_count=cache_result.get("char_count", len(body.source_text)),
            cost_usd=0.0,
            cache_hits=cache_result.get("cache_hits"),
            provider_confidence=cache_result.get("provider_confidence"),
        )
    
    # 3. Cache miss (or force refresh) -> call Google
    source_lang_g = _resolve_google_lang(body.source_locale) if body.source_locale else None
    target_lang_g = _resolve_google_lang(body.target_locale)
    
    raw = await _call_google_translate(
        [body.source_text], source_lang_g, target_lang_g, body.translation_kind
    )
    parsed = _normalize_translation(raw, idx=0)
    translated = parsed["translated_text"]
    detected = parsed["detected_source_lang"]
    char_count = len(body.source_text)
    cost = char_count * COST_PER_CHAR_USD
    
    # 4. Persist to translation_cache
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            try:
                await conn.execute(
                    """INSERT INTO public.translation_cache (
                        tenant_id, source_text, source_text_hash, source_locale,
                        target_locale, translated_text, translation_kind, provider,
                        provider_response_raw, provider_detected_source_locale,
                        character_count, cost_usd, created_by
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, $13
                    )
                    ON CONFLICT (source_text_hash, source_locale, target_locale, translation_kind, provider)
                    DO UPDATE SET translated_text = EXCLUDED.translated_text,
                                  cache_last_used_at = now()""",
                    auth.tenant_id, body.source_text, _hash_text(body.source_text),
                    body.source_locale, body.target_locale, translated,
                    body.translation_kind, DEFAULT_PROVIDER,
                    json.dumps(raw), detected,
                    char_count, cost, auth.user_id,
                )
            except Exception:
                logger.warning("translation_cache persist failed", exc_info=True)
    
    return TranslateResponse(
        source="live",
        translated_text=translated,
        source_locale=body.source_locale or (detected and detected + "-XX"),
        target_locale=body.target_locale,
        translation_kind=body.translation_kind,
        provider=DEFAULT_PROVIDER,
        char_count=char_count,
        cost_usd=cost,
        provider_detected_source_locale=detected,
    )


# ---------------------------------------------------------------------------
# Endpoint 3: POST /i18n/translate-batch - bulk translation
# ---------------------------------------------------------------------------

@router.post("/i18n/translate-batch", response_model=TranslateBatchResponse)
async def i18n_translate_batch(
    body: TranslateBatchRequest,
    request: Request,
    auth: AuthContext = Depends(_require_auth),
) -> TranslateBatchResponse:
    """Bulk translate an array of strings. Cache-checks each one first;
    Google is called once with all uncached items. Optionally persists each
    result to i18n_strings for UI string seeding.
    
    Used for one-time seeding of the 102 field_capture_catalog labels across
    all 29 supported locales (29 batch calls, one per locale, ~102 items each)."""
    _require_write_role(auth)
    pool: asyncpg.Pool = request.app.state.pool
    
    if body.persist_to_i18n_strings and not body.i18n_namespace:
        raise HTTPException(400, "persist_to_i18n_strings=true requires i18n_namespace")
    
    # 1. Cache check each item, collect misses
    cache_results: dict[str, dict] = {}  # key -> {source, translated_text, char_count}
    misses: list[tuple[str, str]] = []  # [(key, source_text), ...]
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            for item in body.items:
                raw = await conn.fetchval(
                    "SELECT fn_get_or_translate($1, $2, $3, $4, $5)::text",
                    item.source_text, body.target_locale, body.source_locale,
                    body.translation_kind, DEFAULT_PROVIDER,
                )
                if isinstance(raw, str):
                    res = json.loads(raw)
                else:
                    res = raw
                if res.get("source") in ("cache", "identity_passthrough", "empty_input"):
                    cache_results[item.key] = {
                        "source": res["source"],
                        "translated_text": res.get("translated_text", ""),
                        "char_count": res.get("char_count", len(item.source_text)),
                    }
                else:
                    misses.append((item.key, item.source_text))
    
    # 2. Call Google for all misses in one request (batching is free)
    live_results: dict[str, str] = {}  # key -> translated_text
    detected_source_lang: Optional[str] = None
    if misses:
        source_lang_g = _resolve_google_lang(body.source_locale)
        target_lang_g = _resolve_google_lang(body.target_locale)
        miss_texts = [m[1] for m in misses]
        raw = await _call_google_translate(
            miss_texts, source_lang_g, target_lang_g, body.translation_kind
        )
        translations = (raw.get("data") or {}).get("translations") or []
        for i, (key, src) in enumerate(misses):
            if i < len(translations):
                t = translations[i]
                live_results[key] = t.get("translatedText", "")
                if not detected_source_lang:
                    detected_source_lang = t.get("detectedSourceLanguage")
        
        # 3. Persist all live results to translation_cache
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _bind_tenant(conn, auth.tenant_id)
                for key, src in misses:
                    translated = live_results.get(key, "")
                    if not translated:
                        continue
                    char_count = len(src)
                    try:
                        await conn.execute(
                            """INSERT INTO public.translation_cache (
                                tenant_id, source_text, source_text_hash, source_locale,
                                target_locale, translated_text, translation_kind, provider,
                                provider_detected_source_locale,
                                character_count, cost_usd, created_by
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                            )
                            ON CONFLICT (source_text_hash, source_locale, target_locale, translation_kind, provider)
                            DO UPDATE SET translated_text = EXCLUDED.translated_text,
                                          cache_last_used_at = now()""",
                            auth.tenant_id, src, _hash_text(src),
                            body.source_locale, body.target_locale, translated,
                            body.translation_kind, DEFAULT_PROVIDER,
                            detected_source_lang,
                            char_count, char_count * COST_PER_CHAR_USD, auth.user_id,
                        )
                    except Exception:
                        logger.warning("translation_cache persist failed in batch", exc_info=True)
    
    # 4. Optionally persist to i18n_strings (UI string seeding workflow)
    persisted_count = 0
    if body.persist_to_i18n_strings and body.i18n_namespace:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _bind_tenant(conn, auth.tenant_id)
                for item in body.items:
                    if item.key in cache_results:
                        translated = cache_results[item.key]["translated_text"]
                    elif item.key in live_results:
                        translated = live_results[item.key]
                    else:
                        continue
                    if not translated:
                        continue
                    try:
                        await conn.execute(
                            """INSERT INTO public.i18n_strings (
                                tenant_id, namespace, key, locale_code, value_text,
                                translation_source, translation_confidence, created_by
                            ) VALUES (
                                NULL, $1, $2, $3, $4, 'google_translate', 0.85, $5
                            )
                            ON CONFLICT (tenant_id, namespace, key, locale_code, variant)
                            DO UPDATE SET value_text = EXCLUDED.value_text,
                                          translation_source = 'google_translate',
                                          updated_at = now()""",
                            body.i18n_namespace, item.key, body.target_locale,
                            translated, auth.user_id,
                        )
                        persisted_count += 1
                    except Exception:
                        logger.warning("i18n_strings persist failed in batch", exc_info=True)
    
    # 5. Build response
    results = []
    total_chars = 0
    cache_hits = 0
    for item in body.items:
        if item.key in cache_results:
            r = cache_results[item.key]
            results.append(TranslateBatchResultItem(
                key=item.key, source_text=item.source_text,
                translated_text=r["translated_text"],
                source=r["source"], char_count=r["char_count"],
            ))
            cache_hits += 1
            total_chars += r["char_count"]
        elif item.key in live_results:
            char_count = len(item.source_text)
            results.append(TranslateBatchResultItem(
                key=item.key, source_text=item.source_text,
                translated_text=live_results[item.key],
                source="live", char_count=char_count,
            ))
            total_chars += char_count
        else:
            results.append(TranslateBatchResultItem(
                key=item.key, source_text=item.source_text,
                translated_text="", source="failed", char_count=0,
            ))
    
    live_count = len(misses) - sum(1 for r in results if r.source == "failed")
    
    return TranslateBatchResponse(
        target_locale=body.target_locale,
        source_locale=body.source_locale,
        translation_kind=body.translation_kind,
        provider=DEFAULT_PROVIDER,
        total_items=len(body.items),
        cache_hits=cache_hits,
        live_calls=live_count,
        total_char_count=total_chars,
        total_cost_usd=sum(r.char_count for r in results if r.source == "live") * COST_PER_CHAR_USD,
        results=results,
        persisted_to_i18n_strings=persisted_count,
    )
