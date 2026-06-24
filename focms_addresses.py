"""focms_addresses.py - Address autocomplete, validation, and phone normalization.

Drops in alongside focms_api.py. Wired up by adding 3 lines to focms_api.py:

    from focms_addresses import router as addresses_router
    # ...inside app creation:
    app.include_router(addresses_router)

v0.5.0a (2026-06-24):
- Hotfix: every DB acquire wrapped in `async with conn.transaction():` and
  tenant binding moved to parameterized `SELECT set_config('app.current_tenant_id',
  $1, true)` — matches the proven tx() helper in focms_api.py. The original
  v0.5.0 release used raw SET LOCAL outside a transaction, which Postgres
  rejects, so every endpoint returned 500 on the first DB call. No API-contract
  change; only the internal implementation.

v0.5.0 (2026-06-24):
- POST /focms/v1/addresses/autocomplete           Google Places (New) proxy
                                                  with 7-day Postgres cache.
                                                  Cache hits cost zero.
- POST /focms/v1/addresses/{address_id}/validate  Google Places Place Details
                                                  -> address_validations row
                                                  + UPDATE student_addresses
                                                  with standardized fields.
- POST /focms/v1/phones/validate                  Calls fn_validate_phone()
                                                  in Postgres (libphonenumber
                                                  upgrade comes later via
                                                  python-phonenumbers; same
                                                  JSON contract).

All endpoints share auth + tenant-context with focms_api.py. They use the
same asyncpg pool stored at app.state.pool and the same Bearer-token →
(tenant_id, user_id, role) resolution.

Provider routing default:
- US addresses: google_places (USPS v3 deferred until the developer portal
  redesign on 2026-07-12 is stable; smartystreets_us is a paid fallback).
- International: google_places (global coverage; ISO 3166-1 + 3166-2 codes
  returned natively).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Path, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("focms.addresses")

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
GOOGLE_PLACES_BASE = "https://places.googleapis.com/v1"
GOOGLE_PLACES_TIMEOUT = float(os.environ.get("GOOGLE_PLACES_TIMEOUT_SEC", "10"))

# Default to Google Places everywhere; SmartyStreets US can be wired later as a
# fallback when GOOGLE_PLACES_API_KEY is missing or returns ambiguous results.
DEFAULT_AUTOCOMPLETE_PROVIDER = "google_places"
DEFAULT_VALIDATE_PROVIDER_US = "google_places"
DEFAULT_VALIDATE_PROVIDER_INTERNATIONAL = "google_places"

router = APIRouter(prefix="/focms/v1", tags=["addresses"])


# ---------------------------------------------------------------------------
# Auth / tenant context (mirrors focms_api.py pattern)
# ---------------------------------------------------------------------------

class AuthContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    tenant_id: UUID
    user_id: UUID
    role: str


def _tokens() -> dict[str, dict[str, str]]:
    return json.loads(os.environ.get("FOCMS_API_TOKENS_JSON", "{}"))


async def _require_auth(
    authorization: Optional[str] = Header(None),
) -> AuthContext:
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
    """Set the per-request tenant id for RLS. MUST be called inside an
    `async with conn.transaction():` block so set_config(..., is_local=true)
    survives subsequent statements in the same transaction. Matches the
    pattern used by tx() in focms_api.py."""
    await conn.execute(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        str(tenant_id),
    )


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class AutocompleteRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=200)
    country_iso2_bias: Optional[str] = Field(None, max_length=2)
    language: str = Field("en", max_length=10)
    session_token: Optional[str] = Field(None, max_length=64)


class AutocompleteSuggestion(BaseModel):
    place_id: str
    description: str
    main_text: Optional[str] = None
    secondary_text: Optional[str] = None
    types: list[str] = Field(default_factory=list)


class AutocompleteResponse(BaseModel):
    source: str  # "cache" | "live" | "min_query_length" | "no_results"
    provider: str
    suggestion_count: int
    suggestions: list[AutocompleteSuggestion]
    cache_hits: Optional[int] = None
    cache_expires_at: Optional[datetime] = None


class ValidateAddressRequest(BaseModel):
    provider: Optional[str] = None
    place_id: Optional[str] = None  # from a prior autocomplete selection
    force_revalidate: bool = False


class ValidateAddressResponse(BaseModel):
    validation_id: UUID
    is_valid: bool
    is_deliverable: Optional[bool]
    provider: str
    standardized: dict[str, Any]
    confidence_score: Optional[float] = None
    messages: list[str] = Field(default_factory=list)
    was_cached_response: bool = False


class PhoneValidateRequest(BaseModel):
    phone_text: str = Field(..., min_length=1, max_length=64)
    country_iso2: str = Field(..., min_length=2, max_length=2)


# ---------------------------------------------------------------------------
# Google Places API client (thin async wrapper)
# ---------------------------------------------------------------------------

async def _gplaces_autocomplete(
    query: str,
    country_iso2: Optional[str],
    language: str,
    session_token: Optional[str],
) -> dict:
    if not GOOGLE_PLACES_API_KEY:
        raise HTTPException(503, "GOOGLE_PLACES_API_KEY not configured")
    body: dict[str, Any] = {
        "input": query,
        "languageCode": language,
    }
    if country_iso2:
        body["includedRegionCodes"] = [country_iso2.lower()]
    if session_token:
        body["sessionToken"] = session_token
    headers = {
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "Content-Type": "application/json",
        # FieldMask omitted -> autocomplete returns all fields by default.
        # https://developers.google.com/maps/documentation/places/web-service/place-autocomplete
    }
    async with httpx.AsyncClient(timeout=GOOGLE_PLACES_TIMEOUT) as client:
        r = await client.post(
            f"{GOOGLE_PLACES_BASE}/places:autocomplete",
            json=body,
            headers=headers,
        )
        if r.status_code >= 400:
            logger.warning("google places autocomplete %s: %s", r.status_code, r.text[:500])
            raise HTTPException(502, f"google places autocomplete error {r.status_code}")
        return r.json()


async def _gplaces_details(place_id: str, session_token: Optional[str], language: str) -> dict:
    if not GOOGLE_PLACES_API_KEY:
        raise HTTPException(503, "GOOGLE_PLACES_API_KEY not configured")
    headers = {
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "id,displayName,formattedAddress,shortFormattedAddress,"
            "addressComponents,location,types,plusCode,viewport"
        ),
    }
    params: dict[str, str] = {"languageCode": language}
    if session_token:
        params["sessionToken"] = session_token
    async with httpx.AsyncClient(timeout=GOOGLE_PLACES_TIMEOUT) as client:
        r = await client.get(
            f"{GOOGLE_PLACES_BASE}/places/{place_id}",
            headers=headers,
            params=params,
        )
        if r.status_code >= 400:
            logger.warning("google places details %s: %s", r.status_code, r.text[:500])
            raise HTTPException(502, f"google places details error {r.status_code}")
        return r.json()


# ---------------------------------------------------------------------------
# Response normalization
# ---------------------------------------------------------------------------

def _normalize_autocomplete(raw: dict) -> list[AutocompleteSuggestion]:
    out: list[AutocompleteSuggestion] = []
    for s in raw.get("suggestions", []) or []:
        pp = s.get("placePrediction") or {}
        if not pp.get("placeId"):
            continue
        text_obj = pp.get("text") or {}
        st_format = pp.get("structuredFormat") or {}
        main_text = (st_format.get("mainText") or {}).get("text")
        secondary_text = (st_format.get("secondaryText") or {}).get("text")
        out.append(AutocompleteSuggestion(
            place_id=pp["placeId"],
            description=text_obj.get("text") or main_text or "",
            main_text=main_text,
            secondary_text=secondary_text,
            types=pp.get("types") or [],
        ))
    return out


def _parse_address_components(components: list[dict]) -> dict:
    result: dict[str, Any] = {
        "street_number": None,
        "street_name": None,
        "street_line_1": None,
        "street_line_2": None,
        "building_or_district": None,
        "city": None,
        "subdivision_iso": None,
        "subdivision_name": None,
        "postal_code": None,
        "country_iso2": None,
        "country_name": None,
    }
    sub_short = None
    for comp in components or []:
        types = comp.get("types", []) or []
        long_name = comp.get("longText") or comp.get("long_name") or ""
        short_name = comp.get("shortText") or comp.get("short_name") or ""
        if "street_number" in types:
            result["street_number"] = long_name
        elif "route" in types:
            result["street_name"] = long_name
        elif "subpremise" in types:
            result["street_line_2"] = long_name
        elif "sublocality" in types or "sublocality_level_1" in types or "neighborhood" in types:
            if not result["building_or_district"]:
                result["building_or_district"] = long_name
        elif "locality" in types or "postal_town" in types:
            result["city"] = long_name
        elif "administrative_area_level_1" in types:
            result["subdivision_name"] = long_name
            sub_short = short_name
        elif "postal_code" in types:
            result["postal_code"] = long_name
        elif "country" in types:
            result["country_iso2"] = (short_name or "").upper()
            result["country_name"] = long_name
    if result["street_number"] and result["street_name"]:
        result["street_line_1"] = f"{result['street_number']} {result['street_name']}"
    elif result["street_name"]:
        result["street_line_1"] = result["street_name"]
    if result["country_iso2"] and sub_short:
        result["subdivision_iso"] = f"{result['country_iso2']}-{sub_short}"
    return result


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/addresses/autocomplete", response_model=AutocompleteResponse)
async def addresses_autocomplete(
    body: AutocompleteRequest,
    request: Request,
    auth: AuthContext = Depends(_require_auth),
) -> AutocompleteResponse:
    """Return address suggestions for a partial query, cache-first.

    Frontend should debounce typing (300ms) and only call when query >= 3 chars.
    Reuse the same session_token across the typing session + the subsequent
    /addresses/{id}/validate call so Google charges a single session instead
    of per-keystroke.
    """
    provider = DEFAULT_AUTOCOMPLETE_PROVIDER
    pool: asyncpg.Pool = request.app.state.pool

    if len(body.query.strip()) < 3:
        return AutocompleteResponse(
            source="min_query_length",
            provider=provider,
            suggestion_count=0,
            suggestions=[],
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)

            # Cache lookup
            cached = await conn.fetchrow(
                """
                SELECT id, suggestions, suggestion_count, cache_hits,
                       cache_expires_at, response_received_at
                FROM public.address_suggestions_cache
                WHERE provider_code = $1
                  AND lower(query_input) = lower($2)
                  AND coalesce(query_country_bias,'') = coalesce($3,'')
                  AND coalesce(query_language,'en') = coalesce($4,'en')
                  AND cache_expires_at > now()
                ORDER BY response_received_at DESC
                LIMIT 1
                """,
                provider,
                body.query,
                body.country_iso2_bias,
                body.language,
            )
            if cached:
                # Bump cache hit counter (best effort, ignore failures)
                try:
                    await conn.execute(
                        "UPDATE public.address_suggestions_cache "
                        "SET cache_hits = cache_hits + 1, "
                        "    cache_last_used_at = now() "
                        "WHERE id = $1",
                        cached["id"],
                    )
                except Exception:
                    logger.warning("cache hit counter bump failed", exc_info=True)

                sugg_raw = cached["suggestions"]
                if isinstance(sugg_raw, str):
                    sugg_raw = json.loads(sugg_raw)
                return AutocompleteResponse(
                    source="cache",
                    provider=provider,
                    suggestion_count=cached["suggestion_count"],
                    suggestions=[AutocompleteSuggestion(**s) for s in sugg_raw],
                    cache_hits=cached["cache_hits"] + 1,
                    cache_expires_at=cached["cache_expires_at"],
                )

    # Cache miss -> call Google Places live
    raw = await _gplaces_autocomplete(
        body.query, body.country_iso2_bias, body.language, body.session_token
    )
    suggestions = _normalize_autocomplete(raw)

    # Persist to cache
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            try:
                await conn.execute(
                    """
                    INSERT INTO public.address_suggestions_cache (
                        tenant_id, provider_code, query_input, query_country_bias,
                        query_language, query_session_token, suggestions,
                        suggestion_count, created_by
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
                    """,
                    auth.tenant_id,
                    provider,
                    body.query,
                    body.country_iso2_bias,
                    body.language,
                    body.session_token,
                    json.dumps([s.model_dump() for s in suggestions]),
                    len(suggestions),
                    auth.user_id,
                )
            except asyncpg.UniqueViolationError:
                # Concurrent insert beat us — fine
                pass

    return AutocompleteResponse(
        source="live" if suggestions else "no_results",
        provider=provider,
        suggestion_count=len(suggestions),
        suggestions=suggestions,
    )


@router.post("/addresses/{address_id}/validate", response_model=ValidateAddressResponse)
async def addresses_validate(
    body: ValidateAddressRequest,
    request: Request,
    address_id: UUID = Path(...),
    auth: AuthContext = Depends(_require_auth),
) -> ValidateAddressResponse:
    """Validate an address via the chosen provider, persist the result, and
    update the student_addresses row with standardized fields."""
    _require_write_role(auth)
    pool: asyncpg.Pool = request.app.state.pool

    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)

            addr = await conn.fetchrow(
                """
                SELECT id, student_id, tenant_id, country_iso2, country, place_id,
                       street_address, street_address_line_2, street_address_line_3,
                       building_or_district, city_town, state_province,
                       subdivision_iso, zip_postal_code, validation_status
                FROM public.student_addresses
                WHERE id = $1 AND deleted_at IS NULL
                """,
                address_id,
            )
            if not addr:
                raise HTTPException(404, f"address {address_id} not found")

            # If already verified and not forcing, return the latest validation row.
            if (
                not body.force_revalidate
                and addr["validation_status"] in ("verified", "geocoded")
            ):
                existing = await conn.fetchrow(
                    """
                    SELECT id, is_valid, is_deliverable, provider_code,
                           standardized_formatted_address, standardized_street_line_1,
                           standardized_city, standardized_subdivision_iso,
                           standardized_postal_code, standardized_country_iso2,
                           lat, lng, confidence_score, was_cached_response
                    FROM public.address_validations
                    WHERE source_table = 'student_addresses'
                      AND source_record_id = $1
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    address_id,
                )
                if existing:
                    return ValidateAddressResponse(
                        validation_id=existing["id"],
                        is_valid=existing["is_valid"],
                        is_deliverable=existing["is_deliverable"],
                        provider=existing["provider_code"],
                        standardized={
                            "street_line_1": existing["standardized_street_line_1"],
                            "city": existing["standardized_city"],
                            "subdivision_iso": existing["standardized_subdivision_iso"],
                            "postal_code": existing["standardized_postal_code"],
                            "country_iso2": existing["standardized_country_iso2"],
                            "formatted_address": existing["standardized_formatted_address"],
                            "lat": float(existing["lat"]) if existing["lat"] is not None else None,
                            "lng": float(existing["lng"]) if existing["lng"] is not None else None,
                        },
                        confidence_score=float(existing["confidence_score"])
                            if existing["confidence_score"] is not None else None,
                        messages=["already verified; pass force_revalidate=true to re-run"],
                        was_cached_response=True,
                    )

            # Resolve provider
            provider = body.provider
            if not provider:
                country = (addr["country_iso2"] or "").upper()
                provider = (
                    DEFAULT_VALIDATE_PROVIDER_US if country == "US"
                    else DEFAULT_VALIDATE_PROVIDER_INTERNATIONAL
                )

    # Currently only google_places is wired live. USPS v3 deferred; SmartyStreets US deferred.
    if provider != "google_places":
        raise HTTPException(
            501,
            f"provider {provider!r} not yet implemented in v0.5.0; "
            "use google_places (default) for now",
        )

    place_id = body.place_id
    if not place_id:
        # No place_id supplied — geocode the existing address fields first.
        query_parts = [
            addr["street_address"], addr["street_address_line_2"],
            addr["city_town"], addr["state_province"],
            addr["zip_postal_code"], addr["country_iso2"] or addr["country"],
        ]
        query = ", ".join(p for p in query_parts if p)
        ac = await _gplaces_autocomplete(query, addr["country_iso2"], "en", None)
        suggestions = _normalize_autocomplete(ac)
        if not suggestions:
            # Persist a validation row anyway (no match)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await _bind_tenant(conn, auth.tenant_id)
                    vid = await conn.fetchval(
                        """
                        INSERT INTO public.address_validations (
                            tenant_id, student_id, source_table, source_record_id,
                            provider_code, validation_kind, request_input,
                            response_status_code, response_received_at,
                            is_valid, is_deliverable, validation_messages,
                            was_cached_response, created_by
                        ) VALUES (
                            $1, $2, 'student_addresses', $3,
                            $4, 'validate', $5::jsonb,
                            200, now(),
                            false, false, $6::jsonb,
                            false, $7
                        )
                        RETURNING id
                        """,
                        auth.tenant_id, addr["student_id"], address_id, provider,
                        json.dumps({"query": query}),
                        json.dumps(["google places returned no suggestions for this query"]),
                        auth.user_id,
                    )
                    await conn.execute(
                        "UPDATE public.student_addresses "
                        "SET validation_status='rejected', validation_source=$2, "
                        "    validated_at=now(), updated_at=now() "
                        "WHERE id=$1",
                        address_id, provider,
                    )
            return ValidateAddressResponse(
                validation_id=vid,
                is_valid=False,
                is_deliverable=False,
                provider=provider,
                standardized={},
                messages=["google places returned no suggestions for the address as entered"],
            )
        place_id = suggestions[0].place_id

    # Fetch place details
    details = await _gplaces_details(place_id, None, "en")
    parsed = _parse_address_components(details.get("addressComponents", []))
    location = details.get("location") or {}
    lat = location.get("latitude")
    lng = location.get("longitude")
    formatted = details.get("formattedAddress") or details.get("shortFormattedAddress")

    is_valid = bool(parsed.get("country_iso2") and (parsed.get("city") or parsed.get("postal_code")))
    is_deliverable = is_valid  # Google doesn't expose USPS DPV; use validity as proxy.

    standardized = {
        "street_line_1": parsed.get("street_line_1"),
        "street_line_2": parsed.get("street_line_2"),
        "building_or_district": parsed.get("building_or_district"),
        "city": parsed.get("city"),
        "subdivision_iso": parsed.get("subdivision_iso"),
        "subdivision_name": parsed.get("subdivision_name"),
        "postal_code": parsed.get("postal_code"),
        "country_iso2": parsed.get("country_iso2"),
        "formatted_address": formatted,
        "lat": lat,
        "lng": lng,
    }

    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)

            validation_id = await conn.fetchval(
                """
                INSERT INTO public.address_validations (
                    tenant_id, student_id, source_table, source_record_id,
                    provider_code, validation_kind,
                    request_input, response_raw,
                    response_status_code, response_received_at,
                    is_valid, is_deliverable,
                    standardized_street_line_1, standardized_street_line_2,
                    standardized_city, standardized_subdivision_iso,
                    standardized_postal_code, standardized_country_iso2,
                    standardized_formatted_address,
                    lat, lng,
                    external_place_id, confidence_score,
                    was_cached_response, created_by
                ) VALUES (
                    $1, $2, 'student_addresses', $3,
                    $4, 'validate',
                    $5::jsonb, $6::jsonb,
                    200, now(),
                    $7, $8,
                    $9, $10, $11, $12, $13, $14, $15,
                    $16, $17,
                    $18, $19,
                    false, $20
                )
                RETURNING id
                """,
                auth.tenant_id, addr["student_id"], address_id,
                provider,
                json.dumps({"place_id": place_id}), json.dumps(details),
                is_valid, is_deliverable,
                parsed.get("street_line_1"), parsed.get("street_line_2"),
                parsed.get("city"), parsed.get("subdivision_iso"),
                parsed.get("postal_code"), parsed.get("country_iso2"),
                formatted,
                lat, lng,
                place_id, 1.0 if is_valid else 0.0,
                auth.user_id,
            )

            # Update the student_addresses row with standardized + verified fields
            await conn.execute(
                """
                UPDATE public.student_addresses
                SET street_address = COALESCE($2, street_address),
                    street_address_line_2 = COALESCE($3, street_address_line_2),
                    building_or_district = COALESCE($4, building_or_district),
                    city_town = COALESCE($5, city_town),
                    state_province = COALESCE($6, state_province),
                    subdivision_iso = COALESCE($7, subdivision_iso),
                    subdivision_name = COALESCE($8, subdivision_name),
                    zip_postal_code = COALESCE($9, zip_postal_code),
                    country = COALESCE($10, country),
                    country_iso2 = COALESCE($11, country_iso2),
                    formatted_address = COALESCE($12, formatted_address),
                    lat = COALESCE($13, lat),
                    lng = COALESCE($14, lng),
                    validation_status = $15,
                    validation_source = $16,
                    validated_at = now(),
                    updated_at = now()
                WHERE id = $1
                """,
                address_id,
                parsed.get("street_line_1"), parsed.get("street_line_2"),
                parsed.get("building_or_district"),
                parsed.get("city"), parsed.get("subdivision_name"),
                parsed.get("subdivision_iso"), parsed.get("subdivision_name"),
                parsed.get("postal_code"), parsed.get("country_name"),
                parsed.get("country_iso2"),
                formatted, lat, lng,
                "verified" if is_valid else "rejected",
                provider,
            )

    return ValidateAddressResponse(
        validation_id=validation_id,
        is_valid=is_valid,
        is_deliverable=is_deliverable,
        provider=provider,
        standardized=standardized,
        confidence_score=1.0 if is_valid else 0.0,
        messages=(
            ["address standardized and verified via Google Places"]
            if is_valid
            else ["Google Places could not confidently resolve this address"]
        ),
    )


@router.post("/phones/validate")
async def phones_validate(
    body: PhoneValidateRequest,
    request: Request,
    auth: AuthContext = Depends(_require_auth),
) -> dict:
    """Validate and normalize a phone number to E.164.

    v0.5.0 backs onto SQL fn_validate_phone(). v0.6.0 will swap to the Python
    `phonenumbers` library (libphonenumber) for better accuracy; the JSON
    contract stays the same.
    """
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _bind_tenant(conn, auth.tenant_id)
            result = await conn.fetchval(
                "SELECT public.fn_validate_phone($1, $2)",
                body.phone_text, body.country_iso2.upper(),
            )
    if isinstance(result, str):
        result = json.loads(result)
    return result
