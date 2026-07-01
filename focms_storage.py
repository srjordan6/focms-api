"""
focms_storage.py - Cloudflare R2 storage abstraction for FOCMS media files.

Architecture: archive_entries source_id='artifact_storage_r2_v1_0'
              archive_entries source_id='v0_10_0_artifact_serving_design_v0_1'

v0.10.0 (2026-07-01) - Hybrid A+B artifact serving:
- Two-bucket support. upload_bytes / get_presigned_url / delete_object now
  accept bucket_kind='private'|'public' (default 'private' for backward
  compat with existing v0.9.0 callers).
- New env vars R2_BUCKET_PUBLIC and PUBLIC_ARTIFACT_URL. Both optional; if
  unset, public bucket routing falls back to R2_BUCKET and the module
  behaves identically to v0.9.0 (single-bucket).
- New helper get_public_url() returns direct CDN URLs for public bucket
  objects served via PUBLIC_ARTIFACT_URL (typically artifacts.outcomestar.app).
  Zero-egress, edge-cached, no presign overhead.
- upload_bytes() return dict now includes a "bucket" key
  ('public'|'private') so callers can persist the routing decision.
- health_check() reports on both buckets independently.

v0.8.0 - Cloudflare R2 object storage for media files.

Provides R2 upload/download/delete with automatic bytea fallback when R2
env vars are not configured. Endpoints check r2_enabled() to decide which
storage path to use; this module hides the boto3 details.

Environment variables (set on focms-api Render service):
    R2_ACCOUNT_ID        Cloudflare account ID
    R2_ACCESS_KEY_ID     S3-compatible access key ID
    R2_SECRET_ACCESS_KEY S3-compatible secret access key
    R2_BUCKET            R2 bucket name for private assets (required)
                         Current: outcomestar-artifacts
    R2_BUCKET_PUBLIC     R2 bucket name for public assets (v0.10.0+, optional)
                         Current: outcomestar-artifacts-public
                         If unset, public uploads route to R2_BUCKET.
    PUBLIC_ARTIFACT_URL  CDN base URL for public bucket (v0.10.0+, optional)
                         Current: https://artifacts.outcomestar.app
                         Required only when serving public bucket objects
                         via the CDN edge (serve_media falls back to
                         presigned URLs if unset).

Object key convention: tenant_id/artifact_id
    - Tenant prefix enables tenant-scoped cleanup (lifecycle rules per tenant)
    - artifact_id is UUID v7 from gen_random_uuid_v7() - naturally time-sorted

Usage pattern in endpoints (v0.10.0):
    from focms_storage import (
        r2_enabled, upload_bytes, get_presigned_url, get_public_url,
        delete_object,
    )

    # On upload - bucket_kind derived from visibility:
    bucket_kind = 'public' if visibility == 'public' else 'private'
    result = upload_bytes(tenant_id, artifact_id, data, content_type,
                          bucket_kind=bucket_kind)
    # result["storage_kind"] is "r2" if R2 active, "inline_bytea" otherwise
    # result["bucket"] is 'public' or 'private' (physical bucket routing)
    # INSERT row with storage_kind, storage_uri, bucket, content=...

    # On download - dispatch on stored bucket:
    if row["bucket"] == "public":
        return RedirectResponse(get_public_url(row["storage_uri"]),
                                status_code=302)
    else:
        url = get_presigned_url(row["storage_uri"], bucket_kind='private',
                                expiry_seconds=300)
        return RedirectResponse(url, status_code=302)

    # On delete - pass bucket_kind so the object comes from the right bucket:
    if row["storage_kind"] == "r2":
        delete_object(row["storage_uri"], bucket_kind=row["bucket"])
"""

import os
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket resolution
# ---------------------------------------------------------------------------

def _resolve_bucket_name(bucket_kind: str) -> str:
    """
    Map a logical bucket_kind ('private'|'public') to an actual R2 bucket name.

    Falls back to R2_BUCKET when bucket_kind='public' but R2_BUCKET_PUBLIC is
    unset. This keeps v0.9.0 single-bucket deployments working after they
    upgrade to v0.10.0 code but before they configure the public bucket.
    """
    if bucket_kind == "public":
        return (
            os.environ.get("R2_BUCKET_PUBLIC")
            or os.environ.get("R2_BUCKET", "")
        )
    return os.environ.get("R2_BUCKET", "")


def r2_enabled() -> bool:
    """Return True if the four core R2 env vars are configured.

    v0.10.0: Only checks the private-bucket set. The public bucket is
    optional - its absence just means public-visibility uploads land in
    the private bucket and are served via presigned URLs.
    """
    return all([
        os.environ.get("R2_ACCOUNT_ID"),
        os.environ.get("R2_ACCESS_KEY_ID"),
        os.environ.get("R2_SECRET_ACCESS_KEY"),
        os.environ.get("R2_BUCKET"),
    ])


@lru_cache(maxsize=1)
def get_r2_client():
    """
    Lazy-initialize a boto3 S3 client pointed at Cloudflare R2.

    Cached on first call. One client serves both buckets (same account,
    same endpoint URL). Raises RuntimeError if env vars are missing.
    """
    if not r2_enabled():
        raise RuntimeError("R2 not configured. Set R2_* env vars first.")

    import boto3
    from botocore.config import Config

    account_id = os.environ["R2_ACCOUNT_ID"]
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="auto",  # R2 ignores region but boto3 requires the field
    )


def build_object_key(tenant_id: str, artifact_id: str) -> str:
    """Construct the R2 object key for a given tenant and artifact."""
    return f"{tenant_id}/{artifact_id}"


# ---------------------------------------------------------------------------
# Upload / download / delete
# ---------------------------------------------------------------------------

def upload_bytes(
    tenant_id: str,
    artifact_id: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    bucket_kind: str = "private",
) -> dict:
    """
    Upload bytes to R2, with bytea fallback when R2 disabled.

    Args:
        tenant_id: UUID string of tenant (for object key prefix).
        artifact_id: UUID string of media_files row (for object key suffix).
        data: raw bytes to upload.
        content_type: MIME type stored as R2 object metadata.
        bucket_kind: 'private' (default) or 'public'. Routes to R2_BUCKET or
            R2_BUCKET_PUBLIC respectively. Default preserves v0.9.0 behavior.
            If bucket_kind='public' but R2_BUCKET_PUBLIC is unset, the
            object lands in R2_BUCKET and the returned "bucket" value
            reflects the actual routing ('private').

    Returns:
        {"storage_kind": "r2",
         "storage_uri": "<key>",
         "bucket": "public"|"private"}
            when R2 is enabled and upload succeeded. The "bucket" value is
            the ACTUAL bucket used (may differ from bucket_kind arg if
            R2_BUCKET_PUBLIC unset), so callers should persist this value
            rather than the requested bucket_kind.

        {"storage_kind": "inline_bytea",
         "storage_uri": None,
         "bucket": "private"}
            when R2 is disabled. Caller stores `data` in media_files.content.

    Raises:
        ValueError if bucket_kind is not 'private' or 'public'.
        Any boto3 exception on R2 failure (caller should let this bubble).
    """
    if bucket_kind not in ("private", "public"):
        raise ValueError(f"Invalid bucket_kind: {bucket_kind!r}")

    if not r2_enabled():
        return {
            "storage_kind": "inline_bytea",
            "storage_uri": None,
            "bucket": "private",
        }

    key = build_object_key(tenant_id, artifact_id)
    bucket = _resolve_bucket_name(bucket_kind)

    # Compute the effective bucket_kind to return - if 'public' was requested
    # but R2_BUCKET_PUBLIC is unset, we're actually writing to the private
    # bucket, so report bucket='private' back to the caller.
    effective_kind = bucket_kind
    if bucket_kind == "public" and not os.environ.get("R2_BUCKET_PUBLIC"):
        effective_kind = "private"
        logger.warning(
            "upload_bytes: bucket_kind='public' requested but R2_BUCKET_PUBLIC "
            "unset; object will land in private bucket instead"
        )

    try:
        client = get_r2_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        logger.info(
            "r2_upload ok bucket_kind=%s bucket=%s key=%s bytes=%d",
            effective_kind, bucket, key, len(data),
        )
        return {
            "storage_kind": "r2",
            "storage_uri": key,
            "bucket": effective_kind,
        }
    except Exception as e:
        logger.error(
            "r2_upload failed bucket_kind=%s bucket=%s key=%s err=%r",
            effective_kind, bucket, key, e,
        )
        raise


def get_presigned_url(
    storage_uri: str,
    expiry_seconds: int = 300,
    bucket_kind: str = "private",
) -> str:
    """
    Generate a short-lived presigned URL for an R2 object.

    Default TTL: 5 minutes. Used by the media-serve endpoint to return a
    302 redirect so the client downloads directly from R2 (zero egress cost,
    no proxy bandwidth on our API).

    For public-bucket objects, prefer get_public_url() - it returns a direct
    CDN URL with no expiry and no signing overhead. This function still
    accepts bucket_kind='public' for the CDN-URL fallback path in serve_media
    (when PUBLIC_ARTIFACT_URL is unset but the object exists in the public
    bucket).

    Args:
        storage_uri: R2 object key ({tenant_id}/{artifact_id}).
        expiry_seconds: presigned URL TTL in seconds (default 300).
        bucket_kind: 'private' (default) or 'public'. Selects the bucket to
            sign against.

    Raises:
        ValueError if bucket_kind is not 'private' or 'public'.
        RuntimeError if R2 not configured.
    """
    if bucket_kind not in ("private", "public"):
        raise ValueError(f"Invalid bucket_kind: {bucket_kind!r}")

    if not r2_enabled():
        raise RuntimeError("Cannot generate presigned URL without R2 configured.")

    bucket = _resolve_bucket_name(bucket_kind)
    client = get_r2_client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": storage_uri},
        ExpiresIn=expiry_seconds,
    )


def get_public_url(storage_uri: str) -> str:
    """
    Build a direct CDN URL for a public-bucket R2 object (v0.10.0+).

    Returns f"{PUBLIC_ARTIFACT_URL}/{storage_uri}". No signing, no expiry,
    served straight off the Cloudflare edge. Zero egress cost.

    Raises:
        RuntimeError if PUBLIC_ARTIFACT_URL env var is unset. Callers must
            handle this - serve_media falls back to a presigned URL against
            the public bucket in that case.
    """
    base = os.environ.get("PUBLIC_ARTIFACT_URL", "").rstrip("/")
    if not base:
        raise RuntimeError(
            "PUBLIC_ARTIFACT_URL not configured. "
            "Cannot build public bucket CDN URL."
        )
    return f"{base}/{storage_uri}"


def delete_object(
    storage_uri: str,
    bucket_kind: str = "private",
) -> bool:
    """
    Delete an object from R2.

    Args:
        storage_uri: R2 object key ({tenant_id}/{artifact_id}).
        bucket_kind: 'private' (default) or 'public'. Selects the bucket to
            delete from. Callers should pass the row's stored bucket value.

    Returns True on successful delete, False if R2 disabled or delete failed.
    Does NOT raise on failure - DB soft-delete should proceed regardless so
    the row is marked deleted even if R2 cleanup needs a manual sweep later.

    Idempotent: deleting a non-existent key is not an error in S3/R2 protocol.
    """
    if bucket_kind not in ("private", "public"):
        raise ValueError(f"Invalid bucket_kind: {bucket_kind!r}")

    if not r2_enabled():
        return False

    bucket = _resolve_bucket_name(bucket_kind)
    try:
        client = get_r2_client()
        client.delete_object(Bucket=bucket, Key=storage_uri)
        logger.info(
            "r2_delete ok bucket_kind=%s bucket=%s key=%s",
            bucket_kind, bucket, storage_uri,
        )
        return True
    except Exception as e:
        logger.error(
            "r2_delete failed bucket_kind=%s bucket=%s key=%s err=%r",
            bucket_kind, bucket, storage_uri, e,
        )
        return False


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def health_check() -> dict:
    """
    Verify R2 connectivity. Safe to call from a /health endpoint.

    v0.10.0: reports on both buckets. Public bucket is optional - if
    R2_BUCKET_PUBLIC is unset the report indicates 'not_configured'.

    Returns:
        {"r2_enabled": False, "status": "disabled"}
            when core R2 env vars are not set.

        {"r2_enabled": True,
         "status": "ok"|"error",
         "private_bucket": {"name": "<name>", "status": "ok"|"error", ...},
         "public_bucket": {"name": "<name>"|None,
                           "status": "ok"|"not_configured"|"error", ...},
         "public_url": "https://..." | None}
            when R2 is reachable. Top-level status is 'ok' only when all
            configured buckets are 'ok'. If either bucket is 'error',
            top-level status is 'error'.
    """
    if not r2_enabled():
        return {"r2_enabled": False, "status": "disabled"}

    private_bucket = os.environ["R2_BUCKET"]
    public_bucket = os.environ.get("R2_BUCKET_PUBLIC")
    public_url = os.environ.get("PUBLIC_ARTIFACT_URL")

    result: dict = {
        "r2_enabled": True,
        "status": "ok",
        "private_bucket": {"name": private_bucket, "status": "unknown"},
        "public_bucket": (
            {"name": public_bucket, "status": "unknown"}
            if public_bucket
            else {"name": None, "status": "not_configured"}
        ),
        "public_url": public_url,
    }

    try:
        client = get_r2_client()
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return result

    try:
        client.list_objects_v2(Bucket=private_bucket, MaxKeys=1)
        result["private_bucket"]["status"] = "ok"
    except Exception as e:
        result["private_bucket"]["status"] = "error"
        result["private_bucket"]["error"] = str(e)
        result["status"] = "error"

    if public_bucket:
        try:
            client.list_objects_v2(Bucket=public_bucket, MaxKeys=1)
            result["public_bucket"]["status"] = "ok"
        except Exception as e:
            result["public_bucket"]["status"] = "error"
            result["public_bucket"]["error"] = str(e)
            result["status"] = "error"

    return result
