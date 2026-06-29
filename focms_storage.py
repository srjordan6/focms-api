"""
focms_storage.py — Cloudflare R2 storage abstraction for FOCMS media files.

Architecture: archive_entries source_id='artifact_storage_r2_v1_0'

Provides R2 upload/download/delete with automatic bytea fallback when R2
env vars are not configured. Endpoints check r2_enabled() to decide which
storage path to use; this module hides the boto3 details.

Environment variables (set on focms-api Render service):
    R2_ACCOUNT_ID        Cloudflare account ID
    R2_ACCESS_KEY_ID     S3-compatible access key ID
    R2_SECRET_ACCESS_KEY S3-compatible secret access key
    R2_BUCKET            R2 bucket name (outcomestar-artifacts)

Object key convention: tenant_id/artifact_id
    - Tenant prefix enables tenant-scoped cleanup (lifecycle rules per tenant)
    - artifact_id is UUID v7 from gen_random_uuid_v7() — naturally time-sorted

Usage pattern in endpoints:
    from focms_storage import (
        r2_enabled, upload_bytes, get_presigned_url, delete_object
    )

    # On upload:
    result = upload_bytes(tenant_id, artifact_id, data, content_type)
    # result["storage_kind"] is "r2" if R2 active, "inline_bytea" otherwise
    # INSERT row with storage_kind=result["storage_kind"],
    #                  storage_uri=result["storage_uri"],
    #                  content=(data if result["storage_kind"]=="inline_bytea" else None)

    # On download (R2 rows):
    redirect_url = get_presigned_url(row["storage_uri"], expiry_seconds=300)
    return RedirectResponse(redirect_url, status_code=302)

    # On download (bytea rows):
    return StreamingResponse(io.BytesIO(row["content"]), media_type=row["mime_type"])

    # On delete:
    if row["storage_kind"] == "r2":
        delete_object(row["storage_uri"])
    # Then UPDATE media_files SET deleted_at = now() WHERE id = ...
"""

import os
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


def r2_enabled() -> bool:
    """Return True if all four R2 env vars are configured."""
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

    Cached on first call. Raises RuntimeError if env vars are missing.
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


def upload_bytes(
    tenant_id: str,
    artifact_id: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> dict:
    """
    Upload bytes to R2, with bytea fallback when R2 disabled.

    Returns:
        {"storage_kind": "r2", "storage_uri": "<key>"}
            when R2 is enabled and upload succeeded.

        {"storage_kind": "inline_bytea", "storage_uri": None}
            when R2 is disabled. Caller stores `data` in media_files.content.

    Raises:
        Any boto3 exception on R2 failure (caller should let this bubble).
    """
    if not r2_enabled():
        return {"storage_kind": "inline_bytea", "storage_uri": None}

    key = build_object_key(tenant_id, artifact_id)
    bucket = os.environ["R2_BUCKET"]

    try:
        client = get_r2_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        logger.info(
            "r2_upload ok bucket=%s key=%s bytes=%d", bucket, key, len(data)
        )
        return {"storage_kind": "r2", "storage_uri": key}
    except Exception as e:
        logger.error(
            "r2_upload failed bucket=%s key=%s err=%r", bucket, key, e
        )
        raise


def get_presigned_url(storage_uri: str, expiry_seconds: int = 300) -> str:
    """
    Generate a short-lived presigned URL for an R2 object.

    Default TTL: 5 minutes. Used by the media-serve endpoint to return
    a 302 redirect so the client downloads directly from R2 (zero egress
    cost, no proxy bandwidth on our API).

    Raises RuntimeError if R2 not configured.
    """
    if not r2_enabled():
        raise RuntimeError("Cannot generate presigned URL without R2 configured.")

    bucket = os.environ["R2_BUCKET"]
    client = get_r2_client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": storage_uri},
        ExpiresIn=expiry_seconds,
    )


def delete_object(storage_uri: str) -> bool:
    """
    Delete an object from R2.

    Returns True on successful delete, False if R2 disabled or delete failed.
    Does NOT raise on failure — DB soft-delete should proceed regardless so
    the row is marked deleted even if R2 cleanup needs a manual sweep later.

    Idempotent: deleting a non-existent key is not an error in S3/R2 protocol.
    """
    if not r2_enabled():
        return False

    bucket = os.environ["R2_BUCKET"]
    try:
        client = get_r2_client()
        client.delete_object(Bucket=bucket, Key=storage_uri)
        logger.info("r2_delete ok bucket=%s key=%s", bucket, storage_uri)
        return True
    except Exception as e:
        logger.error(
            "r2_delete failed bucket=%s key=%s err=%r", bucket, storage_uri, e
        )
        return False


def health_check() -> dict:
    """
    Verify R2 connectivity. Safe to call from a /health endpoint.

    Returns:
        {"r2_enabled": False, "status": "disabled"}
            when env vars are not set.

        {"r2_enabled": True, "status": "ok", "bucket": "<name>"}
            when R2 is reachable and the bucket lists successfully.

        {"r2_enabled": True, "status": "error", "error": "<msg>"}
            when env vars are set but R2 call failed.
    """
    if not r2_enabled():
        return {"r2_enabled": False, "status": "disabled"}

    try:
        bucket = os.environ["R2_BUCKET"]
        client = get_r2_client()
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return {"r2_enabled": True, "status": "ok", "bucket": bucket}
    except Exception as e:
        return {"r2_enabled": True, "status": "error", "error": str(e)}
