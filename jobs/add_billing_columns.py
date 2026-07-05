"""add_billing_columns.py — one-off migration (run as focms_user via standard DSN).

Adds storage-billing entitlement columns to tenants and extends
audit_action_enum with the three values from coppa_vpc_method_selection_v0_1.
Idempotent. Usage (PowerShell, after Load-NorthStarEnv):

    python jobs/add_billing_columns.py  (or .\\jobs\\add_billing_columns.py)
"""
import asyncio
import os

import asyncpg

STATEMENTS = [
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS storage_plan text NOT NULL DEFAULT 'free'",
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS storage_quota_gb numeric NOT NULL DEFAULT 1",
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS artifacts_grace_period_ends_at timestamptz",
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS data_retention_policy_version text NOT NULL DEFAULT 'v1.0'",
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS stripe_customer_id text",
    "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS billing_verified_at timestamptz",
    "ALTER TYPE audit_action_enum ADD VALUE IF NOT EXISTS 'coppa_vpc_captured'",
    "ALTER TYPE audit_action_enum ADD VALUE IF NOT EXISTS 'student_portal_access_issued'",
    "ALTER TYPE audit_action_enum ADD VALUE IF NOT EXISTS 'tenant_ownership_transferred'",
]


async def main() -> None:
    dsn = os.environ["FOCMS_DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    try:
        for stmt in STATEMENTS:
            await conn.execute(stmt)
            print("OK:", stmt[:72])
        cols = await conn.fetchval(
            "SELECT count(*) FROM information_schema.columns WHERE table_name='tenants' "
            "AND column_name IN ('storage_plan','storage_quota_gb','artifacts_grace_period_ends_at',"
            "'data_retention_policy_version','stripe_customer_id','billing_verified_at')")
        print(f"VERIFY: tenants billing columns present = {cols}/6")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
