"""nces_scorecard_worker.py - College Scorecard + NCES Ingestion Worker v0.1

Pulls institution-level data from the U.S. Department of Education's College
Scorecard API and upserts into the FOCMS universities and university_cds_facts
tables.

Run modes:
    refresh-top-n     - refresh top N schools by admit selectivity
    refresh-targets   - refresh just the schools in target_universities
    refresh-leaids    - refresh a comma-separated list of LEAIDs

Environment:
    DATABASE_URL_POOLED       - pgbouncer URL (transaction mode)
    SCORECARD_API_KEY         - api.data.gov key
    FOCMS_WORKER_LOG_LEVEL    - INFO (default)

Deployed as a Render Cron Job. See accompanying runbook for schedule.
"""
import argparse, asyncio, logging, os
from typing import Any

import asyncpg, httpx

DATABASE_URL = os.environ.get("DATABASE_URL_POOLED") or os.environ["DATABASE_URL"]
API_KEY = os.environ["SCORECARD_API_KEY"]
FOCMS_TENANT_ID = os.environ.get("FOCMS_TENANT_ID", "019ed384-56fc-7516-bfbf-efaa5231e281")
LOG_LEVEL = os.environ.get("FOCMS_WORKER_LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nces-scorecard-worker")

SCORECARD_BASE = "https://api.data.gov/ed/collegescorecard/v1/schools"
SCORECARD_FIELDS = [
    "id", "school.name", "school.city", "school.state",
    "latest.admissions.admission_rate.overall",
    "latest.admissions.sat_scores.midpoint.math",
    "latest.admissions.sat_scores.midpoint.critical_reading",
    "latest.admissions.sat_scores.75th_percentile.math",
    "latest.admissions.sat_scores.75th_percentile.critical_reading",
    "latest.admissions.act_scores.midpoint.cumulative",
    "latest.cost.attendance.academic_year",
    "latest.student.size",
]

async def fetch_scorecard(leaids: list[str]) -> list[dict[str, Any]]:
    """Pull Scorecard rows for the given IPEDS unit_ids (same as FOCMS leaid)."""
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for chunk in (leaids[i:i+50] for i in range(0, len(leaids), 50)):
            params = {"api_key": API_KEY, "id": ",".join(chunk),
                      "fields": ",".join(SCORECARD_FIELDS), "per_page": 100}
            r = await client.get(SCORECARD_BASE, params=params)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("results", []))
            log.info("scorecard chunk=%d returned=%d", len(chunk), len(data.get("results", [])))
    return out

def row_to_universities(row: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Scorecard row into the universities table shape."""
    return {
        "leaid": str(row.get("id")),
        "name": row.get("school.name"),
        "city": row.get("school.city"),
        "state": row.get("school.state"),
        "admit_rate": row.get("latest.admissions.admission_rate.overall"),
        "cost_attendance": row.get("latest.cost.attendance.academic_year"),
        "enrollment": row.get("latest.student.size"),
        "data_year": 2024,
    }

def row_to_cds_facts(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract per-fact rows for the university_cds_facts table."""
    leaid = str(row.get("id"))
    facts = []
    for fact_key, sc_key in [
        ("sat_25_math", "latest.admissions.sat_scores.midpoint.math"),
        ("sat_25_reading", "latest.admissions.sat_scores.midpoint.critical_reading"),
        ("sat_75_math", "latest.admissions.sat_scores.75th_percentile.math"),
        ("sat_75_reading", "latest.admissions.sat_scores.75th_percentile.critical_reading"),
        ("act_50", "latest.admissions.act_scores.midpoint.cumulative"),
    ]:
        v = row.get(sc_key)
        if v is not None:
            facts.append({
                "university_leaid": leaid, "fact_key": fact_key,
                "fact_value_numeric": float(v), "data_year": 2024,
                "source": "college_scorecard",
            })
    return facts


async def upsert_universities(conn: asyncpg.Connection, rows: list[dict[str, Any]]) -> int:
    """Insert or update universities rows. Returns count."""
    n = 0
    for r in rows:
        if not r["leaid"]:
            continue
        await conn.execute("""
            INSERT INTO universities (leaid, name, city, state, admit_rate,
                                     cost_attendance, enrollment, data_year)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (leaid) DO UPDATE SET
                name = EXCLUDED.name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                admit_rate = COALESCE(EXCLUDED.admit_rate, universities.admit_rate),
                cost_attendance = COALESCE(EXCLUDED.cost_attendance, universities.cost_attendance),
                enrollment = COALESCE(EXCLUDED.enrollment, universities.enrollment),
                data_year = GREATEST(EXCLUDED.data_year, universities.data_year),
                updated_at = now()
        """, r["leaid"], r["name"], r["city"], r["state"],
             r["admit_rate"], r["cost_attendance"], r["enrollment"], r["data_year"])
        n += 1
    return n

async def upsert_cds_facts(conn: asyncpg.Connection, facts: list[dict[str, Any]]) -> int:
    """Insert or update CDS facts. Returns count."""
    n = 0
    for f in facts:
        await conn.execute("""
            INSERT INTO university_cds_facts (university_leaid, fact_key,
                                              fact_value_numeric, data_year, source)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (university_leaid, fact_key, data_year) DO UPDATE SET
                fact_value_numeric = EXCLUDED.fact_value_numeric,
                source = EXCLUDED.source,
                updated_at = now()
        """, f["university_leaid"], f["fact_key"], f["fact_value_numeric"],
             f["data_year"], f["source"])
        n += 1
    return n

async def resolve_leaids(mode: str, value: str | None, pool) -> list[str]:
    log.info("resolve_leaids start mode=%s tenant=%s", mode, FOCMS_TENANT_ID)
    """Determine which LEAIDs to refresh based on mode."""
    if mode == "refresh-leaids":
        return [s.strip() for s in (value or "").split(",") if s.strip()]
    if mode == "refresh-targets":
        async with pool.acquire() as conn:
            async with conn.transaction():
                log.info("resolve_leaids: acquired conn, setting tenant")
                await conn.execute(f"SELECT set_config('app.current_tenant_id', '{FOCMS_TENANT_ID}', true)")
                log.info("resolve_leaids: tenant set, querying targets")
                rows = await conn.fetch(
                    "SELECT DISTINCT university_leaid FROM target_universities WHERE deleted_at IS NULL"
                )
                return [r["university_leaid"] for r in rows]
    if mode == "refresh-top-n":
        n = int(value or "100")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT leaid FROM universities WHERE admit_rate IS NOT NULL ORDER BY admit_rate ASC LIMIT $1",
                n,
            )
            return [r["leaid"] for r in rows]
    raise ValueError(f"unknown mode {mode}")

async def main_async(mode: str, value: str | None) -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4,
                                     statement_cache_size=0)
    try:
        leaids = await resolve_leaids(mode, value, pool)
        log.info("resolved %d LEAIDs mode=%s", len(leaids), mode)
        if not leaids:
            log.warning("nothing to refresh")
            return
        rows = await fetch_scorecard(leaids)
        log.info("fetched %d scorecard rows", len(rows))
        async with pool.acquire() as conn:
            async with conn.transaction():
                n_uni = await upsert_universities(conn, [row_to_universities(r) for r in rows])
                fact_batches = []
                for r in rows:
                    fact_batches.extend(row_to_cds_facts(r))
                n_facts = await upsert_cds_facts(conn, fact_batches)
        log.info("upserted universities=%d cds_facts=%d", n_uni, n_facts)
    finally:
        await pool.close()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["refresh-top-n", "refresh-targets", "refresh-leaids"])
    ap.add_argument("--value", help="n for top-n, or comma-separated leaids")
    args = ap.parse_args()
    asyncio.run(main_async(args.mode, args.value))

if __name__ == "__main__":
    main()

