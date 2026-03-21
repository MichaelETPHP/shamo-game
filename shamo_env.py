"""
SHAMO — single source for database / Supabase variables from .env.

Use these names everywhere in the project:
  SUPABASE_URL          — PostgREST / API base (NOT raw Postgres; see ENV_SELF_HOSTED.sample)
  SUPABASE_KEY          — anon JWT (browser + RLS); optional if SUPABASE_ANON_KEY set
  SUPABASE_ANON_KEY     — same as above (either name works)
  SUPABASE_SERVICE_KEY  — service_role JWT for server (api.py create_client)
  DATABASE_URL          — direct PostgreSQL (migrations, db.py pool)
  DB_SCHEMA             — psycopg search_path (default public)
  POSTGREST_SCHEMA      — supabase-py / PostgREST default schema (default: DB_SCHEMA)
  QR_REST_SCHEMA        — optional; only if qr_codes/qr_scans live in a different schema than POSTGREST_SCHEMA
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

logger = logging.getLogger("shamo.env")

# ─── Supabase HTTP API (supabase-py + mini-app REST) ───────────────────────────
SUPABASE_URL = (
    (os.getenv("SUPABASE_REST_URL") or os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
)

# Anon key for browser: SUPABASE_ANON_KEY or SUPABASE_KEY (your .env uses SUPABASE_KEY)
SUPABASE_ANON_KEY = (
    (os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
)

SUPABASE_SERVICE_KEY = (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()

# Key used by Python API (service bypasses RLS; falls back to anon if service missing)
SUPABASE_SERVER_KEY = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY

# Legacy alias — same as server key for create_client()
SUPABASE_KEY = SUPABASE_SERVER_KEY

# ─── Direct PostgreSQL ───────────────────────────────────────────────────────
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
# Default public = stock Supabase + shamo_one_click_install_public.sql. Use DB_SCHEMA=shamo with shamo_one_click_install_and_seed.sql.
DB_SCHEMA = (os.getenv("DB_SCHEMA") or "public").strip()

# PostgREST default schema for supabase-py (Accept-Profile). Override without changing psycopg search_path.
POSTGREST_SCHEMA = (os.getenv("POSTGREST_SCHEMA") or DB_SCHEMA).strip()

# Optional: PostgREST schema for qr_codes / qr_scans only (split install). Empty = same as POSTGREST_SCHEMA (recommended).
QR_REST_SCHEMA = (os.getenv("QR_REST_SCHEMA") or "").strip()

if SUPABASE_URL:
    _host = SUPABASE_URL.split("//", 1)[-1].split("/")[0].split("@")[-1].lower()
    _u = SUPABASE_URL.lower()
    if _host.startswith("db.") or ":5432" in SUPABASE_URL or "postgresql" in _u:
        logger.error(
            "SUPABASE_URL points at a Postgres-style host (%s). The stack needs the "
            "**REST API** URL (PostgREST/Kong) for supabase-py and the mini-app. "
            "Keep DATABASE_URL for SQL tools; set SUPABASE_REST_URL to your API gateway.",
            _host[:80],
        )
