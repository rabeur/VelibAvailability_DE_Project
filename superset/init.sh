#!/bin/bash
# Superset one-shot initialisation script:
#   1. Creates the superset_meta database on postgres if it doesn't exist yet.
#   2. Runs Superset DB migrations.
#   3. Creates the admin user (idempotent – existing user is silently skipped).
#   4. Loads default Superset roles and permissions.
set -e

echo "==> [1/4] Ensuring superset_meta database exists..."
python3 - <<'PYEOF'
from sqlalchemy import create_engine, text
import importlib.util
import os

if importlib.util.find_spec("psycopg"):
    driver = "psycopg"
elif importlib.util.find_spec("psycopg2"):
    driver = "psycopg2"
else:
    raise RuntimeError("Neither psycopg nor psycopg2 is installed in the Superset image")

pg_url = (
    f"postgresql+{driver}://"
    f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    "@postgres:5432/postgres"
)
engine = create_engine(pg_url, isolation_level="AUTOCOMMIT")
with engine.connect() as conn:
    exists = conn.execute(
        text("SELECT 1 FROM pg_database WHERE datname='superset_meta'")
    ).fetchone()
    if not exists:
        conn.execute(text("CREATE DATABASE superset_meta"))
        print("  Database superset_meta created.")
    else:
        print("  Database superset_meta already exists.")
PYEOF

echo "==> [2/4] Running Superset DB migrations..."
superset db upgrade

echo "==> [3/4] Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname Velib \
    --email admin@velib.com \
    --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "==> [4/4] Loading default roles and permissions..."
superset init

echo "==> Superset initialisation complete."
