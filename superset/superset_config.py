import os
import importlib.util

# ─── Security ────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set True if serving over HTTPS

# ─── Superset metadata database ──────────────────────────────────────────────
# Uses a dedicated 'superset_meta' database on the same postgres instance.
if importlib.util.find_spec("psycopg"):
    postgres_driver = "psycopg"
elif importlib.util.find_spec("psycopg2"):
    postgres_driver = "psycopg2"
else:
    raise RuntimeError("Neither psycopg nor psycopg2 is installed in the Superset image")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+{postgres_driver}://"
    f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    "@postgres:5432/superset_meta"
)

# ─── Feature flags ───────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,  # Allow Jinja templating in SQL queries
    "DASHBOARD_NATIVE_FILTERS": True,
}
