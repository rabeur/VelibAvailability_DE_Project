"""Pipeline target resolution for dual-mode DAGs.

Centralises the ``PIPELINE_TARGET`` switch shared by every Velib DAG so
that local and cloud branches stay consistent. Reading happens at
parse-time (module import) *and* at runtime (each task) — both paths
route through :func:`get_pipeline_target` to avoid drift.

The helper never raises on missing cloud config unless a task actually
asks for it via :func:`get_cloud_config`; this keeps DAG parsing green
on a pure local install that has no GCP env vars set.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

LOCAL = "local"
CLOUD = "cloud"
_VALID_TARGETS = {LOCAL, CLOUD}


def get_pipeline_target() -> str:
    """Return the active pipeline target.

    Resolution order:
    1. Airflow Variable ``pipeline_target`` (if set and Airflow is importable)
    2. Env var ``PIPELINE_TARGET``
    3. Default ``"local"``
    """
    try:
        from airflow.models import Variable

        value = Variable.get("pipeline_target", default_var=None)
        if value:
            value = value.strip().lower()
            if value in _VALID_TARGETS:
                return value
    except Exception:
        # Airflow not importable (unit tests, ruff) or Variable backend
        # unavailable (scheduler warm-up). Fall through to env var.
        pass

    value = os.getenv("PIPELINE_TARGET", LOCAL).strip().lower()
    if value not in _VALID_TARGETS:
        raise ValueError(
            f"Invalid PIPELINE_TARGET={value!r}, expected one of {sorted(_VALID_TARGETS)}"
        )
    return value


def is_cloud() -> bool:
    return get_pipeline_target() == CLOUD


def is_local() -> bool:
    return get_pipeline_target() == LOCAL


@dataclass(frozen=True)
class CloudConfig:
    """Immutable GCP config built from env vars.

    Kept frozen so downstream code can pass it around without worrying
    about accidental mutation across tasks.
    """

    project_id: str
    region: str
    bronze_bucket: str
    silver_dataset: str
    gold_dataset: str
    temp_bucket: str
    service_account: str | None = None

    @property
    def bronze_base_uri(self) -> str:
        return f"gs://{self.bronze_bucket}/bronze/velib"

    @property
    def spark_job_uri(self) -> str:
        return f"gs://{self.bronze_bucket}/spark_jobs/bronze_to_silver.py"


def get_cloud_config() -> CloudConfig:
    """Build a :class:`CloudConfig` from environment variables.

    Raises ``RuntimeError`` if a required variable is missing, so that
    cloud tasks fail fast with a clear message instead of letting the
    GCP SDK surface a cryptic 400 later.
    """
    required = {
        "project_id": "GCP_PROJECT_ID",
        "region": "GCP_REGION",
        "bronze_bucket": "GCP_BRONZE_BUCKET",
        "silver_dataset": "GCP_BIGQUERY_SILVER_DATASET",
        "gold_dataset": "GCP_BIGQUERY_GOLD_DATASET",
    }
    values: dict[str, str] = {}
    missing: list[str] = []
    for field, env in required.items():
        raw = os.getenv(env, "").strip()
        if not raw:
            missing.append(env)
        else:
            values[field] = raw

    if missing:
        raise RuntimeError(
            "Missing required GCP environment variables for cloud mode: " + ", ".join(missing)
        )

    # Temp bucket defaults to the Bronze bucket when not set separately
    # (connector only needs a writable GCS path in the same region).
    temp_bucket = os.getenv("GCP_BQ_TEMP_BUCKET", "").strip() or values["bronze_bucket"]
    service_account = os.getenv("GCP_DATAPROC_SERVICE_ACCOUNT", "").strip() or None

    return CloudConfig(
        project_id=values["project_id"],
        region=values["region"],
        bronze_bucket=values["bronze_bucket"],
        silver_dataset=values["silver_dataset"],
        gold_dataset=values["gold_dataset"],
        temp_bucket=temp_bucket,
        service_account=service_account,
    )
