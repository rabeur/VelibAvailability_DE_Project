from __future__ import annotations

"""
============================================================================
BRONZE TO SILVER TRANSFORMATION - Vélib Data Engineering Project
============================================================================
Description: Transform raw Bronze data into cleaned and normalized
             Silver data with PySpark
Author: Data Team
Date: 2026-02-26
============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
    round as spark_round,
    to_timestamp,
    coalesce,
    trim,
    from_utc_timestamp,
    hour,
    least,
    greatest,
    expr,
    upper,
    udf,
)
from pyspark.sql.types import *
from datetime import datetime
import sys
import os
import logging
from typing import Optional

from paris_arrondissement_utils import get_paris_arrondissement_label
from silver_writers import BigQuerySilverWriter, PostgresSilverWriter, SilverWriter

# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@udf(returnType=StringType())
def enrich_paris_district_name(
    district_municipality_name: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
) -> Optional[str]:
    """Replace the generic `Paris` label with the arrondissement derived from coordinates."""
    if district_municipality_name is None:
        return None

    normalized_name = district_municipality_name.strip()
    if normalized_name.lower() != "paris":
        return normalized_name

    arrondissement = get_paris_arrondissement_label(latitude=latitude, longitude=longitude)
    return arrondissement or normalized_name


def create_spark_session(pipeline_target: str = "local") -> SparkSession:
    """Build a Spark session tuned for the requested target.

    On `local`, add the PostgreSQL JDBC driver and cap driver/executor
    memory to sane local-container defaults. On `cloud`, stay minimal:
    Dataproc Serverless ships the GCS and BigQuery connectors, and sizing
    is controlled at batch submission time, not in the session config.
    """
    logger.info(f"🚀 Creating Spark session (target={pipeline_target})...")

    builder = (
        SparkSession.builder.appName("VelibBronzeToSilver")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.session.timeZone", "Europe/Paris")
    )

    if pipeline_target == "local":
        builder = (
            builder.config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"✅ Spark version: {spark.version}")
    return spark


class VelibBronzeToSilver:
    """
    Bronze -> Silver transformation pipeline for Velib data.

    The class owns the transformation logic; the destination-specific
    I/O is delegated to a SilverWriter so the same pipeline can run
    against PostgreSQL (local) or BigQuery (cloud).
    """

    def __init__(self, spark: SparkSession, silver_writer: SilverWriter):
        """
        Args:
            spark: Pre-configured Spark session (see create_spark_session).
            silver_writer: Destination-aware writer (Postgres or BigQuery).
        """
        self.spark = spark
        self.silver_writer = silver_writer

    def read_bronze_data(self, bronze_path: str, date_filter: str = None, hour_filter: str = None):
        """
        Read Bronze data (Parquet) with an optional date filter

        Args:
            bronze_path: Path to Bronze data
            date_filter: Date filter in YYYY-MM-DD format (optional)
            hour_filter: Hour filter in HH format (optional)

        Returns:
            DataFrame Spark
        """
        logger.info(f"📂 Reading Bronze data: {bronze_path}")

        if date_filter:
            if hour_filter:
                full_path = (
                    f"{bronze_path}/ingestion_date={date_filter}/hour={hour_filter}/*.parquet"
                )
                logger.info(f"   Date filter: {date_filter}, hour: {hour_filter}")
            else:
                full_path = f"{bronze_path}/ingestion_date={date_filter}/**/*.parquet"
                logger.info(f"   Date filter: {date_filter}")
        else:
            full_path = f"{bronze_path}/**/*.parquet"

        try:
            df = self.spark.read.parquet(full_path)
            count = df.count()
            logger.info(f"✅ {count:,} rows loaded")
            logger.info(f"📋 Columns: {df.columns}")
            return df
        except Exception as e:
            logger.error(f"❌ Read error: {e}")
            raise

    def transform_to_silver(self, df_bronze):
        """
        Bronze -> Silver transformations:
        1. Data cleaning
        2. Column normalization
        3. Metric calculations
        4. Data type handling
        """
        logger.info("🔄 Bronze -> Silver transformation...")

        # ========================================
        # STEP 1: Clean and select columns
        # ========================================
        logger.info("  → Step 1: Cleaning columns")

        df_clean = df_bronze.select(
            # Identifiers
            trim(col("stationcode")).alias("station_id"),
            trim(col("name")).alias("station_name"),
            # Capacity
            coalesce(col("capacity").cast("integer"), lit(0)).alias("capacity"),
            # Bike availability
            coalesce(col("numbikesavailable").cast("integer"), lit(0)).alias("num_bikes_available"),
            coalesce(col("mechanical").cast("integer"), lit(0)).alias(
                "num_bikes_available_mechanical"
            ),
            coalesce(col("ebike").cast("integer"), lit(0)).alias("num_bikes_available_ebike"),
            # Dock availability
            coalesce(col("numdocksavailable").cast("integer"), lit(0)).alias("num_docks_available"),
            # Status (booleans stored as text: YES/NO)
            upper(trim(coalesce(col("is_installed"), lit("OUI")))).alias("is_installed_raw"),
            upper(trim(coalesce(col("is_renting"), lit("OUI")))).alias("is_renting_raw"),
            upper(trim(coalesce(col("is_returning"), lit("OUI")))).alias("is_returning_raw"),
            # Location (flattened coordinates)
            col("lon").cast("double").alias("longitude"),
            col("lat").cast("double").alias("latitude"),
            # Geographic information
            trim(col("nom_arrondissement_communes")).alias("district_municipality_names"),
            trim(col("code_insee_commune")).alias("insee_municipality_code"),
            # Time metadata
            from_utc_timestamp(to_timestamp(col("ingestion_timestamp")), "Europe/Paris").alias(
                "ingestion_timestamp"
            ),
            col("snapshot_id"),
        )

        # ========================================
        # STEP 2: Convert booleans
        # ========================================
        logger.info("  → Step 2: Converting booleans")

        df_clean = (
            df_clean.withColumn(
                "is_installed",
                when(col("is_installed_raw").isin("OUI", "YES", "TRUE", "1"), lit(True)).otherwise(
                    lit(False)
                ),
            )
            .withColumn(
                "is_renting",
                when(col("is_renting_raw").isin("OUI", "YES", "TRUE", "1"), lit(True)).otherwise(
                    lit(False)
                ),
            )
            .withColumn(
                "is_returning",
                when(col("is_returning_raw").isin("OUI", "YES", "TRUE", "1"), lit(True)).otherwise(
                    lit(False)
                ),
            )
            .drop("is_installed_raw", "is_renting_raw", "is_returning_raw")
        )

        # ========================================
        # STEP 3: Calculate metrics
        # ========================================
        logger.info("  → Step 3: Calculating metrics")

        df_silver = (
            df_clean.withColumn("snapshot_timestamp", col("ingestion_timestamp"))
            .withColumn("snapshot_hour", hour(col("snapshot_timestamp")))
            .withColumn(
                "occupancy_rate",
                when(
                    col("capacity") > 0,
                    least(
                        lit(100.0),
                        greatest(
                            lit(0.0),
                            spark_round((col("num_bikes_available") / col("capacity")) * 100, 2),
                        ),
                    ),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "availability_rate",
                when(
                    col("capacity") > 0,
                    least(
                        lit(100.0),
                        greatest(
                            lit(0.0),
                            spark_round((col("num_docks_available") / col("capacity")) * 100, 2),
                        ),
                    ),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "service_rate",
                when(
                    col("capacity") > 0,
                    least(
                        lit(100.0),
                        greatest(
                            lit(0.0),
                            spark_round(
                                (
                                    (col("num_docks_available") + col("num_bikes_available"))
                                    / col("capacity")
                                )
                                * 100,
                                2,
                            ),
                        ),
                    ),
                ).otherwise(lit(0.0)),
            )
            .withColumn("is_empty", col("num_bikes_available") == 0)
            .withColumn("is_full", col("num_docks_available") == 0)
            .withColumn(
                "is_operational", col("is_installed") & col("is_renting") & col("is_returning")
            )
            .withColumn(
                "processing_timestamp", from_utc_timestamp(current_timestamp(), "Europe/Paris")
            )
        )

        # ========================================
        # STEP 4: Filter invalid rows
        # ========================================
        logger.info("  → Step 4: Filtering invalid data")

        initial_count = df_silver.count()

        df_silver = df_silver.filter(
            (col("station_id").isNotNull())
            & (col("station_id") != "")
            & (col("capacity") > 0)
            & (col("num_bikes_available") >= 0)
            & (col("num_docks_available") >= 0)
            & (col("snapshot_timestamp").isNotNull())
        )

        final_count = df_silver.count()
        filtered_count = initial_count - final_count

        if filtered_count > 0:
            logger.warning(f"⚠️  {filtered_count:,} rows filtered out (invalid)")

        logger.info(f"✅ {final_count:,} valid rows after transformation")

        return df_silver

    def extract_stations_dimension(self, df_silver):
        """
        Extract the stations dimension (unique by station_id)
        Handles upsert by comparing with existing data
        """
        logger.info("🏗️  Extracting stations dimension...")

        df_stations = (
            df_silver.withColumn(
                "rn",
                expr(
                    "row_number() OVER (PARTITION BY station_id ORDER BY snapshot_timestamp DESC)"
                ),
            )
            .filter(col("rn") == 1)
            .select(
                col("station_id"),
                col("station_name"),
                col("capacity"),
                col("latitude"),
                col("longitude"),
                enrich_paris_district_name(
                    col("district_municipality_names"),
                    col("latitude"),
                    col("longitude"),
                ).alias("district_municipality_names"),
                col("insee_municipality_code"),
                col("snapshot_timestamp").alias("last_seen_at"),
            )
        )

        # For first_seen_at, keep the oldest timestamp
        df_first_seen = (
            df_silver.groupBy("station_id")
            .agg({"snapshot_timestamp": "min"})
            .withColumnRenamed("min(snapshot_timestamp)", "first_seen_at")
        )

        # Join to get first_seen_at and last_seen_at
        df_stations = df_stations.join(df_first_seen, "station_id", "left")

        # Add metadata
        df_stations = (
            df_stations.withColumn("is_active", lit(True))
            .withColumn("created_at", from_utc_timestamp(current_timestamp(), "Europe/Paris"))
            .withColumn("updated_at", from_utc_timestamp(current_timestamp(), "Europe/Paris"))
        )

        station_count = df_stations.count()
        logger.info(f"✅ {station_count:,} unique stations extracted")

        return df_stations

    def extract_availability_facts(self, df_silver):
        """
        Extract availability facts
        """
        logger.info("📊 Extracting availability facts...")

        df_availability = df_silver.select(
            col("station_id"),
            col("snapshot_timestamp"),
            col("num_bikes_available"),
            col("num_bikes_available_mechanical"),
            col("num_bikes_available_ebike"),
            col("num_docks_available"),
            col("is_installed"),
            col("is_renting"),
            col("is_returning"),
            col("occupancy_rate"),
            col("availability_rate"),
            col("service_rate"),
            col("is_empty"),
            col("is_full"),
            col("is_operational"),
            col("ingestion_timestamp"),
            col("processing_timestamp"),
        )

        facts_count = df_availability.count()
        logger.info(f"✅ {facts_count:,} availability facts extracted")

        return df_availability

    def run(self, bronze_path: str, date_filter: str = None, hour_filter: str = None):
        """
        Run the full Bronze -> Silver pipeline

        Args:
            bronze_path: Path to Bronze data
            date_filter: Date in YYYY-MM-DD format (process only that day)
            hour_filter: Hour in HH format (process only that hour)
        """
        logger.info("=" * 60)
        logger.info("🚀 STARTING BRONZE -> SILVER PIPELINE")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            # 1. Read Bronze
            df_bronze = self.read_bronze_data(bronze_path, date_filter, hour_filter)

            # 2. Transform
            df_silver = self.transform_to_silver(df_bronze)

            # 3. Extract dimensions and facts
            df_stations = self.extract_stations_dimension(df_silver)
            df_availability = self.extract_availability_facts(df_silver)

            # 4. Write through the destination-aware writer
            self.silver_writer.write_stations(df_stations)
            self.silver_writer.write_availability(df_availability)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info("=" * 60)
            logger.info(f"✅ PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f}s")
            logger.info("=" * 60)

            return {
                "success": True,
                "duration_seconds": duration,
                "stations_processed": df_stations.count(),
                "facts_processed": df_availability.count(),
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error("=" * 60)
            logger.error(f"❌ PIPELINE FAILED after {duration:.2f}s")
            logger.error(f"Error: {e}")
            logger.error("=" * 60)

            import traceback

            traceback.print_exc()

            return {"success": False, "duration_seconds": duration, "error": str(e)}

        finally:
            self.spark.stop()


def _build_silver_writer(spark: SparkSession, pipeline_target: str) -> SilverWriter:
    """Pick the right writer for the requested target and fail fast on bad config."""
    if pipeline_target == "local":
        postgres_config = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "database": os.getenv("POSTGRES_DB", "velib_dw"),
            "user": os.getenv("POSTGRES_USER", "velib"),
            "password": os.getenv("POSTGRES_PASSWORD", "velib"),
        }
        return PostgresSilverWriter(spark, postgres_config)

    if pipeline_target == "cloud":
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset_id = os.getenv("GCP_BIGQUERY_SILVER_DATASET", "velib_silver")
        temp_bucket = os.getenv("GCP_BQ_TEMP_BUCKET") or os.getenv("GCP_BRONZE_BUCKET")

        missing = [
            name
            for name, value in (
                ("GCP_PROJECT_ID", project_id),
                ("GCP_BQ_TEMP_BUCKET or GCP_BRONZE_BUCKET", temp_bucket),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "PIPELINE_TARGET=cloud requires the following env vars: " + ", ".join(missing)
            )

        return BigQuerySilverWriter(
            spark,
            {"project_id": project_id, "dataset_id": dataset_id, "temp_bucket": temp_bucket},
        )

    raise ValueError(f"Unknown PIPELINE_TARGET: {pipeline_target!r} (expected 'local' or 'cloud')")


def _default_bronze_path(pipeline_target: str) -> str:
    """Path default for each target. Cloud reads the bucket from env, local uses the mounted path."""
    if pipeline_target == "cloud":
        bucket = os.getenv("GCP_BRONZE_BUCKET")
        if not bucket:
            raise RuntimeError("PIPELINE_TARGET=cloud requires GCP_BRONZE_BUCKET to be set")
        return f"gs://{bucket}/bronze/velib"
    return "/opt/data_lake/bronze/velib"


def main():
    """Script entry point.

    Selecting the target:
      - PIPELINE_TARGET=local (default): reads local Parquet, writes to PostgreSQL.
      - PIPELINE_TARGET=cloud: reads Parquet from gs://, writes to BigQuery via the
        Spark BigQuery connector.

    CLI contract is unchanged:
      spark-submit bronze_to_silver.py <date_filter> [hour_filter]
      spark-submit bronze_to_silver.py <bronze_path> <date_filter> [hour_filter]
    If <bronze_path> is omitted, the target-appropriate default is used.
    """
    args = sys.argv[1:]

    pipeline_target = None
    if args:
        first_arg = args[0].lower()
        if first_arg in {"local", "cloud"}:
            pipeline_target = first_arg
            args = args[1:]
        elif first_arg.startswith("--target="):
            pipeline_target = first_arg.split("=", 1)[1].lower()
            args = args[1:]
        elif first_arg.startswith("--pipeline-target="):
            pipeline_target = first_arg.split("=", 1)[1].lower()
            args = args[1:]

    pipeline_target = pipeline_target or os.getenv("PIPELINE_TARGET", "local").lower()
    os.environ["PIPELINE_TARGET"] = pipeline_target

    # Dataproc Serverless does not reliably propagate spark.driverEnv.* into
    # the driver process environment, so the DAG forwards GCP config as
    # named CLI overrides and we inject them into os.environ here. The rest
    # of the script (writer factory, _default_bronze_path) keeps reading env
    # vars unchanged, so local invocation paths are not affected.
    _named_to_env = {
        "--bronze-bucket": "GCP_BRONZE_BUCKET",
        "--gcp-project": "GCP_PROJECT_ID",
        "--silver-dataset": "GCP_BIGQUERY_SILVER_DATASET",
        "--temp-bucket": "GCP_BQ_TEMP_BUCKET",
    }
    _remaining: list[str] = []
    for _a in args:
        _matched = False
        for _prefix, _env in _named_to_env.items():
            if _a.startswith(_prefix + "="):
                os.environ[_env] = _a.split("=", 1)[1]
                _matched = True
                break
        if not _matched:
            _remaining.append(_a)
    args = _remaining

    default_bronze_path = _default_bronze_path(pipeline_target)

    # Flexible argument parsing
    # Supported invocation modes:
    # 1) spark-submit bronze_to_silver.py 2026-02-26
    # 2) spark-submit bronze_to_silver.py 2026-02-26 14
    # 3) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26
    # 4) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26 14

    if len(args) == 0:
        print("Usage: spark-submit bronze_to_silver.py [bronze_path] <date_filter> [hour_filter]")
        print(f"  bronze_path: Path to Bronze data (default: {default_bronze_path})")
        print("  date_filter: Date YYYY-MM-DD (required)")
        print("  hour_filter: Hour HH (optional)")
        print("Example: spark-submit bronze_to_silver.py 2026-02-26 14")
        sys.exit(1)

    date_candidate = args[0]
    bronze_path = default_bronze_path
    date_filter = None
    hour_filter = None

    # Detect whether the first argument is a path or a date
    # Expected date format: YYYY-MM-DD
    if len(date_candidate) == 10 and date_candidate[4] == "-" and date_candidate[7] == "-":
        date_filter = date_candidate
        if len(args) >= 2:
            hour_filter = args[1]
        if len(args) >= 3:
            print("Error: too many arguments for date+hour format")
            sys.exit(1)
    else:
        bronze_path = date_candidate
        if len(args) < 2:
            print("Error: date_filter is required when bronze_path is provided")
            sys.exit(1)
        date_filter = args[1]
        if len(args) >= 3:
            hour_filter = args[2]
        if len(args) >= 4:
            print("Error: too many arguments")
            sys.exit(1)

    if not date_filter:
        print("Error: date_filter is required (YYYY-MM-DD)")
        sys.exit(1)

    logger.info(f"🎯 Pipeline target: {pipeline_target}")
    logger.info(f"📂 Bronze path: {bronze_path}")
    logger.info(f"📅 Processing date: {date_filter}")
    if hour_filter:
        logger.info(f"⏰ Processing hour: {hour_filter}")

    spark = create_spark_session(pipeline_target)
    writer = _build_silver_writer(spark, pipeline_target)
    pipeline = VelibBronzeToSilver(spark, writer)
    result = pipeline.run(bronze_path, date_filter, hour_filter)

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
