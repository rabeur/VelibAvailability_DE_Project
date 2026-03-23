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
    col, lit, current_timestamp, when, round as spark_round,
    to_timestamp, coalesce, trim,
    from_utc_timestamp, hour, least, greatest,
    expr, upper, min as spark_min, max as spark_max
)
from pyspark.sql.types import *
from datetime import datetime
import sys
import os
import logging

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VelibBronzeToSilver:
    """
    Bronze -> Silver transformation pipeline for Velib data
    """

    def __init__(self, postgres_config: dict):
        """
        Initialize the pipeline

        Args:
            postgres_config: PostgreSQL configuration (host, port, db, user, password)
        """
        self.postgres_config = postgres_config
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create a configured Spark session"""
        logger.info("🚀 Creating Spark session...")

        spark = SparkSession.builder \
            .appName("VelibBronzeToSilver") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.sql.session.timeZone", "Europe/Paris") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"✅ Spark version: {spark.version}")
        return spark

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
                full_path = f"{bronze_path}/ingestion_date={date_filter}/hour={hour_filter}/*.parquet"
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
            coalesce(col("mechanical").cast("integer"), lit(0)).alias("num_bikes_available_mechanical"),
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
            from_utc_timestamp(to_timestamp(col("ingestion_timestamp")), "Europe/Paris").alias("ingestion_timestamp"),
            col("snapshot_id")
        )

        # ========================================
        # STEP 2: Convert booleans
        # ========================================
        logger.info("  → Step 2: Converting booleans")

        df_clean = df_clean \
            .withColumn("is_installed",
                       when(col("is_installed_raw").isin("OUI", "YES", "TRUE", "1"), lit(True))
                       .otherwise(lit(False))) \
            .withColumn("is_renting",
                       when(col("is_renting_raw").isin("OUI", "YES", "TRUE", "1"), lit(True))
                       .otherwise(lit(False))) \
            .withColumn("is_returning",
                       when(col("is_returning_raw").isin("OUI", "YES", "TRUE", "1"), lit(True))
                       .otherwise(lit(False))) \
            .drop("is_installed_raw", "is_renting_raw", "is_returning_raw")

        # ========================================
        # STEP 3: Calculate metrics
        # ========================================
        logger.info("  → Step 3: Calculating metrics")

        df_silver = df_clean \
            .withColumn("snapshot_timestamp", col("ingestion_timestamp")) \
            .withColumn("snapshot_hour", hour(col("snapshot_timestamp"))) \
            .withColumn(
                "occupancy_rate",
                when(col("capacity") > 0,
                     least(
                         lit(100.0),
                         greatest(
                             lit(0.0),
                             spark_round((col("num_bikes_available") / col("capacity")) * 100, 2)
                         )
                     ))
                .otherwise(lit(0.0))
            ) \
            .withColumn(
                "availability_rate",
                when(col("capacity") > 0,
                     least(
                         lit(100.0),
                         greatest(
                             lit(0.0),
                             spark_round((col("num_docks_available") / col("capacity")) * 100, 2)
                         )
                     ))
                .otherwise(lit(0.0))
            ) \
            .withColumn(
                "service_rate",
                when(col("capacity") > 0,
                     least(
                         lit(100.0),
                         greatest(
                             lit(0.0),
                             spark_round(((col("num_docks_available") + col("num_bikes_available")) / col("capacity")) * 100, 2)
                         )
                     ))
                .otherwise(lit(0.0))
            ) \
            .withColumn("is_empty", col("num_bikes_available") == 0) \
            .withColumn("is_full", col("num_docks_available") == 0) \
            .withColumn(
                "is_operational",
                col("is_installed") & col("is_renting") & col("is_returning")
            ) \
            .withColumn("processing_timestamp", from_utc_timestamp(current_timestamp(), "Europe/Paris"))

        # ========================================
        # STEP 4: Filter invalid rows
        # ========================================
        logger.info("  → Step 4: Filtering invalid data")

        initial_count = df_silver.count()

        df_silver = df_silver.filter(
            (col("station_id").isNotNull()) &
            (col("station_id") != "") &
            (col("capacity") > 0) &
            (col("num_bikes_available") >= 0) &
            (col("num_docks_available") >= 0) &
            (col("snapshot_timestamp").isNotNull())
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

        df_stations = df_silver \
            .withColumn("rn", expr("row_number() OVER (PARTITION BY station_id ORDER BY snapshot_timestamp DESC)")) \
            .filter(col("rn") == 1) \
            .select(
                col("station_id"),
                col("station_name"),
                col("capacity"),
                col("latitude"),
                col("longitude"),
                col("district_municipality_names"),
                col("insee_municipality_code"),
                col("snapshot_timestamp").alias("last_seen_at")
            )

        # For first_seen_at, keep the oldest timestamp
        df_first_seen = df_silver \
            .groupBy("station_id") \
            .agg({"snapshot_timestamp": "min"}) \
            .withColumnRenamed("min(snapshot_timestamp)", "first_seen_at")

        # Join to get first_seen_at and last_seen_at
        df_stations = df_stations.join(df_first_seen, "station_id", "left")

        # Add metadata
        df_stations = df_stations \
            .withColumn("is_active", lit(True)) \
            .withColumn("created_at", from_utc_timestamp(current_timestamp(), "Europe/Paris")) \
            .withColumn("updated_at", from_utc_timestamp(current_timestamp(), "Europe/Paris"))

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
            col("processing_timestamp")
        )

        facts_count = df_availability.count()
        logger.info(f"✅ {facts_count:,} availability facts extracted")

        return df_availability

    def write_to_postgres(self, df, table_name: str, mode: str = "append"):
        """
        Write to PostgreSQL with error handling

        Args:
            df: DataFrame Spark
            table_name: Table name (schema.table)
            mode: Write mode ("append", "overwrite")
        """
        logger.info(f"💾 Writing to PostgreSQL: {table_name} (mode={mode})")

        jdbc_url = f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"

        jdbc_properties = {
            "user": self.postgres_config['user'],
            "password": self.postgres_config['password'],
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
            "isolationLevel": "READ_COMMITTED"
        }

        try:
            df.write \
                .jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode=mode,
                    properties=jdbc_properties
                )

            row_count = df.count()
            logger.info(f"✅ {row_count:,} rows written to {table_name}")

        except Exception as e:
            logger.error(f"❌ Write error for {table_name}: {e}")
            raise

    def write_availability_with_dedup(self, df_availability):
        """
        Write availability facts while ignoring rows that already exist in
        silver.station_availability (same station_id + snapshot_timestamp).

        This prevents the full batch from failing when a subset of rows
        has already been ingested.
        """
        logger.info("🛡️  Duplicate-safe write for silver.station_availability...")

        # 1) Remove duplicates inside the incoming Spark batch itself
        incoming_count = df_availability.count()
        df_dedup_incoming = df_availability.dropDuplicates(["station_id", "snapshot_timestamp"])
        dedup_incoming_count = df_dedup_incoming.count()
        dropped_incoming = incoming_count - dedup_incoming_count

        if dropped_incoming > 0:
            logger.warning(f"  ⚠️  {dropped_incoming:,} duplicate rows removed from incoming batch")

        if dedup_incoming_count == 0:
            logger.info("  ✅ No availability rows to insert after in-batch dedup")
            return

        # 2) Restrict existing-key lookup to the current batch time range
        ts_bounds = df_dedup_incoming.agg(
            spark_min("snapshot_timestamp").alias("min_ts"),
            spark_max("snapshot_timestamp").alias("max_ts")
        ).collect()[0]

        min_ts = ts_bounds["min_ts"]
        max_ts = ts_bounds["max_ts"]

        if min_ts is None or max_ts is None:
            logger.info("  ✅ No valid timestamps to insert")
            return

        min_ts_str = min_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

        jdbc_url = f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"
        jdbc_properties = {
            "user": self.postgres_config['user'],
            "password": self.postgres_config['password'],
            "driver": "org.postgresql.Driver"
        }

        existing_keys_query = f"""
            (
                SELECT station_id, snapshot_timestamp
                FROM silver.station_availability
                WHERE snapshot_timestamp >= TIMESTAMP '{min_ts_str}'
                  AND snapshot_timestamp <= TIMESTAMP '{max_ts_str}'
            ) existing_keys
        """

        df_existing_keys = self.spark.read.jdbc(
            url=jdbc_url,
            table=existing_keys_query,
            properties=jdbc_properties
        )

        # 3) Keep only rows not already in PostgreSQL
        df_to_insert = df_dedup_incoming.join(
            df_existing_keys,
            on=["station_id", "snapshot_timestamp"],
            how="left_anti"
        )

        to_insert_count = df_to_insert.count()
        skipped_existing = dedup_incoming_count - to_insert_count

        if skipped_existing > 0:
            logger.warning(f"  ⚠️  {skipped_existing:,} rows skipped (already present in PostgreSQL)")

        if to_insert_count == 0:
            logger.info("  ✅ Nothing new to insert into silver.station_availability")
            return

        self.write_to_postgres(df_to_insert, "silver.station_availability", mode="append")
        logger.info(f"✅ Duplicate-safe write completed: {to_insert_count:,} new rows inserted")

    def upsert_stations(self, df_new_stations):
        """
        Upsert stations: update last_seen_at for existing stations,
        insert new ones

        Note: A true SCD Type 2 would require more sophisticated logic
        """
        logger.info("🔄 Upserting stations...")

        # Read existing stations
        jdbc_url = f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"

        jdbc_properties = {
            "user": self.postgres_config['user'],
            "password": self.postgres_config['password'],
            "driver": "org.postgresql.Driver"
        }

        try:
            df_existing = self.spark.read \
                .jdbc(jdbc_url, "silver.stations", properties=jdbc_properties)

            existing_count = df_existing.count()
            logger.info(f"  📊 {existing_count:,} existing stations in the database")

            # Identify new stations
            df_new = df_new_stations.join(
                df_existing.select("station_id"),
                "station_id",
                "left_anti"  # Keep only stations that do not already exist
            )

            new_count = df_new.count()

            if new_count > 0:
                logger.info(f"  ➕ {new_count} new stations to insert")
                self.write_to_postgres(df_new, "silver.stations", mode="append")
            else:
                logger.info("  ✅ No new station")

            # For existing stations, last_seen_at could be updated
            # with an SQL UPDATE query (omitted here for simplicity)

        except Exception as e:
            # If the table does not exist yet, insert everything
            logger.warning("  ⚠️  Stations table empty or missing, performing full insert")
            self.write_to_postgres(df_new_stations, "silver.stations", mode="append")

    def run(self, bronze_path: str, date_filter: str = None, hour_filter: str = None):
        """
        Run the full Bronze -> Silver pipeline

        Args:
            bronze_path: Path to Bronze data
            date_filter: Date in YYYY-MM-DD format (process only that day)
            hour_filter: Hour in HH format (process only that hour)
        """
        logger.info("="*60)
        logger.info("🚀 STARTING BRONZE -> SILVER PIPELINE")
        logger.info("="*60)

        start_time = datetime.now()

        try:
            # 1. Read Bronze
            df_bronze = self.read_bronze_data(bronze_path, date_filter, hour_filter)

            # 2. Transform
            df_silver = self.transform_to_silver(df_bronze)

            # 3. Extract dimensions and facts
            df_stations = self.extract_stations_dimension(df_silver)
            df_availability = self.extract_availability_facts(df_silver)

            # 4. Write to PostgreSQL

            # Stations (upsert to avoid duplicates)
            self.upsert_stations(df_stations)

            # Availability (append while skipping already-existing keys)
            self.write_availability_with_dedup(df_availability)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info("="*60)
            logger.info(f"✅ PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f}s")
            logger.info("="*60)

            return {
                "success": True,
                "duration_seconds": duration,
                "stations_processed": df_stations.count(),
                "facts_processed": df_availability.count()
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error("="*60)
            logger.error(f"❌ PIPELINE FAILED after {duration:.2f}s")
            logger.error(f"Error: {e}")
            logger.error("="*60)

            import traceback
            traceback.print_exc()

            return {
                "success": False,
                "duration_seconds": duration,
                "error": str(e)
            }

        finally:
            self.spark.stop()


def main():
    """Script entry point"""

    # PostgreSQL configuration (adjust as needed)
    postgres_config = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "velib_dw"),
        "user": os.getenv("POSTGRES_USER", "velib"),
        "password": os.getenv("POSTGRES_PASSWORD", "velib")
    }

    # Default paths
    default_bronze_path = "/opt/data_lake/bronze/velib"

    # Flexible argument parsing
    # Supported invocation modes:
    # 1) spark-submit bronze_to_silver.py 2026-02-26
    # 2) spark-submit bronze_to_silver.py 2026-02-26 14
    # 3) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26
    # 4) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26 14
    args = sys.argv[1:]

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
    if len(date_candidate) == 10 and date_candidate[4] == '-' and date_candidate[7] == '-':
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

    logger.info(f"📂 Bronze path: {bronze_path}")
    logger.info(f"📅 Processing date: {date_filter}")
    if hour_filter:
        logger.info(f"⏰ Processing hour: {hour_filter}")


    # Create and run the pipeline
    pipeline = VelibBronzeToSilver(postgres_config)
    result = pipeline.run(bronze_path, date_filter, hour_filter)

    # Exit code
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()