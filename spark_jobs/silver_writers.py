"""Silver-layer writers.

Isolates the write-side I/O so that the transformation pipeline stays
destination-agnostic. Two implementations are provided:

- PostgresSilverWriter: local pipeline, JDBC to the PostgreSQL warehouse.
- BigQuerySilverWriter: cloud pipeline, Spark BigQuery connector on Dataproc.

Both writers apply duplicate-safe logic for ``station_availability`` and
upsert-like behaviour for ``stations``, mirroring the local contract so
downstream dbt models see the same invariants regardless of target.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min

logger = logging.getLogger(__name__)


class SilverWriter(ABC):
    """Destination-agnostic interface consumed by VelibBronzeToSilver."""

    @abstractmethod
    def write_stations(self, df_stations: DataFrame) -> None:
        """Insert new stations, leave existing ones untouched."""

    @abstractmethod
    def write_availability(self, df_availability: DataFrame) -> None:
        """Append availability rows, skipping any (station_id, snapshot_timestamp) that already exists."""


class PostgresSilverWriter(SilverWriter):
    """JDBC writer for the local PostgreSQL Silver layer."""

    def __init__(self, spark: SparkSession, postgres_config: dict):
        self.spark = spark
        self.postgres_config = postgres_config
        self._jdbc_url = (
            f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}"
            f"/{postgres_config['database']}"
        )
        self._jdbc_properties = {
            "user": postgres_config["user"],
            "password": postgres_config["password"],
            "driver": "org.postgresql.Driver",
        }

    def _write(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        logger.info(f"💾 Writing to PostgreSQL: {table_name} (mode={mode})")
        properties = {
            **self._jdbc_properties,
            "batchsize": "5000",
            "isolationLevel": "READ_COMMITTED",
        }
        df.write.jdbc(url=self._jdbc_url, table=table_name, mode=mode, properties=properties)
        row_count = df.count()
        logger.info(f"✅ {row_count:,} rows written to {table_name}")

    def write_stations(self, df_stations: DataFrame) -> None:
        logger.info("🔄 Upserting stations (Postgres)...")

        try:
            df_existing = self.spark.read.jdbc(
                self._jdbc_url, "silver.stations", properties=self._jdbc_properties
            )
            existing_count = df_existing.count()
            logger.info(f"  📊 {existing_count:,} existing stations in the database")

            df_new = df_stations.join(df_existing.select("station_id"), "station_id", "left_anti")
            new_count = df_new.count()

            if new_count > 0:
                logger.info(f"  ➕ {new_count} new stations to insert")
                self._write(df_new, "silver.stations", mode="append")
            else:
                logger.info("  ✅ No new station")
        except Exception:
            # Silver tables are bootstrapped separately. On first run the
            # table may be missing: fall back to a full insert so bootstrap
            # and first-ingestion can happen in any order.
            logger.warning("  ⚠️  Stations table empty or missing, performing full insert")
            self._write(df_stations, "silver.stations", mode="append")

    def write_availability(self, df_availability: DataFrame) -> None:
        logger.info("🛡️  Duplicate-safe write for silver.station_availability (Postgres)...")

        incoming_count = df_availability.count()
        df_dedup_incoming = df_availability.dropDuplicates(["station_id", "snapshot_timestamp"])
        dedup_incoming_count = df_dedup_incoming.count()
        dropped_incoming = incoming_count - dedup_incoming_count

        if dropped_incoming > 0:
            logger.warning(f"  ⚠️  {dropped_incoming:,} duplicate rows removed from incoming batch")

        if dedup_incoming_count == 0:
            logger.info("  ✅ No availability rows to insert after in-batch dedup")
            return

        ts_bounds = df_dedup_incoming.agg(
            spark_min("snapshot_timestamp").alias("min_ts"),
            spark_max("snapshot_timestamp").alias("max_ts"),
        ).collect()[0]

        min_ts = ts_bounds["min_ts"]
        max_ts = ts_bounds["max_ts"]

        if min_ts is None or max_ts is None:
            logger.info("  ✅ No valid timestamps to insert")
            return

        min_ts_str = min_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Restrict the lookup to the incoming batch window to keep the
        # JDBC scan small even when the Silver table grows large.
        existing_keys_query = f"""
            (
                SELECT station_id, snapshot_timestamp
                FROM silver.station_availability
                WHERE snapshot_timestamp >= TIMESTAMP '{min_ts_str}'
                  AND snapshot_timestamp <= TIMESTAMP '{max_ts_str}'
            ) existing_keys
        """

        df_existing_keys = self.spark.read.jdbc(
            url=self._jdbc_url, table=existing_keys_query, properties=self._jdbc_properties
        )

        df_to_insert = df_dedup_incoming.join(
            df_existing_keys, on=["station_id", "snapshot_timestamp"], how="left_anti"
        )

        to_insert_count = df_to_insert.count()
        skipped_existing = dedup_incoming_count - to_insert_count

        if skipped_existing > 0:
            logger.warning(
                f"  ⚠️  {skipped_existing:,} rows skipped (already present in PostgreSQL)"
            )

        if to_insert_count == 0:
            logger.info("  ✅ Nothing new to insert into silver.station_availability")
            return

        self._write(df_to_insert, "silver.station_availability", mode="append")
        logger.info(f"✅ Duplicate-safe write completed: {to_insert_count:,} new rows inserted")


class BigQuerySilverWriter(SilverWriter):
    """Spark BigQuery connector writer for the cloud Silver layer.

    Indirect writes (the default) stage Parquet on `temp_bucket` before
    loading into BigQuery. The caller ensures the bucket lives in the
    same region as the dataset to avoid cross-region transfer fees.
    """

    AVAILABILITY_TABLE = "station_availability"
    STATIONS_TABLE = "stations"

    def __init__(self, spark: SparkSession, bq_config: dict):
        self.spark = spark
        self.project_id = bq_config["project_id"]
        self.dataset_id = bq_config["dataset_id"]
        self.temp_bucket = bq_config["temp_bucket"]

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def _table_exists(self, table_name: str) -> bool:
        # Use the BigQuery REST API directly instead of catching a Spark
        # exception: on Dataproc Serverless 2.x the BQ connector surfaces a
        # missing table as a NullPointerException ("schema is null"), not
        # as a "not found" error, so message-based detection is unreliable.
        # ADC picks up the batch's service account credentials.
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project_id)
        try:
            client.get_table(self._table_ref(table_name))
            return True
        except NotFound:
            return False

    def _read_existing(
        self, table_name: str, selected_columns: list | None = None, filter_expr: str | None = None
    ) -> DataFrame | None:
        """Read an existing Silver table. Returns None if it does not exist yet."""
        if not self._table_exists(table_name):
            logger.warning(
                f"  ⚠️  BigQuery table {table_name} not found, falling back to full insert"
            )
            return None

        reader = self.spark.read.format("bigquery").option("table", self._table_ref(table_name))
        if filter_expr:
            reader = reader.option("filter", filter_expr)
        df = reader.load()
        if selected_columns:
            df = df.select(*selected_columns)
        return df

    def _append(self, df: DataFrame, table_name: str, **extra_options) -> None:
        writer = (
            df.write.format("bigquery")
            .option("table", self._table_ref(table_name))
            .option("temporaryGcsBucket", self.temp_bucket)
            .option("writeMethod", "indirect")
            .mode("append")
        )
        for key, value in extra_options.items():
            writer = writer.option(key, value)
        writer.save()

    def write_stations(self, df_stations: DataFrame) -> None:
        logger.info("🔄 Upserting stations (BigQuery)...")

        df_existing = self._read_existing(self.STATIONS_TABLE, selected_columns=["station_id"])

        if df_existing is None:
            count = df_stations.count()
            logger.info(f"  ➕ First insert: {count:,} stations")
            self._append(
                df_stations,
                self.STATIONS_TABLE,
                clusteredFields="station_id",
            )
            return

        existing_count = df_existing.count()
        logger.info(f"  📊 {existing_count:,} existing stations in BigQuery")

        df_new = df_stations.join(df_existing, "station_id", "left_anti")
        new_count = df_new.count()

        if new_count == 0:
            logger.info("  ✅ No new station")
            return

        logger.info(f"  ➕ {new_count} new stations to insert")
        self._append(df_new, self.STATIONS_TABLE)

    def write_availability(self, df_availability: DataFrame) -> None:
        logger.info("🛡️  Duplicate-safe write for station_availability (BigQuery)...")

        incoming_count = df_availability.count()
        df_dedup_incoming = df_availability.dropDuplicates(["station_id", "snapshot_timestamp"])
        dedup_incoming_count = df_dedup_incoming.count()
        dropped_incoming = incoming_count - dedup_incoming_count

        if dropped_incoming > 0:
            logger.warning(f"  ⚠️  {dropped_incoming:,} duplicate rows removed from incoming batch")

        if dedup_incoming_count == 0:
            logger.info("  ✅ No availability rows to insert after in-batch dedup")
            return

        ts_bounds = df_dedup_incoming.agg(
            spark_min("snapshot_timestamp").alias("min_ts"),
            spark_max("snapshot_timestamp").alias("max_ts"),
        ).collect()[0]

        min_ts = ts_bounds["min_ts"]
        max_ts = ts_bounds["max_ts"]

        if min_ts is None or max_ts is None:
            logger.info("  ✅ No valid timestamps to insert")
            return

        min_ts_str = min_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Push the time-range filter down to BigQuery so the anti-join
        # only pulls back the keys in the current batch window. The
        # connector translates `filter` into a WHERE clause and prunes
        # partitions when the column is the partition key.
        filter_expr = (
            f"snapshot_timestamp >= TIMESTAMP('{min_ts_str}') "
            f"AND snapshot_timestamp <= TIMESTAMP('{max_ts_str}')"
        )

        df_existing_keys = self._read_existing(
            self.AVAILABILITY_TABLE,
            selected_columns=["station_id", "snapshot_timestamp"],
            filter_expr=filter_expr,
        )

        if df_existing_keys is None:
            # First write: create the table partitioned + clustered.
            logger.info(f"  ➕ First insert: {dedup_incoming_count:,} availability rows")
            self._append(
                df_dedup_incoming,
                self.AVAILABILITY_TABLE,
                partitionField="snapshot_timestamp",
                partitionType="DAY",
                clusteredFields="station_id",
            )
            return

        df_to_insert = df_dedup_incoming.join(
            df_existing_keys, on=["station_id", "snapshot_timestamp"], how="left_anti"
        )

        to_insert_count = df_to_insert.count()
        skipped_existing = dedup_incoming_count - to_insert_count

        if skipped_existing > 0:
            logger.warning(f"  ⚠️  {skipped_existing:,} rows skipped (already present in BigQuery)")

        if to_insert_count == 0:
            logger.info("  ✅ Nothing new to insert into station_availability")
            return

        self._append(df_to_insert, self.AVAILABILITY_TABLE)
        logger.info(f"✅ Duplicate-safe write completed: {to_insert_count:,} new rows inserted")
