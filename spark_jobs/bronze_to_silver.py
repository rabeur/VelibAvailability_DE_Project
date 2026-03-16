"""
============================================================================
BRONZE TO SILVER TRANSFORMATION - Vélib Data Engineering Project
============================================================================
Description: Transformation des données brutes (Bronze) en données
             nettoyées et normalisées (Silver) avec PySpark
Author: Data Team
Date: 2026-02-26
============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, round as spark_round,
    to_timestamp, coalesce, trim,
    from_utc_timestamp, hour,
    expr, upper
)
from pyspark.sql.types import *
from datetime import datetime
import sys
import os
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VelibBronzeToSilver:
    """
    Pipeline de transformation Bronze → Silver pour les données Vélib
    """

    def __init__(self, postgres_config: dict):
        """
        Initialise le pipeline

        Args:
            postgres_config: Configuration PostgreSQL (host, port, db, user, password)
        """
        self.postgres_config = postgres_config
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Créer une session Spark configurée"""
        logger.info("🚀 Création de la session Spark...")

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
        Lire les données Bronze (Parquet) avec filtre optionnel sur la date

        Args:
            bronze_path: Chemin vers les données Bronze
            date_filter: Filtre de date au format YYYY-MM-DD (optionnel)
            hour_filter: Filtre d'heure au format HH (optionnel)

        Returns:
            DataFrame Spark
        """
        logger.info(f"📂 Lecture des données Bronze: {bronze_path}")

        if date_filter:
            if hour_filter:
                full_path = f"{bronze_path}/ingestion_date={date_filter}/hour={hour_filter}/*.parquet"
                logger.info(f"   Filtre date: {date_filter}, heure: {hour_filter}")
            else:
                full_path = f"{bronze_path}/ingestion_date={date_filter}/**/*.parquet"
                logger.info(f"   Filtre date: {date_filter}")
        else:
            full_path = f"{bronze_path}/**/*.parquet"

        try:
            df = self.spark.read.parquet(full_path)
            count = df.count()
            logger.info(f"✅ {count:,} lignes chargées")
            logger.info(f"📋 Colonnes: {df.columns}")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur lors de la lecture: {e}")
            raise

    def transform_to_silver(self, df_bronze):
        """
        Transformations Bronze → Silver:
        1. Nettoyage des données
        2. Normalisation des colonnes
        3. Calculs de métriques
        4. Gestion des types de données
        """
        logger.info("🔄 Transformation Bronze → Silver...")

        # ========================================
        # ÉTAPE 1: Nettoyage et sélection des colonnes
        # ========================================
        logger.info("  → Étape 1: Nettoyage des colonnes")

        df_clean = df_bronze.select(
            # Identifiants
            trim(col("stationcode")).alias("station_id"),
            trim(col("name")).alias("station_name"),

            # Capacité
            coalesce(col("capacity").cast("integer"), lit(0)).alias("capacity"),

            # Disponibilité vélos
            coalesce(col("numbikesavailable").cast("integer"), lit(0)).alias("num_bikes_available"),
            coalesce(col("mechanical").cast("integer"), lit(0)).alias("num_bikes_available_mechanical"),
            coalesce(col("ebike").cast("integer"), lit(0)).alias("num_bikes_available_ebike"),

            # Disponibilité places
            coalesce(col("numdocksavailable").cast("integer"), lit(0)).alias("num_docks_available"),

            # Status (booléens sous forme de texte: OUI/NON)
            upper(trim(coalesce(col("is_installed"), lit("OUI")))).alias("is_installed_raw"),
            upper(trim(coalesce(col("is_renting"), lit("OUI")))).alias("is_renting_raw"),
            upper(trim(coalesce(col("is_returning"), lit("OUI")))).alias("is_returning_raw"),

            # Localisation (coordonnées aplaties)
            col("lon").cast("double").alias("longitude"),
            col("lat").cast("double").alias("latitude"),

            # Informations géographiques
            trim(col("nom_arrondissement_communes")).alias("district_municipality_names"),
            trim(col("code_insee_commune")).alias("insee_municipality_code"),

            # Métadonnées temporelles
            from_utc_timestamp(to_timestamp(col("ingestion_timestamp")), "Europe/Paris").alias("ingestion_timestamp"),
            col("snapshot_id")
        )

        # ========================================
        # ÉTAPE 2: Conversion des booléens
        # ========================================
        logger.info("  → Étape 2: Conversion des booléens")

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
        # ÉTAPE 3: Calcul des métriques
        # ========================================
        logger.info("  → Étape 3: Calcul des métriques")

        df_silver = df_clean \
            .withColumn("snapshot_timestamp", col("ingestion_timestamp")) \
            .withColumn("snapshot_hour", hour(col("snapshot_timestamp"))) \
            .withColumn(
                "occupancy_rate",
                when(col("capacity") > 0,
                     spark_round((col("num_bikes_available") / col("capacity")) * 100, 2))
                .otherwise(lit(0.0))
            ) \
            .withColumn(
                "availability_rate",
                when(col("capacity") > 0,
                     spark_round((col("num_docks_available") / col("capacity")) * 100, 2))
                .otherwise(lit(0.0))
            ) \
            .withColumn(
                "service_rate",
                when(col("capacity") > 0,
                     spark_round(((col("num_docks_available") + col("num_bikes_available")) / col("capacity")) * 100, 2))
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
        # ÉTAPE 4: Filtrage des lignes invalides
        # ========================================
        logger.info("  → Étape 4: Filtrage des données invalides")

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
            logger.warning(f"⚠️  {filtered_count:,} lignes filtrées (invalides)")

        logger.info(f"✅ {final_count:,} lignes valides après transformation")

        return df_silver

    def extract_stations_dimension(self, df_silver):
        """
        Extraire la dimension stations (unique par station_id)
        Gère l'upsert en comparant avec les données existantes
        """
        logger.info("🏗️  Extraction de la dimension stations...")

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

        # Pour first_seen_at, on prend le plus ancien timestamp
        df_first_seen = df_silver \
            .groupBy("station_id") \
            .agg({"snapshot_timestamp": "min"}) \
            .withColumnRenamed("min(snapshot_timestamp)", "first_seen_at")

        # Joindre pour avoir first_seen_at et last_seen_at
        df_stations = df_stations.join(df_first_seen, "station_id", "left")

        # Ajouter les métadonnées
        df_stations = df_stations \
            .withColumn("is_active", lit(True)) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())

        station_count = df_stations.count()
        logger.info(f"✅ {station_count:,} stations uniques extraites")

        return df_stations

    def extract_availability_facts(self, df_silver):
        """
        Extraire les faits de disponibilité
        """
        logger.info("📊 Extraction des faits de disponibilité...")

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
        logger.info(f"✅ {facts_count:,} faits de disponibilité extraits")

        return df_availability

    def write_to_postgres(self, df, table_name: str, mode: str = "append"):
        """
        Écrire dans PostgreSQL avec gestion des erreurs

        Args:
            df: DataFrame Spark
            table_name: Nom de la table (schema.table)
            mode: Mode d'écriture ("append", "overwrite")
        """
        logger.info(f"💾 Écriture dans PostgreSQL: {table_name} (mode={mode})")

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
            logger.info(f"✅ {row_count:,} lignes écrites dans {table_name}")

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'écriture dans {table_name}: {e}")
            raise

    def upsert_stations(self, df_new_stations):
        """
        Upsert des stations: met à jour last_seen_at pour les existantes,
        insère les nouvelles

        Note: Nécessite une logique plus sophistiquée pour un vrai SCD Type 2
        """
        logger.info("🔄 Upsert des stations...")

        # Lire les stations existantes
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
            logger.info(f"  📊 {existing_count:,} stations existantes dans la base")

            # Identifier les nouvelles stations
            df_new = df_new_stations.join(
                df_existing.select("station_id"),
                "station_id",
                "left_anti"  # Garde seulement celles qui n'existent pas
            )

            new_count = df_new.count()

            if new_count > 0:
                logger.info(f"  ➕ {new_count} nouvelles stations à insérer")
                self.write_to_postgres(df_new, "silver.stations", mode="append")
            else:
                logger.info("  ✅ Aucune nouvelle station")

            # Pour les stations existantes, on pourrait mettre à jour last_seen_at
            # via une requête SQL UPDATE (pas montré ici pour simplifier)

        except Exception as e:
            # Si la table n'existe pas encore, on insère tout
            logger.warning(f"  ⚠️  Table stations vide ou inexistante, insertion complète")
            self.write_to_postgres(df_new_stations, "silver.stations", mode="append")

    def run(self, bronze_path: str, date_filter: str = None, hour_filter: str = None):
        """
        Exécuter le pipeline complet Bronze → Silver

        Args:
            bronze_path: Chemin vers les données Bronze
            date_filter: Date au format YYYY-MM-DD (traite uniquement ce jour)
            hour_filter: Heure au format HH (traite uniquement cette heure)
        """
        logger.info("="*60)
        logger.info("🚀 DÉMARRAGE DU PIPELINE BRONZE → SILVER")
        logger.info("="*60)

        start_time = datetime.now()

        try:
            # 1. Lire Bronze
            df_bronze = self.read_bronze_data(bronze_path, date_filter, hour_filter)

            # 2. Transformer
            df_silver = self.transform_to_silver(df_bronze)

            # 3. Extraire dimensions et faits
            df_stations = self.extract_stations_dimension(df_silver)
            df_availability = self.extract_availability_facts(df_silver)

            # 4. Écrire dans PostgreSQL

            # Stations (upsert pour éviter les doublons)
            self.upsert_stations(df_stations)

            # Availability (append toujours, avec contrainte d'unicité en base)
            self.write_to_postgres(
                df_availability,
                "silver.station_availability",
                mode="append"
            )

            duration = (datetime.now() - start_time).total_seconds()

            logger.info("="*60)
            logger.info(f"✅ PIPELINE TERMINÉ AVEC SUCCÈS en {duration:.2f}s")
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
            logger.error(f"❌ PIPELINE ÉCHOUÉ après {duration:.2f}s")
            logger.error(f"Erreur: {e}")
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
    """Point d'entrée du script"""

    # Configuration PostgreSQL (à ajuster)
    postgres_config = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "velib_dw"),
        "user": os.getenv("POSTGRES_USER", "velib"),
        "password": os.getenv("POSTGRES_PASSWORD", "velib")
    }

    # Chemins par défaut
    default_bronze_path = "/opt/data_lake/bronze/velib"

    # Parsing des arguments flexibles
    # Modes d'appel acceptés :
    # 1) spark-submit bronze_to_silver.py 2026-02-26
    # 2) spark-submit bronze_to_silver.py 2026-02-26 14
    # 3) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26
    # 4) spark-submit bronze_to_silver.py /path/to/bronze 2026-02-26 14
    args = sys.argv[1:]

    if len(args) == 0:
        print("Usage: spark-submit bronze_to_silver.py [bronze_path] <date_filter> [hour_filter]")
        print(f"  bronze_path: Chemin vers les données Bronze (défaut: {default_bronze_path})")
        print("  date_filter: Date YYYY-MM-DD (obligatoire)")
        print("  hour_filter: Heure HH (optionnel)")
        print("Exemple: spark-submit bronze_to_silver.py 2026-02-26 14")
        sys.exit(1)

    date_candidate = args[0]
    bronze_path = default_bronze_path
    date_filter = None
    hour_filter = None

    # Détecter si le premier argument est un chemin ou une date
    # Date attendue au format YYYY-MM-DD
    if len(date_candidate) == 10 and date_candidate[4] == '-' and date_candidate[7] == '-':
        date_filter = date_candidate
        if len(args) >= 2:
            hour_filter = args[1]
        if len(args) >= 3:
            print("Erreur: trop d'arguments pour le format date+heure")
            sys.exit(1)
    else:
        bronze_path = date_candidate
        if len(args) < 2:
            print("Erreur: date_filter obligatoire si bronze_path est fourni")
            sys.exit(1)
        date_filter = args[1]
        if len(args) >= 3:
            hour_filter = args[2]
        if len(args) >= 4:
            print("Erreur: trop d'arguments")
            sys.exit(1)

    if not date_filter:
        print("Erreur: date_filter est obligatoire (YYYY-MM-DD)")
        sys.exit(1)

    logger.info(f"📂 Bronze path: {bronze_path}")
    logger.info(f"📅 Traitement de la date: {date_filter}")
    if hour_filter:
        logger.info(f"⏰ Traitement de l'heure: {hour_filter}")


    # Créer et exécuter le pipeline
    pipeline = VelibBronzeToSilver(postgres_config)
    result = pipeline.run(bronze_path, date_filter, hour_filter)

    # Code de sortie
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()