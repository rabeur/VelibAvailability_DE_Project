"""
============================================================================
DAG: Vélib Silver Transformation - Daily Pipeline
============================================================================
Description: Orchestre la transformation quotidienne des données Bronze
             vers Silver avec PySpark
Schedule: Tous les jours à 2h du matin
Author: Data Team
============================================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os


# ============================================================================
# Configuration du DAG
# ============================================================================

default_args = {
    'owner': 'velib_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'velib_silver_daily_transformation',
    default_args=default_args,
    description='Transformation quotidienne Bronze → Silver avec PySpark',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['velib', 'silver', 'spark', 'daily'],
    max_active_runs=1,  # Un seul run à la fois
)


# ============================================================================
# Fonctions Python
# ============================================================================

def get_yesterday_date(**context):
    """Récupère la date d'hier au format YYYY-MM-DD"""
    execution_date = context['execution_date']
    yesterday = execution_date - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')

    print(f"📅 Date cible: {date_str}")

    # Push vers XCom pour les tâches suivantes
    context['ti'].xcom_push(key='target_date', value=date_str)

    return date_str


def check_bronze_data_exists(**context):
    """Vérifie que les données Bronze existent pour la date cible"""
    import glob

    ti = context['ti']
    target_date = ti.xcom_pull(task_ids='get_target_date', key='target_date')

    bronze_path = f"/opt/airflow/data_lake/bronze/velib/ingestion_date={target_date}"

    print(f"🔍 Vérification de l'existence de: {bronze_path}")

    # Chercher des fichiers Parquet
    parquet_files = glob.glob(f"{bronze_path}/**/*.parquet", recursive=True)

    if not parquet_files:
        raise FileNotFoundError(
            f"❌ Aucune donnée Bronze trouvée pour {target_date}\n"
            f"Chemin vérifié: {bronze_path}"
        )

    file_count = len(parquet_files)
    print(f"✅ {file_count} fichiers Parquet trouvés pour {target_date}")

    # Push le nombre de fichiers
    ti.xcom_push(key='file_count', value=file_count)

    return file_count


def validate_silver_data(**context):
    """Valide que les données ont bien été chargées dans Silver"""
    import psycopg2

    ti = context['ti']
    target_date = ti.xcom_pull(task_ids='get_target_date', key='target_date')

    # Connexion PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="velib_dw",
        user="velib",
        password="velib"
    )

    try:
        cursor = conn.cursor()

        # Compter les enregistrements pour la date cible
        query = """
        SELECT
            COUNT(*) as row_count,
            COUNT(DISTINCT station_id) as station_count,
            MIN(snapshot_timestamp) as min_timestamp,
            MAX(snapshot_timestamp) as max_timestamp
        FROM silver.station_availability
        WHERE snapshot_timestamp::DATE = %s
        """

        cursor.execute(query, (target_date,))
        result = cursor.fetchone()

        row_count, station_count, min_ts, max_ts = result

        print(f"📊 Validation pour {target_date}:")
        print(f"   - Lignes insérées: {row_count:,}")
        print(f"   - Stations uniques: {station_count}")
        print(f"   - Période: {min_ts} → {max_ts}")

        if row_count == 0:
            raise ValueError(f"❌ Aucune donnée insérée pour {target_date}")

        if station_count < 10:  # Seuil minimal de stations attendues
            raise ValueError(
                f"⚠️  Nombre de stations trop faible: {station_count} "
                f"(attendu: au moins 10)"
            )

        # Push les métriques
        ti.xcom_push(key='rows_inserted', value=row_count)
        ti.xcom_push(key='stations_count', value=station_count)

        print("✅ Validation réussie")

        return {
            'rows_inserted': row_count,
            'stations_count': station_count,
            'date': target_date
        }

    finally:
        conn.close()


# ============================================================================
# Définition des tâches
# ============================================================================

# Tâche 1: Récupérer la date cible (hier)
task_get_date = PythonOperator(
    task_id='get_target_date',
    python_callable=get_yesterday_date,
    dag=dag,
)

# Tâche 2: Vérifier l'existence des données Bronze
task_check_bronze = PythonOperator(
    task_id='check_bronze_data',
    python_callable=check_bronze_data_exists,
    dag=dag,
)

# Tâche 3: Lancer le job Spark de transformation
task_spark_transform = BashOperator(
    task_id='spark_bronze_to_silver',
    bash_command="""
    docker exec velib_spark /opt/spark/bin/spark-submit \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=10 \
        /opt/spark_jobs/bronze_to_silver.py \
        /opt/data_lake/bronze/velib \
        {{ ti.xcom_pull(task_ids='get_target_date', key='target_date') }}
    """,
    dag=dag,
)

# Tâche 4: Valider les données Silver
task_validate = PythonOperator(
    task_id='validate_silver_data',
    python_callable=validate_silver_data,
    dag=dag,
)

# Tâche 5: Mettre à jour les statistiques PostgreSQL
task_update_stats = PostgresOperator(
    task_id='update_postgres_stats',
    postgres_conn_id='postgres_default',  # À configurer dans Airflow
    sql="""
    -- Analyser les tables pour l'optimiseur
    ANALYZE silver.stations;
    ANALYZE silver.station_availability;

    -- Afficher un résumé
    SELECT
        'silver.stations' as table_name,
        COUNT(*) as total_rows,
        pg_size_pretty(pg_total_relation_size('silver.stations')) as size
    FROM silver.stations
    UNION ALL
    SELECT
        'silver.station_availability' as table_name,
        COUNT(*) as total_rows,
        pg_size_pretty(pg_total_relation_size('silver.station_availability')) as size
    FROM silver.station_availability;
    """,
    dag=dag,
)

# Tâche 6: Notification de succès
task_notify_success = BashOperator(
    task_id='notify_success',
    bash_command="""
    echo "✅ Pipeline Silver réussi pour {{ ti.xcom_pull(task_ids='get_target_date', key='target_date') }}"
    echo "📊 Lignes insérées: {{ ti.xcom_pull(task_ids='validate_silver_data', key='rows_inserted') }}"
    echo "🏛️  Stations: {{ ti.xcom_pull(task_ids='validate_silver_data', key='stations_count') }}"
    """,
    dag=dag,
)

# ============================================================================
# Définition du workflow
# ============================================================================

task_get_date >> task_check_bronze >> task_spark_transform >> task_validate >> task_update_stats >> task_notify_success