#!/usr/bin/env python3
"""
Script de test manuel pour la transformation Bronze → Silver
Permet de tester le pipeline sans passer par Airflow
"""

import sys
import os
from datetime import datetime, timedelta
import subprocess
import argparse


def run_command(command: str, description: str) -> bool:
    """
    Exécute une commande shell et affiche le résultat
    
    Returns:
        True si succès, False sinon
    """
    print(f"\n{'='*70}")
    print(f"🔧 {description}")
    print(f"{'='*70}")
    print(f"Commande: {command}\n")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            print(result.stdout)
        
        print(f"✅ {description} - Succès")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - Échec")
        print(f"Code de retour: {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False


def check_prerequisites():
    """Vérifie que tous les prérequis sont OK"""
    print("\n📋 Vérification des prérequis...")
    
    checks = {
        "Docker": "docker --version",
        "Docker Compose": "docker-compose --version",
        "Conteneur Postgres": "docker ps | grep velib_postgres",
        "Conteneur Spark": "docker ps | grep velib_spark",
        "Conteneur Airflow": "docker ps | grep velib_airflow",
    }
    
    all_ok = True
    for name, command in checks.items():
        result = subprocess.run(command, shell=True, capture_output=True)
        if result.returncode == 0:
            print(f"  ✅ {name}")
        else:
            print(f"  ❌ {name} - Non disponible")
            all_ok = False
    
    return all_ok


def check_bronze_data(date: str):
    """Vérifie que les données Bronze existent pour la date"""
    print(f"\n🔍 Vérification des données Bronze pour {date}...")
    
    command = f"""
    docker exec velib_airflow_scheduler bash -c "
        if [ -d '/opt/airflow/data_lake/bronze/velib/ingestion_date={date}' ]; then
            echo 'FOUND'
        else
            echo 'NOT_FOUND'
        fi
    "
    """
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if "FOUND" in result.stdout:
        # Compter les fichiers
        count_cmd = f"""
        docker exec velib_airflow_scheduler bash -c "
            find /opt/airflow/data_lake/bronze/velib/ingestion_date={date} -name '*.parquet' | wc -l
        "
        """
        count_result = subprocess.run(count_cmd, shell=True, capture_output=True, text=True)
        file_count = int(count_result.stdout.strip())
        
        print(f"  ✅ {file_count} fichiers Parquet trouvés")
        return True
    else:
        print(f"  ❌ Aucune donnée Bronze pour {date}")
        print(f"     Chemin vérifié: /opt/airflow/data_lake/bronze/velib/ingestion_date={date}")
        return False


def run_spark_job(date: str):
    """Lance le job Spark de transformation"""
    command = f"""
    docker exec velib_spark /opt/spark/bin/spark-submit \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=10 \
        /opt/spark_jobs/bronze_to_silver.py \
        /opt/data_lake/bronze/velib \
        {date}
    """
    
    return run_command(command, f"Transformation Spark pour {date}")


def validate_silver_data(date: str):
    """Valide que les données ont été chargées dans Silver"""
    print(f"\n📊 Validation des données Silver pour {date}...")
    
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT station_id) as unique_stations,
        MIN(snapshot_timestamp) as earliest,
        MAX(snapshot_timestamp) as latest
    FROM silver.station_availability
    WHERE snapshot_timestamp::DATE = '{date}';
    """
    
    command = f"""docker exec -i velib_postgres psql -U velib -d velib_dw -c "{query}" """
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    print(result.stdout)
    
    if "0 rows" in result.stdout or "ERROR" in result.stderr:
        print("  ❌ Aucune donnée trouvée ou erreur")
        return False
    else:
        print("  ✅ Données validées")
        return True


def show_summary():
    """Affiche un résumé des données Silver"""
    print("\n📈 Résumé des données Silver...")
    
    queries = [
        ("Nombre total de stations", 
         "SELECT COUNT(*) as total FROM silver.stations;"),
        
        ("Nombre total de snapshots",
         "SELECT COUNT(*) as total FROM silver.station_availability;"),
        
        ("Derniers snapshots par date",
         """SELECT 
                snapshot_timestamp::DATE as date,
                COUNT(*) as snapshots,
                COUNT(DISTINCT station_id) as stations
            FROM silver.station_availability
            GROUP BY snapshot_timestamp::DATE
            ORDER BY date DESC
            LIMIT 5;"""),
    ]
    
    for title, query in queries:
        print(f"\n{title}:")
        command = f"""docker exec -i velib_postgres psql -U velib -d velib_dw -c "{query}" """
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result.stdout)


def main():
    parser = argparse.ArgumentParser(
        description="Test manuel de la transformation Bronze → Silver"
    )
    parser.add_argument(
        '--date',
        type=str,
        default=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        help='Date à traiter (format YYYY-MM-DD, défaut: hier)'
    )
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Ignorer la validation des prérequis'
    )
    parser.add_argument(
        '--summary-only',
        action='store_true',
        help='Afficher seulement le résumé (sans lancer le job)'
    )
    
    args = parser.parse_args()
    
    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║   TEST MANUEL - TRANSFORMATION BRONZE → SILVER                     ║
    ║   Vélib Data Engineering Project                                   ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    print(f"📅 Date cible: {args.date}")
    
    # Mode summary-only
    if args.summary_only:
        show_summary()
        return
    
    # Vérifier les prérequis
    if not args.skip_validation:
        if not check_prerequisites():
            print("\n❌ Prérequis manquants. Lancez d'abord 'docker-compose up -d'")
            sys.exit(1)
    
    # Vérifier les données Bronze
    if not check_bronze_data(args.date):
        print(f"\n❌ Données Bronze manquantes pour {args.date}")
        print("   Lancez d'abord le DAG d'ingestion Bronze")
        sys.exit(1)
    
    # Lancer le job Spark
    if not run_spark_job(args.date):
        print("\n❌ Job Spark échoué")
        sys.exit(1)
    
    # Valider les résultats
    if not validate_silver_data(args.date):
        print("\n❌ Validation échouée")
        sys.exit(1)
    
    # Afficher le résumé
    show_summary()
    
    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║   ✅ TEST TERMINÉ AVEC SUCCÈS                                      ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    print("\n📚 Prochaines étapes:")
    print("  1. Vérifier les données dans PostgreSQL")
    print("  2. Activer le DAG Airflow pour automatisation quotidienne")
    print("  3. Configurer le monitoring et les alertes")
    print()


if __name__ == "__main__":
    main()
