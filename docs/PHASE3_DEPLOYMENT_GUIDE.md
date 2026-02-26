# 🎯 Phase 3 : Silver Layer - Guide de Déploiement

## Vue d'ensemble

La Phase 3 transforme les données brutes (Bronze) en données nettoyées et structurées (Silver) en utilisant :
- **PySpark** pour les transformations distribuées
- **PostgreSQL** pour le stockage relationnel
- **Airflow** pour l'orchestration quotidienne

---

## 📋 Prérequis

### 1. Infrastructure
- ✅ Docker Compose opérationnel
- ✅ Conteneurs : postgres, spark, airflow-scheduler, airflow-webserver
- ✅ Données Bronze disponibles dans `/data_lake/bronze/velib`

### 2. Dépendances
- ✅ Driver JDBC PostgreSQL dans Spark
- ✅ PySpark 3.5.0+
- ✅ PostgreSQL 16+

---

## 🚀 Étapes d'installation

### Étape 1 : Initialiser le schéma PostgreSQL Silver

```bash
# Depuis la racine du projet
docker exec -i velib_postgres psql -U velib -d velib_dw < sql/02_init_silver_schema.sql
```

**Vérification :**
```bash
docker exec -it velib_postgres psql -U velib -d velib_dw -c "\dt silver.*"
```

**Sortie attendue :**
```
                List of relations
 Schema |         Name          | Type  | Owner 
--------+-----------------------+-------+-------
 silver | station_availability  | table | velib
 silver | stations              | table | velib
```

### Étape 2 : Installer le driver JDBC PostgreSQL dans Spark

```bash
# Télécharger le driver dans le conteneur Spark
docker exec velib_spark bash -c "
  mkdir -p /opt/spark/jars && 
  cd /opt/spark/jars && 
  wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
"

# Vérifier l'installation
docker exec velib_spark ls -lh /opt/spark/jars/postgresql-42.7.1.jar
```

### Étape 3 : Déployer le script PySpark

```bash
# Copier le script dans le dossier spark_jobs/
cp bronze_to_silver.py spark_jobs/

# Vérifier que le conteneur Spark peut accéder au script
docker exec velib_spark ls -lh /opt/spark_jobs/bronze_to_silver.py
```

### Étape 4 : Déployer le DAG Airflow

```bash
# Copier le DAG dans le dossier airflow/dags/
cp velib_silver_daily_transformation_dag.py airflow/dags/

# Attendre que Airflow détecte le nouveau DAG (30 secondes)
sleep 30

# Vérifier que le DAG est chargé
docker exec velib_airflow_scheduler airflow dags list | grep silver
```

**Sortie attendue :**
```
velib_silver_daily_transformation  | True  | velib, silver, spark, daily
```

### Étape 5 : Configurer la connexion PostgreSQL dans Airflow (optionnel)

```bash
# Créer la connexion pour le PostgresOperator
docker exec velib_airflow_scheduler airflow connections add \
  postgres_default \
  --conn-type postgres \
  --conn-host postgres \
  --conn-port 5432 \
  --conn-login velib \
  --conn-password velib \
  --conn-schema velib_dw
```

---

## 🧪 Tests manuels

### Test 1 : Exécuter le script Spark manuellement

```bash
# Test sur une date spécifique (ex: aujourd'hui)
DATE_TODAY=$(date +%Y-%m-%d)

docker exec velib_spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark_jobs/bronze_to_silver.py \
  /opt/data_lake/bronze/velib \
  $DATE_TODAY
```

**Logs attendus :**
```
============================================================
🚀 DÉMARRAGE DU PIPELINE BRONZE → SILVER
============================================================
📂 Lecture des données Bronze: /opt/data_lake/bronze/velib
   Filtre date: 2026-02-26
✅ 1,400 lignes chargées
🔄 Transformation Bronze → Silver...
  → Étape 1: Nettoyage des colonnes
  → Étape 2: Conversion des booléens
  → Étape 3: Calcul des métriques
  → Étape 4: Filtrage des données invalides
✅ 1,400 lignes valides après transformation
🏗️  Extraction de la dimension stations...
✅ 1,400 stations uniques extraites
📊 Extraction des faits de disponibilité...
✅ 1,400 faits de disponibilité extraits
💾 Écriture dans PostgreSQL: silver.stations (mode=append)
✅ 1,400 lignes écrites dans silver.stations
💾 Écriture dans PostgreSQL: silver.station_availability (mode=append)
✅ 1,400 lignes écrites dans silver.station_availability
============================================================
✅ PIPELINE TERMINÉ AVEC SUCCÈS en 45.32s
============================================================
```

### Test 2 : Vérifier les données dans PostgreSQL

```bash
# Compter les stations
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT COUNT(*) as total_stations FROM silver.stations;
"

# Compter les snapshots de disponibilité
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT 
  COUNT(*) as total_snapshots,
  COUNT(DISTINCT station_id) as unique_stations,
  MIN(snapshot_timestamp) as earliest,
  MAX(snapshot_timestamp) as latest
FROM silver.station_availability;
"

# Voir quelques exemples
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT * FROM silver.v_latest_station_availability LIMIT 5;
"
```

### Test 3 : Déclencher le DAG Airflow manuellement

```bash
# Déclencher une exécution manuelle
docker exec velib_airflow_scheduler airflow dags trigger velib_silver_daily_transformation

# Suivre les logs
docker-compose logs -f airflow-scheduler
```

**Accéder à l'interface Airflow :**
- URL : http://localhost:8081
- Identifiants : admin / admin
- Vérifier le statut des tâches dans la vue Graph

---

## 📊 Monitoring & Validation

### Vérifications quotidiennes à faire

1. **Statut du DAG Airflow**
   ```bash
   docker exec velib_airflow_scheduler airflow dags list-runs -d velib_silver_daily_transformation --limit 10
   ```

2. **Volume de données dans Silver**
   ```sql
   SELECT 
     snapshot_date,
     COUNT(*) as snapshots,
     COUNT(DISTINCT station_id) as stations
   FROM silver.station_availability
   GROUP BY snapshot_date
   ORDER BY snapshot_date DESC
   LIMIT 7;
   ```

3. **Qualité des données**
   ```sql
   SELECT 
     COUNT(*) as total,
     AVG(occupancy_rate) as avg_occupancy,
     SUM(CASE WHEN is_empty THEN 1 ELSE 0 END) as times_empty,
     SUM(CASE WHEN is_full THEN 1 ELSE 0 END) as times_full,
     SUM(CASE WHEN NOT is_operational THEN 1 ELSE 0 END) as not_operational
   FROM silver.station_availability
   WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day';
   ```

4. **Performance du pipeline**
   - Consulter les logs Spark dans Airflow
   - Vérifier les temps d'exécution
   - Surveiller les erreurs/retry

---

## 🔧 Dépannage

### Problème 1 : "Driver JDBC non trouvé"

**Erreur :**
```
java.sql.SQLException: No suitable driver found for jdbc:postgresql
```

**Solution :**
```bash
# Vérifier que le driver est présent
docker exec velib_spark ls -lh /opt/spark/jars/postgresql-42.7.1.jar

# Si absent, le télécharger
docker exec velib_spark bash -c "
  cd /opt/spark/jars && 
  wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
"
```

### Problème 2 : "Cannot subtract tz-naive and tz-aware datetime"

**Solution :** Déjà corrigé dans le script Python (utilisation de `datetime.now(timezone.utc)`)

### Problème 3 : "Duplicate key value violates unique constraint"

**Erreur :**
```
ERROR: duplicate key value violates unique constraint "uk_station_snapshot"
```

**Cause :** Tentative de réinsérer des données déjà présentes

**Solutions :**
1. Supprimer les données existantes pour la date :
   ```sql
   DELETE FROM silver.station_availability 
   WHERE snapshot_timestamp::DATE = '2026-02-26';
   ```

2. Ou utiliser un mode "overwrite" dans le script (à développer si nécessaire)

### Problème 4 : "No data found for date"

**Solution :**
```bash
# Vérifier que les données Bronze existent
ls -lh /chemin/vers/data_lake/bronze/velib/ingestion_date=2026-02-26/

# Si absent, exécuter d'abord le DAG d'ingestion Bronze
docker exec velib_airflow_scheduler airflow dags trigger velib_ingestion_pipeline
```

### Problème 5 : Mémoire insuffisante Spark

**Erreur :**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**Solution :** Augmenter la mémoire dans le DAG :
```python
task_spark_transform = BashOperator(
    bash_command="""
    docker exec velib_spark /opt/spark/bin/spark-submit \
        --driver-memory 4g \       # ← Augmenter
        --executor-memory 4g \     # ← Augmenter
        ...
    """
)
```

---

## 📈 Évolutions futures

### Phase 4 : Gold Layer (à venir)

- Agrégations business (métriques horaires, journalières)
- Tables d'analyse pré-calculées
- Utilisation de dbt pour les transformations SQL

### Améliorations possibles

1. **SCD Type 2 complet**
   - Historiser les changements de capacité des stations
   - Ajouter valid_from / valid_to

2. **Optimisations PostgreSQL**
   - Partitionnement par date
   - Compression des anciennes données
   - Indexes additionnels selon les requêtes

3. **Monitoring avancé**
   - Alertes Slack/email en cas d'échec
   - Dashboard de métriques pipeline
   - Suivi de la data quality

4. **Parallélisation**
   - Traiter plusieurs jours en parallèle
   - Utiliser un vrai cluster Spark

---

## ✅ Checklist de déploiement

- [ ] Schéma PostgreSQL Silver initialisé
- [ ] Driver JDBC installé dans Spark
- [ ] Script `bronze_to_silver.py` déployé dans `spark_jobs/`
- [ ] DAG Airflow déployé dans `airflow/dags/`
- [ ] Test manuel du script Spark réussi
- [ ] Vérification des données dans PostgreSQL
- [ ] DAG Airflow testé manuellement
- [ ] DAG Airflow activé pour exécution quotidienne
- [ ] Documentation mise à jour dans README.md

---

## 📚 Ressources

- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Auteur :** Data Team  
**Dernière mise à jour :** 2026-02-26  
**Version :** 1.0.0
