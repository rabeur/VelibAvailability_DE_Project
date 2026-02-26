# 🎯 Phase 3 : Silver Layer - Transformation & Storage

## Vue d'ensemble

La Phase 3 du projet Vélib Data Engineering implémente la **couche Silver** du Data Lake, transformant les données brutes (Bronze) en données nettoyées, normalisées et structurées, stockées dans PostgreSQL pour faciliter les analyses.

### Architecture Silver Layer

```
┌─────────────────┐
│  Bronze Layer   │
│   (Parquet)     │
└────────┬────────┘
         │
         │ PySpark
         │ Transformation
         ▼
┌─────────────────┐
│  Silver Layer   │
│  (PostgreSQL)   │
├─────────────────┤
│ • stations      │ ← Dimension
│ • availability  │ ← Faits
└─────────────────┘
```

### Transformations appliquées

1. **Nettoyage des données**
   - Gestion des valeurs nulles
   - Normalisation des types (booléens OUI/NON → TRUE/FALSE)
   - Filtrage des lignes invalides

2. **Enrichissement**
   - Calcul du taux d'occupation (`occupancy_rate`)
   - Calcul du taux de disponibilité (`availability_rate`)
   - Flags métier (`is_empty`, `is_full`, `is_operational`)

3. **Structuration**
   - Séparation Dimension (stations) / Faits (availability)
   - Dénormalisation pour performance
   - Indexation optimisée

---

## 📂 Structure des fichiers

```
VelibAvailability_DE_Project/
├── sql/
│   └── 02_init_silver_schema.sql          # Schéma PostgreSQL Silver
├── spark_jobs/
│   └── bronze_to_silver.py                # Script PySpark de transformation
├── airflow/dags/
│   └── velib_silver_daily_transformation_dag.py  # DAG quotidien
├── scripts/
│   ├── deploy_phase3.sh                   # Script de déploiement
│   └── test_silver_transformation.py      # Script de test manuel
└── docs/
    └── PHASE3_DEPLOYMENT_GUIDE.md         # Guide complet
```

---

## 🚀 Installation rapide

### Prérequis

- ✅ Docker & Docker Compose opérationnels
- ✅ Données Bronze disponibles (`/data_lake/bronze/velib`)
- ✅ Conteneurs : `postgres`, `spark`, `airflow-scheduler`, `airflow-webserver`

### Déploiement automatisé

```bash
# Rendre le script exécutable
chmod +x scripts/deploy_phase3.sh

# Lancer le déploiement
./scripts/deploy_phase3.sh
```

Le script effectue automatiquement :
1. ✅ Vérification des prérequis
2. ✅ Initialisation du schéma PostgreSQL
3. ✅ Installation du driver JDBC
4. ✅ Déploiement du script PySpark
5. ✅ Déploiement du DAG Airflow
6. ✅ Tests de validation

---

## 🧪 Tests manuels

### Test 1 : Transformation d'une date spécifique

```bash
# Utiliser le script Python de test
python3 scripts/test_silver_transformation.py --date 2026-02-26

# Ou directement avec Spark
DATE_TARGET="2026-02-26"
docker exec velib_spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark_jobs/bronze_to_silver.py \
  /opt/data_lake/bronze/velib \
  $DATE_TARGET
```

### Test 2 : Vérifier les données dans PostgreSQL

```bash
# Nombre de stations
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT COUNT(*) as total_stations FROM silver.stations;
"

# Snapshots par date
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT 
  snapshot_timestamp::DATE as date,
  COUNT(*) as snapshots,
  COUNT(DISTINCT station_id) as stations
FROM silver.station_availability
GROUP BY snapshot_timestamp::DATE
ORDER BY date DESC
LIMIT 7;
"

# Dernière disponibilité par station
docker exec -it velib_postgres psql -U velib -d velib_dw -c "
SELECT * FROM silver.v_latest_station_availability LIMIT 5;
"
```

### Test 3 : Déclencher le DAG Airflow

```bash
# Activer le DAG
docker exec velib_airflow_scheduler airflow dags unpause velib_silver_daily_transformation

# Déclencher manuellement
docker exec velib_airflow_scheduler airflow dags trigger velib_silver_daily_transformation

# Suivre l'exécution
docker-compose logs -f airflow-scheduler
```

---

## 📊 Schéma de données

### Table `silver.stations` (Dimension)

| Colonne | Type | Description |
|---------|------|-------------|
| `station_id` | VARCHAR(50) | Identifiant unique (PK) |
| `station_name` | VARCHAR(255) | Nom de la station |
| `capacity` | INTEGER | Capacité totale |
| `latitude` | DECIMAL(10,8) | Coordonnée latitude |
| `longitude` | DECIMAL(11,8) | Coordonnée longitude |
| `nom_arrondissement_communes` | VARCHAR(255) | Arrondissement/Commune |
| `code_insee_commune` | VARCHAR(10) | Code INSEE |
| `first_seen_at` | TIMESTAMP | Première apparition |
| `last_seen_at` | TIMESTAMP | Dernière apparition |
| `is_active` | BOOLEAN | Station active |

**Index :**
- Géographique : `(latitude, longitude)`
- Arrondissement : `(nom_arrondissement_communes)`
- Activité : `(is_active)`

### Table `silver.station_availability` (Faits)

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | BIGSERIAL | Clé primaire auto |
| `station_id` | VARCHAR(50) | FK vers stations |
| `snapshot_timestamp` | TIMESTAMP | Timestamp du snapshot |
| `snapshot_date` | DATE | Date générée |
| `snapshot_hour` | INTEGER | Heure générée (0-23) |
| `snapshot_day_of_week` | INTEGER | Jour semaine (0=Dim, 6=Sam) |
| `num_bikes_available` | INTEGER | Vélos disponibles |
| `num_bikes_available_mechanical` | INTEGER | Vélos mécaniques |
| `num_bikes_available_ebike` | INTEGER | Vélos électriques |
| `num_docks_available` | INTEGER | Places disponibles |
| `is_installed` | BOOLEAN | Station installée |
| `is_renting` | BOOLEAN | Location active |
| `is_returning` | BOOLEAN | Retour actif |
| `occupancy_rate` | DECIMAL(5,2) | Taux occupation (%) |
| `availability_rate` | DECIMAL(5,2) | Taux disponibilité (%) |
| `is_empty` | BOOLEAN | Station vide |
| `is_full` | BOOLEAN | Station pleine |
| `is_operational` | BOOLEAN | Station opérationnelle |

**Index :**
- Temporel : `(snapshot_timestamp)`, `(snapshot_date)`, `(snapshot_hour)`
- Jointure : `(station_id, snapshot_timestamp DESC)`
- Analytique : `(occupancy_rate)`, `(is_empty)`, `(is_full)`

**Contrainte :** `UNIQUE(station_id, snapshot_timestamp)`

### Vues disponibles

- `silver.v_latest_station_availability` : Dernière disponibilité par station
- `silver.v_daily_station_stats` : Statistiques quotidiennes agrégées

---

## 🔄 Pipeline quotidien

### Schedule

Le DAG `velib_silver_daily_transformation` s'exécute **tous les jours à 2h du matin**.

### Workflow

```
1. get_target_date
   └─> Calcule la date d'hier
   
2. check_bronze_data
   └─> Vérifie existence des données Bronze
   
3. spark_bronze_to_silver
   └─> Transformation PySpark
   
4. validate_silver_data
   └─> Validation en PostgreSQL
   
5. update_postgres_stats
   └─> Mise à jour statistiques tables
   
6. notify_success
   └─> Notification de succès
```

### Monitoring

**Interface Airflow :**
- URL : http://localhost:8081
- DAG : `velib_silver_daily_transformation`
- Logs : Consultez chaque task pour voir les détails

**Métriques clés :**
```sql
-- Volume quotidien
SELECT 
  snapshot_date,
  COUNT(*) as total_snapshots,
  COUNT(DISTINCT station_id) as unique_stations
FROM silver.station_availability
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;

-- Taux de succès
SELECT 
  COUNT(*) FILTER (WHERE is_operational) * 100.0 / COUNT(*) as operational_pct
FROM silver.station_availability
WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day';
```

---

## 🔧 Dépannage

### Problème : "Driver JDBC non trouvé"

```bash
# Solution
docker exec velib_spark bash -c "
  cd /opt/spark/jars && 
  wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
"
```

### Problème : "Duplicate key constraint violation"

```sql
-- Supprimer les données de la date problématique
DELETE FROM silver.station_availability 
WHERE snapshot_timestamp::DATE = '2026-02-26';

-- Relancer la transformation
```

### Problème : "No data found for date"

```bash
# Vérifier les données Bronze
ls -lh data_lake/bronze/velib/ingestion_date=2026-02-26/

# Si manquantes, lancer l'ingestion Bronze
docker exec velib_airflow_scheduler airflow dags trigger velib_ingestion_pipeline
```

### Logs détaillés

```bash
# Logs Spark
docker-compose logs spark

# Logs Airflow
docker-compose logs airflow-scheduler

# Logs PostgreSQL
docker-compose logs postgres
```

---

## 📈 Performances

### Métriques typiques

| Métrique | Valeur attendue |
|----------|-----------------|
| **Temps d'exécution** | 30-60 secondes |
| **Lignes traitées/jour** | ~50,000 - 100,000 |
| **Stations uniques** | ~1,400 |
| **Taille table availability/mois** | ~100-200 MB |

### Optimisations possibles

1. **Partitionnement PostgreSQL**
   ```sql
   -- Partitionner par mois
   CREATE TABLE silver.station_availability_2026_02 
   PARTITION OF silver.station_availability
   FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
   ```

2. **Compression**
   ```sql
   -- Activer la compression TOAST
   ALTER TABLE silver.station_availability 
   SET (toast_tuple_target = 128);
   ```

3. **Augmenter la mémoire Spark**
   ```python
   # Dans le DAG
   --driver-memory 4g
   --executor-memory 4g
   ```

---

## 🎯 Prochaines étapes (Phase 4)

- [ ] **Gold Layer** : Agrégations business (métriques horaires, hebdomadaires)
- [ ] **dbt** : Transformations SQL avec tests et documentation
- [ ] **Dashboard** : Metabase ou Streamlit pour visualisation
- [ ] **Alerting** : Slack/Email pour anomalies de data quality
- [ ] **CI/CD** : Tests automatisés et déploiement continu

---

## 📚 Références

- [Guide de déploiement complet](docs/PHASE3_DEPLOYMENT_GUIDE.md)
- [Documentation PySpark](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL Best Practices](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [Airflow Documentation](https://airflow.apache.org/docs/)

---

## 📞 Support

En cas de problème :
1. Consultez les logs : `docker-compose logs -f`
2. Vérifiez le guide de dépannage : `PHASE3_DEPLOYMENT_GUIDE.md`
3. Testez manuellement : `python3 scripts/test_silver_transformation.py`

---

**Version :** 1.0.0  
**Dernière mise à jour :** 2026-02-26  
**Auteur :** Data Engineering Team
