# Architecture cible — Migration GCP du pipeline Vélib'

## Principe directeur

Stratégie hybride malin : **préserver le pipeline local existant intact** et ajouter une branche de déploiement cloud parallèle, activable à la demande. Les deux environnements (local et GCP) doivent pouvoir fonctionner indépendamment sur la même codebase.

## Arborescence cible du repo

```
VelibAvailability_DE_Project/
├── airflow/                      # existant, modifié pour supporter les deux modes
│   └── dags/
│       ├── velib_ingestion_pipeline.py         # existant, étendu avec cible GCS optionnelle
│       ├── velib_bronze_cleanup_hourly.py      # existant, mode dual
│       ├── velib_silver_transformation_hourly.py # existant, branche vers Dataproc si cloud
│       └── velib_dbt_gold_transformation.py    # existant, profil dbt paramétré
├── spark_jobs/
│   └── bronze_to_silver.py       # existant, lecture/écriture abstraites (fs local ou gs://)
├── dbt/
│   ├── profiles.yml              # deux profils : postgres_local et bigquery_cloud
│   └── models/                   # modèles compatibles avec les deux moteurs
├── cloud/                        # nouveau dossier, tout le cloud vit ici
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── gcs.tf                # bucket Bronze + lifecycle rules
│   │   ├── bigquery.tf           # datasets silver et gold, partitioning, clustering
│   │   ├── dataproc.tf           # configuration Dataproc Serverless batch
│   │   ├── iam.tf                # service account + rôles de moindre privilège
│   │   └── terraform.tfvars.example
│   ├── scripts/
│   │   ├── deploy_spark_job.sh   # upload du job Spark vers GCS
│   │   ├── run_dataproc_batch.sh # soumission manuelle pour tests
│   │   └── bootstrap_bigquery.sh # création des schémas initiaux
│   ├── docs/
│   │   ├── setup.md              # guide de mise en route GCP
│   │   ├── cost_management.md    # budgets, alertes, free tier
│   │   └── architecture.md       # diagramme cloud cible
│   └── README.md                 # README spécifique au chantier cloud
├── Makefile                      # existant, enrichi de cibles cloud-*
└── .env.cloud.example            # variables d'environnement GCP
```

## Composants cibles et rôles

### Google Cloud Storage

Bucket unique `velib-bronze-<project_id>` organisé selon la même logique de partitionnement que le local :

```
gs://velib-bronze-<project_id>/
├── bronze/velib/
│   ├── ingestion_date=2026-04-16/
│   │   ├── hour=00/
│   │   └── hour=01/
│   └── ...
├── reports/data_quality/
│   └── report_date=2026-04-16/
└── spark_jobs/                  # les fichiers .py des jobs Spark à exécuter
```

Règles de cycle de vie : passage en Nearline après 30 jours, Coldline après 90 jours, suppression après 365 jours pour contenir les coûts.

### BigQuery

Deux datasets dans la région `europe-west1` (Belgique, proche de Paris, coûts modérés) :

- `velib_silver` — tables Silver partitionnées par jour sur `ingestion_timestamp`, clusterisées sur `stationcode`
- `velib_gold` — modèles dbt Gold (dimensions, faits, marts)

### Dataproc Serverless

Pas de cluster permanent. Chaque exécution du job Bronze vers Silver est un batch Serverless à la demande : démarrage en 60 à 90 secondes, facturation à la seconde, autoscaling géré par GCP. Configuration initiale : 2 executors, 4 Go de mémoire chacun, à ajuster selon les volumes réels.

### Service account

Un seul compte de service `velib-pipeline-sa` avec les rôles strictement nécessaires :

- `roles/storage.objectAdmin` limité au bucket Bronze
- `roles/bigquery.dataEditor` sur les deux datasets
- `roles/dataproc.editor` pour soumettre les batchs
- `roles/dataproc.worker` pour l'exécution du batch

### Airflow local orchestrateur

Les DAGs existants restent en place. Deux modifications structurelles :

1. Une variable d'environnement `PIPELINE_TARGET` (`local` ou `cloud`) bascule les chemins et connecteurs
2. Ajout de tâches GCP via les operators natifs : `GCSHook`, `DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`

### dbt

Le fichier `profiles.yml` expose deux cibles. Les modèles SQL sont rendus compatibles avec les deux moteurs via les macros Jinja quand nécessaire (types, fonctions de date).

### Looker Studio

Connexion directe sur les tables `velib_gold.*`. Dashboards publics en lecture seule pour la démo, duplication des vues du Superset local.

## Variables d'environnement

Fichier `.env.cloud` non commité :

```
GCP_PROJECT_ID=velib-analytics-xxxxx
GCP_REGION=europe-west1
GCP_BRONZE_BUCKET=velib-bronze-velib-analytics-xxxxx
GCP_BIGQUERY_SILVER_DATASET=velib_silver
GCP_BIGQUERY_GOLD_DATASET=velib_gold
GCP_SERVICE_ACCOUNT_KEY=/path/to/velib-pipeline-sa.json
PIPELINE_TARGET=cloud
```

## Cibles Makefile à ajouter

```makefile
cloud-init:           # terraform init
cloud-plan:           # terraform plan avec preview des coûts
cloud-up:             # terraform apply
cloud-down:           # terraform destroy (après confirmation)
cloud-deploy-spark:   # upload du job Spark sur GCS
cloud-run-ingestion:  # déclenche un DAG Airflow en mode cloud
cloud-dbt-run:        # dbt run avec profil bigquery_cloud
cloud-dbt-test:       # dbt test avec profil bigquery_cloud
cloud-logs:           # tail des logs Dataproc du dernier batch
cloud-cost:           # rapport de coûts du mois courant
```

## Garde-fous de coût

- Budget GCP configuré à 30 € par mois avec alerte à 50%, 80%, 100%
- Les règles de cycle de vie GCS suppriment automatiquement les données de plus d'un an
- BigQuery : tables partitionnées obligatoirement, clustering activé, pas de `SELECT *` dans les modèles dbt
- Dataproc Serverless uniquement (pas de cluster permanent)
- `make cloud-down` en fin de session de développement pour ne laisser tourner que le stockage

## Points de vigilance techniques

1. **Parquet sur GCS** : vérifier que le schéma écrit par le DAG local est lisible par Spark sur Dataproc (encodage, compression)
2. **dbt macros** : certaines fonctions Postgres (`DATE_TRUNC`, `INTERVAL`) ont des équivalents différents en BigQuery
3. **Timezones** : BigQuery stocke en UTC par défaut, le partitionnement doit être cohérent avec les partitions GCS
4. **Quotas** : Dataproc Serverless a un quota de 60 batchs par jour par défaut, suffisant mais à surveiller
5. **IAM propagation** : les permissions mettent parfois plusieurs minutes à se propager après un `terraform apply`

## Critères de fin de chantier

Le chantier est considéré comme terminé quand :

- [ ] `make cloud-up` provisionne toute l'infrastructure en moins de 10 minutes
- [ ] Un `make cloud-run-ingestion` écrit correctement sur GCS
- [ ] Le batch Dataproc lit Bronze sur GCS et écrit Silver dans BigQuery
- [ ] `make cloud-dbt-run` produit les modèles Gold dans BigQuery sans erreur
- [ ] Les tests dbt passent sur les deux profils (local et cloud)
- [ ] Un dashboard Looker Studio affiche au moins les trois KPIs du README
- [ ] `make cloud-down` détruit proprement toute l'infrastructure
- [ ] Le coût cumulé des tests reste sous 10 €
- [ ] Le README GitHub expose clairement la section "Cloud deployment"