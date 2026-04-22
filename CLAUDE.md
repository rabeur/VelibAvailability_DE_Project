# CLAUDE.md

Contexte permanent pour Claude Code sur le projet Vélib' Data Pipeline.
Ce fichier est lu automatiquement à chaque démarrage de session.

## Identité du projet

Pipeline data end-to-end qui ingère l'API Vélib' temps réel, transforme les
snapshots en datasets analytiques, et expose des dashboards opérationnels
Paris. Stack locale opérationnelle : Airflow 2.9, Spark 3.5, PostgreSQL 17,
dbt, Superset, Docker Compose.

Chantier en cours : ajouter une branche de déploiement GCP parallèle (GCS,
Dataproc Serverless, BigQuery, Looker Studio) sans casser le local.

## Règle d'or

Le pipeline local existant doit rester strictement fonctionnel à chaque
étape. Le cloud est une branche parallèle, activable via la variable
d'environnement `PIPELINE_TARGET=cloud`. Toute modification du code
existant doit préserver le comportement `PIPELINE_TARGET=local` par défaut.

## Arborescence

```
VelibAvailability_DE_Project/
├── airflow/dags/        # DAGs existants, mode dual local/cloud
├── spark_jobs/          # jobs Spark, I/O abstraits (fs local ou gs://)
├── dbt/                 # profils postgres_local et bigquery_cloud
├── cloud/               # tout le code cloud vit ici
│   ├── terraform/       # IaC (gcs, bigquery, dataproc, iam)
│   ├── scripts/         # deploy_spark_job.sh, run_dataproc_batch.sh
│   └── docs/            # setup.md, cost_management.md, architecture.md
├── data_lake/           # bronze, silver (local uniquement)
├── scripts/             # scripts utilitaires locaux
├── docs/                # documentation projet
├── Makefile             # cibles locales et cloud-*
└── docker-compose.yml   # stack locale
```

## Stack cloud cible

GCS pour le Bronze (bucket `velib-bronze-<project_id>`, lifecycle
Nearline 30j, Coldline 90j, suppression 365j). BigQuery pour Silver et
Gold (datasets `velib_silver` et `velib_gold`, région `europe-west1`,
tables partitionnées par jour sur `ingestion_timestamp`, clusterisées
sur `stationcode`). Dataproc Serverless pour Spark (pas de cluster
permanent, batch à la demande, 2 executors 4 Go). Terraform pour l'IaC,
backend local (state non partagé). Airflow reste en local et orchestre
les deux modes.

## Conventions Python

Style : `ruff` + `black`, config dans `pyproject.toml`. Naming snake_case,
fonctions courtes et testables. Commentaires en anglais dans le code,
expliquer le pourquoi (décision, contrainte, edge case) pas le quoi.
Pas de docstrings triviales sur des getters évidents.

## Conventions SQL

Style : `sqlfluff`, config dans `.sqlfluff`. Naming dbt : `stg_`, `dim_`,
`fact_`, `mart_`. Jamais de `SELECT *` dans les modèles dbt (règle de
coût BigQuery). Utiliser les macros Jinja pour les fonctions spécifiques
au moteur (date truncation, intervalles).

## Conventions Terraform

Un fichier par domaine logique (gcs.tf, bigquery.tf, dataproc.tf,
iam.tf). Variables typées dans `variables.tf`, outputs exposés dans
`outputs.tf`. Labels obligatoires sur toutes les ressources (project,
environment, managed_by). Pas de secrets en dur, toujours via
`variable` avec `sensitive = true` quand applicable.

## Garde-fous GCP (CRITIQUES)

Budget cible : moins de 20 €/mois. Alerte GCP configurée à 50%, 80%,
100% sur 20 €.

Ne jamais créer de ressources permanentes coûteuses : pas de cluster
Dataproc persistant, pas de Cloud SQL, pas de Composer managé. Dataproc
Serverless uniquement.

Avant tout `terraform apply` qui crée des ressources nouvelles : lancer
`terraform plan` et montrer le diff. Jamais d'apply automatique même si
l'utilisateur a desserré les permissions.

Après une session de développement, proposer `make cloud-down` pour ne
laisser tourner que le stockage (qui reste peu coûteux).

Toute commande qui pourrait déclencher un coût non prévu doit être
confirmée : `bq query`, `gcloud dataproc batches submit`, `terraform
apply`, transfert de gros volumes vers GCS.

## Style de rédaction

Réponses en français. Prose concise, pas de listes à puces pour les
explications simples. Pour le code : toujours expliquer les choix
techniques importants (choix IAM, partitionnement, clustering,
impacts coûts). Pas de tirets cadratins dans les livrables publiés
(README, docs publiques, CV).

## Documentation en place

Lire ces fichiers pour le contexte technique complet :
- `README.md` : vue d'ensemble du pipeline local
- `cloud/docs/architecture_target.md` : spec détaillée de la migration GCP (chargée automatiquement dans cloud/)
- `docs/diagrams/architecture_diagram.png` : diagramme local
- `cloud/docs/architecture.md` : diagramme cloud (à créer)

## Do's

Privilégier l'édition chirurgicale sur les DAGs existants : ajouter des
branches conditionnelles `if PIPELINE_TARGET == "cloud"` plutôt que
dupliquer des DAGs.

Utiliser les hooks Airflow natifs GCP (`GCSHook`,
`DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`) plutôt que
du gcloud CLI via `BashOperator`.

Tester chaque changement en mode local avant cloud : `make status`
doit rester vert.

Commit atomiques par sous-tâche, messages en anglais suivant le style
conventional commits (`feat(cloud):`, `fix(airflow):`, `docs:`).

## Don'ts

Ne jamais commiter : `.env.cloud`, clés de service account JSON,
fichiers `terraform.tfstate*`, credentials gcloud.

Ne jamais modifier les DAGs ou Spark jobs existants sans préserver
le chemin local par défaut.

Ne jamais créer de ressources GCP en dehors de Terraform (règle : si
c'est dans GCP, c'est dans `cloud/terraform/`).

Ne jamais utiliser `SELECT *` dans un modèle dbt ciblant BigQuery
(coûte en lecture, chaque colonne est facturée).

Ne pas activer d'APIs GCP à la main. Passer par `google_project_service`
dans Terraform pour que `terraform destroy` nettoie proprement.

## Workflow de dev recommandé

Une phase = une branche git = une PR. Avant d'ouvrir une PR : `ruff`,
`black`, `sqlfluff` passent, `make status` reste vert en local, les
cibles Makefile cloud affichées dans le PR description.

Pour les commits touchant au cloud : inclure dans le message le coût
estimé ou "no-cost" si la ressource est gratuite (activation d'API,
variables, scripts).

## État d'avancement

Voir le dernier commit et les issues GitHub ouvertes pour le suivi.
Les phases prévues sont décrites dans `cloud/docs/architecture_target.md`
section "Critères de fin de chantier".