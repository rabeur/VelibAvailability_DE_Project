# CLAUDE.md — Chantier cloud GCP

Contexte spécifique à la migration GCP. Le `CLAUDE.md` racine
s'applique en premier (conventions, garde-fous, style) — ce fichier
le complète pour tout ce qui concerne le dossier `cloud/`.

## Spec d'architecture complète

@docs/architecture_target.md

## Périmètre et règles de ce dossier

Tout le code cloud vit ici. Rien de ce qui est dans `cloud/` ne
doit être référencé en dur depuis `airflow/`, `spark_jobs/` ou
`dbt/` : les deux modes communiquent uniquement via la variable
`PIPELINE_TARGET` et les variables d'environnement du `.env.cloud`.

Toute ressource GCP doit être déclarée dans `cloud/terraform/`.
Jamais de `gcloud ... create` ou `bq mk` en dehors de Terraform,
sauf pour les tests ponctuels documentés dans `cloud/docs/setup.md`.

## Structure du dossier

```
cloud/
├── terraform/          # IaC complet (gcs, bigquery, dataproc, iam)
├── scripts/            # scripts bash pour déploiement et tests manuels
├── docs/               # documentation et specs
│   ├── architecture_target.md   # spec migration GCP (ce fichier est importé)
│   ├── setup.md                # guide de mise en route GCP (à créer)
│   └── cost_management.md      # budgets et alertes (à créer)
└── README.md           # README spécifique au chantier cloud (à créer)
```

## Checklist avant tout terraform apply

1. `terraform validate` passe sans erreur
2. `terraform plan` relu et approuvé explicitement
3. Aucune ressource permanente et coûteuse dans le plan
   (pas de cluster Dataproc, pas de Cloud SQL, pas de NAT Gateway)
4. Labels présents sur toutes les nouvelles ressources
5. `make status` vert côté local (le pipeline local tourne toujours)

## Références croisées utiles

Les DAGs Airflow qui appellent le cloud sont dans `airflow/dags/`.
Le job Spark Bronze-to-Silver est dans `spark_jobs/bronze_to_silver.py`.
Les profils dbt dual (postgres_local / bigquery_cloud) sont dans `dbt/profiles.yml`.
