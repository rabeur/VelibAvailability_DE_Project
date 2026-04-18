# Cloud — Migration GCP du pipeline Vélib'

Branche parallèle de déploiement GCP (GCS, Dataproc Serverless, BigQuery,
Looker Studio). Le pipeline local reste la cible par défaut : tout ce qui
vit ici est activé via `PIPELINE_TARGET=cloud` et les variables de
`.env.cloud`.

## Contenu

```
cloud/
├── terraform/          # IaC (gcs, bigquery, dataproc, iam)
├── scripts/            # déploiement et soumission manuelle
├── docs/               # spec, setup, coûts
└── README.md           # ce fichier
```

## Prérequis

- Un projet GCP dédié avec la facturation activée
- `gcloud` CLI authentifié (`gcloud auth application-default login`)
- `terraform` >= 1.6
- Un budget GCP configuré à 20 € avec alertes 50 / 80 / 100 %

## Démarrage rapide

```bash
cp .env.cloud.example .env.cloud
# renseigner GCP_PROJECT_ID, GCP_REGION, etc.

cd cloud/terraform
cp terraform.tfvars.example terraform.tfvars
# renseigner project_id, region

terraform init
terraform plan        # relire le diff avant tout apply
```

Aucune commande de ce dépôt ne lance `terraform apply` automatiquement.
Le plan doit être relu explicitement. Voir `cloud/docs/setup.md` (à venir).

## Garde-fous

- Dataproc Serverless uniquement (pas de cluster permanent)
- Bucket Bronze avec lifecycle Nearline 30 j / Coldline 90 j / suppression 365 j
- BigQuery : partitionnement + clustering obligatoires, pas de `SELECT *`
- `make cloud-down` en fin de session pour ne laisser tourner que le stockage
- Aucune ressource GCP hors Terraform (si c'est dans GCP, c'est ici)

## Spec complète

Voir `docs/architecture_cible.md`.
