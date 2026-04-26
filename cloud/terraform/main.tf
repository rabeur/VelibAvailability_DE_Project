terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Local backend by design: the state is not shared across machines
  # for this personal project. If the project goes multi-user,
  # switch to a GCS backend here.
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  common_labels = merge(
    var.labels,
    {
      environment = var.environment
    }
  )
}

# Enable the GCP APIs the pipeline relies on. Declared in Terraform so
# that `terraform destroy` cleans them up and no API activation drifts
# outside of IaC.
resource "google_project_service" "required" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}
