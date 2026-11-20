locals {
  # GCP service account IDs must be 6–30 chars, so `sa-ci` (5 chars) uses `sa-ci-wif`.
  service_accounts = {
    ingest = {
      account_id   = "sa-ingest"
      display_name = "sa-ingest: Ingest Cloud Run Job (DLP de-identify, GCS clean, BQ raw load)"
    }
    dbt = {
      account_id   = "sa-dbt"
      display_name = "sa-dbt: dbt Cloud Run Job (transform raw -> stg/int/marts/anon)"
    }
    composer = {
      account_id   = "sa-composer"
      display_name = "sa-composer: Cloud Composer 3 orchestration environment"
    }
    ci = {
      account_id   = "sa-ci-wif"
      display_name = "sa-ci: GitHub Actions CI via Workload Identity Federation"
    }
  }

  project_iam_bindings = [
    # sa-ingest: call Cloud DLP regional deidentify and submit BigQuery load jobs.
    { sa = "ingest", role = "roles/dlp.user" },
    { sa = "ingest", role = "roles/bigquery.jobUser" },

    # sa-dbt: run BigQuery query jobs and read INFORMATION_SCHEMA.JOBS for fct_pipeline_runs.
    { sa = "dbt", role = "roles/bigquery.jobUser" },
    { sa = "dbt", role = "roles/bigquery.resourceViewer" },

    # sa-composer: run Composer 3 workers, execute Cloud Run Jobs with container overrides
    # (jobsExecutorWithOverrides starts the job; run.viewer provides run.operations.get for
    # CloudRunExecuteJobOperator LRO polling), and run the GCSToBigQueryOperator load job
    # (BigQuery data access is scoped to raw.dbt_run_results in bigquery.tf).
    { sa = "composer", role = "roles/composer.worker" },
    { sa = "composer", role = "roles/run.jobsExecutorWithOverrides" },
    { sa = "composer", role = "roles/run.viewer" },
    { sa = "composer", role = "roles/bigquery.jobUser" },

    # sa-ci: create expiring ci_<sha> datasets (bigquery.user grants ownership only on datasets
    # it creates, not existing ones) and read INFORMATION_SCHEMA.JOBS for fct_pipeline_runs.
    { sa = "ci", role = "roles/bigquery.user" },
    { sa = "ci", role = "roles/bigquery.resourceViewer" },
  ]
}

resource "google_service_account" "sa" {
  for_each     = local.service_accounts
  account_id   = each.value.account_id
  display_name = each.value.display_name

  depends_on = [google_project_service.apis]
}

resource "google_project_iam_member" "sa_project_roles" {
  for_each = {
    for b in local.project_iam_bindings : "${b.sa}:${b.role}" => b
  }
  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.sa[each.value.sa].email}"
}

# Secret Manager least-privilege access.
resource "google_secret_manager_secret_iam_member" "ingest_hmac_key" {
  secret_id = google_secret_manager_secret.hmac_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa["ingest"].email}"
}

resource "google_secret_manager_secret_iam_member" "composer_slack_webhook" {
  secret_id = google_secret_manager_secret.slack_webhook.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa["composer"].email}"
}

# GCS data bucket access.
resource "google_storage_bucket_iam_member" "bucket_writers" {
  for_each = toset(["ingest", "dbt"])
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.sa[each.key].email}"
}

resource "google_storage_bucket_iam_member" "composer_bucket_reader" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.sa["composer"].email}"
}

# Workload Identity Federation for GitHub Actions (no service account keys).
# Bound to numeric repository_id so repo renames cannot break or hijack the binding.
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "OIDC federation pool for Tsumugi CI"

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"          = "assertion.sub"
    "attribute.repository"    = "assertion.repository"
    "attribute.repository_id" = "assertion.repository_id"
  }

  attribute_condition = "assertion.repository_id == '${var.github_repository_id}'"
}

resource "google_service_account_iam_member" "ci_wif" {
  service_account_id = google_service_account.sa["ci"].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository_id/${var.github_repository_id}"
}
