# Cloud Composer 3 is gated behind `var.enable_composer` (default false) to stay within the
# $50/month portfolio budget. Create only during live demo runs and destroy immediately after.

resource "google_composer_environment" "main" {
  count  = var.enable_composer ? 1 : 0
  name   = "tsumugi-composer"
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.sa["composer"].email
    }

    software_config {
      image_version = var.composer_image_version

      env_variables = {
        TSUMUGI_PROJECT_ID = var.project_id
        TSUMUGI_REGION     = var.region
        TSUMUGI_BUCKET     = google_storage_bucket.data.name
      }

      airflow_config_overrides = {
        "secrets-backend" = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
        "secrets-backend_kwargs" = jsonencode({
          project_id       = var.project_id
          variables_prefix = "airflow-variables"
        })
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_project_iam_member.sa_project_roles,
  ]
}
