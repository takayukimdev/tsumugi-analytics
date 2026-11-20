# Terraform manages secret containers and IAM only, never secret versions, so secret
# material never enters terraform.tfstate. Add versions once via `gcloud secrets versions add`.

resource "google_secret_manager_secret" "hmac_key" {
  secret_id = "hmac-key"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# Named for Cloud Composer's Secret Manager backend (`variables_prefix = "airflow-variables"`),
# which resolves `Variable.get("slack_webhook_url")` without storing the URL in Airflow's DB.
resource "google_secret_manager_secret" "slack_webhook" {
  secret_id = "airflow-variables-slack_webhook_url"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}
