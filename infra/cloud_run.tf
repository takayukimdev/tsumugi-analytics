locals {
  image_uri = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.app.repository_id}/tsumugi:${var.image_tag}"
}

resource "google_cloud_run_v2_job" "ingest" {
  name                = "ingest"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.sa["ingest"].email
      timeout         = "900s"
      max_retries     = 1

      containers {
        image = local.image_uri
        args  = ["ingest", "--help"]

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "BQ_RAW_DATASET"
          value = google_bigquery_dataset.prod["raw"].dataset_id
        }
        env {
          name  = "HMAC_SECRET_ID"
          value = google_secret_manager_secret.hmac_key.secret_id
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_job" "dbt" {
  name                = "dbt"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.sa["dbt"].email
      timeout         = "900s"
      max_retries     = 1

      containers {
        image = local.image_uri
        args  = ["dbt", "build", "--target", "prod"]

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.data.name
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "cloud_run_job_failed" {
  display_name          = "Tsumugi Cloud Run Job failed execution"
  combiner              = "OR"
  notification_channels = var.alert_notification_channels

  conditions {
    display_name = "Cloud Run Job failed executions > 0"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "metric.type = \"run.googleapis.com/job/completed_execution_count\"",
        "metric.labels.result = \"failed\"",
      ])
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.job_name"]
      }
    }
  }

  documentation {
    content   = "A Tsumugi Cloud Run Job (`ingest` or `dbt`) execution failed. Inspect Cloud Run Job execution logs and `docs/runbooks/`."
    mime_type = "text/markdown"
  }

  depends_on = [google_project_service.apis]
}
