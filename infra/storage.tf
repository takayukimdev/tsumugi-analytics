resource "google_artifact_registry_repository" "app" {
  location      = var.region
  repository_id = "tsumugi"
  description   = "Shared container image for ingest and dbt Cloud Run Jobs."
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-recent-10"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "data" {
  name                        = "${var.project_id}-tsumugi-data"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Match raw.raw_content's 30-day partition expiry on GCS objects holding redacted text or dead letters.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 30
      matches_prefix = ["clean/raw_content/", "dead_letter/"]
    }
  }

  depends_on = [google_project_service.apis]
}
