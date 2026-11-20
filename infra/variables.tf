variable "project_id" {
  description = "GCP project ID hosting Tsumugi analytics."
  type        = string
}

variable "region" {
  description = "Single GCP region for all data, compute, DLP, and secrets."
  type        = string
  default     = "asia-northeast1"
}

variable "image_tag" {
  description = "Tag of the shared ingest/dbt container image in Artifact Registry."
  type        = string
  default     = "latest"
}

variable "enable_composer" {
  description = "Provision the Cloud Composer 3 environment (keep false outside demo windows)."
  type        = bool
  default     = false
}

variable "composer_image_version" {
  description = "Cloud Composer 3 image version matching orchestration/pyproject.toml."
  type        = string
  default     = "composer-3-airflow-3.3.1"
}

variable "github_repository_id" {
  description = "Numeric GitHub repository_id allowed to assume sa-ci via Workload Identity Federation."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+$", var.github_repository_id))
    error_message = "github_repository_id must be the numeric GitHub repository ID (e.g. `gh api repos/<owner>/<repo> --jq .id`)."
  }
}

variable "fine_grained_readers" {
  description = "IAM members (e.g. user:alice@example.com) permitted to read unmasked raw_content text."
  type        = list(string)
  default     = []
}

variable "alert_notification_channels" {
  description = "Optional Cloud Monitoring notification channel IDs for failed Cloud Run Job alerts."
  type        = list(string)
  default     = []
}
