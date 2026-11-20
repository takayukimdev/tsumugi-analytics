output "data_bucket_name" {
  description = "GCS bucket holding clean/ NDJSON, dead_letter/, and dbt run_artifacts/."
  value       = google_storage_bucket.data.name
}

output "image_uri" {
  description = "Shared ingest/dbt image URI in Artifact Registry."
  value       = local.image_uri
}

output "wif_provider_name" {
  description = "Full resource name of the GitHub Actions WIF provider."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "ci_service_account_email" {
  description = "Service account email assumed by GitHub Actions for make test-bq."
  value       = google_service_account.sa["ci"].email
}

output "composer_dag_gcs_prefix" {
  description = "GCS prefix for uploading DAGs when enable_composer = true."
  value       = var.enable_composer ? google_composer_environment.main[0].config[0].dag_gcs_prefix : null
}
