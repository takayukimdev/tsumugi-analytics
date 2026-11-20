locals {
  prod_datasets = {
    raw   = "Raw landing tables loaded from GCS clean/ NDJSON and dbt run stats."
    stg   = "dbt staging views (rename and cast only)."
    int   = "dbt intermediate views (deduplication and enrichment)."
    marts = "dbt analytics marts and MetricFlow semantic models."
    anon  = "k-anonymous aggregated marts for external/partner sharing."
  }

  dbt_editable_datasets = toset(["stg", "int", "marts", "anon"])

  raw_content_raw_schema = jsondecode(
    file("${path.module}/../tracking_plan/bq_schemas/raw_content.json")
  )
  free_text_columns = toset(["prompt_text", "response_text"])

  raw_content_tagged_schema = jsonencode([
    for field in local.raw_content_raw_schema : merge(
      field,
      {
        for k in(contains(local.free_text_columns, field.name) ? ["policyTags"] : []) :
        k => { names = [google_data_catalog_policy_tag.free_text.id] }
      }
    )
  ])
}

resource "google_bigquery_dataset" "prod" {
  for_each                   = local.prod_datasets
  dataset_id                 = each.key
  location                   = var.region
  description                = each.value
  delete_contents_on_destroy = false

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "dev" {
  dataset_id                  = "dev_tsumugi"
  location                    = var.region
  description                 = "Developer sandbox dataset; tables expire after 7 days."
  default_table_expiration_ms = 7 * 24 * 60 * 60 * 1000
  delete_contents_on_destroy  = true

  depends_on = [google_project_service.apis]
}

# Dataset-level IAM.
resource "google_bigquery_dataset_iam_member" "raw_writers" {
  for_each   = toset(["ingest"])
  dataset_id = google_bigquery_dataset.prod["raw"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa[each.key].email}"
}

# sa-composer writes only raw.dbt_run_results, so it gets no dataset-level dataEditor (that
# would let it read raw_content metadata columns and delete any raw table). A load job needs
# bigquery.tables.create on the dataset even with CREATE_NEVER into an existing table, so the
# dataset grant is a custom role holding only that permission; it cannot read, change, or
# delete existing tables. Table-level dataEditor covers the load itself.
resource "google_project_iam_custom_role" "bq_load_job_target" {
  role_id     = "tsumugiBigQueryLoadJobTarget"
  title       = "Tsumugi BigQuery load-job target"
  description = "Dataset-level permission a load job checks; pair with table-level dataEditor."
  permissions = ["bigquery.tables.create"]

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset_iam_member" "composer_raw_load_jobs" {
  dataset_id = google_bigquery_dataset.prod["raw"].dataset_id
  role       = google_project_iam_custom_role.bq_load_job_target.name
  member     = "serviceAccount:${google_service_account.sa["composer"].email}"
}

resource "google_bigquery_table_iam_member" "composer_run_stats_writer" {
  dataset_id = google_bigquery_table.dbt_run_results.dataset_id
  table_id   = google_bigquery_table.dbt_run_results.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["composer"].email}"
}

resource "google_bigquery_dataset_iam_member" "raw_readers" {
  for_each   = toset(["dbt", "ci"])
  dataset_id = google_bigquery_dataset.prod["raw"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa[each.key].email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_writers" {
  for_each   = local.dbt_editable_datasets
  dataset_id = google_bigquery_dataset.prod[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa["dbt"].email}"
}

# Data Catalog taxonomy + policy tag + ALWAYS_NULL masking policy on raw.raw_content free text.
resource "google_data_catalog_taxonomy" "pii" {
  region                 = var.region
  display_name           = "tsumugi-pii"
  description            = "Column-level access control taxonomy for Tsumugi free-text columns."
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]

  depends_on = [google_project_service.apis]
}

resource "google_data_catalog_policy_tag" "free_text" {
  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = "free_text_redacted"
  description  = "DLP-redacted prompt/response text; masked to NULL unless granted fineGrainedReader."
}

resource "google_bigquery_datapolicy_data_policy" "raw_content_always_null" {
  location         = var.region
  data_policy_id   = "raw_content_always_null"
  policy_tag       = google_data_catalog_policy_tag.free_text.name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
    predefined_expression = "ALWAYS_NULL"
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_datapolicy_data_policy_iam_member" "masked_readers" {
  for_each       = toset(["dbt", "ci"])
  project        = google_bigquery_datapolicy_data_policy.raw_content_always_null.project
  location       = google_bigquery_datapolicy_data_policy.raw_content_always_null.location
  data_policy_id = google_bigquery_datapolicy_data_policy.raw_content_always_null.data_policy_id
  role           = "roles/bigquerydatapolicy.maskedReader"
  member         = "serviceAccount:${google_service_account.sa[each.key].email}"
}

resource "google_data_catalog_policy_tag_iam_member" "fine_grained_readers" {
  for_each   = toset(var.fine_grained_readers)
  policy_tag = google_data_catalog_policy_tag.free_text.name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = each.value
}

# Raw tables owned by Terraform (ingest and Composer load jobs use CREATE_NEVER).
resource "google_bigquery_table" "app_events" {
  dataset_id          = google_bigquery_dataset.prod["raw"].dataset_id
  table_id            = "app_events"
  description         = "Product events loaded from GCS clean/app_events/."
  schema              = file("${path.module}/../tracking_plan/bq_schemas/app_events.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "_loaded_at"
  }
}

resource "google_bigquery_table" "inference_requests" {
  dataset_id          = google_bigquery_dataset.prod["raw"].dataset_id
  table_id            = "inference_requests"
  description         = "Inference request metadata (no prompt/response text)."
  schema              = file("${path.module}/../tracking_plan/bq_schemas/inference_requests.json")
  clustering          = ["org_id", "model"]
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "_loaded_at"
  }
}

resource "google_bigquery_table" "raw_content" {
  dataset_id          = google_bigquery_dataset.prod["raw"].dataset_id
  table_id            = "raw_content"
  description         = "DLP-deidentified non-ZDR prompt and response text; 30-day partition expiry."
  schema              = local.raw_content_tagged_schema
  deletion_protection = false

  # 30 days (partition_expiration_days=30), matching the GCS clean/raw_content/ lifecycle rule.
  time_partitioning {
    type          = "DAY"
    field         = "_loaded_at"
    expiration_ms = 30 * 24 * 60 * 60 * 1000
  }

  depends_on = [google_bigquery_datapolicy_data_policy.raw_content_always_null]
}

resource "google_bigquery_table" "dbt_run_results" {
  dataset_id          = google_bigquery_dataset.prod["raw"].dataset_id
  table_id            = "dbt_run_results"
  description         = "Flattened per-node dbt run and freshness results loaded per target_date partition."
  schema              = file("${path.module}/../tracking_plan/bq_schemas/dbt_run_results.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "target_date"
  }
}
