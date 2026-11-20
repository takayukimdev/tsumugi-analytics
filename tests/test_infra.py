"""Offline checks that Terraform and Makefile stay aligned with the repo contracts."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA_DIR = REPO_ROOT / "infra"
BQ_SCHEMA_DIR = REPO_ROOT / "tracking_plan" / "bq_schemas"


def _all_tf_text() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted(INFRA_DIR.glob("*.tf")))


def test_every_generated_bq_schema_has_a_terraform_table():
    bigquery_tf = (INFRA_DIR / "bigquery.tf").read_text(encoding="utf-8")
    schema_files = sorted(p.name for p in BQ_SCHEMA_DIR.glob("*.json"))
    assert schema_files == [
        "app_events.json",
        "dbt_run_results.json",
        "inference_requests.json",
        "raw_content.json",
    ]
    for filename in schema_files:
        assert f"tracking_plan/bq_schemas/{filename}" in bigquery_tf, filename


def test_terraform_enforces_privacy_wif_and_composer_guardrails():
    tf_text = _all_tf_text()
    for sa in ("ingest", "dbt", "composer", "ci"):
        assert f"{sa}" in (INFRA_DIR / "iam.tf").read_text(encoding="utf-8")

    # WIF must bind to numeric repository_id.
    assert "assertion.repository_id == '${var.github_repository_id}'" in tf_text
    assert "roles/run.jobsExecutorWithOverrides" in tf_text

    # raw.raw_content must have 30-day partition expiration and ALWAYS_NULL data policy.
    bigquery_tf = (INFRA_DIR / "bigquery.tf").read_text(encoding="utf-8")
    assert "30 * 24 * 60 * 60 * 1000" in bigquery_tf
    assert 'predefined_expression = "ALWAYS_NULL"' in bigquery_tf
    assert "roles/bigquerydatapolicy.maskedReader" in bigquery_tf
    assert "roles/datacatalog.categoryFineGrainedReader" in bigquery_tf

    # sa-composer writes only raw.dbt_run_results: no dataset-level dataEditor on raw (which
    # would cover raw_content), just tables.create for load jobs plus a table-level grant.
    assert 'for_each   = toset(["ingest"])' in bigquery_tf
    assert 'permissions = ["bigquery.tables.create"]' in bigquery_tf
    assert 'resource "google_bigquery_table_iam_member" "composer_run_stats_writer"' in bigquery_tf

    # Composer must stay gated behind enable_composer (default false).
    composer_tf = (INFRA_DIR / "composer.tf").read_text(encoding="utf-8")
    assert "var.enable_composer ? 1 : 0" in composer_tf
    assert "CloudSecretManagerBackend" in composer_tf


def test_makefile_defines_required_targets_with_help_descriptions():
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    required = (
        "help",
        "setup",
        "schemas",
        "doctor",
        "gen",
        "ingest-local",
        "demo-offline",
        "dbt-parse",
        "metrics-explain",
        "lint",
        "format",
        "dag-test",
        "test",
        "tf-plan",
        "image",
        "run-ingest",
        "run-dbt",
        "test-bq",
        "composer-deploy-dags",
    )
    for target in required:
        header = f"\n{target}:"
        assert header in makefile, target
        first_line = makefile.split(header, 1)[1].splitlines()[0]
        assert "## " in first_line, target


def test_anon_mart_enforces_k_anonymity_and_omits_identifiers():
    import yaml

    anon_sql = (REPO_ROOT / "dbt/models/anon/anon_weekly_model_benchmarks.sql").read_text(
        encoding="utf-8"
    )
    assert "having count(distinct user_id) >= 5" in anon_sql.lower()

    anon_yml = yaml.safe_load(
        (REPO_ROOT / "dbt/models/anon/_anon__models.yml").read_text(encoding="utf-8")
    )
    model_spec = anon_yml["models"][0]
    assert model_spec["config"]["contract"]["enforced"] is True
    col_names = {c["name"] for c in model_spec["columns"]}
    forbidden = {
        "user_id",
        "org_id",
        "request_id",
        "api_key_id",
        "conversation_id",
        "message_id",
        "session_id",
        "prompt_text",
        "response_text",
    }
    assert col_names.isdisjoint(forbidden)
    assert (REPO_ROOT / "dbt/tests/assert_anon_k_anonymity.sql").is_file()
