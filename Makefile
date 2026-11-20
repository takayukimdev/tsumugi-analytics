-include .env
export
export PATH := $(HOME)/.local/bin:$(PATH)

.DEFAULT_GOAL := help
UV ?= uv
DAYS ?= 28
INGEST_DAYS ?= 1
SEED ?= 42
DBT_OFFLINE = cd dbt && DBT_PROFILES_DIR=../ci
METRICS ?= requests
GROUP_BY ?= metric_time__week

GCP_REGION ?= asia-northeast1
IMAGE_TAG ?= latest
AR_REPO ?= tsumugi
IMAGE_URI = $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(AR_REPO)/tsumugi:$(IMAGE_TAG)
GCS_BUCKET ?= $(GCP_PROJECT_ID)-tsumugi-data
TARGET_DATE ?= 2026-11-18
COMPOSER_ENV ?= tsumugi-composer
GIT_SHA ?= $(shell git rev-parse --short=12 HEAD)

.PHONY: help setup schemas gen ingest-local demo-offline dbt-parse dag-test doctor \
	metrics-explain lint format test tf-plan image run-ingest run-dbt test-bq composer-deploy-dags

help: ## List all targets
	@grep -h -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

setup: ## Install Python dependencies (uv): root project and the separate Airflow project
	$(UV) sync
	cd orchestration && $(UV) sync

schemas: ## Regenerate JSON Schemas and BigQuery raw schemas from tracking_plan/events.yaml
	$(UV) run python -m tracking_plan.codegen

doctor: ## Check local tools and GCP auth needed for the GCP steps
	@for t in uv gh gcloud bq terraform docker gitleaks; do \
		if command -v $$t >/dev/null 2>&1; then echo "ok       $$t"; else echo "MISSING  $$t"; fi; \
	done
	@test -n "$$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null)" \
		&& echo "ok       gcloud CLI account" || echo "MISSING  gcloud CLI account (gcloud auth login)"
	@gcloud auth application-default print-access-token >/dev/null 2>&1 \
		&& echo "ok       gcloud ADC" || echo "MISSING  gcloud ADC (gcloud auth application-default login)"
	@test -f .env && echo "ok       .env" || echo "MISSING  .env (cp .env.example .env)"
	@test -f infra/terraform.tfvars && echo "ok       infra/terraform.tfvars" || echo "MISSING  infra/terraform.tfvars"

gen: ## Generate synthetic data into data/landing (DAYS, SEED)
	$(UV) run python -m generator --days $(DAYS) --seed $(SEED) --out data/landing

ingest-local: ## Run ingest offline (regex redactor, demo HMAC key) into data/ingest
	$(UV) run python -m ingest --landing data/landing --out data/ingest --redactor regex --demo-key

demo-offline: gen ingest-local ## Generate data and run ingest end to end without GCP

dbt-parse: ## Parse dbt offline (dummy profile) and validate MetricFlow semantics
	$(DBT_OFFLINE) $(UV) run dbt parse --no-partial-parse --quiet
	$(DBT_OFFLINE) $(UV) run mf validate-configs --skip-dw

metrics-explain: dbt-parse ## Print SQL MetricFlow generates (METRICS=a,b GROUP_BY=metric_time__week)
	$(DBT_OFFLINE) $(UV) run mf query --metrics $(METRICS) --group-by $(GROUP_BY) \
		--start-time 2026-11-01 --end-time 2026-11-14 --explain

lint: ## Lint Python (ruff), SQL (sqlfluff, offline jinja templater), and Terraform
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run sqlfluff lint dbt
	terraform -chdir=infra fmt -check

format: ## Auto-fix lint issues and format Python and Terraform
	$(UV) run ruff check --fix .
	$(UV) run ruff format .
	terraform -chdir=infra fmt

dag-test: ## Offline DAG checks (imports, graph, templates); the DAG itself runs only on Composer
	cd orchestration && $(UV) run --frozen pytest -q

test: dbt-parse dag-test ## Offline tests: schema drift, pytest, dbt parse, DAG checks, Terraform validate, secret scan
	$(UV) run python -m tracking_plan.codegen --check
	$(UV) run pytest -q
	terraform -chdir=infra init -backend=false -input=false >/dev/null
	terraform -chdir=infra validate
	@if command -v gitleaks >/dev/null 2>&1; then \
		gitleaks detect --no-banner --redact; \
	elif [ -n "$$CI" ]; then \
		echo "ERROR: gitleaks is required in CI"; exit 1; \
	else \
		echo "WARN: gitleaks not installed locally; secret scan runs in CI"; \
	fi

tf-plan: ## Initialize Terraform backend and run terraform plan (never apply automatically)
	terraform -chdir=infra init
	terraform -chdir=infra plan

image: ## Build and push the shared ingest/dbt container image to Artifact Registry
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: set GCP_PROJECT_ID (in .env or env)"; exit 1)
	gcloud auth configure-docker $(GCP_REGION)-docker.pkg.dev --quiet
	docker build --platform linux/amd64 -t $(IMAGE_URI) .
	docker push $(IMAGE_URI)

run-ingest: ## Execute the ingest Cloud Run Job on GCP (TARGET_DATE=YYYY-MM-DD, INGEST_DAYS=1)
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: set GCP_PROJECT_ID (in .env or env)"; exit 1)
	gcloud run jobs execute ingest \
		--project=$(GCP_PROJECT_ID) \
		--region=$(GCP_REGION) \
		--args="ingest,--date,$(TARGET_DATE),--days,$(INGEST_DAYS),--stats-uri,gs://$(GCS_BUCKET)/run_artifacts/dt=$(TARGET_DATE)/ingest_stats.json" \
		--wait

run-dbt: ## Execute the dbt Cloud Run Job on GCP (TARGET_DATE=YYYY-MM-DD)
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: set GCP_PROJECT_ID (in .env or env)"; exit 1)
	gcloud run jobs execute dbt \
		--project=$(GCP_PROJECT_ID) \
		--region=$(GCP_REGION) \
		--args="dbt,build,--target,prod,--artifacts-uri,gs://$(GCS_BUCKET)/run_artifacts/dt=$(TARGET_DATE)" \
		--wait

test-bq: ## Create expiring ci_<sha> dataset (24h) and run dbt build against BigQuery raw
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: set GCP_PROJECT_ID (in .env or env)"; exit 1)
	@ds="ci_$(GIT_SHA)"; \
	echo "Using expiring BigQuery dataset $(GCP_PROJECT_ID):$$ds"; \
	bq show --project_id=$(GCP_PROJECT_ID) "$(GCP_PROJECT_ID):$$ds" >/dev/null 2>&1 || \
		bq --location=$(GCP_REGION) --project_id=$(GCP_PROJECT_ID) mk --dataset \
			--default_table_expiration=86400 \
			--label datacloud:antigravity \
			"$(GCP_PROJECT_ID):$$ds"; \
	cd dbt && DBT_PROFILES_DIR=../ci DBT_CI_DATASET="$$ds" $(UV) run dbt build --target ci_bq

composer-deploy-dags: ## Upload orchestration/dags/tsumugi_daily.py to the Composer 3 DAG bucket
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: set GCP_PROJECT_ID (in .env or env)"; exit 1)
	@dag_prefix=$$(gcloud composer environments describe $(COMPOSER_ENV) \
		--project=$(GCP_PROJECT_ID) \
		--location=$(GCP_REGION) \
		--format="value(config.dagGcsPrefix)"); \
	test -n "$$dag_prefix" || (echo "ERROR: could not resolve dagGcsPrefix for $(COMPOSER_ENV)"; exit 1); \
	gcloud storage cp orchestration/dags/tsumugi_daily.py "$$dag_prefix/tsumugi_daily.py"
