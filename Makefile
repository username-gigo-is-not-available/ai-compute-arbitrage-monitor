TF_SHARED      := infra/terraform/shared
TF_ENV_DEV     := infra/terraform/envs/dev
TF_ENV_GCP     := infra/terraform/envs/gcp
AIRFLOW_DIR    := infra/airflow

.DEFAULT_GOAL  := help

# ── Help ───────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "GPU Arbitrage Monitor"
	@echo "============================================================================================================="
	@echo ""
	@echo "Terraform - Shared (GCS + BQ, always on)"
	@echo "  make init-shared       Init shared terraform"
	@echo "  make plan-shared       Plan shared infra"
	@echo "  make apply-shared      Provision shared infra"
	@echo "  make destroy-shared    Destroy shared infra"
	@echo "  make output-shared     Show shared outputs"
	@echo ""
	@echo "Terraform - Dev (no extra resources)"
	@echo "  make init-dev          Init dev terraform"
	@echo "  make plan-dev          Plan dev infra"
	@echo "  make apply-dev         Provision dev infra"
	@echo "  make destroy-dev       Destroy dev infra"
	@echo ""
	@echo "Terraform - GCP (Dataproc, VPC, AR, IAM, WIF)"
	@echo "  make init-gcp          Init gcp terraform"
	@echo "  make plan-gcp          Plan full GCP infra"
	@echo "  make apply-gcp         Provision full GCP infra"
	@echo "  make destroy-gcp       Destroy full GCP infra"
	@echo "  make output-gcp        Show gcp outputs"
	@echo ""
	@echo "Airflow"
	@echo "  make up                Start Airflow (docker compose)"
	@echo "  make down              Stop Airflow"
	@echo "  make logs              Tail Airflow logs"
	@echo "  make restart           Restart Airflow"
	@echo ""
	@echo "dbt"
	@echo "  make dbt-run           Run all dbt models"
	@echo "  make dbt-test          Run all dbt tests"
	@echo "  make dbt-docs          Generate and serve dbt docs"
	@echo "============================================================================================================="

# ── Terraform — Shared ────────────────────────────────────────────────────────

.PHONY: init-shared
init-shared:
	cd $(TF_SHARED) && terraform init

.PHONY: plan-shared
plan-shared:
	cd $(TF_SHARED) && terraform plan

.PHONY: apply-shared
apply-shared:
	cd $(TF_SHARED) && terraform apply -auto-approve

.PHONY: destroy-shared
destroy-shared:
	cd $(TF_SHARED) && terraform destroy -auto-approve

.PHONY: output-shared
output-shared:
	cd $(TF_SHARED) && terraform output

# ── Terraform — Dev ───────────────────────────────────────────────────────────

.PHONY: init-dev
init-dev:
	cd $(TF_ENV_DEV) && terraform init

.PHONY: plan-dev
plan-dev:
	cd $(TF_ENV_DEV) && terraform plan

.PHONY: apply-dev
apply-dev:
	cd $(TF_ENV_DEV) && terraform apply -auto-approve

.PHONY: destroy-dev
destroy-dev:
	cd $(TF_ENV_DEV) && terraform destroy -auto-approve

# ── Terraform — GCP ───────────────────────────────────────────────────────────

.PHONY: init-gcp
init-gcp:
	cd $(TF_ENV_GCP) && terraform init

.PHONY: plan-gcp
plan-gcp:
	cd $(TF_ENV_GCP) && terraform plan

.PHONY: apply-gcp
apply-gcp:
	cd $(TF_ENV_GCP) && terraform apply -auto-approve

.PHONY: destroy-gcp
destroy-gcp:
	cd $(TF_ENV_GCP) && terraform destroy -auto-approve

.PHONY: output-gcp
output-gcp:
	cd $(TF_ENV_GCP) && terraform output

# ── Airflow ───────────────────────────────────────────────────────────────────

.PHONY: up
up:
	cd $(AIRFLOW_DIR) && docker compose --env-file ../../.env up

.PHONY: build
build:
	cd $(AIRFLOW_DIR) && docker compose --env-file ../../.env up --build

.PHONY: down
down:
	cd $(AIRFLOW_DIR) && docker compose --env-file ../../.env down

.PHONY: logs
logs:
	cd $(AIRFLOW_DIR) && docker compose logs -f

.PHONY: restart
restart: down up

# ── dbt ───────────────────────────────────────────────────────────────────────

DBT := cd src/transform &&

.PHONY: dbt-run
dbt-run:
	$(DBT) dbt run

.PHONY: dbt-test
dbt-test:
	$(DBT) dbt test

.PHONY: dbt-docs
dbt-docs:
	$(DBT) dbt docs generate && dbt docs serve
