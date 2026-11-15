# ADR 028: Local Airflow runs the pipeline daily; Composer is only a showcase, and Airflow only starts commands

## Status

Accepted. Shapes #64.

## Context

The pipeline was designed for Cloud Composer (Terraform, `execution_type: gcp`), but Composer bills ~$400–525/month
even when idle, so it was never deployed: the daily pipeline ran from Airflow in Docker on the developer's laptop with
`execution_type: local`, writing to BigLake and BigQuery on GCP. In `gcp` mode, ingest ran inside Airflow as a
`PythonOperator`, so the DAG files imported the ingest code and every Airflow that loads them, Composer included,
needed pyiceberg and the rest installed; the Composer Terraform installed only beautifulsoup4.

## Decision

- **Daily runs:** local Airflow, `execution_type: local`. All compute on the laptop, data on GCP.
- **Composer is a showcase:** switched on for a day or two (`enable_composer` and `enable_network`, both `false` by
  default) to show the whole pipeline runs on the cloud, then switched off.
- **Airflow only starts commands.** Every DAG step runs our code as a separate program, never as an imported function:
  locally a `BashOperator` (`python -m ingest.main <dataset> <scheduled_at>`, `python -m lakehouse.main <dataset>`,
  `python -m refine.main <dataset>`, `dbt ...`); on GCP a Cloud Run job (setup, ingest, dbt) or a Dataproc batch
  (refine). The DAG files import none of the pipeline packages, so Composer needs none of them.
- **Setup and ingest share one Cloud Run job**, `python`, with different args per run, like the one `dbt` job.
  Images and jobs are named after what runs inside them (`dbt`, `dataproc`, `python`), not after a pipeline step.
- **The API keys** the ingest job needs live in Secret Manager; Terraform creates the empty secrets and the access
  rule, and the values are set by hand, so they are in neither Terraform state nor the job's config.

## Considered Options

- **Composer 24/7:** the cloud-native answer, rejected on cost for a single-developer project.
- **A cheap GCP scheduler** (Cloud Scheduler + Workflows): a few $/month, but a second orchestration codebase next to
  the Airflow DAGs.
- **Installing our packages in Composer** instead of Cloud Run jobs: one Terraform line, but Composer would run our
  code and its package versions would have to match Composer's image.

## Consequences

- The pipeline runs only while the laptop is on.
- `gcp` mode is exercised only during a showcase, so it can rot unnoticed between showcases; CI's DAG import tests are
  its only daily guard.
- Refine stays on Dataproc in `gcp` mode, so a showcase also needs `enable_network = true` (VPC and Cloud NAT).
