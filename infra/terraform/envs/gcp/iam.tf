# ── Dataproc Service Account ───────────────────────────────────────────────────

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Serverless Service Account"

  depends_on = [google_project_service.enabled_services["iam"]]
}

resource "google_project_iam_member" "dataproc_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/dataproc.admin",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/artifactregistry.reader",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ── Composer Service Account ───────────────────────────────────────────────────

resource "google_service_account" "composer_sa" {
  account_id   = "composer-sa"
  display_name = "Cloud Composer Service Account"

  depends_on = [google_project_service.enabled_services["iam"]]
}

resource "google_project_iam_member" "composer_roles" {
  for_each = toset([
    "roles/composer.worker",
    "roles/composer.user",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# ── Composer Service Account impersonates Dataproc Service Account ───────────────────────────────────────────────────

resource "google_service_account_iam_member" "composer_impersonates_dataproc" {
  service_account_id = google_service_account.dataproc_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer_sa.email}"
}

# ── GitHub Actions Service Account ─────────────────────────────────────────────

resource "google_service_account" "github_actions_sa" {
  account_id   = "github-actions-sa"
  display_name = "GitHub Actions Service Account"

  depends_on = [google_project_service.enabled_services["iam"]]
}

resource "google_project_iam_member" "github_actions_roles" {
  for_each = toset([
    "roles/artifactregistry.writer",
    "roles/dataproc.editor",
    "roles/storage.objectAdmin",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.github_actions_sa.email}"
}

# ── GitHub Workload Identity Federation ────────────────────────────────────────

resource "google_iam_workload_identity_pool" "github_workload_identity_pool" {
  workload_identity_pool_id = var.gh_wif_pool_id

}

resource "google_iam_workload_identity_pool_provider" "github_workload_identity_pool_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_workload_identity_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = var.gh_wif_pool_provider_id

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.repository == '${var.gh_repository_uri}'"
}

resource "google_service_account_iam_member" "dataproc_wif_grant" {
  service_account_id = google_service_account.github_actions_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_workload_identity_pool.name}/attribute.repository/${var.gh_repository_uri}"
}

resource "google_service_account_iam_member" "composer_wif_grant" {
  service_account_id = google_service_account.composer_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_workload_identity_pool.name}/attribute.repository/${var.gh_repository_uri}"
}
