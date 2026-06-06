#!/bin/bash
set -e

PROJECT_ID="ai-compute-arbitrage-monitor"
POOL_ID="gh-wif-pool"
PROVIDER_ID="gh-wif-provider"
TF_RESOURCE_POOL="google_iam_workload_identity_pool.github_workload_identity_pool"
TF_RESOURCE_PROVIDER="google_iam_workload_identity_pool_provider.github_workload_identity_pool_provider"

echo "==> Undeleting WIF pool: $POOL_ID"
gcloud iam workload-identity-pools undelete "$POOL_ID" \
  --location=global \
  --project="$PROJECT_ID"

echo "==> Undeleting WIF provider: $PROVIDER_ID"
gcloud iam workload-identity-pools providers undelete "$PROVIDER_ID" \
  --workload-identity-pool="$POOL_ID" \
  --location=global \
  --project="$PROJECT_ID"

echo "==> Importing pool into Terraform state"
terraform import "$TF_RESOURCE_POOL" \
  "projects/${PROJECT_ID}/locations/global/workloadIdentityPools/${POOL_ID}"

echo "==> Importing provider into Terraform state"
terraform import "$TF_RESOURCE_PROVIDER" \
  "projects/${PROJECT_ID}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"

echo "==> Done. Run 'terraform plan' to verify state."