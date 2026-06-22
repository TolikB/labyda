# GCP Spot e2-micro infrastructure

This module creates one Spot `e2-micro` in a zonal managed instance group and preserves a separate state disk across
instance recreation. It intentionally does not run `terraform apply` from CI.

Before every plan, refresh the current Spot VM, standard persistent-disk, snapshot-free backup, and expected egress
prices in the official GCP calculator. Pass that value as `estimated_monthly_cost_usd`; Terraform rejects values above
USD 13, leaving USD 2 headroom under the operator's USD 15 budget. This variable is a review gate, not a billing cap.

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -var project_id=PROJECT -var estimated_monthly_cost_usd=CURRENT_ESTIMATE
```

`terraform apply` requires separate operator authorization. After creation, query the current zone, instance name and
ephemeral IP with `gcloud`; do not reuse inventory from a prior run. Deploy native Ubuntu/systemd services only after
the persistent disk is mounted and PostgreSQL `data_directory` points under `/srv/arbitrage-state/postgresql`.
