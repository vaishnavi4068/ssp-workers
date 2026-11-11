output "cloud_resource_id" {
  description = "Canonical cloud_resource_id for idp.resource_observed."
  value       = "projects/${var.project}/instances/${var.instance}/databases/${google_spanner_database.this.name}"
}

output "name" {
  description = "Spanner database name."
  value       = google_spanner_database.this.name
}

output "observed_projection" {
  description = "Non-sensitive Observed projection (no labels — Quirk 1)."
  value = {
    exists                   = true
    tf_address               = "module.spanner_database[\"${var.instance}/${google_spanner_database.this.name}\"]"
    project                  = var.project
    instance                 = var.instance
    name                     = google_spanner_database.this.name
    database_dialect         = var.database_dialect
    version_retention_period = var.version_retention_period
    encryption_config = {
      kms_key_names = length(local.kms_key_names) > 0 ? local.kms_key_names : [local.effective_kms_key_name]
    }
    deletion_protection    = var.deletion_protection
    enable_drop_protection = var.enable_drop_protection
    backup_schedule        = var.backup_schedule
    iam_bindings           = var.iam_bindings
  }
}
