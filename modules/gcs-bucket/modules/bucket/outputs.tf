output "cloud_resource_id" {
  description = "Canonical cloud_resource_id for idp.resource_observed (projects/_/buckets/<name>, ADR-SVC-001)."
  value       = "projects/_/buckets/${google_storage_bucket.this.name}"
}

output "name" {
  description = "Name of the provisioned GCS bucket."
  value       = google_storage_bucket.this.name
}

output "self_link" {
  description = "URI of the created GCS bucket."
  value       = google_storage_bucket.this.self_link
}

output "url" {
  description = "Base gs:// URL of the bucket."
  value       = google_storage_bucket.this.url
}

output "observed_projection" {
  description = "Non-sensitive Observed projection (< 2000 bytes) matching specs/examples/gcs_bucket.gates.json."
  value = {
    exists                      = true
    tf_address                  = "module.gcs_bucket[\"${google_storage_bucket.this.name}\"]"
    self_link                   = google_storage_bucket.this.self_link
    url                         = google_storage_bucket.this.url
    project                     = google_storage_bucket.this.project
    name                        = google_storage_bucket.this.name
    location                    = lower(google_storage_bucket.this.location)
    storage_class               = google_storage_bucket.this.storage_class
    uniform_bucket_level_access = google_storage_bucket.this.uniform_bucket_level_access
    public_access_prevention    = google_storage_bucket.this.public_access_prevention
    versioning                  = var.versioning
    autoclass                   = var.autoclass
    default_kms_key_name        = local.effective_kms_key_name
    retention_policy = var.retention_policy != null && !var.versioning ? {
      retention_period = var.retention_policy.retention_period
      is_locked        = coalesce(var.retention_policy.is_locked, false)
    } : null
    soft_delete_policy = {
      retention_duration_seconds = var.soft_delete_policy.retention_duration_seconds
    }
    lifecycle_rules = var.lifecycle_rules
    labels          = google_storage_bucket.this.labels
    iam_bindings    = var.iam_bindings
  }
}
