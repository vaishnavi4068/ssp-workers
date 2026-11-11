output "cloud_resource_id" {
  description = "Canonical cloud_resource_id for idp.resource_observed."
  value       = "projects/${var.project}/instances/${google_spanner_instance.this.name}"
}

output "name" {
  description = "Spanner instance name."
  value       = google_spanner_instance.this.name
}

output "observed_projection" {
  description = "Non-sensitive Observed projection (< 2000 bytes) per specs/examples/spanner.gates.json."
  value = {
    exists                       = true
    tf_address                   = "module.spanner_instance[\"${google_spanner_instance.this.name}\"]"
    project                      = var.project
    name                         = google_spanner_instance.this.name
    display_name                 = google_spanner_instance.this.display_name
    config                       = google_spanner_instance.this.config
    edition                      = var.edition
    instance_type                = var.instance_type
    processing_units             = var.processing_units
    num_nodes                    = var.num_nodes
    autoscaling_config           = var.autoscaling_config
    default_backup_schedule_type = var.default_backup_schedule_type
    labels                       = google_spanner_instance.this.labels
    iam_bindings                 = var.iam_bindings
    force_destroy                = var.force_destroy
    tenancy                      = var.tenancy
  }
}
