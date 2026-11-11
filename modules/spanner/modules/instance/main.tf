resource "google_spanner_instance" "this" {
  project       = var.project
  name          = var.name
  config        = var.config
  edition       = var.edition
  labels        = var.labels
  force_destroy = var.force_destroy

  display_name = var.display_name

  processing_units = var.autoscaling_config == null && var.num_nodes == null ? var.processing_units : null
  num_nodes        = var.num_nodes

  default_backup_schedule_type = var.default_backup_schedule_type

  dynamic "autoscaling_config" {
    for_each = var.autoscaling_config != null ? [var.autoscaling_config] : []
    content {
      autoscaling_limits {
        min_processing_units = autoscaling_config.value.min_processing_units
        max_processing_units = autoscaling_config.value.max_processing_units
      }
      autoscaling_targets {
        high_priority_cpu_utilization_percent = autoscaling_config.value.high_priority_cpu_utilization_percent
        storage_utilization_percent           = autoscaling_config.value.storage_utilization_percent
      }
    }
  }

  lifecycle {
    precondition {
      condition     = var.config == "regional-us-east1"
      error_message = "config must be 'regional-us-east1' only (SPN-CFG-001, ADR-SVC-002 GR-1)."
    }
    precondition {
      condition     = var.force_destroy == false
      error_message = "force_destroy must be false (SPN-DEL-001)."
    }
    precondition {
      condition = (
        var.autoscaling_config != null ||
        (var.processing_units >= 100 && var.processing_units <= 900 && var.processing_units % 100 == 0) ||
        var.processing_units % 1000 == 0
      )
      error_message = "processing_units must be 100..900 in steps of 100 or a multiple of 1000 when not autoscaled (ADR-SVC-002 REQ-3)."
    }
  }
}

resource "google_spanner_instance_iam_binding" "bindings" {
  for_each = var.iam_bindings

  instance = google_spanner_instance.this.name
  project  = var.project
  role     = each.key
  members  = each.value
}
