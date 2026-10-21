locals {
  effective_kms_key_name = length(trimspace(var.default_kms_key_name)) > 0 ? var.default_kms_key_name : var.kms_key_name
}

resource "google_storage_bucket" "this" {
  name                        = var.name
  project                     = var.project
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.uniform_bucket_level_access
  public_access_prevention    = var.public_access_prevention
  force_destroy               = var.force_destroy
  labels                      = var.labels

  encryption {
    default_kms_key_name = local.effective_kms_key_name
  }

  versioning {
    enabled = var.versioning
  }

  dynamic "autoclass" {
    for_each = var.autoclass ? [1] : []
    content {
      enabled = true
    }
  }

  dynamic "retention_policy" {
    for_each = var.retention_policy != null && !var.versioning ? [var.retention_policy] : []
    content {
      retention_period = retention_policy.value.retention_period
      is_locked        = coalesce(retention_policy.value.is_locked, false)
    }
  }

  soft_delete_policy {
    retention_duration_seconds = var.soft_delete_policy.retention_duration_seconds
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action.type
        storage_class = lifecycle_rule.value.action.storage_class
      }
      condition {
        age                        = coalesce(lifecycle_rule.value.condition.age, lifecycle_rule.value.condition.age_days)
        with_state                 = lifecycle_rule.value.condition.with_state
        num_newer_versions         = lifecycle_rule.value.condition.num_newer_versions
        days_since_noncurrent_time = lifecycle_rule.value.condition.days_since_noncurrent_time
        matches_storage_class      = lifecycle_rule.value.condition.matches_storage_class
      }
    }
  }

  lifecycle {
    precondition {
      condition     = can(regex("^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$", local.effective_kms_key_name))
      error_message = "Effective CMEK key name must be a valid Cloud KMS CryptoKey URI in locations/us-east1 (GCS-ENC-001)."
    }
    precondition {
      condition     = !(var.versioning && var.retention_policy != null)
      error_message = "Bucket retention_policy and object versioning cannot both be enabled concurrently (ADR-SVC-001 REQ-2)."
    }
  }
}

resource "google_storage_bucket_iam_binding" "bindings" {
  for_each = var.iam_bindings

  bucket  = google_storage_bucket.this.name
  role    = each.key
  members = each.value
}
