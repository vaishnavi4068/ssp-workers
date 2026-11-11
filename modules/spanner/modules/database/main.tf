locals {
  kms_key_names          = try(var.encryption_config.kms_key_names, [])
  effective_kms_key_name = length(local.kms_key_names) > 0 ? local.kms_key_names[0] : var.default_kms_key_name
}

resource "google_spanner_database" "this" {
  project  = var.project
  instance = var.instance
  name     = var.name

  database_dialect         = var.database_dialect
  version_retention_period = var.version_retention_period
  deletion_protection      = var.deletion_protection
  enable_drop_protection   = var.enable_drop_protection
  ddl                      = var.ddl

  dynamic "encryption_config" {
    for_each = length(trimspace(local.effective_kms_key_name)) > 0 ? [local.effective_kms_key_name] : []
    content {
      kms_key_name = encryption_config.value
    }
  }

  lifecycle {
    ignore_changes = [ddl]
    precondition {
      condition     = var.database_dialect == "GOOGLE_STANDARD_SQL"
      error_message = "database_dialect must be GOOGLE_STANDARD_SQL."
    }
    precondition {
      condition     = var.deletion_protection == true && var.enable_drop_protection == true
      error_message = "deletion_protection and enable_drop_protection must both be true (SPN-DEL-002)."
    }
    precondition {
      condition     = can(regex("^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$", local.effective_kms_key_name))
      error_message = "CMEK key must be a CryptoKey URI in locations/us-east1 (SPN-ENC-001)."
    }
  }
}

resource "google_spanner_backup_schedule" "this" {
  count = var.backup_schedule != null ? 1 : 0

  project            = var.project
  instance           = var.instance
  database           = google_spanner_database.this.name
  name               = "${var.name}-daily"
  retention_duration = var.backup_schedule.retention_duration

  spec {
    cron_spec {
      text = var.backup_schedule.cron_spec
    }
  }

  full_backup_spec {}

  encryption_config {
    encryption_type = "USE_DATABASE_ENCRYPTION"
  }
}

resource "google_spanner_database_iam_binding" "bindings" {
  for_each = var.iam_bindings

  project  = var.project
  instance = var.instance
  database = google_spanner_database.this.name
  role     = each.key
  members  = each.value
}
