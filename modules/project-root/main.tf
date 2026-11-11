module "gcs_bucket" {
  source = "../gcs-bucket"

  gcs_buckets = var.gcs_buckets
}

module "spanner_instance" {
  source   = "../spanner/modules/instance"
  for_each = var.spanner_instances

  project                      = each.value.project
  name                         = each.value.name
  display_name                 = each.value.display_name
  config                       = lookup(each.value, "config", "regional-us-east1")
  edition                      = lookup(each.value, "edition", "STANDARD")
  instance_type                = lookup(each.value, "instance_type", "PROVISIONED")
  processing_units             = lookup(each.value, "processing_units", 100)
  num_nodes                    = lookup(each.value, "num_nodes", null)
  autoscaling_config           = lookup(each.value, "autoscaling_config", null)
  default_backup_schedule_type = lookup(each.value, "default_backup_schedule_type", "AUTOMATIC")
  labels                       = each.value.labels
  iam_bindings                 = lookup(each.value, "iam_bindings", {})
  force_destroy                = lookup(each.value, "force_destroy", false)
  tenancy                      = lookup(each.value, "tenancy", "dedicated")
}

module "spanner_database" {
  source   = "../spanner/modules/database"
  for_each = var.spanner_databases

  project                  = each.value.project
  instance                 = each.value.instance
  name                     = each.value.name
  database_dialect         = lookup(each.value, "database_dialect", "GOOGLE_STANDARD_SQL")
  version_retention_period = lookup(each.value, "version_retention_period", "1h")
  encryption_config        = lookup(each.value, "encryption_config", { kms_key_names = [] })
  default_kms_key_name     = lookup(
    each.value,
    "default_kms_key_name",
    "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek"
  )
  deletion_protection      = lookup(each.value, "deletion_protection", true)
  enable_drop_protection   = lookup(each.value, "enable_drop_protection", true)
  ddl                      = lookup(each.value, "ddl", [])
  backup_schedule          = lookup(each.value, "backup_schedule", null)
  iam_bindings             = lookup(each.value, "iam_bindings", {})
}
