locals {
  effective_spanner_instances = length(var.spanner_instances) > 0 ? var.spanner_instances : (
    length(trimspace(var.instance_name)) > 0 ? {
      (var.instance_name) = {
        project                      = var.project
        name                         = var.instance_name
        display_name                 = var.display_name
        config                       = var.config
        edition                      = var.edition
        instance_type                = var.instance_type
        processing_units             = var.processing_units
        num_nodes                    = var.num_nodes
        autoscaling_config           = var.autoscaling_config
        default_backup_schedule_type = var.default_backup_schedule_type
        labels                       = var.labels
        iam_bindings                 = var.instance_iam_bindings
        force_destroy                = var.force_destroy
        tenancy                      = var.tenancy
      }
    } : {}
  )

  effective_spanner_databases = length(var.spanner_databases) > 0 ? var.spanner_databases : (
    length(trimspace(var.database_name)) > 0 && length(trimspace(var.instance_name)) > 0 ? {
      ("${var.instance_name}/${var.database_name}") = {
        project                  = var.project
        instance                 = var.instance_name
        name                     = var.database_name
        database_dialect         = var.database_dialect
        version_retention_period = var.version_retention_period
        encryption_config        = var.encryption_config
        default_kms_key_name     = var.default_kms_key_name
        deletion_protection      = var.deletion_protection
        enable_drop_protection   = var.enable_drop_protection
        ddl                      = var.ddl
        backup_schedule          = var.backup_schedule
        iam_bindings             = var.database_iam_bindings
      }
    } : {}
  )
}

module "spanner_instance" {
  source   = "./modules/instance"
  for_each = local.effective_spanner_instances

  project                      = each.value.project
  name                         = each.value.name
  display_name                 = each.value.display_name
  config                       = each.value.config
  edition                      = each.value.edition
  instance_type                = each.value.instance_type
  processing_units             = each.value.processing_units
  num_nodes                    = each.value.num_nodes
  autoscaling_config           = each.value.autoscaling_config
  default_backup_schedule_type = each.value.default_backup_schedule_type
  labels                       = each.value.labels
  iam_bindings                 = each.value.iam_bindings
  force_destroy                = each.value.force_destroy
  tenancy                      = each.value.tenancy
}

module "spanner_database" {
  source   = "./modules/database"
  for_each = local.effective_spanner_databases

  project                  = each.value.project
  instance                 = each.value.instance
  name                     = each.value.name
  database_dialect         = each.value.database_dialect
  version_retention_period = each.value.version_retention_period
  encryption_config        = each.value.encryption_config
  default_kms_key_name     = each.value.default_kms_key_name
  deletion_protection      = each.value.deletion_protection
  enable_drop_protection   = each.value.enable_drop_protection
  ddl                      = each.value.ddl
  backup_schedule          = each.value.backup_schedule
  iam_bindings             = each.value.iam_bindings
}
