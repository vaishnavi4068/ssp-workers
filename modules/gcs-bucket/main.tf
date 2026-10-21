locals {
  effective_gcs_buckets = length(var.gcs_buckets) > 0 ? var.gcs_buckets : (
    length(trimspace(var.name)) > 0 ? {
      (var.name) = {
        project                     = var.project
        name                        = var.name
        location                    = var.location
        storage_class               = var.storage_class
        uniform_bucket_level_access = var.uniform_bucket_level_access
        public_access_prevention    = var.public_access_prevention
        versioning                  = var.versioning
        autoclass                   = var.autoclass
        default_kms_key_name        = var.default_kms_key_name
        kms_key_name                = var.kms_key_name
        retention_policy            = var.retention_policy
        soft_delete_policy          = var.soft_delete_policy
        lifecycle_rules             = var.lifecycle_rules
        labels                      = var.labels
        iam_bindings                = var.iam_bindings
        force_destroy               = var.force_destroy
      }
    } : {}
  )
}

module "gcs_bucket" {
  source   = "./modules/bucket"
  for_each = local.effective_gcs_buckets

  project                     = each.value.project
  name                        = each.value.name
  location                    = each.value.location
  storage_class               = each.value.storage_class
  uniform_bucket_level_access = each.value.uniform_bucket_level_access
  public_access_prevention    = each.value.public_access_prevention
  versioning                  = each.value.versioning
  autoclass                   = each.value.autoclass
  default_kms_key_name        = each.value.default_kms_key_name
  kms_key_name                = each.value.kms_key_name
  retention_policy            = each.value.retention_policy
  soft_delete_policy          = each.value.soft_delete_policy
  lifecycle_rules             = each.value.lifecycle_rules
  labels                      = each.value.labels
  iam_bindings                = each.value.iam_bindings
  force_destroy               = each.value.force_destroy
}
