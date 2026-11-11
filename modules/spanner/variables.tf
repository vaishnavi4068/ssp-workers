variable "project" {
  description = "Default GCP project ID."
  type        = string
  default     = "spp-nonprod-orders-svc"
}

# --- Single-resource convenience inputs (mirror gcs-bucket root) ---
variable "instance_name" {
  type    = string
  default = ""
}

variable "display_name" {
  type    = string
  default = "SPP Orders (nonprod)"
}

variable "config" {
  type    = string
  default = "regional-us-east1"
}

variable "edition" {
  type    = string
  default = "STANDARD"
}

variable "instance_type" {
  type    = string
  default = "PROVISIONED"
}

variable "processing_units" {
  type    = number
  default = 100
}

variable "num_nodes" {
  type     = number
  default  = null
  nullable = true
}

variable "autoscaling_config" {
  type = object({
    min_processing_units                  = number
    max_processing_units                  = number
    high_priority_cpu_utilization_percent = optional(number, 65)
    storage_utilization_percent           = optional(number, 95)
  })
  default  = null
  nullable = true
}

variable "default_backup_schedule_type" {
  type    = string
  default = "AUTOMATIC"
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "instance_iam_bindings" {
  type    = map(list(string))
  default = {}
}

variable "force_destroy" {
  type    = bool
  default = false
}

variable "tenancy" {
  type    = string
  default = "dedicated"
}

variable "database_name" {
  type    = string
  default = ""
}

variable "database_dialect" {
  type    = string
  default = "GOOGLE_STANDARD_SQL"
}

variable "version_retention_period" {
  type    = string
  default = "1h"
}

variable "encryption_config" {
  type = object({
    kms_key_names = list(string)
  })
  default = {
    kms_key_names = []
  }
}

variable "default_kms_key_name" {
  type    = string
  default = "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek"
}

variable "deletion_protection" {
  type    = bool
  default = true
}

variable "enable_drop_protection" {
  type    = bool
  default = true
}

variable "ddl" {
  type    = list(string)
  default = []
}

variable "backup_schedule" {
  type = object({
    cron_spec          = string
    retention_duration = string
  })
  default  = null
  nullable = true
}

variable "database_iam_bindings" {
  type    = map(list(string))
  default = {}
}

variable "spanner_instances" {
  description = "Expected tfvars map spanner_instances (tf_address = module.spanner_instance[\"{name}\"])."
  type = map(object({
    project          = string
    name             = string
    display_name     = string
    config           = optional(string, "regional-us-east1")
    edition          = optional(string, "STANDARD")
    instance_type    = optional(string, "PROVISIONED")
    processing_units = optional(number, 100)
    num_nodes        = optional(number)
    autoscaling_config = optional(object({
      min_processing_units                  = number
      max_processing_units                  = number
      high_priority_cpu_utilization_percent = optional(number, 65)
      storage_utilization_percent           = optional(number, 95)
    }))
    default_backup_schedule_type = optional(string, "AUTOMATIC")
    labels                       = map(string)
    iam_bindings                 = optional(map(list(string)), {})
    force_destroy                = optional(bool, false)
    tenancy                      = optional(string, "dedicated")
  }))
  default = {}
}

variable "spanner_databases" {
  description = "Expected tfvars map spanner_databases (key = \"{instance}/{name}\")."
  type = map(object({
    project                  = string
    instance                 = string
    name                     = string
    database_dialect         = optional(string, "GOOGLE_STANDARD_SQL")
    version_retention_period = optional(string, "1h")
    encryption_config = optional(object({
      kms_key_names = list(string)
    }), { kms_key_names = [] })
    default_kms_key_name   = optional(string, "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek")
    deletion_protection    = optional(bool, true)
    enable_drop_protection = optional(bool, true)
    ddl                    = optional(list(string), [])
    backup_schedule = optional(object({
      cron_spec          = string
      retention_duration = string
    }))
    iam_bindings = optional(map(list(string)), {})
  }))
  default = {}
}
