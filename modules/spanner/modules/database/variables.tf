variable "project" {
  description = "Target GCP project ID."
  type        = string
}

variable "instance" {
  description = "Parent Spanner instance name."
  type        = string
}

variable "name" {
  description = "Database name ({app}[_{purpose}], snake_case, max 30 chars, SPN-NAME-002)."
  type        = string

  validation {
    condition = (
      length(var.name) >= 1 &&
      length(var.name) <= 30 &&
      can(regex("^[a-z][a-z0-9_]*$", var.name))
    )
    error_message = "name must be snake_case alphanumeric (SPN-NAME-002), max 30 characters."
  }
}

variable "database_dialect" {
  description = "Spanner database dialect."
  type        = string
  default     = "GOOGLE_STANDARD_SQL"
}

variable "version_retention_period" {
  description = "Version retention period (e.g. 1h, 7d)."
  type        = string
  default     = "1h"
}

variable "encryption_config" {
  description = "CMEK configuration (SPN-ENC-001); kms_key_names must be in us-east1."
  type = object({
    kms_key_names = list(string)
  })
  default = {
    kms_key_names = []
  }
}

variable "default_kms_key_name" {
  description = "Fallback CMEK when encryption_config.kms_key_names is empty."
  type        = string
  default     = "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek"
}

variable "deletion_protection" {
  description = "Terraform-side delete guard (must be true, SPN-DEL-002)."
  type        = bool
  default     = true

  validation {
    condition     = var.deletion_protection == true
    error_message = "deletion_protection must be true (SPN-DEL-002)."
  }
}

variable "enable_drop_protection" {
  description = "GCP drop protection (must be true, SPN-DEL-002)."
  type        = bool
  default     = true

  validation {
    condition     = var.enable_drop_protection == true
    error_message = "enable_drop_protection must be true (SPN-DEL-002)."
  }
}

variable "ddl" {
  description = "Initial DDL statements applied at create; drift-ignored thereafter (Quirk 3)."
  type        = list(string)
  default     = []
}

variable "backup_schedule" {
  description = "Optional explicit backup schedule (SPN-BKP-002 for Tier 1 / prod); null for Tier 2-4 nonprod."
  type = object({
    cron_spec          = string # cron text, e.g. "0 2 * * *"
    retention_duration = string # e.g. "31622400s"
  })
  default  = null
  nullable = true
}

variable "iam_bindings" {
  description = "Authoritative per-role IAM bindings on the database."
  type        = map(list(string))
  default     = {}
}
