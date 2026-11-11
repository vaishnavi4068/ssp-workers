variable "project" {
  description = "Target GCP project ID for the Spanner instance."
  type        = string

  validation {
    condition     = length(trimspace(var.project)) > 0
    error_message = "project must be non-empty."
  }
}

variable "name" {
  description = "Spanner instance ID ({lob}-{env}-{app}[-{purpose}], max 64 chars, SPN-NAME-001)."
  type        = string

  validation {
    condition = (
      length(var.name) >= 3 &&
      length(var.name) <= 64 &&
      can(regex("^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$", var.name))
    )
    error_message = "name must match ^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$ and be at most 64 characters (SPN-NAME-001)."
  }
}

variable "display_name" {
  description = "Human-readable display name (SPN-NAME-003, 4..30 chars at Approved gate)."
  type        = string

  validation {
    condition     = length(var.display_name) >= 4 && length(var.display_name) <= 30
    error_message = "display_name must be between 4 and 30 characters (SPN-NAME-003)."
  }
}

variable "config" {
  description = "Instance configuration. Must be regional-us-east1 (SPN-CFG-001)."
  type        = string
  default     = "regional-us-east1"

  validation {
    condition     = var.config == "regional-us-east1"
    error_message = "config must be 'regional-us-east1' (no nam3, no regional-us-east4)."
  }
}

variable "edition" {
  description = "Spanner edition (SPN-EDT-001/002): STANDARD, ENTERPRISE, or ENTERPRISE_PLUS."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "ENTERPRISE", "ENTERPRISE_PLUS"], var.edition)
    error_message = "edition must be STANDARD, ENTERPRISE, or ENTERPRISE_PLUS."
  }
}

variable "instance_type" {
  description = "Instance type label for projections (PROVISIONED or AUTOSCALED)."
  type        = string
  default     = "PROVISIONED"

  validation {
    condition     = contains(["PROVISIONED", "AUTOSCALED"], var.instance_type)
    error_message = "instance_type must be PROVISIONED or AUTOSCALED."
  }
}

variable "processing_units" {
  description = "Provisioned processing units when autoscaling_config is null."
  type        = number
  default     = 100
}

variable "num_nodes" {
  description = "Optional node count (null when using processing_units)."
  type        = number
  default     = null
  nullable    = true
}

variable "autoscaling_config" {
  description = "Optional autoscaling configuration (ENTERPRISE edition in nonprod when set)."
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
  description = "Instance backup schedule type (SPN-BKP-001)."
  type        = string
  default     = "AUTOMATIC"

  validation {
    condition     = var.default_backup_schedule_type == "AUTOMATIC"
    error_message = "default_backup_schedule_type must be AUTOMATIC (SPN-BKP-001)."
  }
}

variable "labels" {
  description = "8 mandatory labels on spanner_instance only (LBL-001, ADR-GLB-008)."
  type        = map(string)
  default     = {}
}

variable "iam_bindings" {
  description = "IAM bindings on the instance (SPN-IAM-001 enforced in Rego for shared tenancy)."
  type        = map(list(string))
  default     = {}
}

variable "force_destroy" {
  description = "Must remain false (SPN-DEL-001)."
  type        = bool
  default     = false

  validation {
    condition     = var.force_destroy == false
    error_message = "force_destroy must be false (SPN-DEL-001)."
  }
}

variable "tenancy" {
  description = "dedicated or shared (SPN-IAM-001)."
  type        = string
  default     = "dedicated"

  validation {
    condition     = contains(["dedicated", "shared"], var.tenancy)
    error_message = "tenancy must be dedicated or shared."
  }
}
