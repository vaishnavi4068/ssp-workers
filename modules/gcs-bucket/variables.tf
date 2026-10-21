variable "project" {
  description = "Target GCP project ID where the GCS bucket will be provisioned."
  type        = string
  default     = "microdrive-dev"
}

variable "name" {
  description = "Globally unique GCS bucket name ({lob}-{env}-{app}-{purpose}-{suffix}, ADR-GLB-006)."
  type        = string
  default     = ""
}

variable "location" {
  description = "Approved GCP region for the bucket. Must be us-east1 only (ADR-SVC-001 GR-1)."
  type        = string
  default     = "us-east1"

  validation {
    condition     = var.location == "us-east1"
    error_message = "location must be 'us-east1'."
  }
}

variable "storage_class" {
  description = "Storage class of the GCS bucket."
  type        = string
  default     = "STANDARD"
}

variable "uniform_bucket_level_access" {
  description = "Enforce uniform bucket-level access (GCS-UBLA-001)."
  type        = bool
  default     = true
}

variable "public_access_prevention" {
  description = "Enforce public access prevention on the bucket (GCS-PAP-001)."
  type        = string
  default     = "enforced"
}

variable "versioning" {
  description = "Whether object versioning is enabled on the bucket."
  type        = bool
  default     = false
}

variable "autoclass" {
  description = "Whether Autoclass is enabled on the bucket (must be false)."
  type        = bool
  default     = false
}

variable "kms_key_name" {
  description = "Cloud KMS CMEK CryptoKey resource ID in us-east1 (90-day rotation)."
  type        = string
  default     = "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek"
}

variable "default_kms_key_name" {
  description = "Cloud KMS CMEK CryptoKey resource ID in locations/us-east1 (GCS-ENC-001)."
  type        = string
  default     = ""
}

variable "retention_policy" {
  description = "Optional bucket retention policy in seconds (not locked by default, GCS-RET-003)."
  type = object({
    retention_period = number
    is_locked        = optional(bool, false)
  })
  default  = null
  nullable = true
}

variable "soft_delete_policy" {
  description = "Soft-delete retention window in seconds (default 604800 = 7 days, GCS-SDP-001)."
  type = object({
    retention_duration_seconds = number
  })
  default = {
    retention_duration_seconds = 604800
  }
}

variable "lifecycle_rules" {
  description = "Optional user-configurable object lifecycle rules (default []). "
  type = list(object({
    action = object({
      type          = string
      storage_class = optional(string)
    })
    condition = object({
      age                        = optional(number)
      age_days                   = optional(number)
      with_state                 = optional(string)
      num_newer_versions         = optional(number)
      days_since_noncurrent_time = optional(number)
      matches_storage_class      = optional(list(string))
    })
  }))
  default = []
}

variable "labels" {
  description = "8 mandatory labels (sppappid, costcenter, environmenttype, primarylob, datasensitivity, owner, provisionedby, idp_resource_id)."
  type        = map(string)
  default     = {}
}

variable "iam_bindings" {
  description = "Authoritative per-role IAM bindings map."
  type        = map(list(string))
  default     = {}
}

variable "force_destroy" {
  description = "Never force-destroy a bucket with objects in it (must be false, GCS-DEL-001)."
  type        = bool
  default     = false
}

variable "gcs_buckets" {
  description = "Project-level Expected tfvars collection map of GCS buckets (tfvars_collection = 'gcs_buckets', tf_address = 'module.gcs_bucket[\"{name}\"]')."
  type = map(object({
    project                     = string
    name                        = string
    location                    = optional(string, "us-east1")
    storage_class               = optional(string, "STANDARD")
    uniform_bucket_level_access = optional(bool, true)
    public_access_prevention    = optional(string, "enforced")
    versioning                  = optional(bool, false)
    autoclass                   = optional(bool, false)
    default_kms_key_name        = optional(string, "")
    kms_key_name                = optional(string, "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-gcs-cmek")
    retention_policy = optional(object({
      retention_period = number
      is_locked        = optional(bool, false)
    }))
    soft_delete_policy = optional(object({
      retention_duration_seconds = number
    }), { retention_duration_seconds = 604800 })
    lifecycle_rules = optional(list(object({
      action = object({
        type          = string
        storage_class = optional(string)
      })
      condition = object({
        age                        = optional(number)
        age_days                   = optional(number)
        with_state                 = optional(string)
        num_newer_versions         = optional(number)
        days_since_noncurrent_time = optional(number)
        matches_storage_class      = optional(list(string))
      })
    })), [])
    labels        = map(string)
    iam_bindings  = optional(map(list(string)), {})
    force_destroy = optional(bool, false)
  }))
  default = {}
}
