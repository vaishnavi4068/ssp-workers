variable "project" {
  description = "Target GCP project ID where the GCS bucket will be provisioned."
  type        = string

  validation {
    condition     = length(trimspace(var.project)) > 0
    error_message = "project must be a non-empty GCP project ID."
  }
}

variable "name" {
  description = "Globally unique GCS bucket name ({lob}-{env}-{app}-{purpose}-{suffix}, ADR-GLB-006)."
  type        = string

  validation {
    condition = (
      length(var.name) >= 3 &&
      length(var.name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.name)) &&
      !startswith(var.name, "goog") &&
      !strcontains(var.name, "google") &&
      !strcontains(var.name, "g00gle")
    )
    error_message = "name must match ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ (max 63 chars) and must not start with 'goog' or contain 'google'/'g00gle' (GCS-NAME-001, GCS-NAME-002)."
  }
}

variable "location" {
  description = "Approved GCP region for the bucket. Must be us-east1 only (ADR-SVC-001 GR-1, ADR-GLB-007)."
  type        = string
  default     = "us-east1"

  validation {
    condition     = var.location == "us-east1"
    error_message = "location must be 'us-east1' (multi-region 'US' and other regions are forbidden per ADR-SVC-001 GR-1)."
  }
}

variable "storage_class" {
  description = "Storage class of the GCS bucket."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"], var.storage_class)
    error_message = "storage_class must be one of STANDARD, NEARLINE, COLDLINE, or ARCHIVE."
  }
}

variable "uniform_bucket_level_access" {
  description = "Enforce uniform bucket-level access (GCS-UBLA-001)."
  type        = bool
  default     = true

  validation {
    condition     = var.uniform_bucket_level_access == true
    error_message = "uniform_bucket_level_access must be true (GCS-UBLA-001)."
  }
}

variable "public_access_prevention" {
  description = "Enforce public access prevention on the bucket (GCS-PAP-001)."
  type        = string
  default     = "enforced"

  validation {
    condition     = var.public_access_prevention == "enforced"
    error_message = "public_access_prevention must be 'enforced' (GCS-PAP-001)."
  }
}

variable "versioning" {
  description = "Whether object versioning is enabled on the bucket."
  type        = bool
  default     = false
}

variable "autoclass" {
  description = "Whether Autoclass is enabled on the bucket (must be false per ADR-SVC-001)."
  type        = bool
  default     = false

  validation {
    condition     = var.autoclass == false
    error_message = "autoclass must be false (ADR-SVC-001)."
  }
}

variable "kms_key_name" {
  description = "Cloud KMS CMEK CryptoKey resource ID in us-east1 (alias for default_kms_key_name; TODO value from user: projects/microdrive-dev/locations/us-east1/keyRings/<TODO>/cryptoKeys/<TODO>)."
  type        = string
  default     = "projects/microdrive-dev/locations/us-east1/keyRings/TODO-keyring/cryptoKeys/TODO-gcs-key"
}

variable "default_kms_key_name" {
  description = "Cloud KMS CMEK CryptoKey resource ID in locations/us-east1 (GCS-ENC-001, GCS-ENC-002). Overrides kms_key_name when non-empty."
  type        = string
  default     = ""

  validation {
    condition = (
      var.default_kms_key_name == "" ||
      can(regex("^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$", var.default_kms_key_name))
    )
    error_message = "default_kms_key_name must be a valid Cloud KMS CryptoKey ID in locations/us-east1: projects/<project>/locations/us-east1/keyRings/<ring>/cryptoKeys/<key> (GCS-ENC-001)."
  }
}

variable "retention_policy" {
  description = "Optional bucket retention policy in seconds. Not locked by default (is_locked = false per GCS-RET-003; retention_period >= 7776000 [90 days] when set per GCS-RET-001)."
  type = object({
    retention_period = number
    is_locked        = optional(bool, false)
  })
  default  = null
  nullable = true

  validation {
    condition = (
      var.retention_policy == null ||
      (var.retention_policy.retention_period >= 7776000 && coalesce(var.retention_policy.is_locked, false) == false)
    )
    error_message = "When retention_policy is set, retention_period must be >= 7776000 seconds (90 days, GCS-RET-001) and is_locked must be false by default (GCS-RET-003)."
  }
}

variable "soft_delete_policy" {
  description = "Soft-delete retention window in seconds (default 604800 = 7 days, GCS-SDP-001)."
  type = object({
    retention_duration_seconds = number
  })
  default = {
    retention_duration_seconds = 604800
  }

  validation {
    condition     = var.soft_delete_policy.retention_duration_seconds >= 604800
    error_message = "soft_delete_policy.retention_duration_seconds must be >= 604800 seconds (7 days, GCS-SDP-001)."
  }
}

variable "lifecycle_rules" {
  description = "Optional user-configurable object lifecycle rules (default [] / NONE per ADR-SVC-001 REQ-3)."
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

  validation {
    condition = alltrue([
      for r in var.lifecycle_rules :
      contains(["Delete", "SetStorageClass", "AbortIncompleteMultipartUpload"], r.action.type)
    ])
    error_message = "Each lifecycle_rules action.type must be one of Delete, SetStorageClass, or AbortIncompleteMultipartUpload."
  }
}

variable "labels" {
  description = "Resource labels including all 8 mandatory keys (sppappid, costcenter, environmenttype, primarylob, datasensitivity, owner, provisionedby, idp_resource_id) and forbidding environmentsubtype (ADR-GLB-008)."
  type        = map(string)

  validation {
    condition = (
      alltrue([
        for k in [
          "sppappid",
          "costcenter",
          "environmenttype",
          "primarylob",
          "datasensitivity",
          "owner",
          "provisionedby",
          "idp_resource_id"
        ] :
        contains(keys(var.labels), k) && length(trimspace(lookup(var.labels, k, ""))) > 0
      ]) &&
      contains(["nonprod", "prod"], lookup(var.labels, "environmenttype", "")) &&
      can(regex("^apm[0-9]{5}$", lookup(var.labels, "sppappid", ""))) &&
      (!contains(keys(var.labels), "apmid") || can(regex("^apm[0-9]{5}$", lookup(var.labels, "apmid", "")))) &&
      !contains(keys(var.labels), "environmentsubtype") &&
      alltrue([
        for k, v in var.labels :
        can(regex("^[a-z][a-z0-9_-]{0,62}$", k)) && can(regex("^[a-z0-9_-]{1,63}$", v))
      ])
    )
    error_message = "labels must contain all 8 non-empty mandatory keys (sppappid matching ^apm[0-9]{5}$ [lowercase 'apm' + 5 digits], costcenter, environmenttype in nonprod|prod, primarylob, datasensitivity, owner, provisionedby, idp_resource_id), must NOT contain environmentsubtype, and must match GCP label format (LBL-001, LBL-002)."
  }
}

variable "iam_bindings" {
  description = "Authoritative per-role IAM bindings map (role -> list of group: or serviceAccount: members, ADR-SVC-001, ADR-BP-003)."
  type        = map(list(string))
  default     = {}

  validation {
    condition = alltrue(flatten([
      for role, members in var.iam_bindings : [
        for m in members :
        (startswith(m, "group:") || startswith(m, "serviceAccount:")) &&
        !startswith(m, "user:") &&
        m != "allUsers" &&
        m != "allAuthenticatedUsers"
      ]
    ]))
    error_message = "iam_bindings members must only use 'group:' or 'serviceAccount:' principals (no 'user:', 'allUsers', or 'allAuthenticatedUsers', ADR-BP-003 GR-3)."
  }

  validation {
    condition = alltrue([
      for role in keys(var.iam_bindings) :
      contains([
        "roles/storage.objectAdmin",
        "roles/storage.objectViewer",
        "roles/storage.objectCreator",
        "roles/storage.legacyBucketReader"
      ], role)
    ])
    error_message = "iam_bindings roles must be in the bucket role allowlist (roles/storage.objectAdmin, roles/storage.objectViewer, roles/storage.objectCreator, roles/storage.legacyBucketReader; roles/storage.admin is forbidden)."
  }
}

variable "force_destroy" {
  description = "Never force-destroy a bucket with objects in it (must be false per GCS-DEL-001)."
  type        = bool
  default     = false

  validation {
    condition     = var.force_destroy == false
    error_message = "force_destroy must be false; the platform never force-destroys a bucket with objects in it (GCS-DEL-001)."
  }
}
