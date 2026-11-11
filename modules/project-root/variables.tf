variable "gcs_buckets" {
  description = "Project-level GCS bucket tfvars map."
  type        = any
  default     = {}
}

variable "spanner_instances" {
  description = "Project-level Spanner instance tfvars map."
  type        = any
  default     = {}
}

variable "spanner_databases" {
  description = "Project-level Spanner database tfvars map (keys instance/name)."
  type        = any
  default     = {}
}
