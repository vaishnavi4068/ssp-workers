output "gcs_buckets" {
  description = "Map of provisioned GCS bucket outputs keyed by bucket name."
  value = {
    for k, m in module.gcs_bucket : k => {
      cloud_resource_id   = m.cloud_resource_id
      name                = m.name
      self_link           = m.self_link
      url                 = m.url
      observed_projection = m.observed_projection
    }
  }
}
