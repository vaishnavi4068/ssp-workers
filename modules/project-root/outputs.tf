output "gcs_buckets" {
  value = module.gcs_bucket.gcs_buckets
}

output "spanner_instances" {
  value = { for k, m in module.spanner_instance : k => {
    cloud_resource_id   = m.cloud_resource_id
    name                = m.name
    observed_projection = m.observed_projection
  } }
}

output "spanner_databases" {
  value = { for k, m in module.spanner_database : k => {
    cloud_resource_id   = m.cloud_resource_id
    name                = m.name
    observed_projection = m.observed_projection
  } }
}
