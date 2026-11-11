output "spanner_instances" {
  description = "Provisioned Spanner instance outputs keyed by instance name."
  value = {
    for k, m in module.spanner_instance : k => {
      cloud_resource_id   = m.cloud_resource_id
      name                = m.name
      observed_projection = m.observed_projection
    }
  }
}

output "spanner_databases" {
  description = "Provisioned Spanner database outputs keyed by instance/name."
  value = {
    for k, m in module.spanner_database : k => {
      cloud_resource_id   = m.cloud_resource_id
      name                = m.name
      observed_projection = m.observed_projection
    }
  }
}
