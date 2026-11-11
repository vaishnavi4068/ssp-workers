mock_provider "google" {}

variables {
  project       = "spp-nonprod-orders-svc"
  instance_name = "spp-nonprod-orders"
  display_name  = "SPP Orders (nonprod)"
  config        = "regional-us-east1"
  edition       = "STANDARD"

  processing_units             = 100
  default_backup_schedule_type = "AUTOMATIC"

  labels = {
    app                = "orders"
    costcenter         = "000000"
    dataclassification = "proprietary"
    datasensitivity    = "restricted"
    environmenttype    = "nonprod"
    idp_resource_id    = "3936dbd5-d13d-5d84-ad7a-a1e3a595f213"
    owner              = "data_owner_example_com"
    primarylob         = "spp"
    provisionedby      = "idp"
    sharedemailaddress = "orders-team_example_com"
    sppappid           = "apm0017299"
    tenancy            = "dedicated"
  }

  instance_iam_bindings = {
    "roles/spanner.viewer" = [
      "group:gcp-spp-nonprod-orders-dba@example.com"
    ]
  }

  force_destroy = false
  tenancy       = "dedicated"

  database_name            = "orders"
  version_retention_period = "1h"
  encryption_config = {
    kms_key_names = [
      "projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek"
    ]
  }

  ddl = [
    "CREATE TABLE orders (order_id STRING(36) NOT NULL, created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (order_id)"
  ]

  database_iam_bindings = {
    "roles/spanner.databaseReader" = [
      "group:gcp-spp-nonprod-orders-read@example.com"
    ]
    "roles/spanner.databaseUser" = [
      "serviceAccount:orders-api@spp-nonprod-orders-svc.iam.gserviceaccount.com"
    ]
  }
}

run "root_spanner_instance_and_database" {
  command = plan

  assert {
    condition     = output.spanner_instances["spp-nonprod-orders"].cloud_resource_id == "projects/spp-nonprod-orders-svc/instances/spp-nonprod-orders"
    error_message = "Instance cloud_resource_id must match projects/<p>/instances/<name>."
  }

  assert {
    condition     = output.spanner_instances["spp-nonprod-orders"].observed_projection.config == "regional-us-east1"
    error_message = "SPN-CFG-001: config must be regional-us-east1."
  }

  assert {
    condition     = output.spanner_databases["spp-nonprod-orders/orders"].observed_projection.tf_address == "module.spanner_database[\"spp-nonprod-orders/orders\"]"
    error_message = "Database tf_address must use instance/name key."
  }

  assert {
    condition     = !can(output.spanner_databases["spp-nonprod-orders/orders"].observed_projection.labels)
    error_message = "Quirk 1: database observed projection must not include labels."
  }
}
