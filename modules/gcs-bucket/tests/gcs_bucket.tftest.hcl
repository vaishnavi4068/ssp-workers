mock_provider "google" {}

variables {
  project       = "spp-nonprod-orders-svc"
  name          = "spp-nonprod-orders-landing-a1b2"
  location      = "us-east1"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  versioning                  = false
  autoclass                   = false
  default_kms_key_name        = "projects/microdrive-dev/locations/us-east1/keyRings/TODO-keyring/cryptoKeys/TODO-gcs-key"

  retention_policy = {
    retention_period = 7776000
    is_locked        = false
  }

  soft_delete_policy = {
    retention_duration_seconds = 604800
  }

  lifecycle_rules = []

  labels = {
    app                = "orders"
    costcenter         = "000000"
    dataclassification = "proprietary"
    datasensitivity    = "restricted"
    environmenttype    = "nonprod"
    idp_resource_id    = "9478bcfc-c2cf-583b-8bd8-096476647840"
    owner              = "bucket_owner_example_com"
    primarylob         = "spp"
    provisionedby      = "idp"
    sharedemailaddress = "spp-orders-team_example_com"
    sppappid           = "apm12345"
    tenancy            = "dedicated"
  }

  iam_bindings = {
    "roles/storage.objectAdmin" = [
      "serviceAccount:orders-pipeline@spp-nonprod-orders-svc.iam.gserviceaccount.com"
    ]
    "roles/storage.objectViewer" = [
      "group:gcp-spp-nonprod-orders-read@example.com"
    ]
  }

  force_destroy = false
}

run "root_wrapper_single_bucket_and_tfvars_collection" {
  command = plan

  assert {
    condition     = output.gcs_buckets["spp-nonprod-orders-landing-a1b2"].cloud_resource_id == "projects/_/buckets/spp-nonprod-orders-landing-a1b2"
    error_message = "Root module gcs_buckets output must include cloud_resource_id."
  }

  assert {
    condition     = output.gcs_buckets["spp-nonprod-orders-landing-a1b2"].observed_projection.tf_address == "module.gcs_bucket[\"spp-nonprod-orders-landing-a1b2\"]"
    error_message = "tf_address must equal module.gcs_bucket[\"<name>\"]."
  }
}
