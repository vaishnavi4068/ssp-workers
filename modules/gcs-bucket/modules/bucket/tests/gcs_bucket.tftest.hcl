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

run "compliant_default_bucket_in_us_east1" {
  command = plan

  assert {
    condition     = google_storage_bucket.this.name == "spp-nonprod-orders-landing-a1b2"
    error_message = "Bucket name must match expected 2.0 naming convention."
  }

  assert {
    condition     = google_storage_bucket.this.location == "us-east1"
    error_message = "Bucket location must be us-east1."
  }

  assert {
    condition     = google_storage_bucket.this.uniform_bucket_level_access == true
    error_message = "uniform_bucket_level_access must be true (GCS-UBLA-001)."
  }

  assert {
    condition     = google_storage_bucket.this.public_access_prevention == "enforced"
    error_message = "public_access_prevention must be enforced (GCS-PAP-001)."
  }

  assert {
    condition     = google_storage_bucket.this.force_destroy == false
    error_message = "force_destroy must be false (GCS-DEL-001)."
  }

  assert {
    condition     = output.cloud_resource_id == "projects/_/buckets/spp-nonprod-orders-landing-a1b2"
    error_message = "cloud_resource_id must equal projects/_/buckets/<name>."
  }
}

run "compliant_bucket_with_optional_lifecycle_rules" {
  command = plan

  variables {
    lifecycle_rules = [
      {
        action = {
          type          = "SetStorageClass"
          storage_class = "NEARLINE"
        }
        condition = {
          age_days = 30
        }
      },
      {
        action = {
          type = "Delete"
        }
        condition = {
          age_days   = 180
          with_state = "ANY"
        }
      }
    ]
  }

  assert {
    condition     = length(output.observed_projection.lifecycle_rules) == 2
    error_message = "Expected 2 optional lifecycle rules."
  }
}

run "reject_multi_region_us_location" {
  command = plan

  variables {
    location = "US"
  }

  expect_failures = [
    var.location,
  ]
}

run "reject_force_destroy_true" {
  command = plan

  variables {
    force_destroy = true
  }

  expect_failures = [
    var.force_destroy,
  ]
}

run "reject_public_access_prevention_inherited" {
  command = plan

  variables {
    public_access_prevention = "inherited"
  }

  expect_failures = [
    var.public_access_prevention,
  ]
}

run "reject_environmentsubtype_label" {
  command = plan

  variables {
    labels = {
      costcenter         = "000000"
      datasensitivity    = "restricted"
      environmenttype    = "nonprod"
      environmentsubtype = "dev"
      idp_resource_id    = "9478bcfc-c2cf-583b-8bd8-096476647840"
      owner              = "bucket_owner_example_com"
      primarylob         = "spp"
      provisionedby      = "idp"
      sppappid           = "apm12345"
    }
  }

  expect_failures = [
    var.labels,
  ]
}

run "reject_missing_mandatory_label" {
  command = plan

  variables {
    labels = {
      costcenter      = "000000"
      datasensitivity = "restricted"
      environmenttype = "nonprod"
      owner           = "bucket_owner_example_com"
      primarylob      = "spp"
      provisionedby   = "idp"
      sppappid        = "apm12345"
    }
  }

  expect_failures = [
    var.labels,
  ]
}

run "reject_7_digit_apm_id_label" {
  command = plan

  variables {
    labels = {
      costcenter      = "000000"
      datasensitivity = "restricted"
      environmenttype = "nonprod"
      idp_resource_id = "9478bcfc-c2cf-583b-8bd8-096476647840"
      owner           = "bucket_owner_example_com"
      primarylob      = "spp"
      provisionedby   = "idp"
      sppappid        = "apm0017299"
    }
  }

  expect_failures = [
    var.labels,
  ]
}

run "reject_user_iam_principal" {
  command = plan

  variables {
    iam_bindings = {
      "roles/storage.objectViewer" = [
        "user:someone@example.com"
      ]
    }
  }

  expect_failures = [
    var.iam_bindings,
  ]
}

run "reject_google_substring_in_bucket_name" {
  command = plan

  variables {
    name = "spp-nonprod-orders-google-a1b2"
  }

  expect_failures = [
    var.name,
  ]
}
