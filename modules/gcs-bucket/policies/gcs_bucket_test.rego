package idp.gcp.gcs_bucket_test

import data.idp.gcp.gcs_bucket
import rego.v1

valid_requested := {
	"api_version": "idp.spp/v1",
	"kind": "gcs_bucket",
	"action": "create",
	"metadata": {
		"apm_id": "APM12345",
		"project_id": "spp-nonprod-orders-svc",
		"environment": "nonprod",
		"lob": "spp",
		"data_sensitivity": "restricted",
		"data_classification": "proprietary",
		"owner": "bucket.owner@example.com",
		"shared_mailbox": "spp-orders-team@example.com",
		"requested_by": "requester@example.com",
		"cost_center": "000000",
		"tenancy": "dedicated",
		"labels": {"app": "orders"},
	},
	"spec": {
		"name": "spp-nonprod-orders-landing-a1b2",
		"location": "us-east1",
		"storage_class": "STANDARD",
		"versioning": false,
		"lifecycle_rules": [],
		"access": [
			{
				"role": "roles/storage.objectAdmin",
				"members": ["serviceAccount:orders-pipeline@spp-nonprod-orders-svc.iam.gserviceaccount.com"],
			},
			{
				"role": "roles/storage.objectViewer",
				"members": ["group:gcp-spp-nonprod-orders-read@example.com"],
			},
		],
		"purpose": "Landing zone for order events",
	},
}

valid_approved := {
	"api_version": "idp.spp/v1",
	"kind": "gcs_bucket",
	"action": "create",
	"metadata": {
		"apm_id": "APM12345",
		"project_id": "spp-nonprod-orders-svc",
		"environment": "nonprod",
		"lob": "spp",
		"data_sensitivity": "restricted",
		"owner": "bucket.owner@example.com",
		"requested_by": "requester@example.com",
		"cost_center": "000000",
		"labels": {
			"app": "orders",
			"costcenter": "000000",
			"dataclassification": "proprietary",
			"datasensitivity": "restricted",
			"environmenttype": "nonprod",
			"idp_resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
			"owner": "bucket_owner_example_com",
			"primarylob": "spp",
			"provisionedby": "idp",
			"sharedemailaddress": "spp-orders-team_example_com",
			"sppappid": "apm12345",
			"tenancy": "dedicated",
		},
	},
	"spec": {
		"name": "spp-nonprod-orders-landing-a1b2",
		"location": "us-east1",
		"storage_class": "STANDARD",
		"versioning": false,
		"lifecycle_rules": [],
		"access": [
			{
				"role": "roles/storage.objectAdmin",
				"members": ["serviceAccount:orders-pipeline@spp-nonprod-orders-svc.iam.gserviceaccount.com"],
			},
		],
		"purpose": "Landing zone for order events",
		"retention_days": 90,
		"retention_locked": false,
		"uniform_bucket_level_access": true,
		"public_access_prevention": "enforced",
		"kms_key_name": "projects/microdrive-dev/locations/us-east1/keyRings/TODO-keyring/cryptoKeys/TODO-gcs-key",
		"soft_delete_retention_days": 7,
		"force_destroy": false,
	},
	"approval": {
		"gate_key": "gcs_bucket_owner",
	},
}

valid_plan := {
	"resource_changes": [
		{
			"address": "module.gcs_bucket[\"spp-nonprod-orders-landing-a1b2\"].google_storage_bucket.this",
			"type": "google_storage_bucket",
			"change": {
				"actions": ["create"],
				"after": {
					"name": "spp-nonprod-orders-landing-a1b2",
					"location": "us-east1",
					"uniform_bucket_level_access": true,
					"public_access_prevention": "enforced",
					"force_destroy": false,
					"encryption": [
						{
							"default_kms_key_name": "projects/microdrive-dev/locations/us-east1/keyRings/TODO-keyring/cryptoKeys/TODO-gcs-key",
						},
					],
					"labels": {
						"costcenter": "000000",
						"datasensitivity": "restricted",
						"environmenttype": "nonprod",
						"idp_resource_id": "9478bcfc-c2cf-583b-8bd8-096476647840",
						"owner": "bucket_owner_example_com",
						"primarylob": "spp",
						"provisionedby": "idp",
						"sppappid": "apm12345",
					},
				},
			},
		},
	],
}

test_valid_requested_allows if {
	res := gcs_bucket.deny with input as valid_requested
	count(res) == 0
}

test_valid_approved_allows if {
	res := gcs_bucket.deny with input as valid_approved
	count(res) == 0
}

test_valid_plan_allows if {
	res := gcs_bucket.deny with input as valid_plan
	count(res) == 0
}

test_reject_multi_region_us if {
	bad := json.patch(valid_requested, [{"op": "replace", "path": "/spec/location", "value": "US"}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "GCS-LOC-001")
}

test_reject_google_in_name if {
	bad := json.patch(valid_requested, [{"op": "replace", "path": "/spec/name", "value": "spp-nonprod-orders-google-a1b2"}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "GCS-NAME-002")
}

test_reject_reserved_purpose_in_name if {
	bad := json.patch(valid_requested, [{"op": "replace", "path": "/spec/name", "value": "spp-nonprod-orders-restricted-a1b2"}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "NAM-RSV-001")
}

test_reject_environmentsubtype_label if {
	bad := json.patch(valid_approved, [{"op": "add", "path": "/metadata/labels/environmentsubtype", "value": "dev"}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "environmentsubtype")
}

test_reject_7_digit_apm_id_label if {
	bad := json.patch(valid_approved, [{"op": "replace", "path": "/metadata/labels/sppappid", "value": "apm0017299"}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "sppappid")
}

test_reject_force_destroy_in_plan if {
	bad := json.patch(valid_plan, [{"op": "replace", "path": "/resource_changes/0/change/after/force_destroy", "value": true}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "GCS-DEL-001")
}

test_reject_unapproved_destroy_in_plan if {
	bad := json.patch(valid_plan, [{"op": "replace", "path": "/resource_changes/0/change/actions", "value": ["delete"]}])
	res := gcs_bucket.deny with input as bad
	some m in res
	contains(m, "destroy_without_delete_request")
}
