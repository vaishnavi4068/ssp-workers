package idp.gcp.spanner_database_test

import data.idp.gcp.spanner_database
import rego.v1

valid_requested := {
	"api_version": "idp.spp/v1",
	"kind": "spanner_database",
	"action": "create",
	"metadata": {
		"apm_id": "apm0017299",
		"project_id": "spp-nonprod-orders-svc",
		"environment": "nonprod",
	},
	"spec": {
		"instance": "spp-nonprod-orders",
		"name": "orders",
		"dialect": "GOOGLE_STANDARD_SQL",
		"version_retention_period": "1h",
		"access": [
			{
				"role": "roles/spanner.databaseReader",
				"members": ["group:gcp-spp-nonprod-orders-read@example.com"],
			},
		],
	},
}

valid_approved := {
	"api_version": "idp.spp/v1",
	"kind": "spanner_database",
	"action": "create",
	"approval": {},
	"metadata": {
		"apm_id": "apm0017299",
		"project_id": "spp-nonprod-orders-svc",
		"environment": "nonprod",
	},
	"spec": {
		"instance": "spp-nonprod-orders",
		"name": "orders_history",
		"database_dialect": "GOOGLE_STANDARD_SQL",
		"version_retention_period": "1h",
		"deletion_protection": true,
		"enable_drop_protection": true,
		"encryption_config": {
			"kms_key_names": [
				"projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek",
			],
		},
	},
}

test_valid_requested_no_deny if {
	res := spanner_database.deny with input as valid_requested
	count(res) == 0
}

test_spn_name_002_rejects_hyphen if {
	bad := object.union(valid_requested, {"spec": object.union(valid_requested.spec, {"name": "orders-history"})})
	res := spanner_database.deny with input as bad
	some msg in res
	contains(msg, "hyphenated")
}

test_spn_name_002_accepts_snake_case if {
	doc := object.union(valid_requested, {"spec": object.union(valid_requested.spec, {"name": "orders_history"})})
	res := spanner_database.deny with input as doc
	count(res) == 0
}

test_approved_has_kms if {
	res := spanner_database.deny with input as valid_approved
	count(res) == 0
}
