package idp.gcp.spanner_instance_test

import data.idp.gcp.spanner_instance
import rego.v1

valid_requested := {
	"api_version": "idp.spp/v1",
	"kind": "spanner_instance",
	"action": "create",
	"metadata": {
		"apm_id": "apm0017299",
		"project_id": "spp-nonprod-orders-svc",
		"environment": "nonprod",
		"lob": "spp",
		"tenancy": "dedicated",
	},
	"spec": {
		"name": "spp-nonprod-orders",
		"display_name": "SPP Orders (nonprod)",
		"config": "regional-us-east1",
		"compute": {"processing_units": 100},
		"access": [
			{
				"role": "roles/spanner.viewer",
				"members": ["group:gcp-spp-nonprod-orders-dba@example.com"],
			},
		],
	},
}

test_valid_requested_no_deny if {
	res := spanner_instance.deny with input as valid_requested
	count(res) == 0
}

test_spn_iam_001_shared_denies_db_reader if {
	shared_metadata := object.union(valid_requested.metadata, {"tenancy": "shared"})
	shared_spec := object.union(valid_requested.spec, {"access": [{"role": "roles/spanner.databaseReader", "members": ["group:x@example.com"]}]})
	shared_doc := object.union(valid_requested, {"metadata": shared_metadata, "spec": shared_spec})
	res := spanner_instance.deny with input as shared_doc
	some msg in res
	contains(msg, "SPN-IAM-001")
}

test_spn_cfg_rejects_nam3 if {
	bad := object.union(valid_requested, {"spec": object.union(valid_requested.spec, {"config": "nam3"})})
	res := spanner_instance.deny with input as bad
	count(res) > 0
}
