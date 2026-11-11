package idp.gcp.spanner_instance

import rego.v1

mandatory_labels := {
	"sppappid",
	"costcenter",
	"environmenttype",
	"primarylob",
	"datasensitivity",
	"owner",
	"provisionedby",
	"idp_resource_id",
}

database_data_roles := {
	"roles/spanner.databaseReader",
	"roles/spanner.databaseUser",
	"roles/spanner.databaseAdmin",
}

allowed_instance_roles := {
	"roles/spanner.viewer",
	"roles/spanner.databaseReader",
	"roles/spanner.admin",
}

is_spanner_instance_doc if {
	input.kind == "spanner_instance"
}

is_approved_doc if {
	is_spanner_instance_doc
	input.approval != null
}

deny contains msg if {
	is_spanner_instance_doc
	name := object.get(input.spec, "name", "")
	not regex.match(`^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$`, name)
	msg := sprintf("SPN-NAME-001: instance name '%v' is invalid (max 64, {lob}-{env}-{app})", [name])
}

deny contains msg if {
	is_spanner_instance_doc
	cfg := object.get(input.spec, "config", "regional-us-east1")
	cfg != "regional-us-east1"
	msg := sprintf("SPN-CFG-001: config must be 'regional-us-east1' (got '%v')", [cfg])
}

deny contains msg if {
	is_spanner_instance_doc
	cfg := lower(object.get(input.spec, "config", ""))
	cfg in {"nam3", "regional-us-east4"}
	msg := sprintf("SPN-CFG-001: forbidden config '%v' (us-east1 regional only)", [cfg])
}

deny contains msg if {
	is_approved_doc
	labels := object.get(input.metadata, "labels", {})
	some label in mandatory_labels
	not label in object.keys(labels)
	msg := sprintf("LBL-001: missing mandatory label '%v'", [label])
}

deny contains msg if {
	is_spanner_instance_doc
	object.get(input.metadata, "tenancy", "") == "shared"
	some b in object.get(input.spec, "access", [])
	b.role in database_data_roles
	msg := sprintf("SPN-IAM-001: role '%v' denied on shared tenancy instance", [b.role])
}

deny contains msg if {
	is_spanner_instance_doc
	some b in object.get(input.spec, "access", [])
	not b.role in allowed_instance_roles
	msg := sprintf("spanner_instance: role '%v' not in allowlist", [b.role])
}

deny contains msg if {
	is_approved_doc
	pu := object.get(object.get(input.spec, "compute", {}), "processing_units", 0)
	auto := object.get(input.spec, "autoscaling_config", null)
	auto == null
	not valid_processing_units(pu)
	msg := sprintf("ADR-SVC-002 REQ-3: processing_units %v invalid (100..900 step 100 or multiple of 1000)", [pu])
}

valid_processing_units(pu) if {
	pu >= 100
	pu <= 900
	pu % 100 == 0
}

valid_processing_units(pu) if {
	pu >= 1000
	pu % 1000 == 0
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "force_destroy", false) == true
	msg := "SPN-DEL-001: force_destroy must be false"
}
