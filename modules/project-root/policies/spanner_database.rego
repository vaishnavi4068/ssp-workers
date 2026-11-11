package idp.gcp.spanner_database

import rego.v1

allowed_database_roles := {
	"roles/spanner.databaseReader",
	"roles/spanner.databaseUser",
	"roles/spanner.databaseAdmin",
}

is_spanner_database_doc if {
	input.kind == "spanner_database"
}

is_approved_doc if {
	is_spanner_database_doc
	input.approval != null
}

deny contains msg if {
	is_spanner_database_doc
	name := object.get(input.spec, "name", "")
	not regex.match(`^[a-z][a-z0-9_]*$`, name)
	msg := sprintf("SPN-NAME-002: database name '%v' must be snake_case", [name])
}

deny contains msg if {
	is_spanner_database_doc
	name := object.get(input.spec, "name", "")
	contains(name, "-")
	msg := sprintf("SPN-NAME-002: hyphenated database name '%v' is rejected", [name])
}

deny contains msg if {
	is_spanner_database_doc
	count(object.get(input.spec, "name", "")) > 30
	msg := "SPN-NAME-002: database name exceeds 30 characters"
}

deny contains msg if {
	is_spanner_database_doc
	some b in object.get(input.spec, "access", [])
	not b.role in allowed_database_roles
	msg := sprintf("spanner_database: role '%v' not allowed at database scope", [b.role])
}

deny contains msg if {
	is_approved_doc
	keys := object.keys(object.get(input.metadata, "labels", {}))
	count(keys) > 0
	msg := "Quirk 1: spanner_database must not carry GCP labels in metadata"
}

deny contains msg if {
	is_approved_doc
	kms := object.get(object.get(input.spec, "encryption_config", {}), "kms_key_names", [])
	count(kms) == 0
	msg := "SPN-ENC-001: encryption_config.kms_key_names must be non-empty"
}

deny contains msg if {
	is_approved_doc
	some key in object.get(object.get(input.spec, "encryption_config", {}), "kms_key_names", [])
	not regex.match(`^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$`, key)
	msg := sprintf("SPN-ENC-001: kms key '%v' must be in locations/us-east1", [key])
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "deletion_protection", true) != true
	msg := "SPN-DEL-002: deletion_protection must be true"
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "enable_drop_protection", true) != true
	msg := "SPN-DEL-002: enable_drop_protection must be true"
}
