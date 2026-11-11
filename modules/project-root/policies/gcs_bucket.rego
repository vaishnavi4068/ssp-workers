package idp.gcp.gcs_bucket

import rego.v1

# ---------------------------------------------------------------------------
# SPP GCS Bucket Policy Rules (ADR-SVC-001, ADR-GLB-006..008)
#Evaluated at:
# - requested (input.kind == "gcs_bucket", no input.approval)
# - approved  (input.kind == "gcs_bucket", input.approval present)
# - plan      (input.resource_changes present, terraform show -json planfile)
# ---------------------------------------------------------------------------

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

allowed_storage_classes := {"STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"}

allowed_roles := {
	"roles/storage.objectAdmin",
	"roles/storage.objectViewer",
	"roles/storage.objectCreator",
	"roles/storage.legacyBucketReader",
}

reserved_purposes := {
	"non-prod",
	"nonprod",
	"prod",
	"sandbox",
	"dev",
	"qa",
	"test",
	"pdev",
	"pqa",
	"preprod",
	"restricted",
	"unrestricted",
}

secret_key_names := {
	"password",
	"passwd",
	"client_secret",
	"private_key",
	"secret_value",
	"api_key",
	"token",
}

is_gcs_doc if {
	input.kind == "gcs_bucket"
}

is_approved_doc if {
	is_gcs_doc
	input.approval != null
}

is_tf_plan if {
	is_array(input.resource_changes)
}

# --- Global Secret Check (INV-1) ---
deny contains msg if {
	walk(input, [path, val])
	some k in object.keys(val)
	lower(k) in secret_key_names
	msg := sprintf("INV-1: secret key '%v' is forbidden at path %v", [k, path])
}

# --- Region Enforcement (ADR-SVC-001 GR-1) ---
deny contains msg if {
	is_gcs_doc
	lower(object.get(input.spec, "location", "")) != "us-east1"
	msg := sprintf("GCS-LOC-001: spec.location must be 'us-east1' (got '%v')", [object.get(input.spec, "location", "")])
}

# --- Naming Rules (GCS-NAME-001, GCS-NAME-002, NAM-RSV-001) ---
deny contains msg if {
	is_gcs_doc
	input.action != "import"
	name := object.get(input.spec, "name", "")
	not regex.match(`^[a-z][a-z0-9]{1,5}-(nonprod|prod)-[a-z][a-z0-9]{1,11}-[a-z][a-z0-9]*(-[a-z0-9]+)*-[a-z0-9]{4}$`, name)
	msg := sprintf("GCS-NAME-001: bucket name '%v' does not match {lob}-{env}-{app}-{purpose}-{suffix} with env in nonprod|prod", [name])
}

deny contains msg if {
	is_gcs_doc
	input.action != "import"
	name := object.get(input.spec, "name", "")
	env := object.get(input.metadata, "environment", "")
	parts := split(name, "-")
	count(parts) >= 2
	parts[1] != env
	msg := sprintf("GCS-NAME-001: bucket name env token '%v' does not match metadata.environment '%v'", [parts[1], env])
}

deny contains msg if {
	is_gcs_doc
	name := lower(object.get(input.spec, "name", ""))
	startswith(name, "goog")
	msg := sprintf("GCS-NAME-002: bucket name '%v' must not start with 'goog'", [name])
}

deny contains msg if {
	is_gcs_doc
	name := lower(object.get(input.spec, "name", ""))
	some forbidden_sub in ["google", "g00gle"]
	contains(name, forbidden_sub)
	msg := sprintf("GCS-NAME-002: bucket name '%v' must not contain '%v'", [name, forbidden_sub])
}

deny contains msg if {
	is_gcs_doc
	input.action != "import"
	name := object.get(input.spec, "name", "")
	parts := split(name, "-")
	count(parts) >= 5
	purpose_parts := array.slice(parts, 3, count(parts) - 1)
	purpose_token := concat("-", purpose_parts)
	purpose_token in reserved_purposes
	msg := sprintf("NAM-RSV-001: purpose token '%v' in bucket name '%v' is a reserved word", [purpose_token, name])
}

# --- Storage Class ---
deny contains msg if {
	is_gcs_doc
	sc := object.get(input.spec, "storage_class", "STANDARD")
	not sc in allowed_storage_classes
	msg := sprintf("gcs_bucket: invalid storage_class '%v'", [sc])
}

# --- IAM Allowlists (ADR-SVC-001 REQ-2, ADR-BP-003 GR-3) ---
deny contains msg if {
	is_gcs_doc
	some b in object.get(input.spec, "access", [])
	not b.role in allowed_roles
	msg := sprintf("GCS-IAM-001: role '%v' is not in the GCS bucket allowlist", [b.role])
}

deny contains msg if {
	is_gcs_doc
	some b in object.get(input.spec, "access", [])
	some m in b.members
	not startswith(m, "group:")
	not startswith(m, "serviceAccount:")
	msg := sprintf("GCS-IAM-002: IAM member '%v' must start with 'group:' or 'serviceAccount:' (user:/allUsers/allAuthenticatedUsers forbidden)", [m])
}

# --- Label Rules (LBL-001, LBL-002, ADR-GLB-008) ---
deny contains msg if {
	is_gcs_doc
	labels := object.get(input.metadata, "labels", {})
	"environmentsubtype" in object.keys(labels)
	msg := "LBL-001: label 'environmentsubtype' is dropped and forbidden everywhere"
}

deny contains msg if {
	is_approved_doc
	labels := object.get(input.metadata, "labels", {})
	some req_key in mandatory_labels
	val := object.get(labels, req_key, "")
	trim_space(val) == ""
	msg := sprintf("LBL-001: mandatory label '%v' is missing or empty in approved document", [req_key])
}

deny contains msg if {
	is_approved_doc
	labels := object.get(input.metadata, "labels", {})
	env_type := object.get(labels, "environmenttype", "")
	not env_type in {"nonprod", "prod"}
	msg := sprintf("LBL-001: label 'environmenttype' must be 'nonprod' or 'prod' (got '%v')", [env_type])
}

deny contains msg if {
	is_approved_doc
	labels := object.get(input.metadata, "labels", {})
	app_id := object.get(labels, "sppappid", "")
	not regex.match(`^apm[0-9]{5}$`, app_id)
	msg := sprintf("LBL-001: label 'sppappid' ('%v') must match ^apm[0-9]{5}$ (lowercase 'apm' + 5 digits)", [app_id])
}

# --- Approved-Gate Guardrails (GCS-UBLA-001, GCS-PAP-001, GCS-ENC-001, GCS-RET-001/003, GCS-SDP-001, GCS-DEL-001) ---
deny contains msg if {
	is_approved_doc
	input.spec.uniform_bucket_level_access != true
	msg := "GCS-UBLA-001: uniform_bucket_level_access must be true"
}

deny contains msg if {
	is_approved_doc
	input.spec.public_access_prevention != "enforced"
	msg := "GCS-PAP-001: public_access_prevention must be 'enforced'"
}

deny contains msg if {
	is_approved_doc
	kms := object.get(input.spec, "kms_key_name", "")
	not regex.match(`^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$`, kms)
	msg := sprintf("GCS-ENC-001: kms_key_name '%v' must be a Cloud KMS CryptoKey in locations/us-east1", [kms])
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "versioning", false) == false
	object.get(input.spec, "retention_days", 0) < 90
	msg := "GCS-RET-001: retention_days must be >= 90 when versioning is false"
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "versioning", false) == true
	object.get(input.spec, "retention_days", null) != null
	msg := "GCS-RET-001: bucket retention_days cannot be combined with versioning == true"
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "retention_locked", false) != false
	msg := "GCS-RET-003: retention_locked must be false by default"
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "soft_delete_retention_days", 0) < 7
	msg := "GCS-SDP-001: soft_delete_retention_days must be >= 7"
}

deny contains msg if {
	is_approved_doc
	object.get(input.spec, "force_destroy", true) != false
	msg := "GCS-DEL-001: force_destroy must be false"
}

# --- Terraform Plan Gate Checks (stage = 'plan', INV-4, ADR-AUT-002) ---
deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	lower(object.get(after, "location", "")) != "us-east1"
	msg := sprintf("GCS-LOC-001 (plan): bucket '%v' location must be 'us-east1' (got '%v')", [rc.address, object.get(after, "location", "")])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	object.get(after, "uniform_bucket_level_access", false) != true
	msg := sprintf("GCS-UBLA-001 (plan): bucket '%v' uniform_bucket_level_access must be true", [rc.address])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	object.get(after, "public_access_prevention", "") != "enforced"
	msg := sprintf("GCS-PAP-001 (plan): bucket '%v' public_access_prevention must be 'enforced'", [rc.address])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	object.get(after, "force_destroy", true) != false
	msg := sprintf("GCS-DEL-001 (plan): bucket '%v' force_destroy must be false", [rc.address])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	enc := object.get(after, "encryption", [])
	count(enc) == 0
	msg := sprintf("GCS-ENC-001 (plan): bucket '%v' must have CMEK encryption block", [rc.address])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	some enc in object.get(after, "encryption", [])
	kms := object.get(enc, "default_kms_key_name", "")
	not regex.match(`^projects/[^/]+/locations/us-east1/keyRings/[^/]+/cryptoKeys/[^/]+$`, kms)
	msg := sprintf("GCS-ENC-001 (plan): bucket '%v' default_kms_key_name '%v' must be in locations/us-east1", [rc.address, kms])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	labels := object.get(after, "labels", {})
	"environmentsubtype" in object.keys(labels)
	msg := sprintf("LBL-001 (plan): bucket '%v' must not contain label 'environmentsubtype'", [rc.address])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	labels := object.get(after, "labels", {})
	some req_key in mandatory_labels
	val := object.get(labels, req_key, "")
	trim_space(val) == ""
	msg := sprintf("LBL-001 (plan): bucket '%v' is missing mandatory label '%v'", [rc.address, req_key])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	rc.type == "google_storage_bucket"
	actions := rc.change.actions
	not "delete" in actions
	after := rc.change.after
	labels := object.get(after, "labels", {})
	app_id := object.get(labels, "sppappid", "")
	not regex.match(`^apm[0-9]{5}$`, app_id)
	msg := sprintf("LBL-001 (plan): bucket '%v' label 'sppappid' ('%v') must match ^apm[0-9]{5}$ (lowercase 'apm' + 5 digits)", [rc.address, app_id])
}

deny contains msg if {
	is_tf_plan
	some rc in input.resource_changes
	"delete" in rc.change.actions
	object.get(input, "allow_destroy", false) != true
	msg := sprintf("INV-4 / destroy_without_delete_request: resource '%v' has destructive action %v without an explicit human-approved delete request", [rc.address, rc.change.actions])
}
