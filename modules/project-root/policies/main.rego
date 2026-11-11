package main

import data.idp.gcp.gcs_bucket
import data.idp.gcp.spanner_database
import data.idp.gcp.spanner_instance
import rego.v1

deny contains msg if {
	some msg in gcs_bucket.deny
}

deny contains msg if {
	some msg in spanner_instance.deny
}

deny contains msg if {
	some msg in spanner_database.deny
}
