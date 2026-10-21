package main

import data.idp.gcp.gcs_bucket
import rego.v1

deny contains msg if {
	some msg in gcs_bucket.deny
}
