package main

import data.idp.gcp.spanner_instance
import data.idp.gcp.spanner_database
import rego.v1

deny contains msg if {
	some msg in spanner_instance.deny
}

deny contains msg if {
	some msg in spanner_database.deny
}
