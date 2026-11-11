# ssp-module-spanner

Terraform modules and OPA policies for Cloud Spanner **`spanner_instance`** and **`spanner_database`**, per [ADR-SVC-002](https://github.com/vaishnavi4068/ssp-doc/blob/main/adr/endpoint-service/ADR-SVC-002-spanner-instance-and-database.md).

## Layout

- `modules/instance` — `google_spanner_instance` + instance IAM (`spanner_instances` tfvars map).
- `modules/database` — `google_spanner_database`, optional backup schedule, database IAM (`spanner_databases` tfvars map; keys `{instance}/{name}`).
- `policies/` — `idp.gcp.spanner_instance` and `idp.gcp.spanner_database` Rego packages.
- `schemas/spanner.schema.json` — request validation sketch (closes ADR-SVC-002 OQ-3 gap).
- `examples/` — conftest fixture documents for requested gates.

## Usage (project-level tfvars)

```hcl
module "spanner" {
  source = "git::https://github.com/vaishnavi4068/ssp-module-spanner.git"

  spanner_instances = { ... } # see specs/examples/spanner.gates.json
  spanner_databases = { ... }
}
```

CMEK placeholder (BLK-002): `projects/microdrive-dev/locations/us-east1/keyRings/spp-keyring/cryptoKeys/spp-spanner-cmek`.

## Tests

```bash
make test
```

Runs `terraform fmt`, `validate`, `terraform test` (mock Google provider), and `opa test`.
