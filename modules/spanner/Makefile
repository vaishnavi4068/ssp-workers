.PHONY: fmt validate test opa

fmt:
	terraform fmt -recursive .

validate: fmt
	terraform init -backend=false -input=false
	terraform validate
	terraform -chdir=modules/instance init -backend=false -input=false
	terraform -chdir=modules/instance validate
	terraform -chdir=modules/database init -backend=false -input=false
	terraform -chdir=modules/database validate

test: validate
	terraform test
	opa test policies -v
