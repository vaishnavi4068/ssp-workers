.PHONY: test go-test go-build tf-validate lint-helm deploy

go-test:
	go test ./...

go-build:
	go build -o bin/provisioning-worker ./cmd/provisioning-worker

PROJECT_ID ?= microdrive-dev
REGION ?= us-east1
CLUSTER ?= md-dev
NAMESPACE ?= spp-platform
REGISTRY ?= us-east1-docker.pkg.dev/$(PROJECT_ID)/spp
IMAGE_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo v1.0.0)

test:
	PYTHONPATH=. pytest tests/ -v

tf-validate:
	terraform -chdir=modules/gcs-bucket/modules/bucket fmt -check -recursive
	terraform -chdir=modules/gcs-bucket/modules/bucket init -backend=false -input=false
	terraform -chdir=modules/gcs-bucket/modules/bucket validate
	terraform -chdir=modules/gcs-bucket/modules/bucket test
	terraform -chdir=modules/gcs-bucket fmt -check -recursive
	terraform -chdir=modules/gcs-bucket init -backend=false -input=false
	terraform -chdir=modules/gcs-bucket validate
	terraform -chdir=modules/gcs-bucket test
	tflint --chdir=modules/gcs-bucket/modules/bucket
	tflint --chdir=modules/gcs-bucket
	opa test modules/gcs-bucket/policies -v
	conftest test --policy modules/project-root/policies modules/gcs-bucket/examples/requested.json modules/gcs-bucket/examples/approved.json
	terraform -chdir=modules/project-root fmt -check -recursive
	terraform -chdir=modules/project-root init -backend=false -input=false
	terraform -chdir=modules/project-root validate

lint-helm:
	helm lint deploy/helm/spp-workers -f deploy/values-md-dev.yaml
	helm template spp-workers deploy/helm/spp-workers -f deploy/values-md-dev.yaml --set terraform.templateEnabled=true >/dev/null
	DRY_RUN=true ./scripts/deploy.sh >/dev/null

deploy:
	IMAGE_TAG=$(IMAGE_TAG) ./scripts/deploy.sh
