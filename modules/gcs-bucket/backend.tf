terraform {
  backend "gcs" {
    bucket = "microdrive-dev-spp-tfstate"
    # prefix = "projects/<project_id>" is passed via -backend-config="prefix=projects/<project_id>" at terraform init
  }
}
