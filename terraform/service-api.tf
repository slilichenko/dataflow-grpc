resource "google_project_service" "dataflow-api" {
  service = "dataflow.googleapis.com"
}
resource "google_project_service" "containerregistry-api" {
  service = "containerregistry.googleapis.com"
}
