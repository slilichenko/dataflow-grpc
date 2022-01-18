resource "google_service_account" "dataflow-sa" {
  account_id   = "zip-resolver-sa"
  display_name = "Service Account to run Dataflow jobs"
}
