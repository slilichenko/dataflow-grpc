resource "google_storage_bucket" "zip-resolutions" {
  name = "${var.project_id}-zip-resolutions"
  uniform_bucket_level_access = true
  location = "${var.region}"
}

resource "google_storage_bucket_object" "event-generator-template" {
  name = "event-generator-template.json"
  bucket = google_storage_bucket.zip-resolutions.name
  source = "../data-generator/event-generator-template.json"
}

output "data-bucket" {
  value = "gs://${google_storage_bucket.zip-resolutions.name}"
}

output "event-generator-template" {
  value = "gs://${google_storage_bucket_object.event-generator-template.bucket}/${google_storage_bucket_object.event-generator-template.name}"
}

resource "google_storage_bucket" "dataflow-temp" {
  name = "${var.project_id}-dataflow-zip-resolve-temp"
  uniform_bucket_level_access = true
  location = var.region
}

output "dataflow-temp-bucket" {
  value = google_storage_bucket.dataflow-temp.id
}