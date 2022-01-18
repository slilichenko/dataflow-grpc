resource "google_pubsub_topic" "resolve_zip_topic" {
  name = "resolve-zip-topic"
}

output "resolve-zip-topic" {
  value = google_pubsub_topic.resolve_zip_topic.id
}

resource "google_pubsub_subscription" "resolve_zip_sub" {
  name = "resolve-zip-sub"
  topic = google_pubsub_topic.resolve_zip_topic.name
}

output "resolve-zip-subscription" {
  value = google_pubsub_subscription.resolve_zip_sub.id
}