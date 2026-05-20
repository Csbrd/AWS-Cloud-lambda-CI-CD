# ── GCS Service Account — Eventarc용 Pub/Sub 내부 전달 권한 ──────────────────
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

# ── Eventarc Trigger: dynamic_score 완료 → sender ────────────────────────────
# /dynamic-score 완료 후 GCS serving_complete/{date}.done 마커 기록
# → Eventarc 감지 → sender 호출 → AWS API GW 전달
# (/dynamic-score 자체는 Cloud Scheduler 04:30 KST가 시간 기반으로 트리거)
resource "google_eventarc_trigger" "serving_complete" {
  count    = var.sender_image != "" ? 1 : 0
  name     = "lifesync-serving-complete-trigger"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.data_lake.name
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.sender[0].name
      region  = var.region
    }
  }

  service_account = google_service_account.eventarc.email
  depends_on = [
    google_project_iam_member.gcs_pubsub_publisher,
    google_project_iam_member.eventarc_event_receiver,
    google_project_iam_member.eventarc_run_invoker,
  ]
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "eventarc_serving_trigger_name" {
  value = one(google_eventarc_trigger.serving_complete[*].name)
}
