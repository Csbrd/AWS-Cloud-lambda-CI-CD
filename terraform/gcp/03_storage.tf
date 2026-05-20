# ── GCS Data Lake Bucket ──────────────────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket
  location                    = var.gcs_location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle {
    prevent_destroy = false
  }

  # 30일 이후 접근 빈도 낮아지므로 Nearline으로 전환
  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # 90일 이후 BigQuery가 영구 저장소 역할을 하므로 GCS 원본 삭제
  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }
}

# ── IAM ───────────────────────────────────────────────────────────────────────

# STS Service Agent (GCP 자동 생성) — storage.buckets.get + objects.create/list/delete 모두 필요
resource "google_storage_bucket_iam_member" "sts_agent_bucket_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.sts_agent.email}"
}

# predict-runner SA — 마커 쓰기 + 덮어쓰기(delete 포함)
resource "google_storage_bucket_iam_member" "predict_runner_object_user" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.predict_runner.email}"
}

# Vertex AI SA — 피처 파일 읽기 + 배치 예측 결과 쓰기
resource "google_storage_bucket_iam_member" "vertexai_object_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.vertexai.email}"
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "data_lake_bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "data_lake_bucket_url" {
  value = google_storage_bucket.data_lake.url
}
