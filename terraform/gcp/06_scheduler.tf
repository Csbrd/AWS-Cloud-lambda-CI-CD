# ── Cloud Scheduler Jobs ──────────────────────────────────────────────────────
# predict_runner: 04:00 KST — Vertex AI 배치 예측 잡 시간 기반 트리거
# dynamic_score : 04:30 KST — 예측 결과 변환 + 서빙 레이어 갱신 (시간 기반)
# sender        : Eventarc (serving_complete/ GCS 마커 감지 → AWS API GW 전달)

# 03:00 KST — GCS → BigQuery Load (설계 문서: S3→GCS→BQ 모두 03:00 묶음)
resource "google_cloud_scheduler_job" "load_bq" {
  count            = var.predict_runner_image != "" ? 1 : 0
  name             = "lifesync-load-bq-trigger"
  region           = var.region
  schedule         = "0 3 * * *"
  time_zone        = "Asia/Seoul"
  attempt_deadline = "1800s"

  retry_config {
    retry_count = 2
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.predict_runner[0].uri}/load-bq"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.predict_runner[0].uri
    }
  }
}

# 04:00 KST — Vertex AI 배치 예측 잡 실행
resource "google_cloud_scheduler_job" "predict_runner" {
  count            = var.predict_runner_image != "" ? 1 : 0
  name             = "lifesync-predict-runner-trigger"
  region           = var.region
  schedule         = "0 4 * * *"
  time_zone        = "Asia/Seoul"
  attempt_deadline = "60s"

  retry_config {
    retry_count = 0
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.predict_runner[0].uri}/run"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.predict_runner[0].uri
    }
  }
}

# 04:30 KST — Dynamic Score 재계산 (Vertex AI Job 완료 ~30분 후)
resource "google_cloud_scheduler_job" "dynamic_score" {
  count            = var.predict_runner_image != "" ? 1 : 0
  name             = "lifesync-dynamic-score-trigger"
  region           = var.region
  schedule         = "30 4 * * *"
  time_zone        = "Asia/Seoul"
  attempt_deadline = "1800s"

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.predict_runner[0].uri}/dynamic-score"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.predict_runner[0].uri
    }
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "scheduler_predict_runner_name" {
  value = one(google_cloud_scheduler_job.predict_runner[*].name)
}

output "scheduler_dynamic_score_name" {
  value = one(google_cloud_scheduler_job.dynamic_score[*].name)
}
