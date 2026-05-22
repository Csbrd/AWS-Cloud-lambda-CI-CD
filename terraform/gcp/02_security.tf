# ── Data Sources ──────────────────────────────────────────────────────────────
data "google_project" "project" {}

# ── Service Accounts ──────────────────────────────────────────────────────────
resource "google_service_account" "sts" {
  account_id   = "lifesync-sts-sa"
  display_name = "lifesync Storage Transfer Service"
}

resource "google_service_account" "predict_runner" {
  account_id   = "lifesync-predict-runner-sa"
  display_name = "lifesync Cloud Run — predict-runner"
}

resource "google_service_account" "sender" {
  account_id   = "lifesync-sender-sa"
  display_name = "lifesync Cloud Run — sender"
}

resource "google_service_account" "scheduler" {
  account_id   = "lifesync-scheduler-sa"
  display_name = "lifesync Cloud Scheduler"
}

resource "google_service_account" "vertexai" {
  account_id   = "lifesync-vertexai-sa"
  display_name = "lifesync Vertex AI"
}

resource "google_service_account" "eventarc" {
  account_id   = "lifesync-eventarc-sa"
  display_name = "lifesync Eventarc"
}

# ── Project-level IAM ─────────────────────────────────────────────────────────

# STS SA: 전송 잡 생성·관리 권한
resource "google_project_iam_member" "sts_transfer_user" {
  project = var.project_id
  role    = "roles/storagetransfer.user"
  member  = "serviceAccount:${google_service_account.sts.email}"
}

# predict-runner SA: Vertex AI 배치 예측 잡 제출
resource "google_project_iam_member" "predict_runner_aiplatform_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.predict_runner.email}"
}

# predict-runner SA: BigQuery 잡 실행 (/dynamic-score 에서 BQ 읽기·쓰기)
resource "google_project_iam_member" "predict_runner_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.predict_runner.email}"
}

# predict-runner SA: BigQuery 데이터 읽기·쓰기 (뷰 생성 + 서빙 테이블 쓰기)
resource "google_project_iam_member" "predict_runner_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.predict_runner.email}"
}

# sender SA: BigQuery 잡 실행 (서빙 뷰 조회)
resource "google_project_iam_member" "sender_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sender.email}"
}

# Scheduler SA: Cloud Run 서비스 호출
resource "google_project_iam_member" "scheduler_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

# Vertex AI SA: 배치 예측 잡 실행
resource "google_project_iam_member" "vertexai_aiplatform_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: BigQuery 잡 실행 (피처 읽기 + 예측 결과 쓰기)
resource "google_project_iam_member" "vertexai_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: BigQuery 데이터 읽기·쓰기 (피처 뷰 읽기 + 예측 결과 쓰기)
resource "google_project_iam_member" "vertexai_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.vertexai.email}"
}

# Eventarc SA: 이벤트 수신
resource "google_project_iam_member" "eventarc_event_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc.email}"
}

# Eventarc SA: Cloud Run 호출
resource "google_project_iam_member" "eventarc_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.eventarc.email}"
}

# predict-runner SA: Artifact Registry 이미지 pull
resource "google_project_iam_member" "predict_runner_artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.predict_runner.email}"
}

# sender SA: Artifact Registry 이미지 pull
resource "google_project_iam_member" "sender_artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.sender.email}"
}

# STS Service Agent — 존재하지 않으면 자동 생성 후 email 반환
data "google_storage_transfer_project_service_account" "sts_agent" {
  project = var.project_id
}

# ── Secret Manager Secrets ────────────────────────────────────────────────────
resource "google_secret_manager_secret" "aws_access_key_id" {
  secret_id = "lifesync-aws-access-key-id"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_secret_manager_secret" "aws_secret_access_key" {
  secret_id = "lifesync-aws-secret-access-key"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_secret_manager_secret" "aws_api_gw_url" {
  secret_id = "lifesync-aws-api-gw-url"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

# ── Secret Versions (값이 주입된 경우에만 버전 생성) ─────────────────────────
resource "google_secret_manager_secret_version" "aws_access_key_id" {
  count       = var.aws_sts_access_key_id != "" ? 1 : 0
  secret      = google_secret_manager_secret.aws_access_key_id.id
  secret_data = var.aws_sts_access_key_id
}

resource "google_secret_manager_secret_version" "aws_secret_access_key" {
  count       = var.aws_sts_secret_access_key != "" ? 1 : 0
  secret      = google_secret_manager_secret.aws_secret_access_key.id
  secret_data = var.aws_sts_secret_access_key
}

resource "google_secret_manager_secret_version" "aws_api_gw_url" {
  count       = var.aws_api_gw_url != "" ? 1 : 0
  secret      = google_secret_manager_secret.aws_api_gw_url.id
  secret_data = var.aws_api_gw_url
}

# ── Secret IAM — 시크릿별 접근 SA 제한 ───────────────────────────────────────

# sender SA → AWS API GW URL (sender.py 에서 AWS 호출 시 사용)
resource "google_secret_manager_secret_iam_member" "sender_aws_api_gw_url" {
  secret_id = google_secret_manager_secret.aws_api_gw_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sender.email}"
}

# STS SA → AWS Access Key (04_transfer.tf 에서 data source로 참조)
resource "google_secret_manager_secret_iam_member" "sts_aws_access_key_id" {
  secret_id = google_secret_manager_secret.aws_access_key_id.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sts.email}"
}

resource "google_secret_manager_secret_iam_member" "sts_aws_secret_access_key" {
  secret_id = google_secret_manager_secret.aws_secret_access_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sts.email}"
}

# ── Admin Readonly SA (PGA — AWS admin → GCP API) ────────────────────────────
resource "google_service_account" "admin_readonly" {
  account_id   = "lifesync-admin-readonly"
  display_name = "LifeSync admin readonly"
}

resource "google_project_iam_member" "admin_readonly_monitoring_viewer" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.admin_readonly.email}"
}

resource "google_project_iam_member" "admin_readonly_bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.admin_readonly.email}"
}

resource "google_project_iam_member" "admin_readonly_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.admin_readonly.email}"
}

resource "google_project_iam_member" "admin_readonly_aiplatform_viewer" {
  project = var.project_id
  role    = "roles/aiplatform.viewer"
  member  = "serviceAccount:${google_service_account.admin_readonly.email}"
}

resource "google_project_iam_member" "admin_readonly_storage_object_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.admin_readonly.email}"
}

# 키 발급 불가 — 조직 정책 constraints/iam.disableServiceAccountKeyCreation 적용 중
# 대안: GCP Console → IAM → lifesync-admin-readonly SA → 키 탭에서 수동 발급
# 또는 조직 정책 예외 처리 후 아래 리소스 복원

# ── Outputs ───────────────────────────────────────────────────────────────────
output "sts_sa_email" {
  value = google_service_account.sts.email
}

output "predict_runner_sa_email" {
  value = google_service_account.predict_runner.email
}

output "sender_sa_email" {
  value = google_service_account.sender.email
}

output "scheduler_sa_email" {
  value = google_service_account.scheduler.email
}

output "vertexai_sa_email" {
  value = google_service_account.vertexai.email
}

output "eventarc_sa_email" {
  value = google_service_account.eventarc.email
}

output "sts_service_agent_email" {
  description = "STS Service Agent — 03_storage.tf GCS IAM 바인딩에 사용"
  value       = data.google_storage_transfer_project_service_account.sts_agent.email
}

output "admin_readonly_sa_email" {
  value = google_service_account.admin_readonly.email
}
