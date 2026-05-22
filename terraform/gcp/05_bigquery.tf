# ── Datasets ──────────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "curated" {
  dataset_id                 = "lifesync_curated"
  location                   = var.bq_location
  delete_contents_on_destroy = true

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_bigquery_dataset" "ml" {
  dataset_id                 = "lifesync_ml"
  location                   = var.bq_location
  delete_contents_on_destroy = true

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_bigquery_dataset" "serving" {
  dataset_id                 = "lifesync_serving"
  location                   = var.bq_location
  delete_contents_on_destroy = true

  lifecycle {
    prevent_destroy = false
  }
}

# ── Dataset-level IAM ─────────────────────────────────────────────────────────

# Vertex AI SA: curated 피처 읽기
resource "google_bigquery_dataset_iam_member" "vertexai_curated_viewer" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: ML 예측 결과 쓰기
resource "google_bigquery_dataset_iam_member" "vertexai_ml_editor" {
  dataset_id = google_bigquery_dataset.ml.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: 서빙 레이어 갱신 (Scheduled Query 실행 SA)
resource "google_bigquery_dataset_iam_member" "vertexai_serving_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# predict-runner SA: ML 예측 결과 읽기 (/dynamic-score)
resource "google_bigquery_dataset_iam_member" "predict_runner_ml_viewer" {
  dataset_id = google_bigquery_dataset.ml.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.predict_runner.email}"
}

# predict-runner SA: serving 레이어 갱신 (/dynamic-score)
resource "google_bigquery_dataset_iam_member" "predict_runner_serving_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.predict_runner.email}"
}

# sender SA: 서빙 뷰 조회 (AWS API GW 전달용)
resource "google_bigquery_dataset_iam_member" "sender_serving_viewer" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sender.email}"
}

# ── Scheduled Query ───────────────────────────────────────────────────────────
# BQ Data Transfer Service Agent 강제 생성
resource "google_project_service_identity" "bq_dts_agent" {
  provider = google-beta
  project  = var.project_id
  service  = "bigquerydatatransfer.googleapis.com"
}

# BQ Data Transfer Service Agent가 vertexai SA를 impersonate할 수 있도록 허용
resource "google_service_account_iam_member" "bq_dts_token_creator" {
  service_account_id = google_service_account.vertexai.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
  depends_on         = [google_project_service_identity.bq_dts_agent]
}

resource "google_bigquery_data_transfer_config" "ml_training_data" {
  display_name           = "lifesync-ml-training-data"
  location               = var.bq_location
  data_source_id         = "scheduled_query"
  schedule               = "every day 18:25"  # 03:25 KST — GCS→BQ 적재(03:00) 완료 후 ML 학습 데이터 생성
  destination_dataset_id = google_bigquery_dataset.ml.dataset_id
  service_account_name   = google_service_account.vertexai.email

  params = {
    query = <<-EOT
      CREATE OR REPLACE TABLE `${var.project_id}.lifesync_ml.vip_training_data` AS
      SELECT
          global_id,
          balance_30d_avg, asset_growth_90d, card_spend_30d,
          invest_total, invest_ratio, etf_ratio, policy_cnt,
          avg_steps_30d, avg_hr_30d, stress_avg_30d, avg_sleep_30d,
          hospital_visit_90d, health_risk_score, step_growth_30d,
          login_cnt_30d, avg_session_min, push_click_rate, recommend_click_rate, last_active_days,
          affiliate_cnt, consent_ratio, membership_days, cross_product_score,
          spend_growth_90d, invest_growth_90d, wellness_growth_30d,
          inactive_days, card_drop_ratio, asset_drop_ratio, complaint_flag,
          vip_label
      FROM `${var.project_id}.lifesync_curated.ai_feature_table`;

      CREATE OR REPLACE TABLE `${var.project_id}.lifesync_ml.rec_training_data` AS
      SELECT
          global_id,
          balance_30d_avg, asset_growth_90d, card_spend_30d,
          invest_total, invest_ratio, etf_ratio, policy_cnt,
          avg_steps_30d, avg_hr_30d, stress_avg_30d, avg_sleep_30d,
          hospital_visit_90d, health_risk_score, step_growth_30d,
          login_cnt_30d, avg_session_min, push_click_rate, recommend_click_rate, last_active_days,
          affiliate_cnt, consent_ratio, membership_days, cross_product_score,
          spend_growth_90d, invest_growth_90d, wellness_growth_30d,
          inactive_days, card_drop_ratio, asset_drop_ratio, complaint_flag,
          product_purchase_label
      FROM `${var.project_id}.lifesync_curated.ai_feature_table`;

      CREATE OR REPLACE TABLE `${var.project_id}.lifesync_ml.health_training_data` AS
      SELECT
          global_id,
          balance_30d_avg, asset_growth_90d, card_spend_30d,
          invest_total, invest_ratio, etf_ratio, policy_cnt,
          avg_steps_30d, avg_hr_30d, stress_avg_30d, avg_sleep_30d,
          hospital_visit_90d, step_growth_30d,
          login_cnt_30d, avg_session_min, push_click_rate, recommend_click_rate, last_active_days,
          affiliate_cnt, consent_ratio, membership_days, cross_product_score,
          spend_growth_90d, invest_growth_90d, wellness_growth_30d,
          inactive_days, card_drop_ratio, asset_drop_ratio, complaint_flag,
          health_risk_score
      FROM `${var.project_id}.lifesync_curated.ai_feature_table`;
    EOT
  }

  depends_on = [
    google_service_account_iam_member.bq_dts_token_creator,
    google_bigquery_dataset.curated,
    google_bigquery_dataset.ml,
    google_bigquery_dataset.serving,
  ]
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "bq_dataset_curated" {
  value = google_bigquery_dataset.curated.dataset_id
}

output "bq_dataset_ml" {
  value = google_bigquery_dataset.ml.dataset_id
}

output "bq_dataset_serving" {
  value = google_bigquery_dataset.serving.dataset_id
}
