#!/bin/bash
# lifesync GCP — 비용 최적화 Destroy 스크립트
# Cloud Run / Scheduler / Eventarc / Transfer / Monitoring만 삭제
# GCS + BigQuery는 건드리지 않음 (-target 방식)
set -e

# Windows bash 환경에서 terraform이 PATH에 없을 경우 cmd.exe 경유
if ! command -v terraform &>/dev/null; then
  terraform() { cmd.exe /c terraform "$@"; }
fi

PROJECT_ID="project-1f8eb19b-1a9a-45cf-ae6"

echo "=== Terraform Destroy (비용 리소스만) ==="
terraform destroy -auto-approve \
  -target='google_cloud_run_v2_service.predict_runner[0]' \
  -target='google_cloud_run_v2_service.sender[0]' \
  -target='google_cloud_scheduler_job.predict_runner[0]' \
  -target='google_cloud_scheduler_job.dynamic_score[0]' \
  -target='google_eventarc_trigger.serving_complete[0]' \
  -target='google_storage_transfer_job.s3_to_gcs_daily[0]' \
  -target='google_monitoring_alert_policy.cloudrun_error_rate' \
  -target='google_monitoring_notification_channel.email' \
  -target='google_bigquery_data_transfer_config.ml_training_data'

echo ""
echo "Destroy 완료"
echo "  - Cloud Run / Scheduler / Eventarc / Transfer / Monitoring -> 삭제"
echo "  - GCS / BigQuery / VPC / IAM / Secret -> 유지"
echo "  - 다음 사용 시: terraform apply"
