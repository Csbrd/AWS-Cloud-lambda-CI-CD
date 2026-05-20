# ── Storage Transfer Job — S3 → GCS ──────────────────────────────────────────
# AWS 자격증명·버킷이 설정된 경우에만 생성
resource "google_storage_transfer_job" "s3_to_gcs_daily" {
  count       = var.aws_sts_access_key_id != "" && var.aws_sts_secret_access_key != "" && var.aws_s3_bucket != "" ? 1 : 0
  description = "lifesync360 S3 → GCS daily transfer (03:00 KST)"
  project     = var.project_id

  transfer_spec {
    aws_s3_data_source {
      bucket_name = var.aws_s3_bucket
      aws_access_key {
        access_key_id     = var.aws_sts_access_key_id
        secret_access_key = var.aws_sts_secret_access_key
      }
    }

    gcs_data_sink {
      bucket_name = google_storage_bucket.data_lake.name
      path        = "curated/"
    }

    transfer_options {
      overwrite_objects_already_existing_in_sink = false
      delete_objects_unique_in_sink              = false
      delete_objects_from_source_after_transfer  = false
    }
  }

  schedule {
    schedule_start_date {
      year  = 2026
      month = 4
      day   = 1
    }
    start_time_of_day {
      hours   = 18  # 03:00 KST (UTC+9) → 18:00 UTC
      minutes = 0
      seconds = 0
      nanos   = 0
    }
    repeat_interval = "86400s"
  }

  depends_on = [google_storage_bucket_iam_member.sts_agent_bucket_writer]
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "transfer_job_name" {
  value = one(google_storage_transfer_job.s3_to_gcs_daily[*].name)
}
