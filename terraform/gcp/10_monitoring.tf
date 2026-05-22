# ── Notification Channel ──────────────────────────────────────────────────────
resource "google_monitoring_notification_channel" "email" {
  display_name = "lifesync360 Alert Email"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

# ── Alert Policies ────────────────────────────────────────────────────────────

# Cloud Run 5xx 에러 발생
resource "google_monitoring_alert_policy" "cloudrun_error_rate" {
  display_name = "Cloud Run 5xx Error Rate"
  combiner     = "OR"

  conditions {
    display_name = "Cloud Run 5xx responses"
    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"run.googleapis.com/request_count\" AND metric.labels.response_code_class=\"5xx\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.id]
  alert_strategy {
    auto_close = "1800s"
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "notification_channel_id" {
  value = google_monitoring_notification_channel.email.id
}
