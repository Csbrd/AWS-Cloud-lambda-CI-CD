# ── Cloud Run — predict-runner ────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "predict_runner" {
  count    = var.predict_runner_image != "" ? 1 : 0
  name     = "lifesync-predict-runner"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.predict_runner.email
    timeout         = "540s"

    scaling {
      max_instance_count = 1
    }

    containers {
      image = var.predict_runner_image

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "REGION"
        value = var.region
      }
      env {
        name  = "GCS_BUCKET"
        value = var.gcs_bucket
      }
      env {
        name  = "VIP_MODEL_RESOURCE_NAME"
        value = var.vip_model_resource_name
      }
      env {
        name  = "SIGNUP_MODEL_RESOURCE_NAME"
        value = var.signup_model_resource_name
      }
      env {
        name  = "REC_MODEL_RESOURCE_NAME"
        value = var.rec_model_resource_name
      }
      env {
        name  = "HEALTH_MODEL_RESOURCE_NAME"
        value = var.health_model_resource_name
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
    }
  }
}

# ── Cloud Run — sender ────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "sender" {
  count    = var.sender_image != "" ? 1 : 0
  name     = "lifesync-sender"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  depends_on = [google_secret_manager_secret_iam_member.sender_aws_api_gw_url]

  template {
    service_account = google_service_account.sender.email
    timeout         = "3600s"

    scaling {
      max_instance_count = 1
    }

    containers {
      image = var.sender_image

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      dynamic "env" {
        for_each = var.aws_api_gw_url != "" ? [1] : []
        content {
          name = "AWS_API_GW_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.aws_api_gw_url.secret_id
              version = "latest"
            }
          }
        }
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }
    }
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "artifact_registry_url" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/lifesync"
}

output "predict_runner_uri" {
  value = one(google_cloud_run_v2_service.predict_runner[*].uri)
}

output "sender_uri" {
  value = one(google_cloud_run_v2_service.sender[*].uri)
}
