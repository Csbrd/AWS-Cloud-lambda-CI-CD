# ── Common ─────────────────────────────────────────────────────────────────────
variable "project_id" {
  type    = string
}

variable "region" {
  type    = string
  default = "asia-northeast3"
}

variable "gcs_bucket" {
  type    = string
  default = ""
}

variable "aws_s3_bucket" {
  type    = string
  default = ""
}

# ── 01 Networking ──────────────────────────────────────────────────────────────
variable "subnet_primary_cidr" {
  type    = string
  default = "192.168.20.0/24"
}

variable "gcp_bgp_asn" {
  type    = number
}

variable "aws_bgp_asn" {
  type    = number
}

variable "aws_tgw_ip_1" {
  description = "AWS TGW VPN Outside IP — Tunnel 1 (set after AWS TGW VPN is created)"
  type        = string
}

variable "aws_tgw_ip_2" {
  description = "AWS TGW VPN Outside IP — Tunnel 2 (set after AWS TGW VPN is created)"
  type        = string
}

variable "vpn_shared_secret_1" {
  type      = string
  sensitive = true
  default   = ""
}

variable "vpn_shared_secret_2" {
  type      = string
  sensitive = true
  default   = ""
}

variable "aws_vpc_cidrs" {
  description = "AWS VPC + On-Prem CIDRs routed through VPN"
  type        = list(string)
}

# ── 02 Security ────────────────────────────────────────────────────────────────
variable "aws_sts_access_key_id" {
  type      = string
  sensitive = true
  default   = ""
}

variable "aws_sts_secret_access_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "aws_api_gw_url" {
  description = "AWS Private API Gateway endpoint URL (set after AWS API GW is created)"
  type        = string
  sensitive   = true
  default     = ""
}

# ── 03 Storage ─────────────────────────────────────────────────────────────────
variable "gcs_location" {
  type    = string
  default = "ASIA-NORTHEAST3"
}

# ── 05 BigQuery ────────────────────────────────────────────────────────────────
variable "bq_location" {
  type    = string
  default = "asia-northeast3"
}

# ── 07 Cloud Run ───────────────────────────────────────────────────────────────
variable "predict_runner_image" {
  type    = string
  default = ""
}

variable "sender_image" {
  type    = string
  default = ""
}

variable "vip_model_resource_name" {
  description = "Vertex AI VIP 분류 모델 리소스명 (미설정 시 더미 예측)"
  type        = string
  default     = ""
}

variable "signup_model_resource_name" {
  description = "Vertex AI 가입 예측 모델 리소스명"
  type        = string
  default     = ""
}

variable "rec_model_resource_name" {
  description = "Vertex AI 추천 예측 모델 리소스명"
  type        = string
  default     = ""
}

variable "health_model_resource_name" {
  description = "Vertex AI 건강점수 회귀 모델 리소스명"
  type        = string
  default     = ""
}

# ── 10 Monitoring ──────────────────────────────────────────────────────────────
variable "alert_email" {
  type    = string
}
