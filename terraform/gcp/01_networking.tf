locals {
  labels = {
    project = "lifesync"
    env     = "dev"
  }
}

# ── VPC ────────────────────────────────────────────────────────────────────────
resource "google_compute_network" "vpc" {
  name                    = "lifesync-vpc"
  auto_create_subnetworks = false
  routing_mode            = "GLOBAL"
}

resource "google_compute_subnetwork" "primary" {
  name                     = "lifesync-subnet-${var.region}"
  ip_cidr_range            = var.subnet_primary_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# ── Private Service Connect — Google APIs ─────────────────────────────────────
# VPN 경유 On-Prem/AWS 에서 *.googleapis.com 에 private IP 로 접근하기 위한 PSC 엔드포인트
resource "google_compute_global_address" "psc_google_apis" {
  name         = "lifesync-psc-google-apis"
  address_type = "INTERNAL"
  purpose      = "PRIVATE_SERVICE_CONNECT"
  network      = google_compute_network.vpc.id
  address      = "10.200.0.2"
}

resource "google_compute_global_forwarding_rule" "psc_google_apis" {
  provider              = google-beta
  name                  = "lifesyncpsc"
  target                = "all-apis"
  network               = google_compute_network.vpc.id
  ip_address            = google_compute_global_address.psc_google_apis.id
  load_balancing_scheme = ""
}

# PSC 엔드포인트 IP 로 *.googleapis.com 을 해석하는 Private DNS 존
resource "google_dns_managed_zone" "googleapis" {
  name       = "lifesync-googleapis"
  dns_name   = "googleapis.com."
  visibility = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.vpc.id
    }
  }
}

resource "google_dns_record_set" "psc_restricted_a" {
  name         = "restricted.googleapis.com."
  type         = "A"
  ttl          = 300
  managed_zone = google_dns_managed_zone.googleapis.name
  rrdatas      = [google_compute_global_address.psc_google_apis.address]
}

resource "google_dns_record_set" "psc_wildcard_cname" {
  name         = "*.googleapis.com."
  type         = "CNAME"
  ttl          = 300
  managed_zone = google_dns_managed_zone.googleapis.name
  rrdatas      = ["private.googleapis.com."]
}

# ── Firewall Rules ────────────────────────────────────────────────────────────
resource "google_compute_firewall" "allow_internal" {
  name    = "lifesync-allow-internal"
  network = google_compute_network.vpc.name

  allow { protocol = "all" }

  direction     = "INGRESS"
  source_ranges = [var.subnet_primary_cidr]
  priority      = 1000
}

resource "google_compute_firewall" "allow_vpn_ingress" {
  name    = "lifesync-allow-vpn-ingress"
  network = google_compute_network.vpc.name

  allow { protocol = "all" }

  direction     = "INGRESS"
  source_ranges = var.aws_vpc_cidrs
  priority      = 1000
}

resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "lifesync-allow-iap-ssh"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"]
  priority      = 1000
}

resource "google_compute_firewall" "deny_all_ingress" {
  name    = "lifesync-deny-all-ingress"
  network = google_compute_network.vpc.name

  deny { protocol = "all" }

  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  priority      = 65534
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "vpc_self_link" {
  value = google_compute_network.vpc.self_link
}

output "subnet_self_link" {
  value = google_compute_subnetwork.primary.self_link
}


output "psc_endpoint_ip" {
  value = google_compute_global_address.psc_google_apis.address
}
