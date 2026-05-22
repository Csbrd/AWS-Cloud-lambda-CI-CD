# ── HA Cloud VPN Gateway ──────────────────────────────────────────────────────
resource "google_compute_ha_vpn_gateway" "main" {
  name    = "lifesync-ha-vpn-gw"
  region  = var.region
  network = google_compute_network.vpc.id
}

# AWS Transit Gateway 를 외부 VPN 피어로 등록
resource "google_compute_external_vpn_gateway" "aws_tgw" {
  name            = "lifesync-aws-tgw"
  redundancy_type = "TWO_IPS_REDUNDANCY"
  description     = "AWS Transit Gateway (ap-northeast-2)"

  interface {
    id         = 0
    ip_address = var.aws_tgw_ip_1
  }
  interface {
    id         = 1
    ip_address = var.aws_tgw_ip_2
  }
}

# ── Cloud Router (BGP) ────────────────────────────────────────────────────────
resource "google_compute_router" "main" {
  name    = "lifesync-cloud-router"
  region  = var.region
  network = google_compute_network.vpc.id

  bgp {
    asn               = var.gcp_bgp_asn
    advertise_mode    = "CUSTOM"
    advertised_groups = ["ALL_SUBNETS"]
  }
}

# ── VPN Tunnels (IKEv2, 2-tunnel HA) ─────────────────────────────────────────
resource "google_compute_vpn_tunnel" "tunnel_1" {
  name                            = "lifesync-vpn-tunnel-1"
  region                          = var.region
  vpn_gateway                     = google_compute_ha_vpn_gateway.main.id
  peer_external_gateway           = google_compute_external_vpn_gateway.aws_tgw.id
  peer_external_gateway_interface = 0
  shared_secret                   = var.vpn_shared_secret_1
  router                          = google_compute_router.main.id
  vpn_gateway_interface           = 0
  ike_version                     = 2
}

resource "google_compute_vpn_tunnel" "tunnel_2" {
  name                            = "lifesync-vpn-tunnel-2"
  region                          = var.region
  vpn_gateway                     = google_compute_ha_vpn_gateway.main.id
  peer_external_gateway           = google_compute_external_vpn_gateway.aws_tgw.id
  peer_external_gateway_interface = 1
  shared_secret                   = var.vpn_shared_secret_2
  router                          = google_compute_router.main.id
  vpn_gateway_interface           = 0
  ike_version                     = 2
}

# ── BGP Sessions ──────────────────────────────────────────────────────────────
# 링크-로컬 IP는 AWS TGW VPN 터널 설정값과 반드시 일치해야 합니다.
resource "google_compute_router_interface" "if_1" {
  name       = "lifesync-router-if-1"
  router     = google_compute_router.main.name
  region     = var.region
  ip_range   = "169.254.65.198/30"
  vpn_tunnel = google_compute_vpn_tunnel.tunnel_1.name
}

resource "google_compute_router_peer" "peer_1" {
  name                      = "lifesync-bgp-peer-1"
  router                    = google_compute_router.main.name
  region                    = var.region
  peer_ip_address           = "169.254.65.197"
  peer_asn                  = var.aws_bgp_asn
  advertised_route_priority = 100
  interface                 = google_compute_router_interface.if_1.name
}

resource "google_compute_router_interface" "if_2" {
  name       = "lifesync-router-if-2"
  router     = google_compute_router.main.name
  region     = var.region
  ip_range   = "169.254.38.146/30"
  vpn_tunnel = google_compute_vpn_tunnel.tunnel_2.name
}

resource "google_compute_router_peer" "peer_2" {
  name                      = "lifesync-bgp-peer-2"
  router                    = google_compute_router.main.name
  region                    = var.region
  peer_ip_address           = "169.254.38.145"
  peer_asn                  = var.aws_bgp_asn
  advertised_route_priority = 100
  interface                 = google_compute_router_interface.if_2.name
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "vpn_gw_ip_0" {
  description = "HA VPN Gateway interface 0 IP — AWS TGW Customer Gateway 에 등록할 IP"
  value       = google_compute_ha_vpn_gateway.main.vpn_interfaces[0].ip_address
}
