terraform {
  required_version = ">= 1.7.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "terraform_data" "cost_gate" {
  input = var.estimated_monthly_cost_usd

  lifecycle {
    precondition {
      condition     = var.estimated_monthly_cost_usd <= 13
      error_message = "Estimated monthly infrastructure cost must be USD 13 or less before apply."
    }
  }
}

resource "google_compute_network" "arbitrage" {
  name                    = "arbitrage-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "arbitrage" {
  name          = "arbitrage-${var.region}"
  ip_cidr_range = "10.42.0.0/24"
  network       = google_compute_network.arbitrage.id
  region        = var.region
}

resource "google_compute_firewall" "iap_ssh" {
  name          = "arbitrage-iap-ssh"
  network       = google_compute_network.arbitrage.name
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["arbitrage-bot"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_disk" "state" {
  name = "arbitrage-state"
  type = "pd-standard"
  zone = var.zone
  size = var.state_disk_size_gb
}

resource "google_compute_health_check" "ssh" {
  name                = "arbitrage-ssh-health"
  timeout_sec         = 5
  check_interval_sec  = 30
  unhealthy_threshold = 5
  tcp_health_check {
    port = 22
  }
}

resource "google_compute_instance_template" "arbitrage" {
  name_prefix  = "arbitrage-e2-micro-"
  machine_type = "e2-micro"
  tags         = ["arbitrage-bot"]
  labels       = { workload = "polymarket-myriad", lifecycle = "spot" }

  disk {
    boot         = true
    auto_delete  = true
    source_image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
    disk_size_gb = 10
    disk_type    = "pd-standard"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.arbitrage.id
    access_config {}
  }

  scheduling {
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    provisioning_model          = "SPOT"
    instance_termination_action = "DELETE"
  }

  metadata = {
    enable-oslogin = "TRUE"
  }
  metadata_startup_script = file("${path.module}/startup.sh")

  service_account {
    scopes = ["https://www.googleapis.com/auth/logging.write"]
  }

  lifecycle {
    create_before_destroy = true
  }
  depends_on = [terraform_data.cost_gate]
}

resource "google_compute_instance_group_manager" "arbitrage" {
  name               = "arbitrage-spot-mig"
  zone               = var.zone
  base_instance_name = "arbitrage"
  target_size        = 1

  version {
    instance_template = google_compute_instance_template.arbitrage.id
  }

  stateful_disk {
    device_name = "state"
    delete_rule = "NEVER"
  }

  auto_healing_policies {
    health_check      = google_compute_health_check.ssh.id
    initial_delay_sec = 600
  }

  update_policy {
    type                         = "OPPORTUNISTIC"
    minimal_action               = "REPLACE"
    most_disruptive_allowed_action = "REPLACE"
    max_surge_fixed              = 0
    max_unavailable_fixed        = 1
  }
}

resource "google_compute_per_instance_config" "arbitrage" {
  zone                   = var.zone
  instance_group_manager = google_compute_instance_group_manager.arbitrage.name
  name                   = "arbitrage-0"

  preserved_state {
    disk {
      device_name = "state"
      source      = google_compute_disk.state.id
      mode        = "READ_WRITE"
      delete_rule = "NEVER"
    }
  }
}
