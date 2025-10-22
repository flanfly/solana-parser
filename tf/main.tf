locals {
  project_id = "pump-fun-dth3"
  region     = "asia-southeast1"
  machine    = "c4a-standard-2"

  version   = "v1"
  epoch     = "802"
  user_name = "app"
}

provider "google" {
  project = local.project_id
  region  = local.region
  #credentials = file("service-account-key.json")
}

data "google_compute_zones" "all" {
  project = local.project_id
  region  = local.region
}

resource "google_storage_bucket" "provision" {
  name                        = "${local.project_id}-provision"
  location                    = local.region
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "startup_script" {
  name   = "startup.sh"
  bucket = google_storage_bucket.provision.name
  content = templatefile("${path.module}/startup.sh.tftpl", {
    bucket_name = google_storage_bucket.output.name
    version     = local.version,
    epoch       = local.epoch,
    user_name   = local.user_name,
  })
}

resource "google_storage_bucket" "output" {
  name                        = "${local.project_id}-output"
  location                    = local.region
  uniform_bucket_level_access = true
}

resource "google_service_account" "worker" {
  account_id   = "${local.project_id}-worker"
  display_name = "Worker Service Account"
  description  = "Service account for MIG instances to read startup scripts and write output."
}

resource "google_project_iam_member" "worker_iam" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_storage_bucket_iam_member" "provision_bucket_reader" {
  bucket = google_storage_bucket.provision.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_storage_bucket_iam_member" "output_bucket_writer" {
  bucket = google_storage_bucket.output.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["allow-ssh"]
  description   = "Allows SSH access to instances with 'allow-ssh' tag"
}

resource "google_compute_region_instance_template" "default" {
  name_prefix  = "pump-fun-dth3-worker-"
  machine_type = local.machine
  region       = local.region

  disk {
    source_image = "projects/debian-cloud/global/images/family/debian-13-arm64"
    auto_delete  = true
    boot         = true
    disk_type    = "hyperdisk-balanced"
    disk_size_gb = 10
  }

  scheduling {
    automatic_restart   = false
    provisioning_model  = "SPOT"
    preemptible         = true
    on_host_maintenance = "TERMINATE"
  }

  network_interface {
    network = "default"
    access_config {}
  }
  tags = ["allow-ssh"]

  metadata = {
    startup-script-url = "gs://${google_storage_bucket.provision.name}/${google_storage_bucket_object.startup_script.name}"
  }

  service_account {
    email  = google_service_account.worker.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_region_instance_group_manager" "default" {
  name               = "${local.project_id}-mig"
  region             = local.region
  base_instance_name = "${local.project_id}-worker"

  version {
    instance_template = google_compute_region_instance_template.default.id
  }

  update_policy {
    type                         = "PROACTIVE"
    instance_redistribution_type = "PROACTIVE"
    minimal_action               = "REPLACE"
    max_unavailable_fixed        = length(data.google_compute_zones.all.names)
  }

  target_size = 0
}

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}
