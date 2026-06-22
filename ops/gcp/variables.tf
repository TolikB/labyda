variable "project_id" {
  type        = string
  description = "GCP project containing the trading VM."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "state_disk_size_gb" {
  type    = number
  default = 10
  validation {
    condition     = var.state_disk_size_gb >= 10 && var.state_disk_size_gb <= 20
    error_message = "State disk must remain between 10 and 20 GB for this budget profile."
  }
}

variable "estimated_monthly_cost_usd" {
  type        = number
  description = "Current calculator estimate for Spot VM, disks and expected egress."
  validation {
    condition     = var.estimated_monthly_cost_usd > 0 && var.estimated_monthly_cost_usd <= 13
    error_message = "Refresh the estimate and keep it at or below USD 13 before apply."
  }
}
