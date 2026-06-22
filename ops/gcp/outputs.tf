output "instance_group_manager" {
  value = google_compute_instance_group_manager.arbitrage.name
}

output "state_disk" {
  value = google_compute_disk.state.name
}

output "cost_gate_usd" {
  value = terraform_data.cost_gate.output
}
