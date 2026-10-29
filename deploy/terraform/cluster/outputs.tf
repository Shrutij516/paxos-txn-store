output "instance_ids" {
  description = "Instance ID of each node."
  value       = { for n, i in aws_instance.node : "node${n}" => i.id }
}

output "availability_zones" {
  description = "AZ of each node."
  value       = { for n, i in aws_instance.node : "node${n}" => i.availability_zone }
}

output "private_ips" {
  description = "Fixed private IP of each node (deploy/cluster.json's node1..node3)."
  value       = { for n, i in aws_instance.node : "node${n}" => i.private_ip }
}

# Needs the AWS CLI and the Session Manager plugin, as the operator.
output "grafana_port_forward" {
  description = "Forward Grafana on node 1 to http://localhost:3000."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.node["1"].id} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"3000\"],\"localPortNumber\":[\"3000\"]}'"
}

output "jaeger_port_forward" {
  description = "Forward the Jaeger UI on node 1 to http://localhost:16686."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.node["1"].id} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"16686\"],\"localPortNumber\":[\"16686\"]}'"
}
