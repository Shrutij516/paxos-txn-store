# One security group for all three nodes. Inbound only from the group
# itself (plus optional client CIDRs on 7001); no port 22, since operators
# use SSM. Outbound only what the nodes need.
resource "aws_security_group" "node" {
  name        = "${var.project}-node"
  description = "paxosd nodes: cluster-internal traffic only"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-node" }
}

locals {
  # Ports the nodes use among themselves (deploy/compose.yaml):
  internal_ports = {
    paxosd  = { port = 7001, description = "paxosd gRPC: peer Paxos traffic and clients" }
    metrics = { port = 9100, description = "paxosd /metrics, scraped by Prometheus on node 1" }
    otlp    = { port = 4317, description = "OTLP traces from every node to Jaeger on node 1" }
  }

  ingress = merge(
    { for k, p in local.internal_ports : "self-${k}" => merge(p, { cidr = null, self = true }) },
    { for c in var.client_cidrs : "client-${c}" => { port = 7001, description = "paxosd clients from ${c}", cidr = c, self = false } },
  )

  egress = merge(
    { for k, p in local.internal_ports : "self-${k}" => merge(p, { cidr = null, self = true }) },
    {
      # ghcr.io and its blob storage, mirror.gcr.io (Prometheus, Grafana,
      # Jaeger images), the SSM and EC2 messages endpoints, and the Amazon
      # Linux package repositories are all HTTPS. DNS (VPC resolver), NTP
      # (169.254.169.123) and IMDS are link-local and not filtered by
      # security groups.
      https = { port = 443, description = "HTTPS: GHCR, mirror.gcr.io, SSM endpoints, dnf repositories", cidr = "0.0.0.0/0", self = false }
    },
  )
}

resource "aws_vpc_security_group_ingress_rule" "node" {
  for_each = local.ingress

  security_group_id            = aws_security_group.node.id
  description                  = each.value.description
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.self ? aws_security_group.node.id : null

  tags = { Name = "${var.project}-in-${each.key}" }
}

resource "aws_vpc_security_group_egress_rule" "node" {
  for_each = local.egress

  security_group_id            = aws_security_group.node.id
  description                  = each.value.description
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  cidr_ipv4                    = each.value.cidr
  referenced_security_group_id = each.value.self ? aws_security_group.node.id : null

  tags = { Name = "${var.project}-out-${each.key}" }
}
