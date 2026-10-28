# Three nodes, one per AZ, at fixed private IPs. Each runs paxosd in Docker
# with the flags of the matching service in deploy/compose.yaml and the
# unchanged deploy/cluster.json; node 1 also runs Prometheus, Grafana and
# Jaeger. Everything the nodes run comes from the repository's local stack:
# the names in cluster.json and the Prometheus config (node1..node3,
# jaeger) resolve to the fixed IPs through /etc/hosts and --add-host.

# Current Amazon Linux 2023 AMI, from AWS's public parameter.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  compose = yamldecode(file("${path.module}/../../compose.yaml"))
  image   = "ghcr.io/shrutij516/paxos-txn-store@${var.image_digest}"

  # name -> IP for every host the configs refer to.
  hosts = merge(
    { for n, node in local.nodes : "node${n}" => node.ip },
    { jaeger = local.nodes[1].ip },
  )

  # First-boot script of each node (gzipped into user_data).
  user_data = { for n, node in local.nodes : n => templatefile("${path.module}/user_data.sh.tftpl", {
    node_id       = n
    image         = local.image
    hosts         = local.hosts
    paxosd_args   = local.compose.services["node${n}"].command
    cluster_json  = file("${path.module}/../../cluster.json")
    observability = n == "1"
    prometheus = {
      image   = local.compose.services.prometheus.image
      command = local.compose.services.prometheus.command
      config  = file("${path.module}/../../prometheus/compose.yml")
    }
    grafana = {
      image       = local.compose.services.grafana.image
      environment = local.compose.services.grafana.environment
      datasource  = file("${path.module}/../../grafana/provisioning/datasources/prometheus.yaml")
      dashboards  = file("${path.module}/../../grafana/provisioning/dashboards/paxos.yaml")
      dashboard   = file("${path.module}/../../grafana/dashboard.json")
    }
    jaeger_image = local.compose.services.jaeger.image
  }) }
}

resource "aws_instance" "node" {
  #checkov:skip=CKV_AWS_88:public IPv4 is the nodes' only route out (no NAT gateway, DECISIONS.md entry 41); the security group admits nothing from outside the cluster
  #checkov:skip=CKV_AWS_126:detailed monitoring costs extra; paxosd exports its own metrics to Prometheus on node 1
  for_each = local.nodes

  ami                         = data.aws_ssm_parameter.al2023.insecure_value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.node[each.key].id
  private_ip                  = each.value.ip
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.node.id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  ebs_optimized               = true
  monitoring                  = false

  # Standard credits: a busy node is throttled instead of billed for
  # surplus credits (t3 defaults to unlimited).
  credit_specification {
    cpu_credits = "standard"
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1 # containers on the Docker bridge cannot reach IMDS
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 10
    encrypted             = true
    delete_on_termination = true
  }

  # SQLite files of every shard replica on this node, mounted at /data.
  ebs_block_device {
    device_name           = "/dev/sdf"
    volume_type           = "gp3"
    volume_size           = 8
    encrypted             = true
    delete_on_termination = true
  }

  volume_tags = merge(local.tags, { Name = each.value.name })

  user_data_base64 = base64gzip(local.user_data[each.key])

  lifecycle {
    # user_data runs only on first boot; a new image is rolled out with
    # `paxosd-run <digest>` over SSM, not by replacing the node (its data
    # disk goes with it). A newer AMI likewise must not replace running
    # nodes.
    ignore_changes = [ami, user_data_base64]
  }

  tags = { Name = each.value.name }
}
