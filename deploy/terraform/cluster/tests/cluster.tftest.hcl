# Offline checks of the cluster stack: the AWS provider is mocked, so
# `terraform test` makes no AWS calls. They guard the properties the
# deployment relies on (DECISIONS.md entry 41).

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names    = ["us-east-1c", "us-east-1a", "us-east-1d"]
      zone_ids = ["use1-az1", "use1-az2", "use1-az4"]
    }
  }

  mock_data "aws_ssm_parameter" {
    defaults = {
      insecure_value = "ami-0123456789abcdef0"
    }
  }
}

variables {
  image_digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

run "secure_defaults" {
  command = plan

  assert {
    condition = alltrue([
      for r in aws_vpc_security_group_ingress_rule.node :
      r.cidr_ipv4 != "0.0.0.0/0" && r.cidr_ipv6 != "::/0" && r.ip_protocol != "-1"
    ])
    error_message = "ingress from 0.0.0.0/0, ::/0 or on all protocols"
  }

  assert {
    condition = alltrue([
      for r in concat(values(aws_vpc_security_group_ingress_rule.node), values(aws_vpc_security_group_egress_rule.node)) :
      r.ip_protocol != "-1" && !(r.from_port <= 22 && 22 <= r.to_port)
    ])
    error_message = "a security group rule opens port 22 (operators use SSM, not SSH)"
  }

  assert {
    condition = alltrue([
      for i in aws_instance.node :
      i.metadata_options[0].http_tokens == "required" && i.metadata_options[0].http_put_response_hop_limit == 1
    ])
    error_message = "IMDSv2 must be required, with hop limit 1"
  }

  assert {
    condition = alltrue([
      for i in aws_instance.node :
      i.root_block_device[0].encrypted && length(i.ebs_block_device) == 1 && alltrue([for d in i.ebs_block_device : d.encrypted])
    ])
    error_message = "every volume (root and /data) must be encrypted"
  }

  # The bootstrap deploy role may only launch its allowed_instance_types.
  assert {
    condition = alltrue([
      for i in aws_instance.node : contains(
        [for m in regexall("\"([a-z0-9.]+)\"", regex("variable \"allowed_instance_types\"[^}]*default\\s*=\\s*\\[([^\\]]*)\\]", file("../bootstrap/variables.tf"))[0]) : m[0]],
        i.instance_type,
      )
    ])
    error_message = "instance_type is not in the bootstrap's allowed_instance_types"
  }

  assert {
    condition = alltrue([
      for n, ud in local.user_data :
      strcontains(ud, "/usr/local/sbin/paxosd-run '${var.image_digest}'") && !strcontains(ud, "paxos-txn-store:")
    ])
    error_message = "paxosd must run from the image digest, never a tag"
  }

  assert {
    condition     = length(distinct([for s in aws_subnet.node : s.availability_zone])) == 3
    error_message = "the three nodes must be in three different AZs"
  }

  assert {
    condition     = [for n in ["1", "2", "3"] : aws_instance.node[n].private_ip] == ["10.0.1.10", "10.0.2.10", "10.0.3.10"]
    error_message = "node IPs must stay fixed"
  }
}

run "rejects_tag" {
  command = plan

  variables {
    image_digest = "latest"
  }

  expect_failures = [var.image_digest]
}

run "rejects_world_client_cidr" {
  command = plan

  variables {
    client_cidrs = ["0.0.0.0/0"]
  }

  expect_failures = [var.client_cidrs]
}

run "client_cidr_opens_only_7001" {
  command = plan

  variables {
    client_cidrs = ["203.0.113.0/24"]
  }

  assert {
    condition = [
      for r in aws_vpc_security_group_ingress_rule.node : [r.from_port, r.to_port] if r.cidr_ipv4 != null
    ] == [[7001, 7001]]
    error_message = "a client CIDR must open port 7001 and nothing else"
  }
}
