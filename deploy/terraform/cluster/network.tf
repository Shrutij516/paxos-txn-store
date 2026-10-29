# One VPC, three public subnets in three AZs, one route table to the
# internet gateway. No NAT gateway: the nodes have public IPv4 addresses
# for egress (GHCR, SSM, package repos) and the security group admits no
# inbound traffic from outside the cluster. The VPC's default security
# group, network ACL and main route table are left as AWS creates them.
# The three AZs, pinned by zone ID so the set never changes when AWS adds a
# zone (a new AZ name sorting first would otherwise move subnets and replace
# nodes). use1-az3 is skipped: it lacks current instance families.
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "zone-id"
    values = var.zone_ids
  }
}

locals {
  # AZ name of each pinned zone ID, in var.zone_ids order (names differ per
  # account; IDs do not).
  az_by_id = zipmap(data.aws_availability_zones.available.zone_ids, data.aws_availability_zones.available.names)
  azs      = [for id in var.zone_ids : local.az_by_id[id]]

  # Fixed private IPs keep the cluster config static: node N is 10.0.N.10
  # in subnet 10.0.N.0/24, in the N-th AZ.
  nodes = {
    for n in [1, 2, 3] : n => {
      name = "${var.project}-node${n}"
      az   = local.azs[n - 1]
      cidr = "10.0.${n}.0/24"
      ip   = "10.0.${n}.10"
    }
  }
}

resource "aws_vpc" "main" {
  #checkov:skip=CKV2_AWS_11:flow logs need a log group or bucket and cost money; the security group admits nothing from outside and this is a demo cluster under a $15 budget
  #checkov:skip=CKV2_AWS_12:the default security group is deliberately left unmanaged (untagged, so the deploy role cannot adopt it); nothing is attached to it
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

resource "aws_subnet" "node" {
  for_each = local.nodes

  vpc_id            = aws_vpc.main.id
  cidr_block        = each.value.cidr
  availability_zone = each.value.az

  tags = { Name = each.value.name }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = { Name = var.project }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project}-public" }
}

resource "aws_route_table_association" "node" {
  for_each = local.nodes

  subnet_id      = aws_subnet.node[each.key].id
  route_table_id = aws_route_table.public.id
}
