variable "region" {
  description = "AWS region. The bootstrap's deploy role is scoped to us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project tag and name prefix; must match the bootstrap stack, whose deploy role only manages resources tagged Project=<project>."
  type        = string
  default     = "paxos-txn-store"
}

variable "image_digest" {
  description = "Digest of the paxosd image to run, ghcr.io/shrutij516/paxos-txn-store@<digest>. Never a tag: the release workflow pushes one image per commit SHA."
  type        = string

  validation {
    condition     = can(regex("^sha256:[0-9a-f]{64}$", var.image_digest))
    error_message = "image_digest must be a sha256 digest (sha256: and 64 lowercase hex digits), not a tag."
  }
}

variable "instance_type" {
  description = "Instance type of all three nodes. Must be in the bootstrap's allowed_instance_types (tests/cluster.tftest.hcl checks that)."
  type        = string
  default     = "t3.small"
}

variable "client_cidrs" {
  description = "IPv4 CIDRs allowed to reach the nodes' client port (7001). Empty: no access from outside the cluster."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.client_cidrs : can(cidrhost(c, 0)) && !endswith(c, "/0")])
    error_message = "client_cidrs must be IPv4 CIDRs and none may be a /0 (the whole internet)."
  }
}

variable "zone_ids" {
  description = "The three availability zone IDs of nodes 1 to 3. Zone IDs are the same in every account; use1-az3 is avoided because it lacks current instance families."
  type        = list(string)
  default     = ["use1-az1", "use1-az2", "use1-az4"]

  validation {
    condition     = length(var.zone_ids) == 3 && length(distinct(var.zone_ids)) == 3
    error_message = "zone_ids must name three different zones, one per node."
  }
}
