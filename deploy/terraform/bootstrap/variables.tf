variable "region" {
  description = "AWS region for the state bucket and everything the deploy role manages."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project tag and name prefix. The deploy role may only touch EC2 resources tagged Project=<project> and IAM names starting with <project>-node."
  type        = string
  default     = "paxos-txn-store"
}

variable "github_repository" {
  description = "owner/name of the only repository whose workflows may assume the deploy role. Case-sensitive, as GitHub writes it in the token's sub claim."
  type        = string
  default     = "Shrutij516/paxos-txn-store"
}

variable "github_environment" {
  description = "GitHub environment a job must run in to assume the deploy role."
  type        = string
  default     = "aws"
}

variable "state_key_prefix" {
  description = "Key prefix in the state bucket the deploy role may use (the cluster stack's state and its lock file)."
  type        = string
  default     = "cluster/"
}

variable "allowed_instance_types" {
  description = "Instance types the deploy role may launch; keeps a mistake inside the monthly budget."
  type        = list(string)
  default     = ["t3.micro", "t3.small", "t3.medium", "t3a.micro", "t3a.small", "t3a.medium"]
}
