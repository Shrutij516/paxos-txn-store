# Cluster stack: the VPC and the three paxosd nodes (docs/aws.md,
# DECISIONS.md entry 41). Applied by GitHub Actions as the deploy role from
# deploy/terraform/bootstrap, whose policy it must fit inside.
terraform {
  # 1.11: S3 native state locking (use_lockfile) is generally available.
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
  }

  # The bucket is passed at init, so the account ID is not written here:
  #   terraform init -backend-config="bucket=paxos-txn-store-tfstate-<account id>"
  backend "s3" {
    key          = "cluster/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    Project   = var.project
    ManagedBy = "terraform"
  }
}
