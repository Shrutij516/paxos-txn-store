# Offline checks of the bootstrap stack. `terraform test` "applies" it with
# fake credentials, but every resource and the two account lookups are
# overridden, so no AWS API is called; the IAM policy documents are rendered
# by the real provider, locally.

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
}

override_data {
  target = data.aws_caller_identity.current
  values = {
    account_id = "123456789012"
  }
}

override_data {
  target = data.aws_region.current
  values = {
    region = "us-east-1"
  }
}

override_resource {
  target = aws_s3_bucket.state
  values = {
    arn = "arn:aws:s3:::paxos-txn-store-tfstate-123456789012"
  }
}

override_resource {
  target = aws_iam_openid_connect_provider.github
  values = {
    arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
  }
}

override_resource {
  target = aws_s3_bucket_ownership_controls.state
}

override_resource {
  target = aws_s3_bucket_public_access_block.state
}

override_resource {
  target = aws_s3_bucket_versioning.state
}

override_resource {
  target = aws_s3_bucket_server_side_encryption_configuration.state
}

override_resource {
  target = aws_s3_bucket_lifecycle_configuration.state
}

override_resource {
  target = aws_s3_bucket_policy.state
}

override_resource {
  target = aws_iam_role.deploy
}

override_resource {
  target = aws_iam_role_policy.deploy
}

run "render" {
  command = apply

  assert {
    condition     = aws_s3_bucket.state.bucket == "paxos-txn-store-tfstate-123456789012"
    error_message = "state bucket name must be paxos-txn-store-tfstate-<account id>"
  }

  assert {
    condition     = aws_iam_role.deploy.max_session_duration == 3600
    error_message = "deploy role sessions must be limited to 1 hour"
  }

  assert {
    condition = jsondecode(data.aws_iam_policy_document.deploy_trust.json).Statement[0].Condition.StringEquals == {
      "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
      "token.actions.githubusercontent.com:sub" = "repo:Shrutij516@90023638/paxos-txn-store@1396843261:environment:aws"
    }
    error_message = "trust must require aud=sts.amazonaws.com and exactly repo:Shrutij516@90023638/paxos-txn-store@1396843261:environment:aws as sub (GitHub's subject with immutable owner and repo IDs), nothing broader"
  }

  # The only statements allowed to use Resource "*" (DECISIONS.md entry 40).
  assert {
    condition = join(",", sort([
      for s in jsondecode(data.aws_iam_policy_document.deploy.json).Statement : s.Sid if s.Resource == "*"
    ])) == "Ec2Read,SsmReadCommandStatus,TagsReadForCleanupCheck"
    error_message = "a new statement uses Resource \"*\"; scope it or document it in DECISIONS.md entry 40"
  }

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.deploy.json).Statement :
      s.Resource != "*" || alltrue([for a in flatten([s.Action]) : can(regex("^(ec2:Describe|ssm:(Get|List|Describe)|tag:Get)", a))])
    ])
    error_message = "statements on Resource \"*\" must be read-only"
  }

  # iam:PassRole only for the node role, and only to EC2 when a service is
  # named (StringEqualsIfExists also admits AddRoleToInstanceProfile).
  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.deploy.json).Statement :
      s.Resource == "arn:aws:iam::123456789012:role/paxos-txn-store-node*" &&
      s.Condition == { StringEqualsIfExists = { "iam:PassedToService" = "ec2.amazonaws.com" } }
      if contains(flatten([s.Action]), "iam:PassRole")
    ])
    error_message = "iam:PassRole must be limited to the node role and to EC2"
  }

  # The internet gateway is authorized on attach and detach too (gap G1).
  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.deploy.json).Statement :
      contains(s.Resource, "arn:aws:ec2:us-east-1:123456789012:internet-gateway/*")
      if s.Sid == "Ec2ManageTagged"
      ]) && anytrue([
      for s in jsondecode(data.aws_iam_policy_document.deploy.json).Statement :
      contains(flatten([s.Action]), "ec2:AttachInternetGateway") if s.Sid == "Ec2ManageTagged"
    ])
    error_message = "Ec2ManageTagged must allow attaching our tagged internet gateways"
  }

  assert {
    condition     = !strcontains(data.aws_iam_policy_document.deploy.json, "iam:PutRolePolicy")
    error_message = "the deploy role must not write inline policies"
  }

  # Inline role policies may be at most 10,240 characters without whitespace.
  assert {
    condition     = length(replace(jsonencode(jsondecode(data.aws_iam_policy_document.deploy.json)), " ", "")) < 10240
    error_message = "deploy policy is over the 10,240-character inline policy limit"
  }
}
