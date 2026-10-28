# What the deploy role may do: exactly what the cluster stack needs, scoped
# to this project. Every EC2 resource the cluster stack creates must carry
# Project=<project> at creation (the provider's default_tags do that); IAM
# names must start with <project>-node. Each Resource "*" below is listed
# with its reason in DECISIONS.md entry 40.
locals {
  ec2_arn   = "arn:${local.partition}:ec2:${local.region}:${local.account_id}"
  iam_arn   = "arn:${local.partition}:iam::${local.account_id}"
  node_name = "${var.project}-node"

  # Resource types the cluster stack creates with a Project tag.
  ec2_tagged_types = ["vpc", "subnet", "internet-gateway", "route-table", "security-group", "instance"]
  ec2_tagged_arns  = [for t in local.ec2_tagged_types : "${local.ec2_arn}:${t}/*"]
}

data "aws_iam_policy_document" "deploy" {
  # ---- EC2 and VPC ----

  # Describe calls have no resource-level permissions. Read-only.
  statement {
    sid       = "Ec2Read"
    actions   = ["ec2:Describe*"]
    resources = ["*"]
  }

  # New VPCs, subnets, gateways, route tables and security groups must be
  # tagged Project=<project> in the create request itself.
  statement {
    sid = "Ec2CreateTagged"
    actions = [
      "ec2:CreateVpc",
      "ec2:CreateSubnet",
      "ec2:CreateInternetGateway",
      "ec2:CreateRouteTable",
      "ec2:CreateSecurityGroup",
    ]
    resources = [for t in ["vpc", "subnet", "internet-gateway", "route-table", "security-group"] : "${local.ec2_arn}:${t}/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Project"
      values   = [var.project]
    }
  }

  # Creating things inside a VPC, or attaching a gateway to it, also needs
  # the VPC to be one of ours.
  statement {
    sid = "Ec2UseTaggedVpc"
    actions = [
      "ec2:CreateSubnet",
      "ec2:CreateRouteTable",
      "ec2:CreateSecurityGroup",
      "ec2:AttachInternetGateway",
      "ec2:DetachInternetGateway",
    ]
    resources = ["${local.ec2_arn}:vpc/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }
  }

  # Instances: tagged at launch and only small types.
  statement {
    sid       = "Ec2RunTaggedInstances"
    actions   = ["ec2:RunInstances"]
    resources = ["${local.ec2_arn}:instance/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Project"
      values   = [var.project]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:InstanceType"
      values   = var.allowed_instance_types
    }
  }

  # ...into our subnets and security groups only...
  statement {
    sid       = "Ec2RunInTaggedNetwork"
    actions   = ["ec2:RunInstances"]
    resources = ["${local.ec2_arn}:subnet/*", "${local.ec2_arn}:security-group/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }
  }

  # ...from Amazon-owned AMIs only...
  statement {
    sid       = "Ec2RunAmazonImages"
    actions   = ["ec2:RunInstances"]
    resources = ["arn:${local.partition}:ec2:${local.region}::image/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:Owner"
      values   = ["amazon"]
    }
  }

  # ...with the root volume and network interface the launch creates.
  # These exist only as part of a launch the statements above allow.
  statement {
    sid       = "Ec2RunLaunchParts"
    actions   = ["ec2:RunInstances"]
    resources = ["${local.ec2_arn}:volume/*", "${local.ec2_arn}:network-interface/*"]
  }

  # Tags may be written while creating one of the above...
  statement {
    sid       = "Ec2TagOnCreate"
    actions   = ["ec2:CreateTags"]
    resources = concat(local.ec2_tagged_arns, ["${local.ec2_arn}:volume/*", "${local.ec2_arn}:network-interface/*", "${local.ec2_arn}:security-group-rule/*"])

    condition {
      test     = "StringEquals"
      variable = "ec2:CreateAction"
      values = [
        "CreateVpc",
        "CreateSubnet",
        "CreateInternetGateway",
        "CreateRouteTable",
        "CreateSecurityGroup",
        "RunInstances",
        "AuthorizeSecurityGroupIngress",
        "AuthorizeSecurityGroupEgress",
      ]
    }
  }

  # ...and everything else (changing, re-tagging, deleting) needs a
  # resource that is already ours.
  statement {
    sid = "Ec2ManageTagged"
    actions = [
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:DeleteVpc",
      "ec2:ModifyVpcAttribute",
      "ec2:DeleteSubnet",
      "ec2:ModifySubnetAttribute",
      "ec2:DeleteInternetGateway",
      "ec2:DeleteRouteTable",
      "ec2:CreateRoute",
      "ec2:ReplaceRoute",
      "ec2:DeleteRoute",
      "ec2:AssociateRouteTable",
      "ec2:DisassociateRouteTable",
      "ec2:ReplaceRouteTableAssociation",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:ModifySecurityGroupRules",
      "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
      "ec2:UpdateSecurityGroupRuleDescriptionsEgress",
      "ec2:DeleteSecurityGroup",
      "ec2:StartInstances",
      "ec2:StopInstances",
      "ec2:RebootInstances",
      "ec2:TerminateInstances",
      "ec2:ModifyInstanceAttribute",
      "ec2:ModifyInstanceMetadataOptions",
      "ec2:AssociateIamInstanceProfile",
      "ec2:ReplaceIamInstanceProfileAssociation",
      "ec2:DisassociateIamInstanceProfile",
    ]
    resources = local.ec2_tagged_arns

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }
  }

  # Security group rules are separate resources in the authorize/revoke
  # calls; the group itself must still pass Ec2ManageTagged.
  statement {
    sid = "Ec2SecurityGroupRules"
    actions = [
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:ModifySecurityGroupRules",
      "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
      "ec2:UpdateSecurityGroupRuleDescriptionsEgress",
    ]
    resources = ["${local.ec2_arn}:security-group-rule/*"]
  }

  # ---- Terraform state ----

  # Listing is per bucket; the backend lists to find workspaces.
  statement {
    sid       = "StateBucketList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.state.arn]
  }

  statement {
    sid       = "StateObjects"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.state.arn}/${var.state_key_prefix}*"]
  }

  # S3 native locking deletes the .tflock object to release the lock. The
  # state itself can be overwritten (versioning keeps the old ones) but not
  # deleted.
  statement {
    sid       = "StateLockRelease"
    actions   = ["s3:DeleteObject"]
    resources = ["${aws_s3_bucket.state.arn}/${var.state_key_prefix}*.tflock"]
  }

  # ---- IAM: the node instance role and profile only ----

  # <project>-node*, not <project>-*: the deploy role is itself named
  # <project>-deploy and must not be able to change its own permissions.
  statement {
    sid = "IamNodeRole"
    actions = [
      "iam:CreateRole",
      "iam:GetRole",
      "iam:DeleteRole",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:UpdateRole",
      "iam:UpdateRoleDescription",
      "iam:UpdateAssumeRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = ["${local.iam_arn}:role/${local.node_name}*"]
  }

  # The node role may only get AmazonSSMManagedInstanceCore attached, and no
  # inline policies at all (PutRolePolicy is not granted). Otherwise the
  # deploy role could give the node role more than it has itself and use it
  # through an instance.
  statement {
    sid       = "IamNodeRoleSsmPolicyOnly"
    actions   = ["iam:AttachRolePolicy", "iam:DetachRolePolicy"]
    resources = ["${local.iam_arn}:role/${local.node_name}*"]

    condition {
      test     = "ArnEquals"
      variable = "iam:PolicyARN"
      values   = ["arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"]
    }
  }

  statement {
    sid = "IamNodeInstanceProfile"
    actions = [
      "iam:CreateInstanceProfile",
      "iam:GetInstanceProfile",
      "iam:DeleteInstanceProfile",
      "iam:AddRoleToInstanceProfile",
      "iam:RemoveRoleFromInstanceProfile",
      "iam:TagInstanceProfile",
      "iam:UntagInstanceProfile",
    ]
    resources = ["${local.iam_arn}:instance-profile/${local.node_name}*"]
  }

  statement {
    sid       = "IamPassNodeRoleToEc2"
    actions   = ["iam:PassRole"]
    resources = ["${local.iam_arn}:role/${local.node_name}*"]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }

  # ---- SSM ----

  # The current Amazon Linux AMI ID, from AWS's public parameters.
  statement {
    sid       = "SsmPublicAmiParameters"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = ["arn:${local.partition}:ssm:${local.region}::parameter/aws/service/*"]
  }

  # Run shell commands (pull and restart paxosd) on our instances only.
  statement {
    sid       = "SsmRunOnTaggedInstances"
    actions   = ["ssm:SendCommand"]
    resources = ["${local.ec2_arn}:instance/*"]

    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/Project"
      values   = [var.project]
    }
  }

  statement {
    sid       = "SsmRunShellScriptDocument"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:${local.partition}:ssm:${local.region}::document/AWS-RunShellScript"]
  }

  # Command status and the managed-instance list have no resource-level
  # permissions. Read-only.
  statement {
    sid = "SsmReadCommandStatus"
    actions = [
      "ssm:GetCommandInvocation",
      "ssm:ListCommands",
      "ssm:ListCommandInvocations",
      "ssm:DescribeInstanceInformation",
    ]
    resources = ["*"]
  }
}
