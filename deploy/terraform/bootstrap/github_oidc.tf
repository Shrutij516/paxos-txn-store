# GitHub Actions federation. No thumbprint: AWS validates
# token.actions.githubusercontent.com against its own trusted CAs.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# Only jobs of this repository that run in the GitHub environment may assume
# the role: not other repositories, branches, pull requests or forks.
#
# GitHub's sub claim carries the owner's and the repository's immutable
# numeric IDs next to their names:
#   repo:Shrutij516@90023638/paxos-txn-store@1396843261:environment:aws
# Matching it exactly (StringEquals, no wildcards) ties the role to this
# repository even if it is renamed and its old name is taken by someone
# else (DECISIONS.md entry 40).
locals {
  github_owner = split("/", var.github_repository)[0]
  github_repo  = split("/", var.github_repository)[1]
  github_sub   = "repo:${local.github_owner}@${var.github_owner_id}/${local.github_repo}@${var.github_repo_id}:environment:${var.github_environment}"
}

data "aws_iam_policy_document" "deploy_trust" {
  statement {
    sid     = "GitHubEnvironmentOnly"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.github_sub]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.project}-deploy"
  description          = "Assumed by GitHub Actions in ${var.github_repository} (environment ${var.github_environment}) to apply the cluster stack."
  assume_role_policy   = data.aws_iam_policy_document.deploy_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "deploy" {
  name   = "${var.project}-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
