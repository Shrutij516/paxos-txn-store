# Short-lived outputs of the failover-under-load test
# (.github/workflows/aws-loadtest.yml, DECISIONS.md entry 46): node 1
# uploads loadgen's history, timeline and report, and every node a copy of
# its SQLite files, under runs/<run>/; the workflow downloads them to run
# the checkers and attaches them as an artifact. Objects expire after a
# day.
#
# The nodes' instance role gets its write access here, in the bucket
# policy, and not from an IAM policy: the deploy role may attach nothing to
# the node role but AmazonSSMManagedInstanceCore (deploy_policy.tf), and
# this keeps it that way. The node role is created later by the cluster
# stack, so the policy names it by aws:PrincipalArn in a condition rather
# than as a principal (a principal must exist when the policy is set).
resource "aws_s3_bucket" "loadtest" {
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES256) as for the state bucket; a KMS key adds no access control over the bucket policy and costs money (DECISIONS.md entry 40)
  #checkov:skip=CKV_AWS_18:access logs would need a second bucket; objects live one day and are written by one role and read by another
  #checkov:skip=CKV_AWS_144:no cross-region replication: the objects are disposable test outputs that expire after a day
  #checkov:skip=CKV2_AWS_62:no event notifications: nothing consumes them
  #checkov:skip=CKV_AWS_21:no versioning: the objects are disposable and expire after a day; nothing overwrites them (each run has its own prefix)
  bucket = "${var.project}-loadtest-${local.account_id}"

  # Disposable contents: destroying the bootstrap stack may delete them.
  force_destroy = true
}

resource "aws_s3_bucket_ownership_controls" "loadtest" {
  bucket = aws_s3_bucket.loadtest.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "loadtest" {
  bucket = aws_s3_bucket.loadtest.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "loadtest" {
  bucket = aws_s3_bucket.loadtest.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "loadtest" {
  bucket = aws_s3_bucket.loadtest.id

  rule {
    id     = "expire-runs"
    status = "Enabled"

    filter {}

    expiration {
      days = 1
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

locals {
  node_role_arn = "${local.iam_arn}:role/${var.project}-node"
}

data "aws_iam_policy_document" "loadtest_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.loadtest.arn,
      "${aws_s3_bucket.loadtest.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # The node role (exactly, by ARN, in this account) may write run outputs
  # and nothing else: no read, no list, no delete. Fixed values of
  # aws:PrincipalArn and aws:PrincipalAccount keep this policy non-public
  # for S3 Block Public Access.
  statement {
    #checkov:skip=CKV_AWS_70:Principal "*" is narrowed by conditions to the one node role ARN in this account; naming the role as principal would fail before the cluster stack creates it
    sid       = "NodeRoleWritesRuns"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.loadtest.arn}/runs/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = [local.node_role_arn]
    }
  }
}

resource "aws_s3_bucket_policy" "loadtest" {
  bucket = aws_s3_bucket.loadtest.id
  policy = data.aws_iam_policy_document.loadtest_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.loadtest]
}
