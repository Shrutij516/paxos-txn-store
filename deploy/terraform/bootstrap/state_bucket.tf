# The cluster stack's remote state. Locking uses S3 conditional writes
# (backend "s3" { use_lockfile = true }), so there is no DynamoDB table.
resource "aws_s3_bucket" "state" {
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES256) on purpose; a KMS key adds no access control over IAM and the bucket policy here and costs money (DECISIONS.md entry 40)
  #checkov:skip=CKV_AWS_18:access logs would need a second bucket; the bucket is private, TLS-only and reached by one role, and CloudTrail records its API calls
  #checkov:skip=CKV_AWS_144:no cross-region replication: versioning covers mistakes, and losing us-east-1 also loses everything this state describes
  #checkov:skip=CKV2_AWS_62:no event notifications: nothing consumes them; Terraform is the only writer
  bucket = "${var.project}-tfstate-${local.account_id}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket = aws_s3_bucket.state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-S3 (AES256), not SSE-KMS: who may read the state is decided by IAM
# and the bucket policy either way. An AWS managed KMS key adds no access
# control over that; a customer managed key would cost $1/month of the $15
# budget plus request charges (DECISIONS.md entry 40).
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Old state versions are kept 90 days for recovery, then expire.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.state]
}

data "aws_iam_policy_document" "state_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.state.arn,
      "${aws_s3_bucket.state.arn}/*",
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
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = data.aws_iam_policy_document.state_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.state]
}
