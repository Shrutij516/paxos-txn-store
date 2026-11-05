output "state_bucket" {
  description = "Bucket for the cluster stack's backend \"s3\" block."
  value       = aws_s3_bucket.state.bucket
}

output "loadtest_bucket" {
  description = "Bucket for the failover-under-load test's outputs (aws-loadtest.yml); objects expire after a day."
  value       = aws_s3_bucket.loadtest.bucket
}

output "deploy_role_arn" {
  description = "Role for aws-actions/configure-aws-credentials (role-to-assume) in the aws environment."
  value       = aws_iam_role.deploy.arn
}
