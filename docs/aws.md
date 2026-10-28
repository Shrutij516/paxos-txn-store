# Running on AWS

The AWS deployment has two Terraform stacks in `deploy/terraform/`:

- `bootstrap/`: applied once, by hand, from an operator's machine, with local state. It creates the S3 bucket that holds the cluster stack's state, the GitHub OIDC provider, and the IAM role GitHub Actions assumes to deploy.
- the cluster stack (a later phase): the VPC and nodes. It is applied by GitHub Actions as that role, with its state in the bucket.

Why it is split this way, and what the deploy role may do, is in DECISIONS.md entry 40.

## Bootstrap

Account 289112219115, region us-east-1. Run it as an administrator: the IAM user `shruti-admin`, with MFA. It needs Terraform 1.10 or later; this was written and checked with 1.17.1 and the AWS provider 6.71.0.

### What it creates

`terraform plan` should end with **`Plan: 10 to add, 0 to change, 0 to destroy.`** These are the 10 resources:

| Resource | Name |
|---|---|
| `aws_s3_bucket.state` | `paxos-txn-store-tfstate-289112219115` (prevent_destroy) |
| `aws_s3_bucket_ownership_controls.state` | BucketOwnerEnforced (no ACLs) |
| `aws_s3_bucket_public_access_block.state` | all four blocks on |
| `aws_s3_bucket_versioning.state` | Enabled |
| `aws_s3_bucket_server_side_encryption_configuration.state` | AES256 (SSE-S3) |
| `aws_s3_bucket_lifecycle_configuration.state` | old state versions expire after 90 days |
| `aws_s3_bucket_policy.state` | denies any request not made over TLS |
| `aws_iam_openid_connect_provider.github` | `token.actions.githubusercontent.com`, audience `sts.amazonaws.com` |
| `aws_iam_role.deploy` | `paxos-txn-store-deploy`, 1-hour sessions |
| `aws_iam_role_policy.deploy` | `paxos-txn-store-deploy` (inline permissions) |

The plan also reads 6 data sources: the caller identity (the account ID comes from here and is never written into the code), the partition, the region, and three policy documents (the bucket policy, the role's trust policy and its permissions). It creates no DynamoDB table and no budget; the $15 monthly budget already exists. If the plan wants to change or destroy anything, or shows a count other than 10, stop.

### Steps (macOS)

```sh
brew tap hashicorp/tap
brew install hashicorp/tap/terraform    # 1.10 or later
terraform version

# Use shruti-admin's credentials (a named profile, or aws-vault, or
# temporary credentials from `aws sts get-session-token --serial-number
# <your MFA device ARN> --token-code <code>` if your policies require MFA).
export AWS_PROFILE=<your shruti-admin profile>
aws sts get-caller-identity
# Must show "Account": "289112219115" and an Arn ending in user/shruti-admin.

cd deploy/terraform/bootstrap
terraform init                          # downloads hashicorp/aws 6.71.x, checked against .terraform.lock.hcl
terraform test                          # offline policy checks: no AWS calls, should say "1 passed"
terraform plan -out=bootstrap.tfplan    # check the table above: 10 to add
terraform apply bootstrap.tfplan
terraform output
```

`terraform output` prints two values, `state_bucket` and `deploy_role_arn`, which you need in the next step.

Then:

- **Back up the state.** `terraform.tfstate` stays in `deploy/terraform/bootstrap/`, is ignored by git, and is the only record Terraform has of these 10 resources. It holds no secrets. Copy it somewhere private and durable, not into this repository. If it is lost, the resources keep working but have to be re-imported (`terraform import`) before Terraform can change or remove them.
- **Commit the lock file if it changed.** `terraform init` on a Mac may add `zh:` checksums to `.terraform.lock.hcl`; commit that change. The checksums already in the file were taken from HashiCorp's signed release for linux and darwin, amd64 and arm64.

### The GitHub environment "aws"

The deploy role trusts exactly one OIDC subject: `repo:Shrutij516/paxos-txn-store:environment:aws`. Only a job that declares `environment: aws` gets a token with that subject. The environment's own rules decide which branches may use it and whether someone must approve. Create it before any workflow uses the role:

1. In the repository on GitHub, open **Settings → Environments → New environment**. Name it exactly `aws` (lower case) and choose **Configure environment**.
2. Under **Deployment branches and tags**, choose **Selected branches and tags** and add the rule `main`. Do not skip this step. The trust policy only checks the environment, so without this rule a workflow on any branch could deploy.
3. Optional but recommended: under **Required reviewers**, add yourself. Every deploy and destroy then waits for your approval.
4. Under **Environment variables**, add these (they are not secrets):
   - `AWS_ROLE_ARN` = the `deploy_role_arn` output
   - `AWS_REGION` = `us-east-1`
   - `TF_STATE_BUCKET` = the `state_bucket` output
5. Add no AWS access keys anywhere. Workflows get 1-hour credentials with `aws-actions/configure-aws-credentials` (`role-to-assume: ${{ vars.AWS_ROLE_ARN }}`) and `permissions: id-token: write`.

### Undoing the bootstrap

Undo it only after the cluster stack has been destroyed and nothing uses the role. The bucket has `prevent_destroy`, and its versioning keeps every old state, so removing it takes deliberate steps:

```sh
cd deploy/terraform/bootstrap
B=$(terraform output -raw state_bucket)

# 1. Empty the bucket, including old versions and delete markers. Each
#    command fails with "null" if there is nothing of that kind left,
#    which is fine. Each handles up to 1000 objects; repeat if needed.
aws s3api delete-objects --bucket "$B" --delete \
  "$(aws s3api list-object-versions --bucket "$B" --query '{Objects: Versions[].{Key: Key, VersionId: VersionId}}' --output json)"
aws s3api delete-objects --bucket "$B" --delete \
  "$(aws s3api list-object-versions --bucket "$B" --query '{Objects: DeleteMarkers[].{Key: Key, VersionId: VersionId}}' --output json)"

# 2. Allow the bucket to be destroyed: in state_bucket.tf, set
#    prevent_destroy = false (do not commit this change).

# 3. Destroy all 10 resources.
terraform plan -destroy -out=destroy.tfplan    # should say: 10 to destroy
terraform apply destroy.tfplan
```

Finally, delete the GitHub environment `aws` (**Settings → Environments → aws → Delete**) and the backed-up state file.

To remove only GitHub's access and keep the state bucket, delete the environment. No job can then get the role's subject. To also revoke the role, remove the role, its policy and the OIDC provider with `terraform destroy -target=aws_iam_role_policy.deploy -target=aws_iam_role.deploy -target=aws_iam_openid_connect_provider.github`.
