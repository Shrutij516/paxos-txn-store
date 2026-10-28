# Running on AWS

The AWS deployment has two Terraform stacks in `deploy/terraform/`:

- `bootstrap/`: applied once, by hand, from an operator's machine, with local state. It creates the S3 bucket that holds the cluster stack's state, the GitHub OIDC provider, and the IAM role GitHub Actions assumes to deploy.
- `cluster/`: the VPC and the three nodes. It is applied as that role (by GitHub Actions in the `aws` environment, in a later phase), with its state in the bucket. See [Cluster stack](#cluster-stack) below.

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

## Cluster stack

`deploy/terraform/cluster/` creates the following, all tagged `Project=paxos-txn-store`:

- **Network:** one VPC (10.0.0.0/16), three public subnets 10.0.N.0/24 in zones `use1-az1`, `use1-az2` and `use1-az4`, an internet gateway and one route table.
- **Security group:** one, shared by the three nodes.
- **IAM:** the node role and instance profile `paxos-txn-store-node`.
- **Nodes:** three t3.small instances at 10.0.1.10, 10.0.2.10 and 10.0.3.10. Each has an encrypted gp3 root disk and an encrypted gp3 data disk mounted at `/data`.

Every node runs paxosd from `ghcr.io/shrutij516/paxos-txn-store@<image_digest>`. It uses the same flags as its service in `deploy/compose.yaml` and the unchanged `deploy/cluster.json`. The names `node1` to `node3` and `jaeger` resolve to the fixed IPs.

Node 1 also runs Prometheus, Grafana and Jaeger, with the local stack's configs and image tags:
- Their UIs listen on 127.0.0.1 only.
- Jaeger's OTLP port, 4317, listens on the private IP so the other nodes can send traces.

Why it is built this way is in DECISIONS.md entry 41.

### Before the first apply: deploy-role gaps

The cluster stack was checked against `deploy/terraform/bootstrap/deploy_policy.tf`, one API call at a time (table below). These calls are missing or unconfirmed. Fix the blocking ones in the bootstrap and re-apply it before the first cluster apply.

| # | Call | Needed for | Status |
|---|---|---|---|
| G1 | `ec2:AttachInternetGateway`, `ec2:DetachInternetGateway` on `internet-gateway/*` | attaching the IGW (create), detaching it (destroy) | **Blocking.** `Ec2UseTaggedVpc` allows them on `vpc/*` only, and both calls also authorize the gateway. Fix: add both actions to `Ec2ManageTagged`. |
| G2 | `iam:PassRole` while calling `iam:AddRoleToInstanceProfile` | putting the node role into the instance profile | **Unverified, may block.** `IamPassNodeRoleToEc2` requires `iam:PassedToService = ec2.amazonaws.com`, and that key may be absent for this call. If the apply fails here, add a statement that allows `iam:PassRole` on `role/paxos-txn-store-node*` without the service condition. |
| G3 | `ec2:ModifyInstanceCreditSpecification` | only when `credit_specification` changes after create (the create passes it to `RunInstances`) | Not needed now. |
| G4 | `ec2:CreateTags` and `ec2:DeleteTags` on existing `security-group-rule/*` and `volume/*` | only when their tags change after create (for example a new default tag) | Not needed now. `Ec2ManageTagged` does not list those resource types. |
| G5 | `ec2:CreateRoute` with the internet gateway as target | the route table's default route | Unverified: if EC2 also authorizes the gateway resource for this call, it needs `internet-gateway/*` with the same tag condition. |
| G6 | KMS use of the `aws/ebs` key | encrypted volumes at launch | Expected to be covered by the AWS managed key's own policy, which allows EBS use by any principal in the account. Unverified. |

### Deploy role fit, call by call

All `ec2:Describe*` reads are allowed by `Ec2Read`, and all S3 backend calls by `StateBucketList`, `StateObjects` and `StateLockRelease`.

| Resource | Create | Read | Destroy | Statements |
|---|---|---|---|---|
| backend | `ListBucket`; `GetObject`, `PutObject` on `cluster/terraform.tfstate` and `.tflock`; `DeleteObject` on `.tflock` | same | same | StateBucketList, StateObjects, StateLockRelease |
| `data.aws_availability_zones` | — | `DescribeAvailabilityZones` | — | Ec2Read |
| `data.aws_ssm_parameter` | — | `ssm:GetParameter` on `/aws/service/ami-amazon-linux-latest/…` | — | SsmPublicAmiParameters |
| `aws_vpc` | `CreateVpc` + tags, `ModifyVpcAttribute` (DNS) | `DescribeVpcs`, `DescribeVpcAttribute`, `DescribeRouteTables`, `DescribeNetworkAcls`, `DescribeSecurityGroups` | `DeleteVpc` | Ec2CreateTagged, Ec2TagOnCreate, Ec2ManageTagged |
| `aws_subnet` ×3 | `CreateSubnet` + tags (subnet and VPC) | `DescribeSubnets` | `DeleteSubnet` | Ec2CreateTagged, Ec2UseTaggedVpc, Ec2TagOnCreate, Ec2ManageTagged |
| `aws_internet_gateway` | `CreateInternetGateway` + tags, `AttachInternetGateway` | `DescribeInternetGateways` | `DetachInternetGateway`, `DeleteInternetGateway` | Ec2CreateTagged, Ec2TagOnCreate, Ec2UseTaggedVpc (VPC side only), **G1** |
| `aws_route_table` | `CreateRouteTable` + tags, `CreateRoute` | `DescribeRouteTables` | `DeleteRoute`, `DeleteRouteTable` | Ec2CreateTagged, Ec2UseTaggedVpc, Ec2TagOnCreate, Ec2ManageTagged, **G5** |
| `aws_route_table_association` ×3 | `AssociateRouteTable` | `DescribeRouteTables` | `DisassociateRouteTable` | Ec2ManageTagged |
| `aws_security_group` | `CreateSecurityGroup` + tags, `RevokeSecurityGroupEgress` (default allow-all rule) | `DescribeSecurityGroups` | `DeleteSecurityGroup` | Ec2CreateTagged, Ec2UseTaggedVpc, Ec2TagOnCreate, Ec2ManageTagged, Ec2SecurityGroupRules |
| `aws_vpc_security_group_*_rule` ×7 | `AuthorizeSecurityGroupIngress`/`Egress` + tags | `DescribeSecurityGroupRules` | `RevokeSecurityGroupIngress`/`Egress` | Ec2ManageTagged (group), Ec2SecurityGroupRules (rule), Ec2TagOnCreate, **G4** |
| `aws_iam_role` | `CreateRole`, `TagRole` | `GetRole`, `ListRolePolicies`, `ListAttachedRolePolicies` | `ListInstanceProfilesForRole`, `DeleteRole` | IamNodeRole |
| `aws_iam_role_policy_attachment` | `AttachRolePolicy` (AmazonSSMManagedInstanceCore) | `ListAttachedRolePolicies` | `DetachRolePolicy` | IamNodeRoleSsmPolicyOnly, IamNodeRole |
| `aws_iam_instance_profile` | `CreateInstanceProfile`, `TagInstanceProfile`, `AddRoleToInstanceProfile` (+`PassRole`) | `GetInstanceProfile` | `RemoveRoleFromInstanceProfile`, `DeleteInstanceProfile` | IamNodeInstanceProfile, IamPassNodeRoleToEc2, **G2** |
| `aws_instance` ×3 | `RunInstances` (instance, subnet, security group, image, volumes, network interface) + tags, `iam:PassRole` | `DescribeInstances`, `DescribeInstanceAttribute`, `DescribeVolumes`, `DescribeInstanceCreditSpecifications` | `TerminateInstances` | Ec2RunTaggedInstances, Ec2RunInTaggedNetwork, Ec2RunAmazonImages, Ec2RunLaunchParts, Ec2TagOnCreate, IamPassNodeRoleToEc2, **G3**, **G6** |

### Steps

Prerequisites:
- The bootstrap gaps above are fixed.
- You have an image digest from the release workflow, for example from `docker buildx imagetools inspect ghcr.io/shrutij516/paxos-txn-store:<commit sha>`.

The cluster stack is meant to be applied by a workflow in the `aws` environment, which is a later phase. Running it by hand as an administrator works the same way:

```sh
cd deploy/terraform/cluster
terraform init -backend-config="bucket=paxos-txn-store-tfstate-289112219115"
terraform test                                   # offline: 4 passed
terraform plan -var image_digest=sha256:<digest> -out=cluster.tfplan
terraform apply cluster.tfplan
terraform output                                 # IDs, AZs, IPs, SSM port-forward commands
```

The plan should create 23 resources:
- **Network (10):** the VPC, 3 subnets, the internet gateway, the route table and 3 route table associations, plus the security group.
- **Security group rules (7):** 3 ingress (7001, 9100 and 4317 from the group itself) and 4 egress (the same three ports to the group, and 443 to anywhere).
- **IAM (3):** the role, its policy attachment and the instance profile.
- **Nodes (3):** the instances.

**Grafana and Jaeger** are reachable only by SSM port forwarding, with the commands in `terraform output`. You need the AWS CLI and the Session Manager plugin. Grafana's anonymous viewer is on, as in the local stack; its admin login is still `admin`/`admin`, reachable only through the tunnel.

**Rolling out a new image** does not replace the node: run `paxosd-run sha256:<digest>` on each node over SSM. `user_data` and the AMI are ignored after the first apply.

**Cost while running** (us-east-1, on-demand):

| Item | Per hour |
|---|---|
| 3 × t3.small | $0.0624 |
| 54 GB of gp3 | $0.0059 |
| 3 public IPv4 addresses | $0.015 |
| **Total** | **about $0.083** |

That is about $2.00 a day, or $61 a month if left running, which is far over the $15 budget. Apply it for a session, then destroy it:

```sh
terraform destroy -var image_digest=sha256:<digest>
```

Traffic between AZs ($0.01/GB each way) adds to this under load.
