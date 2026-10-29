# Running on AWS

The AWS deployment has two Terraform stacks in `deploy/terraform/`:

- `bootstrap/`: applied once, by hand, from an operator's machine, with local state. It creates the S3 bucket that holds the cluster stack's state, the GitHub OIDC provider, and the IAM role GitHub Actions assumes to deploy.
- `cluster/`: the VPC and the three nodes. It is applied as that role by `.github/workflows/deploy.yml` and removed by `destroy.yml`, both in the `aws` environment, with its state in the bucket. See [Cluster stack](#cluster-stack) below.

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
3. Recommended: under **Required reviewers**, add yourself and choose **Save protection rules**. Every job in the `aws` environment then waits for your approval: in a deploy that is the plan job and the apply job, each approved separately; in a destroy it is the destroy job.
4. Add no AWS access keys anywhere, neither as secrets nor as variables. Workflows get 1-hour credentials from `aws-actions/configure-aws-credentials` and `permissions: id-token: write`.

### Repository variables

The workflows read two repository variables (not secrets: neither value is confidential). Get the values with `terraform output` in `deploy/terraform/bootstrap`, then:

1. In the repository on GitHub, open **Settings → Secrets and variables → Actions**.
2. Choose the **Variables** tab, then **New repository variable**.
3. **Name** `AWS_DEPLOY_ROLE_ARN`, **Value** the `deploy_role_arn` output (`arn:aws:iam::289112219115:role/paxos-txn-store-deploy`). Choose **Add variable**.
4. **New repository variable** again: **Name** `TF_STATE_BUCKET`, **Value** the `state_bucket` output (`paxos-txn-store-tfstate-289112219115`). Choose **Add variable**.

The Variables tab should now list exactly these two. If a variable of the same name was added to the `aws` environment earlier (an older version of this page said to), delete it there: environment variables override repository ones.

### Re-applying the bootstrap after a policy change

Phase 8c-3 changed the deploy role's policy, and nothing else. It added the fixes for gaps G1 and G2 (see [Before the first apply](#before-the-first-apply-deploy-role-gaps)) and read access to the tagging API for the destroy workflow. Re-apply it from your Mac before the first deploy:

```sh
cd deploy/terraform/bootstrap
git pull                                  # the branch with the change, or main once merged
terraform init                            # same provider, 6.71.0
terraform test                            # offline: 1 passed
terraform plan -out=bootstrap.tfplan
terraform apply bootstrap.tfplan
```

The plan must show **`Plan: 0 to add, 1 to change, 0 to destroy.`** The one change is `aws_iam_role_policy.deploy` ("will be updated in-place"), and its `policy` diff should contain exactly these changes:

- **`Ec2ManageTagged`:** gains `ec2:AttachInternetGateway` and `ec2:DetachInternetGateway`.
- **`Ec2UseTaggedVpc`:** loses those two actions. `Ec2ManageTagged` already covers the VPC side, and now also the gateway.
- **`IamPassNodeRoleToEc2`:** its condition changes from `StringEquals` to `StringEqualsIfExists` on `iam:PassedToService` = `ec2.amazonaws.com`.
- **New statement `TagsReadForCleanupCheck`:** `tag:GetResources` on `*`.

The new policy is 6,739 characters, under the 10,240 limit. If the plan changes or replaces the bucket, the OIDC provider or the role itself, stop. (The `#checkov:skip` comments on the bucket are comments and change nothing.)

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

The cluster stack was checked against `deploy/terraform/bootstrap/deploy_policy.tf`, one API call at a time (table below). G1 and G2 are fixed in the bootstrap since Phase 8c-3; [re-apply it](#re-applying-the-bootstrap-after-a-policy-change) before the first deploy. The rest are not needed yet, or will only be confirmed by the first apply.

| # | Call | Needed for | Status |
|---|---|---|---|
| G1 | `ec2:AttachInternetGateway`, `ec2:DetachInternetGateway` on `internet-gateway/*` | attaching the IGW (create), detaching it (destroy) | **Fixed:** both actions are now in `Ec2ManageTagged`, which covers our tagged gateways and VPCs. |
| G2 | `iam:PassRole` while calling `iam:AddRoleToInstanceProfile` | putting the node role into the instance profile | **Fixed:** the condition is now `StringEqualsIfExists`. It still allows passing the node role only, and only to EC2 when a service is named, and it no longer fails when `AddRoleToInstanceProfile` sends no service. |
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
| `aws_internet_gateway` | `CreateInternetGateway` + tags, `AttachInternetGateway` | `DescribeInternetGateways` | `DetachInternetGateway`, `DeleteInternetGateway` | Ec2CreateTagged, Ec2TagOnCreate, Ec2ManageTagged (gateway and VPC, G1 fixed) |
| `aws_route_table` | `CreateRouteTable` + tags, `CreateRoute` | `DescribeRouteTables` | `DeleteRoute`, `DeleteRouteTable` | Ec2CreateTagged, Ec2UseTaggedVpc, Ec2TagOnCreate, Ec2ManageTagged, **G5** |
| `aws_route_table_association` ×3 | `AssociateRouteTable` | `DescribeRouteTables` | `DisassociateRouteTable` | Ec2ManageTagged |
| `aws_security_group` | `CreateSecurityGroup` + tags, `RevokeSecurityGroupEgress` (default allow-all rule) | `DescribeSecurityGroups` | `DeleteSecurityGroup` | Ec2CreateTagged, Ec2UseTaggedVpc, Ec2TagOnCreate, Ec2ManageTagged, Ec2SecurityGroupRules |
| `aws_vpc_security_group_*_rule` ×7 | `AuthorizeSecurityGroupIngress`/`Egress` + tags | `DescribeSecurityGroupRules` | `RevokeSecurityGroupIngress`/`Egress` | Ec2ManageTagged (group), Ec2SecurityGroupRules (rule), Ec2TagOnCreate, **G4** |
| `aws_iam_role` | `CreateRole`, `TagRole` | `GetRole`, `ListRolePolicies`, `ListAttachedRolePolicies` | `ListInstanceProfilesForRole`, `DeleteRole` | IamNodeRole |
| `aws_iam_role_policy_attachment` | `AttachRolePolicy` (AmazonSSMManagedInstanceCore) | `ListAttachedRolePolicies` | `DetachRolePolicy` | IamNodeRoleSsmPolicyOnly, IamNodeRole |
| `aws_iam_instance_profile` | `CreateInstanceProfile`, `TagInstanceProfile`, `AddRoleToInstanceProfile` (+`PassRole`) | `GetInstanceProfile` | `RemoveRoleFromInstanceProfile`, `DeleteInstanceProfile` | IamNodeInstanceProfile, IamPassNodeRoleToEc2 (G2 fixed) |
| `aws_instance` ×3 | `RunInstances` (instance, subnet, security group, image, volumes, network interface) + tags, `iam:PassRole` | `DescribeInstances`, `DescribeInstanceAttribute`, `DescribeVolumes`, `DescribeInstanceCreditSpecifications` | `TerminateInstances` | Ec2RunTaggedInstances, Ec2RunInTaggedNetwork, Ec2RunAmazonImages, Ec2RunLaunchParts, Ec2TagOnCreate, IamPassNodeRoleToEc2, **G3**, **G6** |

The workflows also use, outside Terraform:

| Workflow step | Calls | Statements |
|---|---|---|
| credentials | `sts:AssumeRoleWithWebIdentity` | the role's trust policy |
| smoke check | `ssm:DescribeInstanceInformation`, `ssm:GetCommandInvocation`; `ssm:SendCommand` (`AWS-RunShellScript`, tagged instances) | SsmReadCommandStatus, SsmRunShellScriptDocument, SsmRunOnTaggedInstances |
| leftover check | `tag:GetResources`, `ec2:DescribeInstances`, `ec2:DescribeVpcs` | TagsReadForCleanupCheck, Ec2Read |

### Deploying

Prerequisites, all done once:
- The bootstrap is applied, with the [latest policy](#re-applying-the-bootstrap-after-a-policy-change).
- The [`aws` environment](#the-github-environment-aws) exists.
- The two [repository variables](#repository-variables) are set.

**Run it:**

1. **Actions → deploy → Run workflow.** Leave **Use workflow from** on `main`, since the environment only admits `main`.
2. **image_digest:** leave it empty to deploy the image that `release.yml` pushed for the head of `main`. The workflow looks up `ghcr.io/shrutij516/paxos-txn-store:<commit sha>` anonymously and fails if there is none. If `release.yml` has not finished for that commit yet, wait for it. To deploy another build, paste its digest (`sha256:…`); the workflow checks that it exists.
3. **Run workflow.**

**Approve the plan job:** the run shows **plan: Waiting**. Choose **Review deployments**, tick **aws**, then **Approve and deploy**. The job resolves the digest, runs `terraform plan`, and prints the plan in its log and on the run's summary page.

**Read the plan, then approve the apply job:**
- The first deploy creates 23 resources (listed below) and changes or destroys nothing.
- Later deploys usually show no changes. `user_data` and the AMI are ignored after creation.

Then **Review deployments → aws → Approve and deploy** for **apply**. To stop instead, choose **Reject**, or cancel the run. The saved plan expires with its artifact after 1 day.

**What the apply job does:**
- It applies exactly the saved plan. Terraform refuses it if the state changed in between.
- It then runs the smoke check (`scripts/aws-smoke.sh`):
  1. Waits up to 10 minutes for all three nodes to be online in SSM.
  2. On each node, waits up to 15 minutes for paxosd's `/readyz` to answer 200. That means every shard has a leader and the node is caught up; the first boot has to install Docker and pull images first.
  3. Checks that the node runs the requested digest.
- It prints each node's output and a table on the summary page.

A node that runs an older image fails the check: `user_data` only runs on first boot. Roll the new image out with `paxosd-run` (below), then run deploy again to re-check.

A deploy and a destroy never run at the same time: both use the concurrency group `terraform-cluster`, and a second run waits.

The first deploy creates 23 resources:
- **Network (10):** the VPC, 3 subnets, the internet gateway, the route table, 3 route table associations and the security group.
- **Security group rules (7):** 3 ingress (7001, 9100 and 4317, from the group itself) and 4 egress (the same three ports to the group, and 443 to anywhere).
- **IAM (3):** the role, its policy attachment and the instance profile.
- **Nodes (3):** the instances.

### Reaching the nodes: Session Manager

Install the AWS CLI and the Session Manager plugin on your Mac, once:

```sh
brew install awscli
brew install --cask session-manager-plugin
session-manager-plugin --version     # prints a version when installed correctly
```

Use your own administrator credentials (`shruti-admin`), not the deploy role. The instance IDs are on the deploy run's summary page under "outputs", or:

```sh
aws ec2 describe-instances --region us-east-1 \
  --filters Name=tag:Project,Values=paxos-txn-store Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value, InstanceId, PrivateIpAddress]' --output table
```

**Grafana** (`node1`), then http://localhost:3000. The anonymous viewer is on; admin is `admin`/`admin`.

```sh
aws ssm start-session --region us-east-1 --target <node1 instance id> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["3000"],"localPortNumber":["3000"]}'
```

**Jaeger** (`node1`), then http://localhost:16686:

```sh
aws ssm start-session --region us-east-1 --target <node1 instance id> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["16686"],"localPortNumber":["16686"]}'
```

**Prometheus** works the same way with port `9090`. Each session runs until you press Ctrl-C; use one terminal per port.

**A shell on a node:** `aws ssm start-session --region us-east-1 --target <instance id>`. You get `ssm-user`; use `sudo -i` for root. Useful places:
- `docker ps` and `docker logs paxosd`
- `/var/log/paxosd-setup.log`, the first-boot log
- `/etc/paxosd/image-digest`

**Rolling out a new image** keeps the node and its data. One node at a time, wait for `/readyz` before the next:

```sh
sudo /usr/local/sbin/paxosd-run sha256:<digest>
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9100/readyz    # wait for 200
```

### Leader-kill test

The test kills the paxosd that leads the most shards, without warning, and measures how long until every shard has a leader again on the surviving nodes. Use two terminals with a shell on two different nodes (`aws ssm start-session …`, then `sudo -i`).

**1. Find the leaders.** From any node, list who leads each shard:

```sh
for n in node1 node2 node3; do curl -s http://$n:9100/metrics | grep '^paxos_is_leader{.*} 1$'; done
# paxos_is_leader{node="2",shard="0"} 1  ...
```

The victim is the node that appears most often. If that is `node1`, Prometheus, Grafana and Jaeger keep running, since only the paxosd container is killed.

**2. Start the watcher on a surviving node** (terminal A). It prints a timestamp and the leader of each shard every 200 ms:

```sh
victim=node2   # the node from step 1
while sleep 0.2; do
  t=$(date +%s.%3N)
  for n in node1 node2 node3; do
    [ "$n" = "$victim" ] && continue
    curl -s -m 0.5 http://$n:9100/metrics | grep '^paxos_is_leader{.*} 1$' | sed "s/^/$t /"
  done
done | tee /tmp/leaders.log
```

**3. Kill paxosd on the victim** (terminal B, on the victim). The restart policy is turned off first so Docker does not restart it at once; the timestamp is the kill time:

```sh
docker update --restart=no paxosd
date +%s.%3N; kill -9 "$(docker inspect -f '{{.State.Pid}}' paxosd)"
```

**4. Measure.** For each shard the victim led, the recovery time is the first timestamp in `/tmp/leaders.log` where a surviving node leads it, minus the kill time. The clocks agree to about a millisecond (Amazon Time Sync). Example: kill at `…100.250` and `paxos_is_leader{node="3",shard="0"} 1` first at `…101.050` means 0.8 s for shard 0.

Also watch in Grafana:
- `paxos_leader_elections_total` should rise on the survivors.
- `txn_in_doubt` should return to 0.

**5. Bring the node back** (terminal B), then wait for it to catch up:

```sh
docker update --restart=unless-stopped paxosd && docker start paxosd
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9100/readyz    # 200 when caught up
```

Stop the watcher with Ctrl-C.

### Destroying

1. **Actions → destroy → Run workflow**, on `main`. **confirm:** type `destroy`, then choose **Run workflow**.
   - Anything else fails at once, in the `confirm` job, before any approval is asked.
2. **Review deployments → aws → Approve and deploy** for the **destroy** job. It plans the destroy, prints the plan (23 to destroy), and applies it. Every node's data disk is deleted with it.
3. The last step, "list what is left" (`scripts/aws-leftovers.sh`), prints everything still tagged `Project=paxos-txn-store`:
   - The bootstrap's state bucket, deploy role and OIDC provider are expected.
   - Anything else is a warning, because the tagging API can keep listing deleted resources (terminated instances for up to an hour).
   - The step fails only if EC2 still shows a non-terminated instance or the VPC.

The bootstrap (state bucket, OIDC provider, deploy role) stays; it costs nothing while idle. To remove it as well, see [Undoing the bootstrap](#undoing-the-bootstrap).

### Is everything deleted?

After a destroy, as `shruti-admin` from your Mac (us-east-1). Every command except the last should print nothing, or `[]`:

```sh
export AWS_REGION=us-east-1 T=paxos-txn-store

# Instances that are not terminated
aws ec2 describe-instances --filters Name=tag:Project,Values=$T \
  Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
  --query 'Reservations[].Instances[].InstanceId' --output text

# EBS volumes (data disks are deleted with their instances)
aws ec2 describe-volumes --filters Name=tag:Project,Values=$T --query 'Volumes[].VolumeId' --output text

# The VPC (its subnets, route table, gateway and security group go with it).
# Public IPv4 addresses are released with their instances.
aws ec2 describe-vpcs --filters Name=tag:Project,Values=$T --query 'Vpcs[].VpcId' --output text

# The node role and instance profile
aws iam get-role --role-name $T-node 2>&1 | grep -q NoSuchEntity && echo "role: gone"
aws iam get-instance-profile --instance-profile-name $T-node 2>&1 | grep -q NoSuchEntity && echo "instance profile: gone"

# Everything still tagged: only the bootstrap's state bucket (and, if listed, its deploy role and OIDC provider)
aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values=$T \
  --query 'ResourceTagMappingList[].ResourceARN' --output text
```

Then check the bill: **Billing and Cost Management → Bills**, the current month's **EC2** and **VPC** lines (public IPv4 is billed under VPC). They should stop growing within a few hours. The $15 budget alert covers the rest.

### Running Terraform by hand

If a workflow cannot run (for example GitHub is down), the same stack can be applied as `shruti-admin`:

```sh
cd deploy/terraform/cluster
terraform init -backend-config="bucket=paxos-txn-store-tfstate-289112219115"
terraform plan -var image_digest=$(../../../scripts/resolve-image-digest.sh $(git rev-parse origin/main)) -out=cluster.tfplan
terraform apply cluster.tfplan
```

Both use the same state and the same lock. A hand-run apply while a workflow holds the lock waits or fails; it never overwrites.

### Terraform CI

`.github/workflows/terraform.yml` runs on pull requests that touch `deploy/terraform/`, or the compose, cluster, Prometheus and Grafana files the cluster stack reads. It needs no AWS access, and any finding fails it. For each stack it runs:
- `terraform fmt -check`, `init -backend=false`, `validate` and `test`
- tflint (v0.65.0 with the AWS ruleset 0.50.0, from `deploy/terraform/.tflint.hcl`)
- checkov 3.4.0. Its skips are inline, each with its reason.

### Cost while running

us-east-1, on-demand:

| Item | Per hour |
|---|---|
| 3 × t3.small | $0.0624 |
| 54 GB of gp3 | $0.0059 |
| 3 public IPv4 addresses | $0.015 |
| **Total** | **about $0.083** |

That is about $2.00 a day, or $61 a month if left running, which is far over the $15 budget. Deploy for a session, then destroy. Traffic between AZs ($0.01/GB each way) adds to this under load.
