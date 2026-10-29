#!/usr/bin/env bash
# After destroying the cluster: lists everything still tagged
# Project=paxos-txn-store. The bootstrap's state bucket, deploy role and
# OIDC provider are expected to stay (the IAM ones only if the tagging API
# lists them); any other ARN is reported. The tagging API can keep listing deleted resources
# for a while (terminated instances for up to an hour), so the authoritative
# checks are EC2's own: no instance that is not terminated, and no VPC.
#
# Usage: scripts/aws-leftovers.sh   (AWS credentials, region us-east-1)
set -euo pipefail

region=${AWS_REGION:-us-east-1}
tag=paxos-txn-store
status=0

echo "--- resources tagged Project=$tag (tagging API)"
arns=$(aws resourcegroupstaggingapi get-resources --region "$region" \
  --tag-filters "Key=Project,Values=$tag" --query 'ResourceTagMappingList[].ResourceARN' --output text | tr '\t' '\n' | sed '/^$/d' | sort)
for arn in $arns; do
  case $arn in
    arn:aws:s3:::$tag-tfstate-* | arn:aws:iam::*:role/$tag-deploy | arn:aws:iam::*:oidc-provider/token.actions.githubusercontent.com)
      echo "expected (bootstrap): $arn" ;;
    *) echo "::warning::still listed: $arn"; echo "LEFT: $arn" ;;
  esac
done

echo "--- EC2 instances tagged Project=$tag that are not terminated"
live=$(aws ec2 describe-instances --region "$region" \
  --filters "Name=tag:Project,Values=$tag" "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text)
if [[ -n $live ]]; then echo "$live"; echo "::error::instances still exist"; status=1; else echo none; fi

echo "--- VPCs tagged Project=$tag"
vpcs=$(aws ec2 describe-vpcs --region "$region" --filters "Name=tag:Project,Values=$tag" --query 'Vpcs[].VpcId' --output text)
if [[ -n $vpcs ]]; then echo "$vpcs"; echo "::error::VPC still exists"; status=1; else echo none; fi

exit $status
