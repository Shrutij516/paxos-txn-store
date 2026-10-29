#!/usr/bin/env bash
# After an apply of deploy/terraform/cluster: waits until all three nodes
# are online in SSM, then on every node waits for paxosd's /readyz (200:
# every shard on the node has a leader and is caught up) and checks that it
# runs the expected image. Prints each node's result; exits 1 if any fails.
#
# Run in deploy/terraform/cluster with AWS credentials:
#   ../../../scripts/aws-smoke.sh sha256:<digest>
set -euo pipefail

want=${1:?usage: $0 sha256:<digest>}
region=${AWS_REGION:-us-east-1}

mapfile -t nodes < <(terraform output -json instance_ids | jq -r 'to_entries | sort_by(.key)[] | "\(.key) \(.value)"')
ids=()
for n in "${nodes[@]}"; do ids+=("${n#* }"); done
if (( ${#ids[@]} != 3 )); then
  echo "expected 3 instances in the state, found ${#ids[@]}" >&2
  exit 1
fi

echo "waiting for SSM to see ${ids[*]}"
deadline=$((SECONDS + 600))
while :; do
  # shellcheck disable=SC2016 # the backticks are a JMESPath literal
  online=$(aws ssm describe-instance-information --region "$region" \
    --filters "Key=InstanceIds,Values=$(IFS=,; echo "${ids[*]}")" \
    --query 'length(InstanceInformationList[?PingStatus==`Online`])' --output text)
  [[ $online == 3 ]] && break
  if (( SECONDS > deadline )); then
    echo "only $online of 3 nodes online in SSM after 10 minutes" >&2
    exit 1
  fi
  sleep 15
done
echo "all 3 nodes online in SSM"

# Runs on each node as root. First boot installs Docker and pulls images,
# so give paxosd up to 15 minutes to become ready.
read -r -d '' node_script <<SCRIPT || true
want=$want
code=none
for _ in \$(seq 1 90); do
  code=\$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9100/readyz || true)
  [ "\$code" = 200 ] && break
  sleep 10
done
running=\$(cat /etc/paxosd/image-digest 2>/dev/null || echo none)
echo "readyz=\$code image=\$running"
docker ps -a --format '{{.Names}}: {{.Status}}'
if [ "\$code" != 200 ]; then tail -n 30 /var/log/paxosd-setup.log; exit 1; fi
if [ "\$running" != "\$want" ]; then
  echo "paxosd runs \$running, not \$want: user_data only runs on first boot; roll out with /usr/local/sbin/paxosd-run \$want"
  exit 1
fi
SCRIPT
params=$(jq -n --arg s "$node_script" '{commands: ($s | split("\n")), executionTimeout: ["1200"]}')
cmd=$(aws ssm send-command --region "$region" --document-name AWS-RunShellScript \
  --instance-ids "${ids[@]}" --parameters "$params" --comment "smoke check ${want:7:12}" \
  --query Command.CommandId --output text)
echo "SSM command $cmd"

failed=0
summary="| node | instance | result |"$'\n'"|---|---|---|"
for n in "${nodes[@]}"; do
  name=${n%% *} id=${n#* }
  while :; do
    status=$(aws ssm get-command-invocation --region "$region" --command-id "$cmd" --instance-id "$id" \
      --query Status --output text 2>/dev/null || echo Pending)
    case $status in Pending | InProgress | Delayed) sleep 10 ;; *) break ;; esac
  done
  echo "=== $name ($id): $status"
  aws ssm get-command-invocation --region "$region" --command-id "$cmd" --instance-id "$id" \
    --query '[StandardOutputContent, StandardErrorContent]' --output text
  [[ $status == Success ]] || failed=1
  summary+=$'\n'"| $name | $id | $status |"
done
[[ -n ${GITHUB_STEP_SUMMARY:-} ]] && echo "$summary" >> "$GITHUB_STEP_SUMMARY"
exit $failed
