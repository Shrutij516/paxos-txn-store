#!/usr/bin/env bash
# Prints the digest of the paxosd image release.yml pushed for a commit, or
# checks that a given digest exists. Anonymous: the package is public.
#
# Usage: scripts/resolve-image-digest.sh <40-hex commit SHA | sha256:digest>
set -euo pipefail

repo=shrutij516/paxos-txn-store
ref=${1:?usage: $0 <commit sha | sha256:digest>}
if [[ $ref == sha256:* ]]; then
  [[ $ref =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "not a sha256 digest: $ref" >&2; exit 1; }
elif ! [[ $ref =~ ^[0-9a-f]{40}$ ]]; then
  echo "want a 40-hex commit SHA or a sha256 digest, got: $ref" >&2
  exit 1
fi

token=$(curl -fsS "https://ghcr.io/token?scope=repository:$repo:pull" | jq -r .token)
accept=application/vnd.oci.image.index.v1+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.docker.distribution.manifest.v2+json
if ! headers=$(curl -fsSI -H "Authorization: Bearer $token" -H "Accept: $accept" "https://ghcr.io/v2/$repo/manifests/$ref"); then
  echo "ghcr.io/$repo has no image $ref (release.yml pushes one per commit on main)" >&2
  exit 1
fi
digest=$(tr -d '\r' <<<"$headers" | awk -F': ' 'tolower($1) == "docker-content-digest" { print $2 }')
if ! [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "ghcr.io returned no digest for $ref" >&2
  exit 1
fi
if [[ $ref == sha256:* && $digest != "$ref" ]]; then
  echo "asked for $ref, registry answered $digest" >&2
  exit 1
fi
echo "$digest"
