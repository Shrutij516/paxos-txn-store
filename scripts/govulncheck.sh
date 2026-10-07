#!/usr/bin/env bash
# Runs govulncheck and fails on any vulnerability reachable from our code,
# except the ones listed in ALLOWED. It also fails when an allowed one is no
# longer reported, so the exclusion is deleted as soon as the fix lands.
#
# GO-2026-6443: a grpc server panics on a request that carries neither an
# :authority nor a Host header. Fixed only on grpc master so far
# (v1.85.0-dev.0.20260825072537-93e31b48545e); v1.84.0 is the newest release.
# Risk is low: our peers and the SDK use grpc-go clients, which always send
# :authority, and nodes are not publicly reachable (the compose stack
# publishes its ports on 127.0.0.1 only). See DECISIONS.md entry 34.
# Remove when grpc v1.85.0 ships: bump grpc and delete the entry below.
#
# Usage: scripts/govulncheck.sh [file]. With a file, reads saved
# `govulncheck -format json` output instead of running govulncheck.
set -euo pipefail

ALLOWED=(GO-2026-6443)
VERSION=v1.1.4

if [[ $# -gt 0 ]]; then
  out=$1
else
  out=$(mktemp)
  trap 'rm -f "$out"' EXIT
  go run "golang.org/x/vuln/cmd/govulncheck@$VERSION" -format json ./... > "$out"
fi

# A finding whose innermost trace frame names a function is reachable from
# our code; module and package level findings are not reported, as in
# govulncheck's default text output.
found=$(jq -r 'select(.finding != null and .finding.trace[0].function != null) | .finding.osv' "$out" | sort -u)

status=0
for id in $found; do
  if [[ " ${ALLOWED[*]} " != *" $id "* ]]; then
    echo "govulncheck: $id is reachable (https://pkg.go.dev/vuln/$id)"
    status=1
  fi
done
for id in "${ALLOWED[@]}"; do
  if ! grep -qx "$id" <<< "$found"; then
    echo "govulncheck: $id is allowed but no longer reported; remove it from ALLOWED in $0 and from DECISIONS.md"
    status=1
  fi
done
if [[ $status -eq 0 ]]; then
  echo "govulncheck: no reachable vulnerabilities except the allowed ${ALLOWED[*]}"
fi
exit $status
