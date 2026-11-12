#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/atspm-dashboard-docker-bundle.tar.gz}"
TMP_OUT="$(mktemp "/tmp/atspm-dashboard-docker-bundle.XXXXXX.tar.gz")"

cd "$ROOT"

tar -czf "$TMP_OUT" \
  --exclude='./.git' \
  --exclude='./.agents' \
  --exclude='./.codex' \
  --exclude='./.apptainer' \
  --exclude='./.apptainer-cache' \
  --exclude='./.podman-cache' \
  --exclude='./.podman-config' \
  --exclude='./.podman-data' \
  --exclude='./.python_deps' \
  --exclude='**/__pycache__' \
  --exclude='**/*.py[cod]' \
  --exclude='**/.DS_Store' \
  --exclude='**/*.tsbuildinfo' \
  --exclude='./derived' \
  --exclude='./dashboard-app/node_modules' \
  --exclude='./dashboard-app/dist' \
  --exclude='./anomaly-dashboard' \
  --exclude='./atspm/.git' \
  --exclude='./atspm/*.zip' \
  --exclude='./atspm/sample_data' \
  --exclude='./atspm/tests' \
  --exclude='./atspm_feb_source' \
  --exclude='./atspm_older' \
  --exclude='./*.log' \
  --exclude='./*.tar.gz' \
  .

mv "$TMP_OUT" "$OUT"
chmod 0644 "$OUT"

echo "Wrote Docker handoff bundle:"
echo "$OUT"
echo
echo "Data is not included. Send/copy the required derived/ data separately."
echo "The separate anomaly dashboard repository is not included in this bundle."
