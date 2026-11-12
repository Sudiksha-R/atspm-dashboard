#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="$ROOT/dashboard-app"
APPTAINER_DIR="$ROOT/.apptainer"
APPTAINER_CACHE="$ROOT/.apptainer-cache"
APPTAINER_TMP="${APPTAINER_TMP:-/tmp/atspm-apptainer}"
SIF="$APPTAINER_DIR/atspm-dashboard.sif"
DEF="$ROOT/apptainer/atspm-dashboard.def"
BACKEND_LOG="$ROOT/backend-8000-apptainer.log"
FRONTEND_LOG="$ROOT/frontend-4174-apptainer.log"
BACKEND_PORT="${ATSPM_BACKEND_PORT:-8000}"
FRONTEND_PORT="${ATSPM_FRONTEND_PORT:-4174}"
RAW_DIR="${ATSPM_RAW_DIR:-/app/derived/bee_ridge_atspm_feb_to_may}"
API_BASE_URL="${VITE_API_BASE_URL:-http://127.0.0.1:$BACKEND_PORT}"

mkdir -p "$APPTAINER_DIR" "$APPTAINER_CACHE" "$APPTAINER_TMP"

if [[ ! -f "$SIF" ]]; then
  echo "Building Apptainer image at $SIF..."
  APPTAINER_CACHEDIR="$APPTAINER_CACHE" TMPDIR="$APPTAINER_TMP" \
    apptainer build "$SIF" "$DEF"
fi

echo "Building frontend with API base $API_BASE_URL..."
cd "$FRONTEND_DIR"
VITE_API_BASE_URL="$API_BASE_URL" npm run build

cd "$ROOT"

echo "Stopping old Apptainer dashboard processes..."
pkill -u "$USER" -f "backend/server.py" 2>/dev/null || true
pkill -u "$USER" -f "http.server.*$FRONTEND_PORT" 2>/dev/null || true
sleep 2

echo "Starting backend on 127.0.0.1:$BACKEND_PORT..."
APPTAINER_CACHEDIR="$APPTAINER_CACHE" TMPDIR="$APPTAINER_TMP" \
APPTAINERENV_PYTHONPATH="/app/backend:/app/atspm/src:/app/Correlation_Feature (1)" \
APPTAINERENV_ATSPM_BACKEND_HOST="127.0.0.1" \
APPTAINERENV_ATSPM_BACKEND_PORT="$BACKEND_PORT" \
APPTAINERENV_ATSPM_RAW_DIR="$RAW_DIR" \
APPTAINERENV_ATSPM_RAW_GLOB="${ATSPM_RAW_GLOB:-atspm-*_filtered.csv}" \
  nohup apptainer exec --bind "$ROOT:/app" --pwd /app "$SIF" \
    python -u backend/server.py > "$BACKEND_LOG" 2>&1 < /dev/null &
BACKEND_PID=$!

echo "Starting frontend static server on 127.0.0.1:$FRONTEND_PORT..."
nohup python -m http.server "$FRONTEND_PORT" --bind 127.0.0.1 --directory "$FRONTEND_DIR/dist" \
  > "$FRONTEND_LOG" 2>&1 < /dev/null &
FRONTEND_PID=$!

echo "Backend PID:  $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"

echo "Waiting for backend health..."
BACKEND_OK=0
for _ in $(seq 1 90); do
  if curl --max-time 2 -fsS "http://127.0.0.1:$BACKEND_PORT/api/health" >/tmp/atspm_backend_health.$$ 2>/dev/null; then
    BACKEND_OK=1
    break
  fi
  if ! ps -p "$BACKEND_PID" >/dev/null 2>&1; then
    echo "Backend exited early. Last backend log lines:"
    tail -80 "$BACKEND_LOG" || true
    exit 1
  fi
  sleep 1
done

if [[ "$BACKEND_OK" != "1" ]]; then
  echo "Backend did not become healthy within 90 seconds."
  tail -80 "$BACKEND_LOG" || true
  exit 1
fi

echo "Waiting for frontend..."
FRONTEND_OK=0
for _ in $(seq 1 45); do
  if curl --max-time 2 -fsSI "http://127.0.0.1:$FRONTEND_PORT/" >/tmp/atspm_frontend_health.$$ 2>/dev/null; then
    FRONTEND_OK=1
    break
  fi
  if ! ps -p "$FRONTEND_PID" >/dev/null 2>&1; then
    echo "Frontend exited early. Last frontend log lines:"
    tail -80 "$FRONTEND_LOG" || true
    exit 1
  fi
  sleep 1
done

if [[ "$FRONTEND_OK" != "1" ]]; then
  echo "Frontend did not become reachable within 45 seconds."
  tail -80 "$FRONTEND_LOG" || true
  exit 1
fi

echo
echo "Dashboard is running."
echo "Backend:  http://127.0.0.1:$BACKEND_PORT/api/health"
echo "Frontend: http://127.0.0.1:$FRONTEND_PORT/"
echo
echo "Logs:"
echo "  tail -f '$BACKEND_LOG'"
echo "  tail -f '$FRONTEND_LOG'"

if [[ "${ATSPM_KEEP_ALIVE:-0}" == "1" ]]; then
  echo
  echo "Keeping launcher alive. Press Ctrl+C to stop this session."
  wait "$BACKEND_PID" "$FRONTEND_PID"
fi
