#!/usr/bin/env bash
set -euo pipefail

ROOT="/blue/ranka/rajavaram.s/ATSPM Dashboard"
FRONTEND_DIR="$ROOT/dashboard-app"
BACKEND_LOG="$ROOT/backend-8001.log"
FRONTEND_LOG="$ROOT/frontend-4174.log"
BACKEND_PORT="${ATSPM_BACKEND_PORT:-8001}"
FRONTEND_PORT="${ATSPM_FRONTEND_PORT:-4174}"
PYTHON_BIN="${ATSPM_PYTHON_BIN:-python}"
NPM_BIN="${ATSPM_NPM_BIN:-npm}"

export PATH="/apps/jupyter/6.5.4/bin:$PATH"

cd "$ROOT"

echo "Stopping old dashboard processes..."
pkill -u "$USER" -f "$ROOT/backend/server.py" 2>/dev/null || true
pkill -u "$USER" -f "vite --host 127.0.0.1 --port $FRONTEND_PORT" 2>/dev/null || true
sleep 3

echo "Starting backend on 127.0.0.1:$BACKEND_PORT..."
PYTHONPATH="$ROOT/.python_deps:$ROOT/backend" \
ATSPM_RAW_DIR="$ROOT/derived/bee_ridge_atspm_feb_to_may" \
ATSPM_BACKEND_HOST=127.0.0.1 \
ATSPM_BACKEND_PORT="$BACKEND_PORT" \
nohup "$PYTHON_BIN" -u "$ROOT/backend/server.py" > "$BACKEND_LOG" 2>&1 < /dev/null &
BACKEND_PID=$!

echo "Starting frontend on 127.0.0.1:$FRONTEND_PORT..."
cd "$FRONTEND_DIR"
VITE_API_BASE_URL="http://127.0.0.1:$BACKEND_PORT" \
nohup "$NPM_BIN" run dev -- --host 127.0.0.1 --port "$FRONTEND_PORT" --strictPort > "$FRONTEND_LOG" 2>&1 < /dev/null &
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
    tail -60 "$BACKEND_LOG" || true
    exit 1
  fi
  sleep 1
done

if [[ "$BACKEND_OK" != "1" ]]; then
  echo "Backend did not become healthy within 90 seconds."
  echo "Last backend log lines:"
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
    tail -60 "$FRONTEND_LOG" || true
    exit 1
  fi
  sleep 1
done

if [[ "$FRONTEND_OK" != "1" ]]; then
  echo "Frontend did not become reachable within 45 seconds."
  echo "Last frontend log lines:"
  tail -80 "$FRONTEND_LOG" || true
  exit 1
fi

echo
echo "Dashboard is running."
echo "Backend:  http://127.0.0.1:$BACKEND_PORT/api/health"
echo "Frontend: http://127.0.0.1:$FRONTEND_PORT/"
echo
echo "Open this on your laptop browser after SSH tunneling:"
echo "http://127.0.0.1:$FRONTEND_PORT/"
echo
echo "Logs:"
echo "  tail -f '$BACKEND_LOG'"
echo "  tail -f '$FRONTEND_LOG'"
