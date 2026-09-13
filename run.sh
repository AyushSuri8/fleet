#!/usr/bin/env bash
# Production launcher for fleet supervisor.
# Fixes IDX/Firebase Studio libstdc++.so.6 symlink issue by prepending a
# known-good gcc lib to LD_LIBRARY_PATH before any Python import.
set -euo pipefail
cd "$(dirname "$0")"

# --- fix grpc/libstdc++ on IDX workstations (broken /lib symlink) ---
# Searches both nix and classic debian paths so same script works on Cloud Shell and IDX.
if ! LD_LIBRARY_PATH="" .venv/bin/python -c "import grpc" 2>/dev/null; then
  for d in /nix/store/*gcc*lib/lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu /usr/lib64; do
    [[ -d "$d" ]] || continue
    if LD_LIBRARY_PATH="$d" .venv/bin/python -c "import grpc" 2>/dev/null; then
      export LD_LIBRARY_PATH="$d:${LD_LIBRARY_PATH:-}"
      break
    fi
  done
fi

# --- node identity (must match bootstrap.sh, fail-fast, no silent shell-a) ---
if [[ -z "${FLEET_NODE_ID:-}" ]]; then
  if [[ -s "$HOME/.fleet-node-id" ]]; then
    export FLEET_NODE_ID="$(cat "$HOME/.fleet-node-id")"
  else
    echo "FLEET_NODE_ID unset and $HOME/.fleet-node-id missing; run bootstrap.sh" >&2
    exit 1
  fi
fi
# validate
if ! [[ "$FLEET_NODE_ID" =~ ^shell-[a-d]$ ]]; then
  echo "FLEET_NODE_ID=$FLEET_NODE_ID invalid, must be shell-a..d" >&2
  exit 1
fi

# --- project / credentials: SINGLE shared Firestore for all nodes ---
# Same rule as bootstrap.sh: every shell must use the shared SA key, otherwise
# per-account gcloud ADC silently creates 4 separate fleets. Fail loudly.
SHARED_PROJECT="shell-project-d2b93"
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  if [[ -f "$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json"
  elif [[ -f "$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json"
  fi
fi
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" || ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
  echo "ERROR: shared service-account key not found; refusing per-account gcloud ADC fallback." >&2
  echo "  Copy shell-project-d2b93-*.json to ~/.secrets/ (same key on all shells)." >&2
  exit 1
fi
if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" || "$GOOGLE_CLOUD_PROJECT" == "(unset)" ]]; then
  export GOOGLE_CLOUD_PROJECT="$SHARED_PROJECT"
fi
if [[ "$GOOGLE_CLOUD_PROJECT" != "$SHARED_PROJECT" ]]; then
  echo "WARNING: GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT != shared $SHARED_PROJECT (separate fleet!)" >&2
fi

mkdir -p logs
mkdir -p "$HOME/.fleet"
LOG="logs/${FLEET_NODE_ID}.log"
echo "==> fleet supervisor node=$FLEET_NODE_ID project=$GOOGLE_CLOUD_PROJECT log=$LOG"

# idle keepalive for Cloud Shell: ensure tmux pane stays active even with no jobs
# (Cloud Shell counts detached tmux with no output as idle -> killed after 60m).
# We log a heartbeat to stdout every 120s in supervisor.py, and also touch a
# keepalive file in persistent HOME every 5m as secondary signal.
(
  while true; do
    sleep 300
    date -u +"%FT%TZ keepalive $FLEET_NODE_ID fence=$(cat "$HOME/.fleet/status.json" 2>/dev/null | grep fenceToken | head -1 || echo ?)" >> "$HOME/.fleet/keepalive.log" 2>/dev/null || true
    # keep tmux server from being considered idle (no-op, ignored if tmux missing)
    tmux refresh-client -t fleet 2>/dev/null || true
  done
) &

# idle keepalive NOTE (FIX): the supervisor's 120s "still active" stdout
# heartbeat only keeps the tmux pane non-idle if it reaches the pane. The old
# `exec ... >> "$LOG" 2>&1` sent everything to the file, so the pane was
# permanently silent and Cloud Shell could reap the "idle" session — the
# exact thing this keepalive was added to prevent. tee writes to BOTH.
exec .venv/bin/python supervisor.py 2>&1 | tee -a "$LOG"
