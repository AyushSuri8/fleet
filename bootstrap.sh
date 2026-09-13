#!/usr/bin/env bash
# Bootstrap a Cloud Shell node into the 24/7 rotating server fleet.
# Run on each account's shell (shell-a..d). If THIS shell stops (12h session
# cap / disconnect), re-run this script on the SAME shell until its 50h
# weekly quota is exhausted — only then move to the next shell.
set -euo pipefail
cd "$(dirname "$0")"

# --- fix grpc/libstdc++ on IDX / Cloud Shell where /lib symlink is stale ---
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

# --- stable node identity (persists in $HOME across rebuilds) ---
NODE_ID="${FLEET_NODE_ID:-}"
if [[ -z "$NODE_ID" ]]; then
  if [[ -s "$HOME/.fleet-node-id" ]]; then
    NODE_ID="$(cat "$HOME/.fleet-node-id")"
  else
    echo "First run on this node. Assign a stable id (shell-a / shell-b / shell-c / shell-d):"
    read -r NODE_ID
    [[ "$NODE_ID" =~ ^shell-[a-d]$ ]] || { echo "id must be shell-a..shell-d"; exit 1; }
    tmp=$(mktemp); echo "$NODE_ID" > "$tmp"; mv "$tmp" "$HOME/.fleet-node-id"; chmod 600 "$HOME/.fleet-node-id"
  fi
fi
if ! [[ "$NODE_ID" =~ ^shell-[a-d]$ ]]; then
  echo "NODE_ID=$NODE_ID invalid, must be shell-a..d" >&2
  exit 1
fi
export FLEET_NODE_ID="$NODE_ID"
echo "==> Node id: $NODE_ID"

# --- auth & project ---
# Other-account Firestore is fine: if SA key is provided, use it and skip gcloud login check.
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" && -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
  echo "==> Using service account key: $GOOGLE_APPLICATION_CREDENTIALS"
elif [[ -f "$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
  export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json"
  echo "==> Using service account key: $GOOGLE_APPLICATION_CREDENTIALS"
elif [[ -f "$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
  export GOOGLE_APPLICATION_CREDENTIALS="$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json"
  echo "==> Using service account key: $GOOGLE_APPLICATION_CREDENTIALS"
else
  gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 | grep -q . \
    || { echo "No active gcloud account. Run: gcloud auth login"; exit 1; }
fi
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
[[ "$PROJECT_ID" == "(unset)" ]] && PROJECT_ID=""
[[ -n "$PROJECT_ID" ]] || { echo "Run: gcloud config set project PROJECT_ID"; exit 1; }
export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
echo "==> Project: $PROJECT_ID"

# --- dependencies (root disk is ephemeral; reinstall every boot) ---
echo "==> Installing Python dependencies"
if [[ ! -d .venv ]]; then python3 -m venv .venv; fi
if [[ -d ".venv" ]] && LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" .venv/bin/python -c "from google.cloud import firestore" 2>/dev/null; then
  echo "    dependencies already present in .venv"
else
  python3 -m pip install --quiet -r requirements.txt
  LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" .venv/bin/python -m pip install --quiet -r requirements.txt 2>&1 | tail -n 5 || true
fi

# --- verify Firestore + seed lease + usage docs ---
LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" .venv/bin/python - <<'PY'
import os
from datetime import datetime, timezone
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
db.collection("server").document("bootstrap-check").set(
    {"ok": True, "at": firestore.SERVER_TIMESTAMP})
ref = db.collection("server").document("lease")
if not ref.get().exists:
    ref.set({"activeNode": None, "status": "idle", "fenceToken": 0,
             "leaseExpiresAt": datetime.now(timezone.utc)})
    print("Firestore OK; lease document created")
else:
    print("Firestore OK; lease document exists")
node = os.environ.get("FLEET_NODE_ID", "")
if node:
    uref = db.collection("server").document(f"usage-{node}")
    if not uref.get().exists:
        uref.set({"weekStart": datetime.now(timezone.utc),
                  "activeSeconds": 0.0, "quotaSeconds": 180000})
        print(f"usage document created for {node} (50h weekly quota)")
    else:
        print(f"usage document exists for {node}")
PY

mkdir -p logs
mkdir -p "$HOME/.fleet"

# --- start supervisor via production launcher (handles libstdc++ + logging) ---
echo "==> Starting supervisor (production launcher: ./run.sh)"
pkill -f "supervisor.py" 2>/dev/null || true
sleep 1
# trim logs if too large (10MB)
for f in logs/*.log "$HOME/.fleet/keepalive.log"; do [[ -f "$f" ]] && [[ $(stat -c%s "$f" 2>/dev/null || echo 0) -gt 10485760 ]] && tail -c 5242880 "$f" > "$f.tmp" && mv "$f.tmp" "$f" || true; done
if tmux -V >/dev/null 2>&1 || /nix/store/lpdrfl6n16q5zdf8acp4bni7yczzcx3h-idx-builtins/bin/tmux -V >/dev/null 2>&1; then
  TMUX_BIN="$(command -v tmux 2>/dev/null || echo /nix/store/lpdrfl6n16q5zdf8acp4bni7yczzcx3h-idx-builtins/bin/tmux)"
  "$TMUX_BIN" kill-session -t fleet 2>/dev/null || true
  "$TMUX_BIN" new-session -d -s fleet "./run.sh"
  echo "    tmux: $TMUX_BIN attach -t fleet"
else
  nohup ./run.sh >/dev/null 2>&1 &
  echo "    nohup pid $! (tmux not available)"
fi

echo "==> Done."
echo "    Attach:        tmux attach -t fleet"
echo "    Status:        ./fleetctl.sh status"