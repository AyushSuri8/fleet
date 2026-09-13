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
# ALWAYS persisted to ~/.fleet-node-id (atomic, 0600), even when the id came
# from $FLEET_NODE_ID env. run.sh is spawned as a separate tmux process and
# needs the file when the env var isn't exported into it.
NODE_ID="${FLEET_NODE_ID:-}"
if [[ -z "$NODE_ID" ]]; then
  if [[ -s "$HOME/.fleet-node-id" ]]; then
    NODE_ID="$(cat "$HOME/.fleet-node-id")"
  else
    echo "First run on this node. Assign a stable id (shell-a / shell-b / shell-c / shell-d):"
    read -r NODE_ID
  fi
fi
if ! [[ "$NODE_ID" =~ ^shell-[a-d]$ ]]; then
  echo "NODE_ID=$NODE_ID invalid, must be shell-a..d" >&2
  exit 1
fi
tmp=$(mktemp)
printf '%s\n' "$NODE_ID" > "$tmp"
chmod 600 "$tmp"
mv "$tmp" "$HOME/.fleet-node-id" 2>/dev/null || true
export FLEET_NODE_ID="$NODE_ID"
echo "==> Node id: $NODE_ID"

# --- auth & project ---
# SINGLE shared Firestore: shell-project-d2b93 holds server/lease for ALL nodes.
# Every shell (shell-a..d, different Google accounts) must authenticate as the
# SAME service account, otherwise each account silently gets its own Firestore
# and you end up with 4 independent fleets. Fail loudly instead of allowing
# per-account gcloud auth fallback.
SHARED_PROJECT="shell-project-d2b93"
SA_KEY_CANDIDATES=(
  "${GOOGLE_APPLICATION_CREDENTIALS:-}"
  "$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json"
  "$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json"
)
SA_KEY=""
for cand in "${SA_KEY_CANDIDATES[@]}"; do
  [[ -n "$cand" && -f "$cand" ]] || continue
  SA_KEY="$cand"
  break
done
if [[ -z "$SA_KEY" ]]; then
  echo "ERROR: shared service-account key not found." >&2
  echo "  All fleet nodes must use the SAME key so they share one Firestore." >&2
  echo "  Copy shell-project-d2b93-*.json to ~/.secrets/ on this shell, then re-run." >&2
  echo "  (Refusing per-account 'gcloud auth' fallback: it creates 4 separate fleets.)" >&2
  exit 1
fi
export GOOGLE_APPLICATION_CREDENTIALS="$SA_KEY"
echo "==> Using shared service account key: $SA_KEY"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$SHARED_PROJECT}"
if [[ "$PROJECT_ID" != "$SHARED_PROJECT" ]]; then
  echo "WARNING: GOOGLE_CLOUD_PROJECT=$PROJECT_ID overrides shared project $SHARED_PROJECT." >&2
  echo "  Only override if you really mean a separate fleet. Continuing in 5s (Ctrl-C to abort)..." >&2
  sleep 5
fi
if [[ "$PROJECT_ID" == "(unset)" || -z "$PROJECT_ID" ]]; then
  PROJECT_ID="$SHARED_PROJECT"
fi
export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
echo "==> Project: $PROJECT_ID (shared Firestore for all nodes)"

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