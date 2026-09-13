#!/usr/bin/env bash
# Ops helper / runbook commands.
# Usage: ./fleetctl.sh {status|quota|stop|rotate|force-takeover}
set -euo pipefail
cd "$(dirname "$0")"

# fix IDX libstdc++ for any Python invocation
if ! LD_LIBRARY_PATH="" .venv/bin/python -c "import grpc" 2>/dev/null; then
  for d in /nix/store/*gcc*lib/lib; do
    if LD_LIBRARY_PATH="$d" .venv/bin/python -c "import grpc" 2>/dev/null; then
      export LD_LIBRARY_PATH="$d:${LD_LIBRARY_PATH:-}"
      break
    fi
  done
fi

if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  if [[ -f "$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.secrets/shell-project-d2b93-331fa174bc3e.json"
  elif [[ -f "$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$(dirname "$0")/.secrets/shell-project-d2b93-331fa174bc3e.json"
  fi
fi

CMD="${1:-status}"
PYTHON=".venv/bin/python"
if ! LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" "$PYTHON" -c "import google.cloud.firestore" 2>/dev/null; then
  PYTHON="python3"
fi
export GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT:-shell-project-d2b93}"

case "$CMD" in
  status)
    echo "--- lease ---"
    $PYTHON - <<'PY'
import os, json
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
d = db.collection("server").document("lease").get().to_dict() or {}
print(json.dumps({k: str(v) for k, v in d.items()}, indent=2))
PY
    echo "--- health docs ---"
    $PYTHON - <<'PY'
import os, json
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
for s in db.collection("server").stream():
    if not s.id.startswith("health-"):
        continue
    print(s.id, json.dumps({k: str(v) for k, v in s.to_dict().items()}))
PY
    echo "--- quota (usage-*) ---"
    $PYTHON - <<'PY'
import os
from datetime import datetime, timezone
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
now = datetime.now(timezone.utc)
for s in db.collection("server").stream():
    if not s.id.startswith("usage-"):
        continue
    d = s.to_dict() or {}
    used = float(d.get("activeSeconds", 0.0) or 0.0)
    left = max(0.0, 180000 - used)
    print(f"{s.id}: {left/3600:.1f}h left of 50h (used {used/3600:.1f}h, weekStart={d.get('weekStart')})")
PY
    echo "--- tmux ---"; tmux ls 2>/dev/null || /nix/store/lpdrfl6n16q5zdf8acp4bni7yczzcx3h-idx-builtins/bin/tmux ls 2>&1 || echo "no tmux session"
    echo "--- server status ---"; cat "$HOME/.fleet/status.json" 2>/dev/null || cat /tmp/fleet-worker-status.json 2>/dev/null || echo "no server status"
    ;;
  quota)
    $PYTHON - <<'PY'
import os
from datetime import datetime, timezone
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
for s in db.collection("server").stream():
    if not s.id.startswith("usage-"):
        continue
    d = s.to_dict() or {}
    used = float(d.get("activeSeconds", 0.0) or 0.0)
    left = max(0.0, 180000 - used)
    print(f"{s.id}: {left/3600:.1f}h left of 50h (used {used/3600:.1f}h, weekStart={d.get('weekStart')})")
PY
    ;;
  stop)
    tmux kill-session -t fleet 2>/dev/null || true
    pkill -f "supervisor.py" 2>/dev/null || true
    pkill -f "worker.py" 2>/dev/null || true
    pkill -f "keepalive.py" 2>/dev/null || true
    echo "node stopped"
    ;;
  rotate)
    $PYTHON - <<'PY'
import os
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
db.collection("server").document("lease").set({"requestRotation": True}, merge=True)
print("rotation requested; active node will drain and release on its next loop")
PY
    ;;
  force-takeover)
    $PYTHON - <<'PY'
import os
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
db.collection("server").document("lease").set(
    {"leaseExpiresAt": datetime.now(timezone.utc) - timedelta(seconds=1)}, merge=True)
print("lease expired manually; a standby takes over within its poll interval")
PY
    ;;
  *) echo "usage: fleetctl.sh {status|quota|stop|rotate|force-takeover}"; exit 1 ;;
esac