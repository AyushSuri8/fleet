#!/usr/bin/env python3
"""Usage: GH_REPO=owner/repo GH_PAT=token FIRESTORE_API_KEY=... GCP_PROJECT_ID=... \
           python3 enqueue_client.py echo '{"hello":"world"}'"""
import json, os, sys, time, uuid, urllib.request, urllib.error

GH_REPO   = os.environ["GH_REPO"]
GH_PAT    = os.environ["GH_PAT"]
API_KEY   = os.environ["FIRESTORE_API_KEY"]
PROJECT   = os.environ["GCP_PROJECT_ID"]
TIMEOUT_S = 600


def http(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "content-type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def from_value(v):
    if not isinstance(v, dict): return v
    for k, cast in (("stringValue", str), ("integerValue", int),
                    ("doubleValue", float), ("booleanValue", bool),
                    ("timestampValue", str)):
        if k in v: return cast(v[k])
    if "mapValue" in v:
        return {k: from_value(x) for k, x in (v["mapValue"].get("fields") or {}).items()}
    if "arrayValue" in v:
        return [from_value(x) for x in (v["arrayValue"].get("values") or [])]
    return v


def main():
    job_type = sys.argv[1] if len(sys.argv) > 1 else "echo"
    payload = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    job_id = str(uuid.uuid4())

    http("POST", f"https://api.github.com/repos/{GH_REPO}/dispatches",
         {"event_type": "enqueue",
          "client_payload": {"jobId": job_id, "type": job_type, "payload": payload}},
         {"Authorization": f"token {GH_PAT}",
          "Accept": "application/vnd.github+json"})
    print("dispatched jobId =", job_id, flush=True)

    base = (f"https://firestore.googleapis.com/v1/projects/{PROJECT}"
            f"/databases/(default)/documents/requests/{job_id}?key={API_KEY}")
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        time.sleep(5)
        try:
            doc = http("GET", base)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue  # workflow hasn't written the doc yet
            raise
        fields = {k: from_value(v) for k, v in doc.get("fields", {}).items()}
        if fields.get("status") in ("completed", "failed"):
            print(json.dumps(fields, indent=2, default=str))
            return 0 if fields["status"] == "completed" else 1
    print("timeout waiting for result")
    return 2


if __name__ == "__main__":
    sys.exit(main())