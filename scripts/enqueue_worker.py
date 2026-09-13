import json, os, uuid
from datetime import datetime, timezone
from google.cloud import firestore

job_id = os.environ.get("JOB_ID") or uuid.uuid4().hex
job_type = os.environ.get("JOB_TYPE") or "echo"
raw = os.environ.get("JOB_PAYLOAD") or "{}"
try:
    payload = json.loads(raw)
except (json.JSONDecodeError, TypeError):
    payload = {}
if isinstance(payload, str):  # workflow_dispatch passes it as a string
    try:
        payload = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        payload = {}
if not isinstance(payload, dict):
    payload = {"value": payload} if payload else {}

db = firestore.Client(project=os.environ["GCP_PROJECT_ID"])
db.collection("requests").document(job_id).set({
    "jobId": job_id,
    "type": job_type,
    "payload": payload,
    "status": "queued",
    "createdAt": datetime.now(timezone.utc),
    "attempts": 0,
})
print(f"enqueued jobId={job_id} type={job_type}")