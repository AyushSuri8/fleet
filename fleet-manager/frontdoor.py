"""HTTP API for the fleet manager (stdlib http.server).

Public:
  GET  /healthz
  POST /jobs {"type","payload"}   -> 202 {jobId,status}   (optional frontdoor_token)
  GET  /jobs/<id>                 -> job doc
  GET  /fleet/status              -> fleet summary

Agent (x-fleet-token must equal agent_token):
  POST /agent/claim      {"node"}                       -> 200 job | 204 (long-poll)
  POST /agent/heartbeat  {"node"}
  POST /agent/result/<id>     {"node","result"} or {"node","error","retryable"}
  POST /agent/checkpoint/<id> {"node","checkpoint"}     (extends the job lease)
"""
import json
import ssl
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


def make_server(fleet, cfg):
    lease_s = int(cfg.get("job_lease_seconds", 300))
    long_poll_s = float(cfg.get("agent_long_poll_seconds", 25))
    allow_any = bool(cfg.get("allow_standby_claims", False))
    agent_token = cfg.get("agent_token", "")
    frontdoor_token = cfg.get("frontdoor_token", "")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def log_message(self, fmt, *args):
            pass  # keep journald clean; the manager logs the interesting parts

        def _body(self):
            n = int(self.headers.get("content-length") or 0)
            if n <= 0:
                return {}
            try:
                return json.loads(self.rfile.read(n).decode() or "{}")
            except json.JSONDecodeError:
                return {}

        def _json(self, obj, status=200):
            raw = json.dumps(obj, default=str).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _token(self):
            auth = self.headers.get("authorization") or ""
            return (self.headers.get("x-fleet-token")
                    or auth.replace("Bearer ", "").strip())

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/healthz":
                return self._json({"ok": True})
            if path == "/fleet/status":
                return self._json(fleet.summary())
            if path.startswith("/jobs/"):
                job = fleet.store.get_job(path[len("/jobs/"):])
                if not job:
                    return self._json({"error": "not found"}, 404)
                return self._json(job)
            return self._json({"error": "not found"}, 404)

        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/jobs":
                if frontdoor_token and self._token() != frontdoor_token:
                    return self._json({"error": "unauthorized"}, 401)
                body = self._body()
                jtype = body.get("type")
                if not isinstance(jtype, str) or not jtype:
                    return self._json({"error": "missing 'type'"}, 400)
                payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
                jid = uuid.uuid4().hex
                fleet.store.enqueue_job(jid, jtype, payload,
                                        int(cfg.get("max_attempts", 3)))
                return self._json({"jobId": jid, "status": "queued"}, 202)

            if not agent_token or self._token() != agent_token:
                return self._json({"error": "unauthorized"}, 401)

            if path == "/agent/claim":
                node = self._body().get("node", "")
                job = fleet.store.claim_job(node, fleet.active, allow_any,
                                            lease_s, long_poll_s)
                if job is None:
                    self.send_response(204)
                    self.end_headers()
                    return
                return self._json(job)

            if path == "/agent/heartbeat":
                fleet.store.heartbeat(self._body().get("node", ""))
                return self._json({"ok": True})

            if path.startswith("/agent/result/"):
                jid = path[len("/agent/result/"):]
                body = self._body()
                ok = (fleet.store.complete_job(jid, body.get("node"), body.get("result"))
                      if "result" in body
                      else fleet.store.fail_job(jid, body.get("node"),
                                                str(body.get("error", ""))[:1000],
                                                bool(body.get("retryable", True))))
                return self._json({"ok": True} if ok else {"error": "rejected"},
                                  200 if ok else 409)

            if path.startswith("/agent/checkpoint/"):
                jid = path[len("/agent/checkpoint/"):]
                body = self._body()
                ok = fleet.store.checkpoint_job(jid, body.get("node"),
                                                body.get("checkpoint"), lease_s)
                return self._json({"ok": True} if ok else {"error": "rejected"},
                                  200 if ok else 409)

            return self._json({"error": "not found"}, 404)

    srv = ThreadingHTTPServer((cfg.get("listen_host", "0.0.0.0"),
                               int(cfg.get("listen_port", 8443))), Handler)
    srv.daemon_threads = True
    if cfg.get("tls_cert") and cfg.get("tls_key"):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cfg["tls_cert"], cfg["tls_key"])
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    return srv
