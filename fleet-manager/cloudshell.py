"""Cloud Shell API client for ONE account (stdlib only).

Mints access tokens from that account's refresh token (the only Google
credential the manager holds), resolves users/me/environments/default, boots
with :start, and registers the manager's SSH key with :addPublicKey.

Note: the refresh tokens are Testing-mode and die every ~7 days. When one
dies, update secrets/SHELL_X_REFRESH.txt and restart the manager."""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://cloudshell.googleapis.com/v1"
TOKEN_URL = "https://oauth2.googleapis.com/token"


class CloudShellError(Exception):
    pass


def _is_quota_response(status, payload):
    """True when Google's response means 'weekly quota exhausted'.

    Google rejects :start (or fails its long-running op) with HTTP 429 /
    RESOURCE_EXHAUSTED when the account has burned its weekly hours. The
    payload shape varies (top-level error vs. nested details), so match on
    the status code AND on the standard reason strings anywhere in the body.
    """
    try:
        blob = json.dumps(payload or {})
    except (TypeError, ValueError):
        blob = str(payload or "")
    return status == 429 or "RESOURCE_EXHAUSTED" in blob or "QUOTA_EXHAUSTED" in blob


class CloudShell:
    def __init__(self, client_id, client_secret, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._tok = None
        self._tok_exp = 0.0

    def _token(self):
        now = time.time()
        if self._tok and self._tok_exp > now + 60:
            return self._tok
        data = urllib.parse.urlencode({
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }).encode()
        req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                resp = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:200]
            if "invalid_grant" in detail:
                raise CloudShellError(
                    "refresh token dead (invalid_grant): re-consent per DEPLOY.md §4/§5, "
                    "update secrets/SHELL_X_REFRESH.txt, restart manager")
            raise CloudShellError(f"token mint HTTP {e.code}: {detail}")
        except urllib.error.URLError as e:
            raise CloudShellError(f"token mint network error: {e}")
        self._tok = resp["access_token"]
        self._tok_exp = now + float(resp.get("expires_in", 3600))
        return self._tok

    def _call(self, method, path, body=None, timeout=25):
        url = path if path.startswith("http") else f"{API}/{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "authorization": f"Bearer {self._token()}",
            "content-type": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode() or "{}")
            except json.JSONDecodeError:
                return e.code, {}

    def get_env(self):
        st, env = self._call("GET", "users/me/environments/default")
        if st != 200:
            raise CloudShellError(f"get_env HTTP {st}: {json.dumps(env)[:200]}")
        return env

    def start(self, timeout_s=300):
        """Boot the VM; polls the long-running op, then waits for RUNNING.

        Raises CloudShellError("QUOTA_EXHAUSTED ...") when Google officially
        rejects the start because the account's weekly quota is gone
        (HTTP 429 / RESOURCE_EXHAUSTED). Callers use this — not any local
        timer — as the source of truth for quota exhaustion.
        """
        st, op = self._call("POST", "users/me/environments/default:start", body={})
        if st not in (200, 201):
            if _is_quota_response(st, op):
                raise CloudShellError(
                    f"QUOTA_EXHAUSTED: :start HTTP {st}: {json.dumps(op)[:200]}")
            raise CloudShellError(f":start HTTP {st}: {json.dumps(op)[:200]}")
        name = op.get("name", "")
        if name:
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                _, cur = self._call("GET", name)
                if cur.get("done"):
                    if "error" in cur:
                        if _is_quota_response(0, cur.get("error")):
                            raise CloudShellError(
                                "QUOTA_EXHAUSTED: "
                                f"start op failed: {json.dumps(cur['error'])[:200]}")
                        raise CloudShellError(
                            f"start op failed: {json.dumps(cur['error'])[:200]}")
                    break
                time.sleep(5)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            env = self.get_env()
            if env.get("state") == "RUNNING":
                return env
            time.sleep(5)
        raise CloudShellError("VM did not reach RUNNING in time")

    def add_public_key(self, pub):
        """Register the manager's public key. Best-effort idempotent: a
        duplicate registration is fine — SSH connectivity is the real test.

        Sends only the bare "type blob" pair: the API answers 500 to ed25519
        keys and to trailing "user@host" comments (ssh-keygen default), while
        bare ECDSA registers with done:true."""
        bare = " ".join(str(pub).split()[:2])
        st, op = self._call("POST", "users/me/environments/default:addPublicKey",
                            body={"key": bare})
        if st not in (200, 201):
            return False
        name = op.get("name", "")
        if not name:
            return True
        for _ in range(60):
            _, cur = self._call("GET", name)
            if cur.get("done"):
                return "error" not in cur
            time.sleep(3)
        return False
