# Fleet — Deployment & Ops Reference

All fleet state lives in ONE shared Firestore project: **`shell-project-d2b93`**.
Four Google accounts each run one Cloud Shell VM (`shell-a`..`shell-d`) that
take turns holding `server/lease`. A Cloudflare Worker (`fleet-frontdoor`)
is the public front door + every-minute fleet watchdog.

This file is the complete config reference lost-forever-proof. Keep it in git.

---

## 1. Cloudflare Worker — code

1. Open your Worker's **worker.js** in the Cloudflare dashboard.
2. Replace the ENTIRE file with the contents of **`frontdoor/cf-worker.js`**
   (from this repo, `main`). Set `compatibility_date = "2024-11-01"`.
3. **Cron Triggers → Add:** `* * * * *`  (every minute — the watchdog tick).

Current/latest: `d1f578c` (worker has the OAuth VM truth + `oauth-help` helper).
Verify the deployed version answers:

```bash
curl https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/oauth-help   # 200 = latest code, 404 = old code, re-paste
```

---

## 2. Cloudflare Worker — text variables

| Variable | Value |
|---|---|
| `GCP_PROJECT_ID` | `shell-project-d2b93` |
| `SHELL_ACCOUNT_A` | `g852943@gmail.com` |
| `SHELL_ACCOUNT_B` | `<shell-b google email>` |
| `SHELL_ACCOUNT_C` | `<shell-c google email>` |
| `SHELL_ACCOUNT_D` | `<shell-d google email>` |

Emails are plain vars (not secrets); they tell the Worker which
`users/<email>/environments/default` to query. The b/c/d emails are unknown
here — fill them in.

---

## 3. Cloudflare Worker — secrets

| Secret | Value / where it comes from |
|---|---|
| `FIRESTORE_API_KEY` | Firebase project **Settings → Service accounts** → "Firebase Admin SDK" → **Web API key** |
| `FLEET_SA_KEY` | full contents of `~/.secrets/shell-project-d2b93-331fa174bc3e.json` (the `fleet-worker` service-account JSON) |
| `OAUTH_CLIENT_ID` | `676527033494-f7rsl109mvq0c37p83c0u1eoqgnhe5uq.apps.googleusercontent.com` (from `~/.secrets/oauth-client.json` → `installed.client_id`) |
| `OAUTH_CLIENT_SECRET` | `installed.client_secret` from `~/.secrets/oauth-client.json` |
| `SHELL_A_REFRESH` | refresh token from shell-a consent (see §4/§5) |
| `SHELL_B_REFRESH` | refresh token from shell-b consent |
| `SHELL_C_REFRESH` | refresh token from shell-c consent |
| `SHELL_D_REFRESH` | refresh token from shell-d consent |
| `ALERT_WEBHOOK_URL` | optional — Slack/Discord webhook for watchdog alerts |

The OAuth client file lives at `~/.secrets/oauth-client.json` on the IDX;
copy it to the same path on any admin box you refresh from.

---

## 4. One-time consent per shell account (every 7 days in Testing mode)

Open this EXACT URL **logged in as that shell's Google account** (to refresh
shell-c, sign out first and sign in as shell-c's user):

```text
https://accounts.google.com/o/oauth2/v2/auth?client_id=676527033494-f7rsl109mvq0c37p83c0u1eoqgnhe5uq.apps.googleusercontent.com&redirect_uri=http%3A%2F%2Flocalhost&response_type=code&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&access_type=offline&prompt=consent
```

OR generate it on demand from the live worker (uses the currently configured
client id):

```bash
curl "https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/oauth-help"                    # all nodes
curl "https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/oauth-help?node=shell-b"       # one node
```

- Each shell's Google account must be a **Test user** (GCP Console → APIs &
  Services → OAuth consent screen → Test users) or consent fails with
  "Access blocked".
- After consent you land on `http://localhost/?code=4/0...` (page errors —
  fine). Copy the `code=` value; codes are single-use, exchange within minutes.

---

## 5. Exchange code → refresh token, then store

On any machine with the client secret handy:

```bash
CS="$(python3 -c "import json; print(json.load(open('$HOME/.secrets/oauth-client.json'))['installed']['client_secret'])")"
CID="676527033494-f7rsl109mvq0c37p83c0u1eoqgnhe5uq.apps.googleusercontent.com"
CODE="4/0PASTE_HERE"

curl -s -X POST https://oauth2.googleapis.com/token \
  -d "client_id=$CID" -d "client_secret=$CS" \
  -d "code=$CODE" -d "grant_type=authorization_code" -d "redirect_uri=http://localhost" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['refresh_token'])"
```

Prints `1//0e...`. Immediately:

- Cloudflare dashboard → **Settings → Variables → Secrets** → set the matching
  `SHELL_<A|B|C|D>_REFRESH` to that value → **Deploy**.
- Old worker returns `invalid_grant` for the dead token; the new token starts
  being used as soon as the secret + deploy land.

### Secret values already created (do NOT re-exchange):

- shell-a consent done → refresh token starts `1//0ekLypG2B...` → store as `SHELL_A_REFRESH`.
- b/c/d: still to do (one consent each + one exchange each).

## 6. Shell-side checklist (each of the 4 VMs)

```bash
git clone https://github.com/AyushSuri8/fleet.git && cd fleet
mkdir -p ~/.secrets
# upload the SAME service-account key to ~/.secrets/shell-project-d2b93-331fa174bc3e.json
chmod 600 ~/.secrets/shell-project-d2b93-331fa174bc3e.json

cp scripts/customize_environment ~/.customize_environment && chmod +x ~/.customize_environment

FLEET_NODE_ID=shell-x ./bootstrap.sh
./fleetctl.sh status
```

Order: `shell-b`, `shell-c`, `shell-d`, real `shell-a` last.
`bootstrap.sh` now REQUIRES the shared SA key (refuses `gcloud auth` fallback)
and defaults the project to `shell-project-d2b93` — no
`gcloud config set project` needed.

---

## 7. Verify end-to-end

```bash
# Worker is live + on latest code
curl https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/status
# want: "vm":{"shell-a":"RUNNING",...} and "oauthHelp" present

# Submit a real job
curl -s -X POST https://fleet-frontdoor.ayushsuri37.workers.dev/jobs \
  -H "content-type: application/json" \
  -d '{"type":"echo","payload":{"hello":"world"}}'
# -> {"jobId":"<id>","status":"queued"}

# Poll until the active shell's worker completes it
curl "https://fleet-frontdoor.ayushsuri37.workers.dev/jobs/<id>"
# -> status "completed" with result

# Auto-boot proof: kill the active shell -> lease opens -> >3 min ->
# cron starts the steered SUSPENDED VM -> its .customize_environment hook
# rejoins -> fenceToken+1 in /fleet/status.
```

---

## 8. Weekly (7-day) maintenance — the only recurring chore

Testing-mode refresh tokens die after 7 days (`invalid_grant`). The watchdog
detects this and raises `AUTH-EXPIRED` for exactly the affected nodes; the
status output links to `/fleet/oauth-help?node=<shell-x>`.

1. `GET /fleet/status` → note which nodes show `AUTH-EXPIRED` / `refresh-dead`.
2. For each: open the consent URL as that account (§4), exchange (§5), update
   secret, redeploy. ~1 minute per account, 4×/week at most.

Publishing the OAuth app (Google verification, sensitive scope
`cloud-platform`) would remove the 7-day expiry but needs review/domain/demo
video — not worth it for 4 known users.

---

## 9. FAQ / gotchas

- **`invalid_grant` on exchange:** code already used or >few minutes old —
  re-consent.
- **`invalid_client` on exchange:** wrong client id (that was the earlier
  service-account-id mistake) — use §4/§5 values, don't substitute.
- **"Access blocked" on consent:** account missing from OAuth consent screen
  Test users.
- **Worker `404 /fleet/oauth-help`:** old code — re-paste
  `frontdoor/cf-worker.js`.
- **`vm: {"shell-a":"refresh-dead"}`:** token died; see §8.
- **`/fleet/status` shows fleet dark (`ok:false, no activeNode`):** correct —
  no shell currently holds the lease. The watchdog manages the agreement, the
  shells + the auto-boot hook make it real.

