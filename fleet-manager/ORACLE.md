# Oracle VM setup — full runbook (1 GB Ubuntu box)

One script does everything. One manual click in the Oracle console. Secrets are the only files you carry over.

## 0. What goes where

| Thing | Source | Destination on Oracle VM |
|---|---|---|
| `fleet-manager/` code | this repo (`git clone` or `scp -r`) | `/opt/fleet-manager` |
| `secrets/oauth-client.json` | your existing setup (`~/.secrets/oauth-client.json`) | `/opt/fleet-manager/secrets/` |
| `secrets/SHELL_{A,B,C,D}_REFRESH.txt` | existing refresh tokens, one line each | `/opt/fleet-manager/secrets/` |
| TLS cert/key | auto-created by the script | `tls/fleet.crt`, `tls/fleet.key` |
| manager SSH key | auto-created by `provision.py` | `~/.fleet-manager/id_ed25519` |

## 1. Copy the code (pick A or B)

```bash
# --- on the Oracle VM ---
sudo mkdir -p /opt/fleet-manager && sudo chown $USER /opt/fleet-manager

# A) from GitHub:
git clone https://github.com/AyushSuri8/fleet.git /tmp/fleet \
  && cp -r /tmp/fleet/fleet-manager/* /opt/fleet-manager/ \
  && cd /opt/fleet-manager

# B) from this machine (run HERE, not on the VM):
scp -r fleet-manager/ <oracle-user>@<oracle-ip>:/opt/fleet-manager/
```

## 2. Copy the secrets (5 files, nothing else is sensitive)

```bash
# --- from the machine that has them ---
scp ~/.secrets/oauth-client.json <oracle-user>@<oracle-ip>:/opt/fleet-manager/secrets/
for x in A B C D; do
  scp .secrets/SHELL_${x}_REFRESH.txt <oracle-user>@<oracle-ip>:/opt/fleet-manager/secrets/ 2>/dev/null \
  || echo "missing SHELL_${x}_REFRESH.txt — re-consent per DEPLOY.md §4/§5 first"
done

# --- on the Oracle VM ---
chmod 600 /opt/fleet-manager/secrets/*
ls -l /opt/fleet-manager/secrets/
# want: oauth-client.json + SHELL_A/B/C/D_REFRESH.txt
```

Refresh-token filenames must match exactly (`SHELL_A_REFRESH.txt`, not `shell-a.txt`).
A dead token (`invalid_grant`) is the only recurring chore: re-consent, replace the one file, `sudo systemctl restart fleet-manager`.

## 3. The one manual click (Oracle console, not the VM)

VCN → your subnet's **Security List** → **Add Ingress Rule**: source `0.0.0.0/0`, protocol TCP, port `8443`.
The script opens the VM's own firewall, but without this rule no agent can reach `:8443` from outside.

## 4. Run the setup (does steps 5–9 by itself)

```bash
cd /opt/fleet-manager
chmod +x oracle-setup.sh
./oracle-setup.sh
# flags: --skip-provision  --skip-service  --yes
#        extra args shell-a.. are passed to provision.py, e.g.:
./oracle-setup.sh shell-b shell-c
```

What it does, in order:

1. system check (OS, RAM, disk, python3, ssh, openssl, `py_compile` all files)
2. `manager_config.json`: fills `public_url` from the detected public IP and generates `agent_token` — only if still placeholders, never overwrites yours
3. `tls/fleet.crt` + `tls/fleet.key` self-signed (skips if present; agents use `insecure_tls` against these)
4. host firewall: `ufw allow 8443/tcp` if ufw is active, else `iptables -I INPUT -p tcp --dport 8443 -j ACCEPT` (+ persist hint)
5. secrets presence check — stops here listing exactly which of the 5 files is missing
6. `python3 provision.py` — boots each shell VM if needed, registers the manager SSH key, uploads `agent/agent.py` + `handlers.py` + per-node `agent_config.json`, starts the agent
7. installs `fleet-manager.service`, `daemon-reload`, `enable --now`, fails with the last 50 log lines if the service isn't active
8. `curl -sk https://<IP>:8443/fleet/status | python3 -m json.tool` as the end-to-end proof

## 5. Verify (what "working" looks like)

```bash
curl -sk https://<YOUR-IP>:8443/fleet/status | python3 -m json.tool
# want: "activeNode": "shell-x", that node's "lastSeenAgeS" small, "ok": true

curl -sk -X POST https://<YOUR-IP>:8443/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"echo","payload":{"hello":"world"}}'
# -> {"jobId": "...", "status": "queued"}

curl -sk https://<YOUR-IP>:8443/jobs/<jobId>
# -> "status": "completed", result.processedBy = the active shell

journalctl -u fleet-manager -f
# want: "activated ... agent heartbeating", then silence (tick every 30s)
```

Cap-path test without waiting 11h: set `"cap_hours": 0.02` in `manager_config.json`, `sudo systemctl restart fleet-manager`, watch a warm-rotate in the journal, set it back to `11`.

## 6. Troubleshooting (the short list)

| Symptom | Cause → fix |
|---|---|
| `oracle-setup.sh` stops at §5 listing MISSING files | copy §2's 5 files, re-run (idempotent) |
| `provision.py`: `no SSH endpoint` | VM never reached RUNNING — check the refresh token for that node (`invalid_grant` = re-consent) |
| `provision.py`: `agent not provisioned` follow-up fails | SSH ok but `~/fleet-agent/` upload failed — re-run `python3 provision.py <node>` |
| `/fleet/status` unreachable from laptop | OCI security list rule missing (§3) or host firewall — `sudo iptables -L -n \| grep 8443` |
| `fleet-manager` active but `NO-ACTIVE-NODE` | provision hadn't heartbeated yet — `python3 provision.py` again, then `sudo systemctl restart fleet-manager` |
| OOM / box slow (1 GB) | expected: everything is stdlib-only, no pip/venv. If tight, add 1 GB swap: `sudo fallocate -l 1G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile` |

## 7. After it works

- `frontdoor_token`: set it in `manager_config.json` + restart if the `/jobs` URL will ever be visible — otherwise job submission is open.
- Retire the old stack only after a full rotation cycle: Cloudflare Worker cron, GH keeper/monitor workflows, then Firestore.
