/**
 * Cloudflare Worker: public front door + fleet watchdog.
 *
 * Front door (API-key Firestore REST, requests collection only):
 * POST /jobs    {"type":"echo","payload":{...}} -> 202 {"jobId","status"}
 * GET  /jobs/<id>                               -> job doc as plain JSON
 * GET  /healthz                                 -> {"ok":true}
 *
 * Watchdog (service-account Firestore REST, server/*):
 * GET  /fleet/status  -> read-only fleet summary {ok, activeNode, problems, notes}
 * cron * * * * *      -> fleetCheck + management actions + alert webhook
 *
 * VM truth + auto-boot (Option A: per-account OAuth refresh tokens):
 * env SHELL_ACCOUNT_<A|B|C|D> = Google account email owning that shell.
 * secrets OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET = ONE Google Cloud OAuth
 *   client (type "Desktop app", any of your projects) used to mint tokens
 *   for all 4 shell accounts.
 * secrets SHELL_A_REFRESH / SHELL_B_REFRESH / SHELL_C_REFRESH / SHELL_D_REFRESH
 *   = per-account refresh tokens from a one-time consent per shell account.
 * Each tick the watchdog mints that node's access token and calls
 * GET users/<email>/environments/default -> state RUNNING|SUSPENDED|PENDING.
 * VM state is reported in notes; when the lease has been open >VM_OPEN_AFTER_S
 * with quota left and the steered node's VM is SUSPENDED, the watchdog calls
 * POST ...:start (gated by VM_START_COOLDOWN_S fleet-wide) so the VM boots and
 * its .customize_environment hook rejoins the fleet on its own.
 *
 * Management actions (Firestore-level; Workers cannot boot Cloud Shell VMs):
 *  1. stuck holder (lease unexpired, holder heartbeat stale >180s): force-expire
 *     the lease so a RUNNING standby takes over instead of waiting out the TTL.
 *  2. open lease past the takeover grace with nobody taking it: point
 *     preferredNextNode at the next ring node with quota (rotates past dead
 *     hints), so the first shell that comes up claims without a fence fight.
 * Every run writes server/watchdog {ok, problems, actions, lastAlertKey/At}.
 * Alerts dedupe on problem-set change + 30m reminder while problems persist.
 *
 * Writes to Firestore via REST. Job doc shape is identical to what
 * worker.py/JobQueue expects, so the Cloud Shell side is untouched.
 */

const BASE = (project) =>
    `https://firestore.googleapis.com/v1/projects/${project}/databases/(default)/documents`;
  
  function json(obj, status = 200) {
    return new Response(JSON.stringify(obj), {
      status,
      headers: { "content-type": "application/json" },
    });
  }
  
  function toValue(v) {
    if (v === null || v === undefined) return { nullValue: null };
    switch (typeof v) {
      case "string":  return { stringValue: v };
      case "boolean": return { booleanValue: v };
      case "number":
        return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
    }
    if (Array.isArray(v)) return { arrayValue: { values: v.map(toValue) } };
    return {
      mapValue: {
        fields: Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toValue(x)])),
      },
    };
  }
  
  function fromValue(val) {
    if (!val || typeof val !== "object") return val;
    if ("stringValue" in val)  return val.stringValue;
    if ("booleanValue" in val) return val.booleanValue;
    if ("integerValue" in val) return Number(val.integerValue);
    if ("doubleValue" in val)  return val.doubleValue;
    if ("nullValue" in val)    return null;
    if ("timestampValue" in val) return val.timestampValue;
    if ("arrayValue" in val)   return (val.arrayValue.values || []).map(fromValue);
    if ("mapValue" in val)     return fromFields(val.mapValue.fields || {});
    return val;
  }
  
  function fromFields(fields) {
    const out = {};
    for (const [k, v] of Object.entries(fields || {})) out[k] = fromValue(v);
    return out;
  }
  
  async function fsFetch(url, init) {
    const r = await fetch(url, init);
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${JSON.stringify(body.error || body)}`);
    return body;
  }

  const RING = ["shell-a", "shell-b", "shell-c", "shell-d"];
  const HEARTBEAT_MAX_AGE_S = 90;    // matches scripts/monitor.py default
  const LEASE_MAX_AGE_S = 60;        // lease stale this long past expiry
  const TAKEOVER_GRACE_S = 45;       // mirrors lease.takeover_grace_seconds
  const HOLDER_STUCK_S = 180;        // holder silent this long -> force-expire
  const BACKLOG_LIMIT = 50;
  const QUOTA_SECONDS = 180000;      // 50h per shell/week
  const WEEK_SECONDS = 604800;       // 168h sliding window
  const ALERT_REMIND_S = 1800;       // re-alert every 30m while bad
  const WATCHDOG_DOC = "watchdog";
  const VM_OPEN_AFTER_S = 180;       // lease open this long before a VM start is attempted
  const VM_START_COOLDOWN_S = 600;   // min gap between VM start attempts fleet-wide
  const CLOUDSHELL_API = "https://cloudshell.googleapis.com/v1";

  function strVal(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === "string") return v;
    if (typeof v === "number" || typeof v === "boolean") return String(v);
    return JSON.stringify(v);
  }

  function b64urlEncode(bytes) {
    let bin = "";
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }

  function b64urlEncodeJson(obj) {
    return b64urlEncode(new TextEncoder().encode(JSON.stringify(obj)));
  }

  function pemToDer(pem) {
    const b64 = pem.replace(/-----[^-]+-----/g, "").replace(/\s+/g, "");
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return bytes;
  }

  // Mint a Google OAuth token from a service-account JSON key via WebCrypto
  // (RS256). Cached on `state` until ~60s before expiry.
  async function saAccessToken(saJson, state) {
    const nowS = Math.floor(Date.now() / 1000);
    if (state.token && state.tokenExp > nowS + 60) return state.token;
    const sa = JSON.parse(saJson);
    const header = b64urlEncodeJson({ alg: "RS256", typ: "JWT" });
    const claim = b64urlEncodeJson({
      iss: sa.client_email,
      scope: "https://www.googleapis.com/auth/datastore",
      aud: "https://oauth2.googleapis.com/token",
      iat: nowS,
      exp: nowS + 3600,
    });
    const data = new TextEncoder().encode(`${header}.${claim}`);
    const key = await crypto.subtle.importKey(
      "pkcs8", pemToDer(sa.private_key),
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"],
    );
    const sig = new Uint8Array(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, data));
    const assertion = `${header}.${claim}.${b64urlEncode(sig)}`;
    const r = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
        assertion,
      }).toString(),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`oauth exchange failed: ${r.status} ${JSON.stringify(body).slice(0, 200)}`);
    state.token = body.access_token;
    state.tokenExp = nowS + (body.expires_in || 3600);
    return state.token;
  }

  const saHeaders = (token) => ({ authorization: `Bearer ${token}` });

  // ---- Cloud Shell per-account OAuth (Option A) ----
  // One shared OAuth client (Desktop app), one refresh token per shell
  // account. Minted access tokens cached on `state` per node.
  function shellAccountEmail(env, node) {
    const suffix = node.split("-")[1].toUpperCase(); // shell-a -> A
    return (env[`SHELL_ACCOUNT_${suffix}`] || "").trim() || null;
  }

  function shellRefresh(env, node) {
    const suffix = node.split("-")[1].toUpperCase();
    return env[`SHELL_${suffix}_REFRESH`] || null;
  }

  function oauthClient(env) {
    if (!env.OAUTH_CLIENT_ID || !env.OAUTH_CLIENT_SECRET) return null;
    return { id: env.OAUTH_CLIENT_ID, secret: env.OAUTH_CLIENT_SECRET };
  }

  async function shellAccessToken(env, state, node) {
    state.shellTok = state.shellTok || {};
    const cached = state.shellTok[node];
    const nowS = Math.floor(Date.now() / 1000);
    if (cached && cached.exp > nowS + 60) return cached.token;
    const client = oauthClient(env);
    const refresh = shellRefresh(env, node);
    if (!client || !refresh) throw new Error(`no OAuth configured for ${node}`);
    const r = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: client.id,
        client_secret: client.secret,
        refresh_token: refresh,
        grant_type: "refresh_token",
      }).toString(),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`refresh ${node} failed: ${r.status} ${JSON.stringify(body).slice(0, 160)}`);
    state.shellTok[node] = { token: body.access_token, exp: nowS + (body.expires_in || 3600) };
    return body.access_token;
  }

  // Ground truth from Google: RUNNING | SUSPENDED | PENDING | ... (or
  // "unconfigured" when no OAuth for that node, "error:..." on failure).
  async function shellVmState(env, state, node) {
    const email = shellAccountEmail(env, node);
    if (!email) return "unconfigured";
    let token;
    try {
      token = await shellAccessToken(env, state, node);
    } catch (e) {
      return `error: ${String(e).slice(0, 120)}`;
    }
    try {
      const r = await fetch(
        `${CLOUDSHELL_API}/users/${encodeURIComponent(email)}/environments/default`,
        { headers: { authorization: `Bearer ${token}` } },
      );
      const body = await r.json().catch(() => ({}));
      if (!r.ok) return `error: ${r.status} ${JSON.stringify(body.error || body).slice(0, 120)}`;
      return body.state || "unknown";
    } catch (e) {
      return `error: ${String(e).slice(0, 120)}`;
    }
  }

  async function shellVmStart(env, state, node) {
    const email = shellAccountEmail(env, node);
    if (!email) throw new Error(`no account email for ${node}`);
    const token = await shellAccessToken(env, state, node);
    const r = await fetch(
      `${CLOUDSHELL_API}/users/${encodeURIComponent(email)}/environments/default:start`,
      { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: "{}" },
    );
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${JSON.stringify(body.error || body).slice(0, 200)}`);
    return body;
  }

  async function saGetDoc(project, token, path) {
    const r = await fetch(`${BASE(project)}/${path}`, { headers: saHeaders(token) });
    if (r.status === 404) return null;
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${JSON.stringify(body.error || body).slice(0, 200)}`);
    return body;
  }

  async function saListDocs(project, token, collectionId, pageSize = 100) {
    const out = [];
    let pageToken = "";
    for (let i = 0; i < 10; i++) {
      let url = `${BASE(project)}/${collectionId}?pageSize=${pageSize}`;
      if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;
      const r = await fetch(url, { headers: saHeaders(token) });
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(`${r.status} ${JSON.stringify(body.error || body).slice(0, 200)}`);
      for (const d of body.documents || []) out.push(d);
      pageToken = body.nextPageToken || "";
      if (!pageToken) break;
    }
    return out;
  }

  async function saPatchDoc(project, token, path, fields) {
    const mask = Object.keys(fields).map((f) => `updateMask.fieldPaths=${encodeURIComponent(f)}`).join("&");
    const r = await fetch(`${BASE(project)}/${path}?${mask}`, {
      method: "PATCH",
      headers: { ...saHeaders(token), "content-type": "application/json" },
      body: JSON.stringify({ fields }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${JSON.stringify(body.error || body).slice(0, 200)}`);
    return body;
  }

  function docName(doc) {
    const m = (doc.name || "").match(/\/documents\/(.+)$/);
    return m ? m[1] : "";
  }

  function docId(doc, collection) {
    const n = docName(doc);
    return n.startsWith(collection + "/") ? n.slice(collection.length + 1) : n;
  }

  // Mirror of scripts/monitor.py, trimmed for the Worker sandbox (no SDK).
  // vmByNode: optional ground truth from Cloud Shell API (RUNNING/SUSPENDED/
  // PENDING/unconfigured/error:...); shown in notes, never assumed.
  function fleetCheck(serverDocs, queuedCount, nowMs, vmByNode) {
    const problems = [];
    const notes = [];
    const byId = {};
    for (const d of serverDocs) byId[docId(d, "server")] = fromFields(d.fields || {});

    const lease = byId["lease"] || {};
    const active = typeof lease.activeNode === "string" ? lease.activeNode : null;
    const fence = Number(lease.fenceToken || 0);
    const expiresMs = lease.leaseExpiresAt ? Date.parse(lease.leaseExpiresAt) : NaN;

    if (!active) {
      problems.push("NO-ACTIVE-NODE: lease has no activeNode");
    } else {
      if (!Number.isFinite(expiresMs) || expiresMs < nowMs - LEASE_MAX_AGE_S * 1000) {
        problems.push(`LEASE-STALE: lease of ${active} expired at ${lease.leaseExpiresAt || "unknown"}`);
      }
      const hb = byId[`health-${active}`] || {};
      const ts = hb.timestamp ? Date.parse(hb.timestamp) : NaN;
      if (!Number.isFinite(ts) || nowMs - ts > HEARTBEAT_MAX_AGE_S * 1000) {
        problems.push(`HEARTBEAT-STALE: nothing from ${active} in ${HEARTBEAT_MAX_AGE_S}s`);
      } else if (!hb.appHealthy) {
        problems.push(`APP-UNHEALTHY: ${active} reports unhealthy`);
      }
    }

    const roster = [];
    for (const [id, d] of Object.entries(byId)) {
      if (!id.startsWith("health-")) continue;
      const node = d.node || id.slice("health-".length);
      const ts = d.timestamp ? Date.parse(d.timestamp) : NaN;
      const fresh = Number.isFinite(ts) && nowMs - ts <= HEARTBEAT_MAX_AGE_S * 1000;
      roster.push(`${node}(${fresh ? "up" : "down"})`);
      if (fresh && active && node !== active && d.appHealthy) {
        problems.push(`SPLIT-BRAIN: ${node} reports healthy but lease says ${active}`);
      }
    }
    notes.push(`roster: ${roster.join(", ") || "none"}`);
    const joined = Object.keys(byId).filter((id) => id.startsWith("health-")).map((id) => id.slice(7));
    if (vmByNode) {
      const vm = RING.map((n) => `${n}=${vmByNode[n] || "unknown"}`).join(", ");
      notes.push(`vm: ${vm}`);
      const running = RING.filter((n) => vmByNode[n] === "RUNNING");
      notes.push(`vms running: ${running.length ? running.join(", ") : "none"}`);
      for (const n of RING) {
        if (vmByNode[n] === "SUSPENDED" && !joined.includes(n)) {
          notes.push(`${n}: VM suspended, never joined (no health doc)`);
        }
      }
    } else {
      const missing = RING.filter((n) => !joined.includes(n));
      if (missing.length) notes.push(`never joined (no health doc): ${missing.join(", ")}`);
    }

    const quotaNotes = [];
    let successors = 0;
    const leftByNode = {};
    for (const n of RING) {
      const u = byId[`usage-${n}`];
      let left = QUOTA_SECONDS;
      if (u) {
        const used = Number(u.activeSeconds || 0);
        const startMs = u.weekStart ? Date.parse(u.weekStart) : NaN;
        if (Number.isFinite(startMs) && nowMs - startMs >= WEEK_SECONDS * 1000) {
          quotaNotes.push(`${n}: ${(left / 3600).toFixed(1)}h left (week expired, resets on next loop)`);
        } else {
          left = Math.max(0, QUOTA_SECONDS - used);
          quotaNotes.push(`${n}: ${(left / 3600).toFixed(1)}h left`);
        }
      } else {
        quotaNotes.push(`${n}: 50.0h left (no usage doc)`);
      }
      leftByNode[n] = left;
      if (left > 0 && n !== active) successors += 1;
    }
    notes.push(`quota: ${quotaNotes.join(", ")}`);
    if (active) {
      const aleft = leftByNode[active] ?? QUOTA_SECONDS;
      if (aleft < 3600 && successors === 0) {
        problems.push(`QUOTA-LOW: ${active} has ${(aleft / 3600).toFixed(1)}h left and no successor`);
      }
    }
    const totalLeft = RING.reduce((s, n) => s + (leftByNode[n] ?? QUOTA_SECONDS), 0);
    if (totalLeft < 24 * 3600) {
      problems.push(`QUOTA-FLEET-LOW: only ${(totalLeft / 3600).toFixed(1)}h left across fleet`);
    }

    notes.push(`queued jobs: ${queuedCount}`);
    if (queuedCount > BACKLOG_LIMIT) {
      problems.push(`BACKLOG: ${queuedCount} queued > limit ${BACKLOG_LIMIT}`);
    }
    if (vmByNode) {
      const running = RING.filter((n) => vmByNode[n] === "RUNNING");
      if (!active && running.length === 0 && !Object.values(vmByNode).some((v) => String(v).startsWith("error"))) {
        problems.push("VMS-DARK: no Cloud Shell VM is RUNNING (Google ground truth)");
      }
    }
    return { ok: problems.length === 0, problems, notes, active, fence, leftByNode };
  }

  // Firestore-level management: free a stuck lease / steer an open lease at
  // the next ring node with quota. Never touches fenceToken (only a lease
  // holder's transaction may increment it). Returns {actions, steerTarget}.
  async function fleetManage(project, token, check, serverDocs, nowMs) {
    const actions = [];
    let steerTarget = null;
    const byId = {};
    for (const d of serverDocs) byId[docId(d, "server")] = d;
    const leaseDoc = byId["lease"];
    if (!leaseDoc) return { actions, steerTarget };
    const lease = fromFields(leaseDoc.fields || {});
    const active = typeof lease.activeNode === "string" ? lease.activeNode : null;
    const expiresMs = lease.leaseExpiresAt ? Date.parse(lease.leaseExpiresAt) : NaN;

    if (active) {
      const hbDoc = byId[`health-${active}`];
      const hb = hbDoc ? fromFields(hbDoc.fields || {}) : {};
      const ts = hb.timestamp ? Date.parse(hb.timestamp) : NaN;
      const silentMs = Number.isFinite(ts) ? nowMs - ts : Infinity;
      const unexpired = Number.isFinite(expiresMs) && expiresMs >= nowMs;
      if (unexpired && silentMs > HOLDER_STUCK_S * 1000) {
        // Holder dead but lease not yet expired: force-expire so a RUNNING
        // standby takes over on its next 10s poll instead of waiting the TTL.
        await saPatchDoc(project, token, "server/lease", {
          leaseExpiresAt: { timestampValue: new Date(nowMs - 1000).toISOString() },
        });
        actions.push(`force-expired lease of silent holder ${active} (no heartbeat for ${Math.round(silentMs / 1000)}s)`);
      }
      return { actions, steerTarget };
    }

    // No active node: only steer once the previous hint's grace has passed,
    // otherwise we fight the normal same-shell reclaim path.
    const pref = typeof lease.preferredNextNode === "string" ? lease.preferredNextNode : null;
    const prefMs = lease.preferredAt ? Date.parse(lease.preferredAt) : NaN;
    if (Number.isFinite(prefMs) && nowMs - prefMs < TAKEOVER_GRACE_S * 1000) return { actions, steerTarget };
    const startIdx = RING.indexOf(pref);
    for (let step = 1; step <= RING.length; step++) {
      const node = RING[((startIdx < 0 ? -1 : startIdx) + step) % RING.length];
      if ((check.leftByNode[node] ?? QUOTA_SECONDS) > 0) {
        if (node !== pref) {
          await saPatchDoc(project, token, "server/lease", {
            preferredNextNode: { stringValue: node },
            preferredAt: { timestampValue: new Date(nowMs).toISOString() },
          });
          actions.push(`steered preferredNextNode ${pref || "(none)"} -> ${node} (open lease, next with quota)`);
        }
        steerTarget = node;
        break;
      }
    }
    return { actions, steerTarget };
  }

  // One watchdog pass: VM truth -> check fleet, manage lease, maybe boot the
  // steered VM, write server/watchdog, alert on change (+30m reminder).
  async function watchdogPass(env, state) {
    const project = env.GCP_PROJECT_ID;
    const nowMs = Date.now();
    const token = await saAccessToken(env.FLEET_SA_KEY, state);
    const serverDocs = await saListDocs(project, token, "server");
    // Bounded backlog probe: list capped at BACKLOG_LIMIT+1 (no index needed).
    let queuedCount = 0;
    try {
      const reqDocs = await saListDocs(project, token, "requests", BACKLOG_LIMIT + 1);
      for (const d of reqDocs) {
        const f = fromFields(d.fields || {});
        if (f.status === "queued") {
          queuedCount += 1;
          if (queuedCount > BACKLOG_LIMIT) break;
        }
      }
    } catch (e) {
      queuedCount = 0;
    }
    // VM ground truth (Option A). Sequential per node: 4 GETs, each with its
    // own account token. Null when no OAuth configured at all.
    let vmByNode = null;
    if (oauthClient(env) && RING.some((n) => shellRefresh(env, n) && shellAccountEmail(env, n))) {
      vmByNode = {};
      for (const n of RING) vmByNode[n] = await shellVmState(env, state, n);
    }
    const check = fleetCheck(serverDocs, queuedCount, nowMs, vmByNode);
    const { actions, steerTarget } = await fleetManage(project, token, check, serverDocs, nowMs);

    // Auto-boot (gated): lease open a while, steered node has quota, its VM
    // is SUSPENDED, and no start attempted fleet-wide within cooldown.
    // Booting only puts the VM at a login prompt; .customize_environment on
    // the shell must launch run.sh for it to actually rejoin.
    if (!check.active && steerTarget && vmByNode && vmByNode[steerTarget] === "SUSPENDED"
        && (check.leftByNode[steerTarget] ?? QUOTA_SECONDS) > 0) {
      const prevDoc0 = await saGetDoc(project, token, `server/${WATCHDOG_DOC}`);
      const prev0 = prevDoc0 ? fromFields(prevDoc0.fields || {}) : {};
      const lastStart = prev0.lastVmStartAt ? Date.parse(prev0.lastVmStartAt) : NaN;
      const leaseDoc0 = serverDocs.find((d) => docId(d, "server") === "lease");
      const lease0 = leaseDoc0 ? fromFields(leaseDoc0.fields || {}) : {};
      const openMs = lease0.releasedAt ? nowMs - Date.parse(lease0.releasedAt)
        : (lease0.leaseExpiresAt ? nowMs - Date.parse(lease0.leaseExpiresAt) : 0);
      const cooled = !Number.isFinite(lastStart) || nowMs - lastStart >= VM_START_COOLDOWN_S * 1000;
      if (openMs >= VM_OPEN_AFTER_S * 1000 && cooled) {
        try {
          await shellVmStart(env, state, steerTarget);
          actions.push(`started Cloud Shell VM for ${steerTarget} (lease open ${Math.round(openMs / 1000)}s)`);
          await saPatchDoc(project, token, `server/${WATCHDOG_DOC}`, {
            lastVmStartAt: { timestampValue: new Date(nowMs).toISOString() },
            lastVmStartNode: { stringValue: steerTarget },
          });
        } catch (e) {
          actions.push(`VM start FAILED for ${steerTarget}: ${String(e).slice(0, 160)}`);
        }
      }
    }

    const prevDoc = await saGetDoc(project, token, `server/${WATCHDOG_DOC}`);
    const prev = prevDoc ? fromFields(prevDoc.fields || {}) : {};
    const alertKey = [...check.problems].sort().join("|") || "ok";
    const prevKey = typeof prev.lastAlertKey === "string" ? prev.lastAlertKey : "";
    const prevAt = prev.lastAlertAt ? Date.parse(prev.lastAlertAt) : NaN;
    const shouldAlert = !check.ok && (alertKey !== prevKey
      || !Number.isFinite(prevAt) || nowMs - prevAt >= ALERT_REMIND_S * 1000);
    if (shouldAlert && env.ALERT_WEBHOOK_URL) {
      try {
        await fetch(env.ALERT_WEBHOOK_URL, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: `fleet watchdog: ${check.problems.join("; ")}` }),
        });
      } catch (e) { /* alert best-effort; state below still records */ }
    }

    const summary = {
      ok: check.ok, activeNode: check.active, problems: check.problems,
      notes: check.notes, actions,
      vm: vmByNode,
      at: new Date(nowMs).toISOString(),
    };
    const wfields = {
      ok: { booleanValue: check.ok },
      activeNode: check.active ? { stringValue: check.active } : { nullValue: null },
      problems: { arrayValue: { values: check.problems.map((p) => ({ stringValue: p })) } },
      notes: { arrayValue: { values: check.notes.map((n) => ({ stringValue: n })) } },
      actions: { arrayValue: { values: actions.map((a) => ({ stringValue: a })) } },
      checkedAt: { timestampValue: new Date(nowMs).toISOString() },
      lastAlertKey: { stringValue: shouldAlert ? alertKey : (prevKey || alertKey) },
      lastAlertAt: { timestampValue: shouldAlert ? new Date(nowMs).toISOString()
        : (prev.lastAlertAt || new Date(nowMs).toISOString()) },
    };
    await saPatchDoc(project, token, `server/${WATCHDOG_DOC}`, wfields);
    return summary;
  }
  
  export default {
    async fetch(request, env) {
      const { pathname } = new URL(request.url);
  
      if (pathname === "/healthz") return json({ ok: true });

      if (request.method === "GET" && pathname === "/fleet/status") {
        if (!env.FLEET_SA_KEY) return json({ error: "watchdog not configured" }, 503);
        try {
          const summary = await watchdogPass(env, {});
          return json(summary);
        } catch (e) {
          return json({ error: "watchdog failed", detail: String(e).slice(0, 300) }, 502);
        }
      }
  
      if (request.method === "POST" && pathname === "/jobs") {
        let body;
        try { body = await request.json(); } catch { return json({ error: "invalid json" }, 400); }
        const type = body.type;
        if (typeof type !== "string" || !type) return json({ error: "missing 'type'" }, 400);
        const payload = body.payload && typeof body.payload === "object" ? body.payload : {};
        const jobId = crypto.randomUUID();
  
        const fields = {
          jobId:    { stringValue: jobId },
          type:     { stringValue: type },
          payload:  { mapValue: { fields: Object.fromEntries(
                        Object.entries(payload).map(([k, v]) => [k, toValue(v)])) } },
          status:   { stringValue: "queued" },
          createdAt:{ timestampValue: new Date().toISOString() },
          attempts: { integerValue: "0" },
        };
        const url = `${BASE(env.GCP_PROJECT_ID)}/requests?documentId=${jobId}&key=${env.FIRESTORE_API_KEY}`;
        try {
          await fsFetch(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ fields }),
          });
        } catch (e) {
          return json({ error: "firestore write failed", detail: String(e) }, 502);
        }
        return json({ jobId, status: "queued" }, 202);
      }
  
      const m = pathname.match(/^\/jobs\/([A-Za-z0-9_-]+)$/);
      if (request.method === "GET" && m) {
        const url = `${BASE(env.GCP_PROJECT_ID)}/requests/${m[1]}?key=${env.FIRESTORE_API_KEY}`;
        try {
          const doc = await fsFetch(url);
          return json(fromFields(doc.fields || {}));
        } catch (e) {
          if (String(e).startsWith("404")) return json({ error: "not found" }, 404);
          return json({ error: "firestore read failed", detail: String(e) }, 502);
        }
      }
  
      return json({ error: "not found" }, 404);
    },

    async scheduled(event, env, ctx) {
      // Cron watchdog: runs fleetCheck + management every minute.
      // Needs FLEET_SA_KEY secret; without it the tick is a no-op.
      if (!env.FLEET_SA_KEY) {
        console.log("watchdog tick skipped: FLEET_SA_KEY not set");
        return;
      }
      const state = {};
      ctx.waitUntil(watchdogPass(env, state).then(
        (s) => console.log(`watchdog ok=${s.ok} active=${s.activeNode} actions=${s.actions.length}`),
        (e) => console.log(`watchdog error: ${String(e).slice(0, 300)}`),
      ));
    },
  };