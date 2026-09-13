/**
 * Cloudflare Worker front door.
 * POST /jobs    {"type":"echo","payload":{...}} -> 202 {"jobId","status"}
 * GET  /jobs/<id>                               -> job doc as plain JSON
 * GET  /healthz                                 -> {"ok":true}
 *
 * Writes to Firestore via REST. Doc shape is identical to what
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
  
  export default {
    async fetch(request, env) {
      const { pathname } = new URL(request.url);
  
      if (pathname === "/healthz") return json({ ok: true });
  
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
  };