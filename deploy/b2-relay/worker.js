// b2-relay: fetches one presigned B2 GET for the hub's bundler (scripts/bundler.py), so the bytes go from B2 to Cloudflare, which is free
// under the Bandwidth Alliance, instead of straight to the VPS. It holds no keys: only URLs the hub signed work, and only for env.BUCKET.
// Deploy: python3 deploy/b2-relay/deploy.py, then set B2_RELAY_URL for scrape-hub-bundler (see README).
export default {
  async fetch(request, env) {
    if (request.method !== "GET") return new Response("method not allowed", { status: 405 });
    let target;
    try { target = new URL(new URL(request.url).searchParams.get("u") || ""); } catch { return new Response("b2 relay", { status: 400 }); }
    const bucket = env.BUCKET || "gepnic-scrape";
    if (target.protocol !== "https:" || !/^s3\.[a-z0-9-]+\.backblazeb2\.com$/.test(target.hostname)
        || !target.pathname.startsWith(`/${bucket}/`) || !target.searchParams.get("X-Amz-Signature")) {
      return new Response("forbidden", { status: 403 });
    }
    const headers = new Headers();
    const range = request.headers.get("range");
    if (range) headers.set("range", range);
    const r = await fetch(target.toString(), { headers });
    const out = new Headers({ "cache-control": "no-store" });
    for (const h of ["content-type", "content-length", "content-range", "etag", "last-modified"]) {
      const v = r.headers.get(h);
      if (v) out.set(h, v);
    }
    return new Response(r.body, { status: r.status, headers: out });
  },
};
