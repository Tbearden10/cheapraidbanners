// VisitorCounter Durable Object (SSE) - dedupes tabs by client uid
export class VisitorCounter {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.encoder = new TextEncoder();

    // in-memory maps while DO instance is alive
    // clientMap: clientId -> { writer, uid }
    // uidMap: uid -> Set(clientId)
    this.clientMap = new Map();
    this.uidMap = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === "/events") return this.handleSSE(request);
    if (pathname === "/count") {
      return new Response(JSON.stringify({ count: this.uidMap.size }), { headers: { "Content-Type": "application/json" }});
    }
    if (pathname === "/count/raw") {
      return new Response(JSON.stringify({
        connections: this.clientMap.size,
        unique: this.uidMap.size
      }), { headers: { "Content-Type": "application/json" }});
    }

    return new Response("Not found", { status: 404 });
  }

  async handleSSE(request) {
    const url = new URL(request.url);
    let uid = url.searchParams.get("uid");
    if (!uid) uid = request.headers.get("x-visitor-uid") || crypto.randomUUID();

    const stream = new TransformStream();
    const writer = stream.writable.getWriter();
    const clientId = crypto.randomUUID();

    // register the new client
    this.clientMap.set(clientId, { writer, uid });

    if (!this.uidMap.has(uid)) this.uidMap.set(uid, new Set());
    this.uidMap.get(uid).add(clientId);

    // send initial count
    try {
      await writer.write(this.encoder.encode(`event: liveCount\ndata: ${JSON.stringify({ count: this.uidMap.size })}\n\n`));
    } catch (e) {
      // ignore write errors here
    }

    // keepalive to avoid intermediate proxies closing connection
    const keepAlive = setInterval(() => {
      try {
        writer.write(this.encoder.encode(":keepalive\n\n"));
      } catch (e) {
        // ignore; cleanup will handle it
      }
    }, 15000);

    const abortHandler = () => {
      clearInterval(keepAlive);
      this._removeClient(clientId);
      try { writer.close(); } catch (e) {}
      this.broadcast(); // notify remaining clients
    };

    request.signal.addEventListener("abort", abortHandler);

    // broadcast count because a new unique visitor may have joined
    this.broadcast();

    return new Response(stream.readable, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  }

  _removeClient(clientId) {
    const client = this.clientMap.get(clientId);
    if (!client) return;
    const { uid } = client;
    this.clientMap.delete(clientId);

    const set = this.uidMap.get(uid);
    if (!set) return;
    set.delete(clientId);
    if (set.size === 0) this.uidMap.delete(uid);
  }

  async broadcast() {
    const payload = `event: liveCount\ndata: ${JSON.stringify({ count: this.uidMap.size })}\n\n`;
    const data = this.encoder.encode(payload);

    for (const [clientId, { writer }] of this.clientMap.entries()) {
      try {
        await writer.write(data);
      } catch (err) {
        // on failure, remove the client
        this._removeClient(clientId);
        try { writer.close(); } catch (e) {}
      }
    }
  }
}