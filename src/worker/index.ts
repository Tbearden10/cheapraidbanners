import { Hono } from "hono";

export class LiveUsers {
  connections = new Set<WebSocket>();

  constructor(public state: DurableObjectState) {}

  async fetch(request: Request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.handleSession(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  handleSession(ws: WebSocket) {
    ws.accept();
    this.connections.add(ws);
    this.broadcast();

    ws.addEventListener("close", () => {
      this.connections.delete(ws);
      this.broadcast();
    });

    ws.addEventListener("error", () => {
      this.connections.delete(ws);
      this.broadcast();
    });
  }

  broadcast() {
    const count = this.connections.size;
    const msg = JSON.stringify({ online: count });
    for (const ws of this.connections) {
      try {
        ws.send(msg);
      } catch {}
    }
  }
}

const app = new Hono<{ Bindings: { LIVE_USERS: DurableObjectNamespace } }>();

app.get("/api/live", async (c) => {
  const id = c.env.LIVE_USERS.idFromName("global");
  const stub = c.env.LIVE_USERS.get(id);
  return stub.fetch(c.req.raw);
});

export default app;
