export class LiveUsers {
  connections = new Set<WebSocket>();
  state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(request: Request) {
    const upgradeHeader = request.headers.get("Upgrade");

    // Handle WebSocket upgrade
    if (upgradeHeader === "websocket") {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();

      this.connections.add(server);

      // Remove when disconnected
      server.addEventListener("close", () => {
        this.connections.delete(server);
        this.broadcastCount();
      });

      // Optional: send count when first connected
      server.send(JSON.stringify({ online: this.connections.size }));

      return new Response(null, { status: 101, webSocket: server });
    }

    // Return current live count for normal fetch
    return new Response(JSON.stringify({ online: this.connections.size }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  // Broadcast to all connected clients
  broadcastCount() {
    const count = JSON.stringify({ online: this.connections.size });
    for (const ws of this.connections) {
      try {
        ws.send(count);
      } catch {}
    }
  }
}
