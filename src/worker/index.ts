import { Hono } from "hono";
import { LiveUsers } from "./liveUsers";

const app = new Hono<{ Bindings: { LIVE_USERS: DurableObjectNamespace } }>();

app.all("/api/live", async (c) => {
  const id = c.env.LIVE_USERS.idFromName("global");
  const stub = c.env.LIVE_USERS.get(id);
  // Forward the request (normal fetch or websocket upgrade)
  return stub.fetch(c.req.raw);
});

export default app;
