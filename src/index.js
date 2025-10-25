// Router worker: forwards /events and /count to the VisitorCounter Durable Object
// Also re-export the VisitorCounter class so Wrangler can create the Durable Object binding.
export { VisitorCounter } from "./visitor_counter.js";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // Visitor counter endpoints
    if (path.startsWith("/events") || path === "/count" || path === "/count/raw") {
      const id = env.VISITOR_COUNTER.idFromName("main");
      const obj = env.VISITOR_COUNTER.get(id);
      return obj.fetch(request);
    }

    // Fallback response
    return new Response("Visitor counter worker running. Use /events and /count", { status: 200 });
  },
};