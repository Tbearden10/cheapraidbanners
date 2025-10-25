```markdown
# live-visitor-counter

Cloudflare Workers + Durable Objects example for a simple live visitor count (SSE).

- VisitorCounter DO (SSE) provides an accurate live count.
- Router worker forwards `/events` and `/count` to the Durable Object.
- Example static client in `public/index.html` with styles in `public/styles.css`.

## Directory structure

- wrangler.toml
- src/index.js
- src/visitor_counter.js
- public/index.html
- public/styles.css

## Quick local dev

1. Install wrangler (v2): `npm install -g wrangler`
2. Login: `wrangler login`
3. From project root: `wrangler dev`

## Set account id

Edit `wrangler.toml` and replace `YOUR_ACCOUNT_ID` with your Cloudflare account ID (from the dashboard).

## Publish

1. `wrangler publish`

Durable Object namespaces/bindings declared in `wrangler.toml` will be created as part of the publish.

## Routes (Cloudflare Dashboard)

If you want the worker to run under your domain:

1. In Cloudflare Dashboard → Workers → Manage Workers, find `live-visitor-counter`.
2. Add route patterns for the endpoints:
   - example.com/events*
   - example.com/count*
3. If you host static assets on the same domain, ensure the route patterns only match the API paths, not your static assets.

## How the client works

- The client generates (and persists in localStorage) a visitor UID to dedupe multiple tabs.
- The client opens an EventSource to `/events?uid=<uid>` and listens for `liveCount` events (SSE).
- `/count` returns a JSON snapshot: `{ count: <uniqueVisitors> }`.

## Notes

- SSE is one-way (server → client) and auto-reconnects in browsers.
- Durable Objects remove the need for an external DB for presence coordination.
- Check DO pricing/limits if you expect very high concurrent SSE connections.
- If you want the Worker to serve the static site directly (Workers Sites or Pages), I can add that configuration.
```