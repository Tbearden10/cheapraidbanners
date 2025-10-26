# Cloudflare Workers + D1 + KV + Queues Architecture (simple)

Overview
- Workers: HTTP API endpoints and queue consumers
- D1: primary DB for members, characters, totals
- KV: caching layer for member list and fast reads
- Queues: per-member and per-character jobs
- Durable Objects: real-time notifications (optional)
- Cron Trigger: run every 5 minutes to sync member list

Key endpoints
- GET /api/members
  - Return member list + cached stats from KV (fallback D1)
  - Enqueue background update check for members
- POST (Queue) member-check
  - Payload: { memberId, memberVersion, online }
  - If online -> fetch characters -> enqueue character jobs
- POST (Queue) character-job
  - Payload: { memberId, characterId, cursor?, aggregationKey }
  - Fetch activities (paginated), update running total, persist incremental update
- Durable Object endpoint (optional)
  - Broadcasts update events to connected clients

Database schema (simple)
- members(id, display_name, online, known_clears, updated_at, version)
- characters(id, member_id, platform, last_sync_cursor)
- user_aggregates(member_id, total_clears, last_checked_at)

Worker secrets
- EXTERNAL_API_KEY
- RATE_LIMIT_CONFIG

Deployment
- Use Wrangler v2
- Configure queues, D1, KV, DO bindings in wrangler.toml