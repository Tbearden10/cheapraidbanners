# Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Cheap Raid Banners                                  │
│                    Cloudflare Workers Application                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐         ┌──────────────────────┐
│   Cron Triggers      │         │   Admin API          │
│                      │         │                      │
│  • Member Sync       │         │  • /admin/refresh    │
│    (Hourly)          │         │  • /admin/recompute  │
│  • Stats Sync        │         │  • /admin/process-   │
│    (Daily 7PM UTC)   │         │    member            │
│  • Aggregate         │         │                      │
│    (Daily 8PM UTC)   │         │                      │
└──────────┬───────────┘         └──────────┬───────────┘
           │                                 │
           └────────────┬────────────────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │   Main Worker          │
           │   (index.ts)           │
           └────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Member Sync  │ │  Stats Sync  │ │  Aggregates  │
│              │ │              │ │              │
│ • Fetch clan │ │ • Compare    │ │ • Recompute  │
│   roster     │ │   Bungie vs  │ │   clan       │
│ • Update DB  │ │   DB stats   │ │   totals     │
│ • Track new/ │ │ • Queue only │ │              │
│   left       │ │   changed    │ │              │
│   members    │ │   members    │ │              │
└──────────────┘ └──────┬───────┘ └──────────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │  MEMBER_STATS_QUEUE    │
           │  (max_batch_size: 1)   │
           └────────────┬───────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │  Member Job Processor  │
           │  (memberJobProcessor)  │
           │                        │
           │  1. Fetch aggregate    │
           │     stats from Bungie  │
           │  2. Compare with DB    │
           │  3. Skip if no new     │
           │     activities         │
           │  4. Fetch activity     │
           │     history if needed  │
           │  5. Group by dungeon   │
           │  6. Queue batches      │
           └────────────┬───────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │   STATS_QUEUE          │
           │   (batches of 30)      │
           └────────────┬───────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │  Stats Queue Processor │
           │  (statsQueueProcessor) │
           │                        │
           │  1. Fetch PGCRs        │
           │  2. Determine full     │
           │     clears             │
           │  3. Extract playtime   │
           │  4. Update DB (delta)  │
           │  5. Notify coordinator │
           └────────────┬───────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │  Member Coordinator    │
           │  (Durable Object)      │
           │                        │
           │  • Tracks batch        │
           │    completion          │
           │  • Aggregates results  │
           │  • Updates member      │
           │    stats in DB         │
           └────────────────────────┘
```

## Data Flow: Member Processing

```
1. Stats Sync Cron Trigger
   └─> Fetch all active members from DB
       └─> For each member:
           ├─> Fetch Bungie aggregate stats (API)
           ├─> Fetch DB stats
           ├─> Compare totals
           └─> If different:
               └─> Queue to MEMBER_STATS_QUEUE

2. Member Job Processor (Queue Consumer)
   └─> Fetch member's characters (API)
       └─> For each character:
           ├─> Fetch aggregate stats (API)
           └─> Build dungeon completion targets
       └─> Compare with current DB stats
       └─> If aggregate == DB:
           └─> SKIP (no new activities) ✅ OPTIMIZATION
       └─> If aggregate > DB:
           ├─> Fetch activity history (API, paginated)
           ├─> Group activities by dungeon
           ├─> Deduplicate by instanceId
           ├─> Filter to new activities only
           ├─> Create batches (30 activities each)
           └─> Queue batches to STATS_QUEUE

3. Stats Queue Processor (Queue Consumer)
   └─> For each batch of activities:
       ├─> Fetch PGCR for each activity (API)
       ├─> Determine if full clear (based on season)
       ├─> Extract playtime for member
       ├─> Calculate deltas (new clears, full clears, time)
       ├─> Write to DB (incremental updates)
       └─> Notify Member Coordinator

4. Member Coordinator (Durable Object)
   └─> Track all batches for member
       └─> When all batches complete:
           ├─> Aggregate all results
           ├─> Update member_dungeon_stats (DB)
           └─> Cleanup coordinator state
```

## Key Optimizations

### 1. Aggregate Stats Comparison (NEW)
**Problem**: Previously fetched activity history for all members, even if no new activities.

**Solution**: 
- Fetch Bungie's aggregate stats (total completions per dungeon)
- Compare with DB stats
- Skip fetching activity history if numbers match
- **Result**: Massive reduction in API calls and processing time

### 2. Single Member Processing (NEW)
**Problem**: New clan members had to wait for the next hourly cron to be processed.

**Solution**:
- Added `/admin/process-member` endpoint
- Accepts membershipId and queues immediately
- Enables real-time processing on member join
- **Result**: Instant stats for new members

### 3. Removed RunTracker Durable Object (NEW)
**Problem**: RunTracker added complexity with minimal benefit.

**Solution**:
- Removed RunTracker DO entirely
- Rely on daily aggregate recompute cron
- **Result**: Simpler architecture, fewer moving parts

## Technology Stack

### Core Platform
- **Cloudflare Workers**: Serverless compute platform
- **Cloudflare D1**: SQLite-based serverless database
- **Cloudflare Queues**: Message queues for async processing
- **Cloudflare Durable Objects**: Stateful coordination (MemberCoordinator)
- **Cloudflare Cron Triggers**: Scheduled job execution

### APIs
- **Bungie.net API**: Source of all Destiny 2 data
  - Clan roster endpoint
  - Character data
  - Aggregate activity stats (NEW: used for optimization)
  - Activity history
  - Post-Game Carnage Reports (PGCRs)

### Data Storage
- **clan_members**: Member roster and online status
- **member_dungeon_stats**: Individual member stats per dungeon
- **clan_aggregate_stats**: Clan-wide totals per dungeon

## Cron Schedule

| Time (UTC) | Trigger | Purpose |
|------------|---------|---------|
| Every hour (`:00`) | Member Sync | Fetch roster, track online status |
| Daily at 19:00 | Stats Sync | Queue members with new activities |
| Daily at 20:00 | Aggregate Recompute | Recalculate clan totals |

## API Endpoints

### Public Endpoints
- `GET /members` - List all clan members
- `GET /stats` - Member and clan dungeon statistics
- `GET /activity-history` - Activity history for a character
- `GET /recent-activities` - 3 most recent dungeon completions
- `GET /pgcr` - Post-game carnage report details

### Admin Endpoints (Authenticated)
- `POST /admin/refresh` - Force member/stats sync
- `POST /admin/recompute` - Force aggregate recalculation
- `POST /admin/process-member` - Process single member immediately (NEW)

### Debug Endpoints
- `GET /debug/user-completions` - Deep dive into member's completion data

## Performance Characteristics

### Before Optimization
- **All members processed daily**: ~50 members × 3 characters × 250 activities = ~37,500 API calls
- **Processing time**: ~30-45 minutes
- **Rate limiting**: Frequent 429 errors

### After Optimization
- **Only changed members processed**: ~5-10 members typically
- **Aggregate comparison**: 50 members × 3 characters = 150 quick API calls
- **Skip if no changes**: 80-90% of members skipped
- **Processing time**: ~5-10 minutes
- **Rate limiting**: Rare

## Queue Configuration

### MEMBER_STATS_QUEUE
- **max_batch_size**: 1 (one member at a time)
- **max_concurrency**: 1 (sequential processing)
- **Purpose**: Prevents overwhelming Bungie API with concurrent requests

### STATS_QUEUE
- **max_batch_size**: 1 batch (of ~30 PGCRs)
- **max_concurrency**: 1 (sequential processing)
- **Purpose**: Batch processing of activity details

## Error Handling

- **Retry logic**: 2 retries with exponential backoff on API failures
- **Rate limiting**: Automatic retry after rate limit headers
- **Dead letter queues**: Failed jobs sent to DLQ for investigation
- **Fail-safe**: Member sync aborts if roster fetch fails (prevents mass inactivation)

## Security

- **API Authentication**: Bearer token required for admin endpoints
- **CORS**: Restricted to cheapraidbanners.com domain
- **Security Headers**: X-Content-Type-Options, X-Frame-Options, etc.
- **Rate Limiting**: Per-IP rate limiting on public endpoints (100 req/min)

## Monitoring & Logging

**Logging Strategy**: Minimal, actionable logs
- Member sync: New/left member count
- Stats sync: Members queued count
- Member processor: Skipped (no changes) or new activities per dungeon
- Stats processor: Batch completion, errors only
- Coordinator: Final aggregation results

**Key Metrics**:
- Members processed
- Activities queued
- Processing duration
- API calls made
- Skipped members (optimization working)
