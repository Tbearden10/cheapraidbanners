# Summary of Changes

## Problem Statement
Certain users with very large clear counts weren't working with the existing admin refresh functionality. There was no way to run the admin refresh on a single user, which made it difficult to troubleshoot and fix issues with specific high-activity members.

## Solution Implemented

Added a new `/admin/refresh-member` endpoint that allows targeted refresh of a single user's statistics with optional force flag.

## Changes Made

### 1. New Endpoint: `/admin/refresh-member` (src/index.ts, lines 716-811)

**Features:**
- Accepts `membershipId` (required) - The Bungie membership ID of the user to refresh
- Accepts `force` flag (optional) - When true, clears existing stats before refresh
- Accepts `clanId` (optional) - Defaults to configured BUNGIE_CLAN_ID
- Validates that the member exists and is active
- Returns detailed status including runId for tracking

**Request Validation:**
- Returns 400 if membershipId is missing
- Returns 404 if member not found in database
- Returns 400 if member is inactive

**Force Flag Behavior:**
- When `force=false` (default): Incremental update, only processes new activities
- When `force=true`: Deletes existing stats for the member and reprocesses everything
- Unlike `/admin/refresh` with force, this only clears stats for the single member

**Processing Flow:**
1. Validates request and retrieves member info from database
2. Optionally clears existing stats if force=true
3. Initializes RunTracker with expectedCount=1
4. Retrieves last processed date (or null if force=true)
5. Queues single member job to MEMBER_STATS_QUEUE
6. Returns success response with runId for tracking

### 2. Documentation Added

**API_ADMIN_ENDPOINTS.md:**
- Comprehensive API documentation for all admin endpoints
- Request/response examples
- Error codes and messages
- Authentication requirements

**EXAMPLE_REQUESTS.md:**
- Practical curl examples for testing
- Development and production endpoints
- Common use cases demonstrated

## Key Benefits

1. **Targeted Refresh**: Can refresh single problematic users without affecting all members
2. **Resource Efficient**: Processes only one user at a time, avoiding timeouts
3. **Force Option**: Can rebuild stats from scratch for a single user
4. **Minimal Changes**: ~100 lines of code, follows existing patterns
5. **Well Documented**: Complete API and usage documentation included
6. **Properly Authenticated**: Requires API token in production (bypassed in dev)
7. **Type Safe**: Uses TypeScript with proper error handling
8. **Security Verified**: Passed CodeQL security scan with no issues

## Testing & Validation

- ✅ TypeScript compilation successful (wrangler dry-run)
- ✅ Code review completed (minor nitpicks only, consistent with existing code)
- ✅ CodeQL security scan passed (0 alerts)
- ✅ Follows existing code patterns and conventions
- ✅ Compatible with existing infrastructure (queues, durable objects)

## Usage Examples

### Incremental refresh (default):
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"membershipId": "4611686018467765794"}'
```

### Force refresh (clear and rebuild):
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"membershipId": "4611686018467765794", "force": true}'
```

## Files Modified

- `src/index.ts` - Added new endpoint (97 lines)
- `API_ADMIN_ENDPOINTS.md` - New file (158 lines)
- `EXAMPLE_REQUESTS.md` - New file (112 lines)

**Total**: 367 lines added across 3 files

## Backward Compatibility

- ✅ No breaking changes to existing endpoints
- ✅ Existing `/admin/refresh` endpoint unchanged
- ✅ No changes to database schema
- ✅ No changes to queue structure
- ✅ No changes to existing data processing logic

## Security Considerations

- ✅ Endpoint requires authentication (production only)
- ✅ SQL injection prevented via parameterized queries
- ✅ Input validation on all required parameters
- ✅ Error handling prevents information leakage
- ✅ No secrets or sensitive data exposed in logs or responses
