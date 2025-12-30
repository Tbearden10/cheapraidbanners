# Admin Refresh User Endpoint

## Overview
The `/admin/refresh-user` endpoint allows you to trigger a stats refresh for a single user (clan member) instead of refreshing all users.

## Endpoint Details

**URL**: `POST /admin/refresh-user`

**Authentication**: Requires authentication (API token or dev environment)

**Content-Type**: `application/json`

## Request Body

```json
{
  "membershipId": "string (required)",
  "force": "boolean (optional, default: false)",
  "clanId": "string (optional, defaults to env.BUNGIE_CLAN_ID)"
}
```

### Parameters

- **membershipId** (required): The Bungie membership ID of the user to refresh
- **force** (optional): If `true`, clears existing stats for this user before processing (forces a complete recount)
- **clanId** (optional): The clan ID to use (defaults to the configured clan ID)

## Response

### Success (200 OK)
```json
{
  "success": true,
  "results": {
    "membershipId": "string",
    "force": boolean,
    "clanId": "string",
    "cleared": boolean,
    "queued": true,
    "runId": "string",
    "displayName": "string"
  }
}
```

### Errors

#### 400 Bad Request - Missing membershipId
```json
{
  "error": "Missing membershipId parameter"
}
```

#### 404 Not Found - Member not found
```json
{
  "error": "Member not found or inactive",
  "membershipId": "string"
}
```

#### 401 Unauthorized
```json
{
  "error": "Unauthorized"
}
```

#### 500 Internal Server Error
```json
{
  "error": "Failed to queue member refresh",
  "message": "error details"
}
```

## Usage Examples

### Basic Refresh
Refreshes stats for a user, only processing new activities since last refresh:

```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "4611686018467765462"
  }'
```

### Force Refresh
Completely recounts all stats for a user (clears existing data first):

```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "4611686018467765462",
    "force": true
  }'
```

## How It Works

1. Validates that the `membershipId` is provided
2. Looks up the member in the database (must be active)
3. If `force=true`, deletes existing stats for this user
4. Determines the cutoff date (last processed date) for incremental processing
5. Queues a job to process this user's activities
6. The job processor will:
   - Fetch all characters for the user
   - Fetch all activities for each character
   - Filter to completed dungeon runs
   - Process only activities after the cutoff date (or all if force=true)
   - Fetch PGCR data for each activity
   - Update stats in the database

## Important Notes

- The endpoint returns immediately after queueing the job - actual processing happens asynchronously
- Use `force=true` when you suspect stats are incorrect or missing older clears
- The `runId` in the response can be used for tracking/debugging
- Processing time depends on how many activities the user has

## Related Endpoints

- `/admin/refresh` - Refresh all members or specific type (members/stats/all)
- `/members` - Get list of clan members
- `/stats` - Get current stats for all members
