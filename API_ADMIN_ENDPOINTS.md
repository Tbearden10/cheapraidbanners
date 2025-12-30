# Admin API Endpoints

This document describes the admin endpoints available for managing clan member statistics.

## Authentication

All admin endpoints require authentication via Bearer token in the `Authorization` header:
```
Authorization: Bearer YOUR_API_TOKEN
```

In development mode (`ENVIRONMENT=dev`), authentication is bypassed.

---

## POST /admin/refresh

Refresh all clan members and/or statistics.

### Request Body

```json
{
  "type": "all|members|stats",  // Optional, defaults to "all"
  "force": true|false,           // Optional, defaults to false
  "clanId": "5335552"            // Optional, defaults to env.BUNGIE_CLAN_ID
}
```

### Parameters

- **type** (string, optional): What to refresh
  - `"all"` - Refresh both members and stats (default)
  - `"members"` - Only sync clan roster
  - `"stats"` - Only sync statistics
- **force** (boolean, optional): When `true`, clears all existing stats before sync. Defaults to `false`.
- **clanId** (string, optional): Target clan ID. Defaults to configured clan ID.

### Response

```json
{
  "success": true,
  "results": {
    "cleared": true,
    "members": null,
    "stats": null
  }
}
```

---

## POST /admin/refresh-member

Refresh statistics for a single clan member. This is useful for users with very large clear counts that may cause issues when refreshing all members.

### Request Body

```json
{
  "membershipId": "4611686018467765794",  // Required
  "force": true|false,                    // Optional, defaults to false
  "clanId": "5335552"                     // Optional, defaults to env.BUNGIE_CLAN_ID
}
```

### Parameters

- **membershipId** (string, required): The Bungie membership ID of the member to refresh
- **force** (boolean, optional): When `true`, clears existing stats for this member before sync. Defaults to `false`.
- **clanId** (string, optional): Target clan ID. Defaults to configured clan ID.

### Response

#### Success (200 OK)

```json
{
  "success": true,
  "results": {
    "membershipId": "4611686018467765794",
    "cleared": true,
    "queued": true,
    "runId": "run-single-1735554321123-a1b2c3",
    "force": true
  }
}
```

#### Error Responses

**400 Bad Request** - Missing membershipId:
```json
{
  "error": "membershipId is required"
}
```

**404 Not Found** - Member not found:
```json
{
  "error": "Member not found"
}
```

**400 Bad Request** - Member is inactive:
```json
{
  "error": "Member is not active"
}
```

### Example Usage

**Refresh a single member with force:**
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "force": true
  }'
```

**Refresh a single member incrementally:**
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794"
  }'
```

---

## POST /admin/recompute

Recompute clan aggregate statistics from member statistics.

### Request Body

```json
{
  "clanId": "5335552"  // Optional, defaults to env.BUNGIE_CLAN_ID
}
```

### Response

```json
{
  "success": true,
  "clanId": "5335552"
}
```
