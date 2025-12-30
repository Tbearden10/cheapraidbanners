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

Refresh statistics for a single clan member. This is useful for users with very large clear counts that may cause issues when refreshing all members. **Now supports fresh users not yet in the database.**

### Request Body

```json
{
  "membershipId": "4611686018467765794",  // Required
  "membershipType": 3,                    // Required (1=Xbox, 2=PSN, 3=Steam, 4=Blizzard, 5=Stadia, 6=EGS, 10=Demon, 254=BungieNext)
  "displayName": "PlayerName#1234",       // Optional (used for fresh users)
  "force": true|false,                    // Optional, defaults to false
  "clanId": "5335552"                     // Optional, defaults to env.BUNGIE_CLAN_ID
}
```

### Parameters

- **membershipId** (string, required): The Bungie membership ID of the member to refresh
- **membershipType** (number, required): The platform membership type (1=Xbox, 2=PSN, 3=Steam, etc.)
- **displayName** (string, optional): Display name for the member (only needed for fresh users not in DB)
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
    "force": true,
    "membershipType": 3,
    "displayName": "PlayerName#1234",
    "isNewUser": false
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

**400 Bad Request** - Missing membershipType:
```json
{
  "error": "membershipType is required for fresh users (1=Xbox, 2=PSN, 3=Steam, etc.)"
}
```

**400 Bad Request** - Member is inactive:
```json
{
  "error": "Member is not active"
}
```

### Example Usage

**Refresh a fresh user (not in database):**
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "membershipType": 3,
    "displayName": "PlayerName#1234",
    "force": true
  }'
```

**Refresh an existing member with force:**
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "membershipType": 3,
    "force": true
  }'
```

**Refresh an existing member incrementally:**
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-member \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "membershipType": 3
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
