# Example API Requests for Admin Endpoints

This file contains example curl commands for testing the admin endpoints.

## Setup

First, set your API token as an environment variable:

```bash
export API_TOKEN="your_api_token_here"
```

## Examples

### 1. Refresh a single member (incremental update)

This will only process new activities since the last refresh:

```bash
curl -X POST http://localhost:8787/admin/refresh-member \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794"
  }'
```

### 2. Refresh a single member with force flag

This will clear all existing stats for the member and reprocess everything:

```bash
curl -X POST http://localhost:8787/admin/refresh-member \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "force": true
  }'
```

### 3. Refresh a single member with custom clan ID

```bash
curl -X POST http://localhost:8787/admin/refresh-member \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "4611686018467765794",
    "clanId": "5335552"
  }'
```

### 4. Refresh all members (existing endpoint)

```bash
curl -X POST http://localhost:8787/admin/refresh \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "all",
    "force": false
  }'
```

## Expected Responses

### Success Response

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

### Error: Missing membershipId

```json
{
  "error": "membershipId is required"
}
```

### Error: Member not found

```json
{
  "error": "Member not found"
}
```

### Error: Member not active

```json
{
  "error": "Member is not active"
}
```

## Notes

- In development mode (ENVIRONMENT=dev), authentication is bypassed
- The endpoint requires authentication in production
- Use the force flag with caution as it will delete all stats for the member
- For production, replace `http://localhost:8787` with `https://api.cheapraidbanners.com`
