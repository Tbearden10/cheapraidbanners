# Testing Guide for Admin Refresh User Endpoint

## Prerequisites

1. **API Token**: You need a valid API token for authentication
2. **User Membership ID**: Get the Bungie membership ID of the user to refresh
3. **API Endpoint**: `https://api.cheapraidbanners.com/admin/refresh-user` (or your local dev URL)

## Test Scenarios

### Scenario 1: Basic User Refresh (Incremental)
**Purpose**: Test that the endpoint works for a normal user with incremental refresh

**Request**:
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "NORMAL_USER_ID"
  }'
```

**Expected Response**:
```json
{
  "success": true,
  "results": {
    "membershipId": "NORMAL_USER_ID",
    "force": false,
    "clanId": "5335552",
    "queued": true,
    "runId": "run-user-1234567890-abc123",
    "displayName": "PlayerName#1234"
  }
}
```

**What to Check**:
1. Response is 200 OK
2. `queued: true` in response
3. Check worker logs for processing messages:
   ```
   [MemberJob] START: PlayerName#1234
   [MemberJob] Fetched X total activities for PlayerName#1234
   [MemberJob] Total after deduplication: Y (removed Z duplicates)
   [MemberJob] DungeonName: N new activities to process
   [MemberJob] Queued M batch(es) for DungeonName (N activities)
   [MemberJob] Initialized MemberCoordinator with M batches
   [MemberJob] COMPLETE: PlayerName#1234 | Queued: N activities in M batches | X.Xs
   ```

### Scenario 2: Force Refresh (Complete Recount)
**Purpose**: Test force flag clears existing stats and recounts everything

**Request**:
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "NORMAL_USER_ID",
    "force": true
  }'
```

**Expected Response**:
```json
{
  "success": true,
  "results": {
    "membershipId": "NORMAL_USER_ID",
    "force": true,
    "clanId": "5335552",
    "cleared": true,
    "queued": true,
    "runId": "run-user-1234567890-xyz789",
    "displayName": "PlayerName#1234"
  }
}
```

**What to Check**:
1. Response includes `"cleared": true`
2. Database query before and after:
   ```sql
   -- Before force refresh
   SELECT * FROM member_dungeon_stats 
   WHERE membership_id = 'NORMAL_USER_ID';
   
   -- Should return existing stats
   
   -- After force refresh starts (immediate)
   SELECT * FROM member_dungeon_stats 
   WHERE membership_id = 'NORMAL_USER_ID';
   
   -- Should return no rows (cleared)
   
   -- After processing completes (wait ~30 seconds)
   SELECT * FROM member_dungeon_stats 
   WHERE membership_id = 'NORMAL_USER_ID';
   
   -- Should return new stats with all historical data
   ```
3. All activities should be processed (no cutoff date filtering)

### Scenario 3: High-Activity User (9000+ Clears)
**Purpose**: Test the streaming batch processing with a user who has 9000+ total clears

**Request**:
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "HIGH_ACTIVITY_USER_ID",
    "force": true
  }'
```

**Expected Response**: Same as Scenario 2

**What to Check in Logs**:
1. **Total activities fetched** should be 9000+:
   ```
   [MemberJob] Fetched 9234 total activities for HighActivityUser#1234
   ```

2. **Deduplication** should show reasonable numbers:
   ```
   [MemberJob] Total after deduplication: 9156 (removed 78 duplicates)
   ```

3. **Per-dungeon processing** should show large numbers:
   ```
   [MemberJob] Grasp of Avarice: 2458 new activities to process
   [MemberJob] Queued 82 batch(es) for Grasp of Avarice (2458 activities)
   [MemberJob] Duality: 1876 new activities to process
   [MemberJob] Queued 63 batch(es) for Duality (1876 activities)
   ```

4. **MemberCoordinator initialization** should show total batch count:
   ```
   [MemberJob] Initialized MemberCoordinator with 305 batches
   ```

5. **Final summary** should show all activities queued:
   ```
   [MemberJob] COMPLETE: HighActivityUser#1234 | Queued: 9156 activities in 305 batches | 45.3s
   ```

6. **StatsQueue processing** should show batches being processed:
   ```
   [StatsQueue] Processing 30 PGCRs (HIGH_ACTIVITY_USER_ID-1234567-0)
   [StatsQueue] ✓ 30/30 in 2.5s (12.0 req/s) | Clears: 28 | Failed: 0
   ```

7. **Database verification**:
   ```sql
   -- Check total clears per dungeon
   SELECT 
     dungeon_hash,
     total_clears,
     total_full_clears,
     last_processed_date
   FROM member_dungeon_stats
   WHERE membership_id = 'HIGH_ACTIVITY_USER_ID'
   ORDER BY total_clears DESC;
   
   -- Check aggregate stats
   SELECT SUM(total_clears) as total_clears_all_dungeons
   FROM member_dungeon_stats
   WHERE membership_id = 'HIGH_ACTIVITY_USER_ID';
   ```

### Scenario 4: Error Cases

#### 4.1: Missing membershipId
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{}'
```

**Expected**: 400 Bad Request
```json
{
  "error": "Missing membershipId parameter"
}
```

#### 4.2: Invalid/Non-existent membershipId
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "membershipId": "9999999999999999999"
  }'
```

**Expected**: 404 Not Found
```json
{
  "error": "Member not found or inactive",
  "membershipId": "9999999999999999999"
}
```

#### 4.3: Missing Authentication
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Content-Type: application/json" \
  -d '{
    "membershipId": "NORMAL_USER_ID"
  }'
```

**Expected**: 401 Unauthorized
```json
{
  "error": "Unauthorized"
}
```

## Performance Expectations

### Normal User (< 1000 clears)
- **Processing Time**: 5-15 seconds
- **Batches**: 1-30 batches
- **Memory**: < 1 MB

### High-Activity User (1000-5000 clears)
- **Processing Time**: 15-60 seconds
- **Batches**: 30-150 batches
- **Memory**: 1-3 MB

### Very High-Activity User (5000-10000+ clears)
- **Processing Time**: 60-180 seconds
- **Batches**: 150-350+ batches
- **Memory**: 3-5 MB (streaming keeps it low)

## Monitoring

### Key Metrics to Watch
1. **Queue Depth**: Should not build up excessively
2. **Processing Time**: Should be proportional to activity count
3. **Error Rate**: Should be < 1% (some PGCRs may fail to fetch)
4. **Memory Usage**: Should remain stable even for 9000+ activities

### Database Queries for Verification
```sql
-- Check if user stats were updated
SELECT 
  m.display_name,
  COUNT(DISTINCT mds.dungeon_hash) as dungeons_with_stats,
  SUM(mds.total_clears) as total_clears,
  SUM(mds.total_full_clears) as total_full_clears,
  MAX(mds.last_processed_date) as last_processed
FROM clan_members m
LEFT JOIN member_dungeon_stats mds ON m.membership_id = mds.membership_id
WHERE m.membership_id = 'USER_ID'
GROUP BY m.membership_id, m.display_name;

-- Check processing history
SELECT 
  dungeon_hash,
  total_clears,
  total_full_clears,
  total_playtime_seconds,
  last_processed_date,
  updated_at
FROM member_dungeon_stats
WHERE membership_id = 'USER_ID'
ORDER BY updated_at DESC;
```

## Success Criteria

✅ **Pass**: All scenarios return expected responses
✅ **Pass**: High-activity user (9000+) processes without errors
✅ **Pass**: All clears are counted correctly in database
✅ **Pass**: MemberCoordinator receives accurate batch counts
✅ **Pass**: Logs show detailed per-dungeon information
✅ **Pass**: Memory usage remains stable
✅ **Pass**: No batch count mismatches
✅ **Pass**: Force refresh clears old stats and recounts correctly

## Troubleshooting

### Issue: No logs appearing
**Cause**: Job queued but not processing
**Check**: Queue consumer status, worker logs

### Issue: Some clears missing
**Cause**: Date filtering excluding activities
**Check**: `last_processed_date` in database vs activity dates in Bungie API

### Issue: Batch count mismatch
**Cause**: Should be fixed with streaming approach
**Check**: MemberCoordinator initialization logs

### Issue: Memory/timeout errors
**Cause**: Should be fixed with streaming approach
**Check**: Activity count, ensure streaming is working properly
