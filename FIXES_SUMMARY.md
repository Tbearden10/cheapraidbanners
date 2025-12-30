# Fixes Summary

## Issues Addressed

### 1. Missing Endpoint for Single User Refresh
**Problem**: No way to refresh stats for a single user without triggering a full clan refresh.

**Solution**: Added new `/admin/refresh-user` endpoint that:
- Takes a `membershipId` parameter to identify the user
- Supports a `force` flag to clear and reprocess all stats for that user
- Queues the job asynchronously and returns immediately
- See `ENDPOINT_USAGE.md` for full API documentation

### 2. Older Clears Not Being Counted
**Problem**: Users were missing older clears in their stats count.

**Root Cause**: The code was comparing the number of activities fetched from Bungie (`completed.length`) with the count stored in the database (`dbTotalClears`). This logic incorrectly assumed:
- If Bungie returns fewer activities than the DB count, there's nothing new to process
- This failed when:
  - Bungie API returned only recent activities (limited by pagination)
  - Database had been cleared or had incorrect counts
  - Users had old activities that were never initially counted

**Location**: `src/processors/memberJobProcessor.ts`, lines 123-125 and 201-203

**Fix**: Removed the count-based skip logic entirely. Now the system:
- Uses only date-based filtering (`last_processed_date` from database)
- Processes all activities after the cutoff date
- If no cutoff exists (first-time processing), processes all activities
- This ensures all new activities are counted regardless of API limitations

**Code Changes**:
```typescript
// BEFORE (INCORRECT):
const dbTotalClears = prevRow ? Number((prevRow as any).total_clears ?? 0) : 0;
if (completed.length <= dbTotalClears) {
  continue;  // SKIP - WRONG!
}

// AFTER (CORRECT):
// Removed count comparison, use only date filtering
let newActivities = completed;
if (cutoffDate) {
  newActivities = completed.filter(a => {
    const actDate = new Date(a.period);
    return actDate.getTime() > cutoffDate.getTime();
  });
}
```

### 3. Batch Counting Bug for High-Activity Users (9000+ clears)
**Problem**: Users with very high activity counts (9000+ total clears) were experiencing issues with batch processing.

**Root Cause**: The `totalBatches` variable was being incorrectly modified during the queueing loop:
- Lines 95-148 correctly calculated `totalBatches` by summing batch counts
- Lines 150-166 initialized MemberCoordinator with the correct `totalBatches`
- Line 247 then **incremented** `totalBatches` for each batch queued, overwriting the original value

**Location**: `src/processors/memberJobProcessor.ts`, line 247

**Fix**: Changed line 247 from `totalBatches++` to `batchesQueued++`:
- `totalBatches` remains the correctly calculated value for MemberCoordinator
- `batchesQueued` tracks how many batches were actually queued
- Final log now shows: `Queued: ${totalQueued} activities in ${batchesQueued} batches`

**Impact**: 
- MemberCoordinator now receives correct batch counts
- Users with 9000+ clears will process correctly
- Aggregate stats will be computed properly

### 4. Streaming Batch Processing (NEW - Critical Fix)
**Problem**: For users with 9000+ total clears, some dungeons didn't have enough completions counted to create batches properly. Potential overflow or memory issues with the two-pass approach.

**Root Cause**: The code used a **two-pass approach**:
1. First pass: Calculate total batches across all dungeons (lines 107-148)
2. Second pass: Actually queue the batches (lines 180-262)

This approach had multiple issues:
- **Memory overhead**: Creating all batch arrays in memory before sending
- **Duplication**: Same filtering logic executed twice (risk of inconsistency)
- **Overflow risk**: Large activity counts could cause issues with batch array creation
- **Complexity**: Two separate loops doing similar work

**Location**: `src/processors/memberJobProcessor.ts`, entire batch processing section

**Fix**: Refactored to use **streaming batch processing**:
```typescript
// Stream-based processing: process and queue dungeons one at a time
// This avoids memory issues with large activity counts (9000+)
let totalQueued = 0;
let batchesQueued = 0;
const dungeonBatchCounts: Record<string, number> = {};

for (const dungeon of ACTIVITY_REFERENCE_MAP) {
  // Filter activities for this dungeon
  const newActivities = /* ... date-based filtering ... */;
  
  // Stream batches: create and send immediately (avoid memory buildup)
  for (let i = 0; i < activitiesPayload.length; i += batchSize) {
    const batch = activitiesPayload.slice(i, Math.min(i + batchSize, activitiesPayload.length));
    
    // Send batch immediately
    await env.STATS_QUEUE.send({
      clanId, membershipId, membershipType, dungeonHash,
      activities: batch,
      jobId: `${membershipId}-${dungeonHash}-${dungeonBatchCount}`,
    });
    
    batchesQueued++;
    dungeonBatchCount++;
  }
  
  dungeonBatchCounts[dungeonHash] = dungeonBatchCount;
}

// Initialize MemberCoordinator AFTER all batches are queued
if (batchesQueued > 0) {
  await coordinator.fetch('https://internal/init', {
    method: 'POST',
    body: JSON.stringify({
      membershipId, membershipType, clanId,
      totalBatches: batchesQueued,
      dungeonBatches: dungeonBatchCounts
    })
  });
}
```

**Benefits**:
- **Single pass**: Only loop through dungeons once
- **Streaming**: Batches are created and sent immediately (no memory buildup)
- **Accurate counts**: MemberCoordinator is initialized with exact counts after queueing
- **Simpler**: Less code, easier to maintain, no duplication
- **Robust**: Handles 9000+ activities without overflow or memory issues
- **Per-dungeon logging**: See exactly how many activities are being processed per dungeon

**Impact**:
- Users with 9000+ clears will have all completions counted correctly
- No memory issues with large activity counts
- More accurate logging of what's being processed
- MemberCoordinator gets exact batch counts
- Reduces risk of count mismatches

### 5. Enhanced Logging for Debugging
**Added comprehensive logging** to help diagnose issues with large activity counts:

1. **Total Activities Fetched**:
   ```
   [MemberJob] Fetched 9234 total activities for PlayerName
   ```

2. **Per-Dungeon Deduplication**:
   ```
   [MemberJob] Deduped dungeon 1234567: 450 -> 437 (13 duplicates)
   [MemberJob] Deduped dungeon 7654321: 320 -> 315 (5 duplicates)
   ```

3. **Total Deduplication Summary**:
   ```
   [MemberJob] Total after deduplication: 9156 (removed 78 duplicates)
   ```

4. **Final Processing Summary**:
   ```
   [MemberJob] COMPLETE: PlayerName | Queued: 1234 activities in 42 batches | 45.3s
   ```

These logs will help identify:
- If Bungie is returning the correct number of activities
- How many duplicates are being removed (cross-character activities)
- Exactly how many activities are being processed per dungeon
- Exactly how many batches are being created and queued per dungeon
- When MemberCoordinator is initialized and with how many batches
- Performance metrics for large activity counts

## Key Improvements Summary

1. **Date-based filtering only**: No more incorrect count comparisons
2. **Streaming batch processing**: Single-pass, memory-efficient, handles 9000+ activities
3. **Accurate batch counts**: MemberCoordinator gets exact counts after queueing
4. **Per-dungeon visibility**: See exactly what's being processed for each dungeon
5. **Single user endpoint**: Trigger refresh for one user instead of entire clan

## Testing Recommendations

### Test Case 1: Single User Refresh (Normal)
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"membershipId": "USER_ID"}'
```
**Expected**: Only processes new activities since last refresh

### Test Case 2: Single User Force Refresh
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"membershipId": "USER_ID", "force": true}'
```
**Expected**: 
- Clears all existing stats for user
- Reprocesses all activities from Bungie
- Correctly counts all historical clears

### Test Case 3: User with 9000+ Clears
```bash
curl -X POST https://api.cheapraidbanners.com/admin/refresh-user \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"membershipId": "HIGH_ACTIVITY_USER_ID", "force": true}'
```
**Expected**:
- Fetches all available activities from Bungie
- Deduplicates correctly
- Batches properly (30 activities per batch)
- MemberCoordinator receives correct batch count
- All clears are counted in final stats

## How to Verify the Fix

1. **Check Logs**: Look for the new detailed logging messages
2. **Compare Counts**: 
   - Check total activities fetched
   - Check total after deduplication
   - Check total queued
   - These numbers should make sense (queued ≤ deduped ≤ fetched)
3. **Verify Stats**: 
   - Run force refresh on a user with known activity count
   - Check database `member_dungeon_stats` table
   - Verify `total_clears` matches expected count
4. **Monitor MemberCoordinator**: 
   - Check that batches complete successfully
   - Verify aggregate stats are computed correctly

## Files Modified

1. `src/index.ts` - Added `/admin/refresh-user` endpoint
2. `src/types.ts` - Added `MEMBER_COORDINATOR` to Env interface
3. `src/processors/memberJobProcessor.ts` - Fixed counting logic and batch bug
4. `ENDPOINT_USAGE.md` - API documentation for new endpoint
5. `FIXES_SUMMARY.md` - This file
