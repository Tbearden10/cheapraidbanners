# New Member Processing Issues (Current Incorrect Code)

## 1) Queueing new members is fire-and-forget (can be dropped)
**Issue:** The code logs queueing, but does not `await` queue sends. Worker lifecycle can end before all sends complete.

### Incorrect code
```typescript
if (newMembers.length > 0) {
    console.log(`[MemberSync] Queuing ${newMembers.length} new members for processing`);
    
    // Don't await - fire and forget
    Promise.all(
      newMembers.map(async (newMember: { membershipId: any; membershipType: any; }) => {
        try {
          const displayName = formatDisplayName(newMember);
          
          // Queue the member - coordinator will handle the rest
          await env.MEMBER_STATS_QUEUE.send({
            clanId,
            membershipId: newMember.membershipId,
            membershipType: newMember.membershipType,
            displayName,
            lastProcessedDate: null, // Let the job processor fetch this
          });
        } catch (err) {
          console.warn(`[MemberSync] Failed to queue new member ${newMember.membershipId}:`, err);
        }
      })
    ).catch(err => console.warn('[MemberSync] Error queueing new members:', err));
    
    console.log(`[MemberSync] New members queued (processing async)`);
  }
```

---

## 2) “Queued” log can be misleading
**Issue:** This log implies success before all queue sends are guaranteed complete/successful.

### Incorrect code
```typescript
console.log(`[MemberSync] New members queued (processing async)`);
```

---

## 3) Member sync cron comment does not match actual schedule
**Issue:** Comment says every 30 minutes, but scheduled handler runs hourly (`0 * * * *`).

### Incorrect code
```typescript
// ============================================================================
// MEMBER SYNC CRON - Every 30 minutes
// ============================================================================
```
