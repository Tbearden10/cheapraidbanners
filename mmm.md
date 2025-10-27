Project

### Let's design a **robust, self-sufficient architecture** for your clan homepage app. The goal is to make it **accurate, efficient, and maintainable** while keeping the implementation as straightforward as possible.

---

## **1. Goals and Key Requirements**

### **Features**
1. **Member List with Live Activity Timers**:
   - Display all members split into **online** and **offline** groups.
   - Track **live activity timers** for online members or fireteams dynamically.
   - Show detailed **member stats** (kills, activities completed, etc.).

2. **Global Clan Stats**:
   - Aggregate totals from all members (e.g., total kills, raid completions, etc.).
   - Automatically updates when new activities are completed.

3. **Recent Clan Activities**:
   - Show the **3 most recent activities** across all members, ensuring no duplicates.
   - Pre-fetch PGCRs (Post Game Carnage Reports) for detailed activity data.

4. **Clan Live Feed**:
   - Real-time updates for events like:
     - Member joins the clan.
     - Raid/activity starts.
     - Raid/activity clears.
   - Should update dynamically on the frontend.

### **Self-Sufficiency**
- The system should **always stay up-to-date**, even if no users are online for a few hours.
- Data should be **fetched dynamically** when needed but cached intelligently to minimize redundant API calls.

---

## **2. Recommendations for Tools**

### **Cloudflare KV (Key-Value Storage)**
- **Use For**:
  - **Caching short-lived, frequently accessed data**:
    - **Member List**: Cached for 5–10 minutes.
    - **Global Clan Stats**: Cached for 5–10 minutes.
    - **Recent Clan Activities**: Cached for 24 hours.
    - **Live Feed Events**: Cached for 5–10 minutes as a fallback (e.g., in case the Durable Object is temporarily unavailable).
- **Why KV?**:
  - Low-latency, globally distributed reads.
  - TTL (Time-to-Live) cleans up data automatically, reducing storage overhead.

---

### **Durable Objects (DOs)**
- **Use For**:
  - **Live Activity Timers**:
    - Maintain real-time activity state for **online members** or **fireteams**.
    - Dynamically create a Durable Object for each **online member** or **fireteam**.
    - Push updates directly to the frontend via **WebSockets** or **Server-Sent Events (SSE)**.
  - **Clan Live Feed**:
    - Manage live feed events in real-time, pushing updates to connected clients.
- **Why Durable Objects?**:
  - Strong consistency for real-time updates.
  - Stateful operations, perfect for dynamic timers and live feeds.

---

### **Cloudflare Cron Triggers**
- **Use For**:
  - **Periodic Updates**:
    - Member list refresh (every 5–10 minutes).
    - Online/offline status refresh (every 1–2 minutes).
    - Recent clan activities refresh (every 5 minutes).
    - Global clan stats aggregation (every 5–10 minutes).
- **Why Cron Triggers?**:
  - Ensures data freshness even when no users are online.

---

### **Bungie API**
- **Use For**:
  - Fetching **member lists**, **activity history**, **PGCRs**, and **online/offline status**.
- **Why Bungie API?**:
  - It's the source of truth for player and activity data.

---

## **3. Recommended Architecture**

### **A. Member List with Live Timers**
1. **Data Flow**:
   - **Cron Job** (Every 5–10 minutes):
     - Fetch the full member list from Bungie API.
     - Cache the list in KV with TTL.
   - **DOs for Online Members**:
     - Dynamically create or update a Durable Object for each **online member** or **fireteam**.
     - Manage activity timers in real time.
   - **Frontend**:
     - Fetch the member list from KV.
     - Subscribe to live timers via DO (using WebSockets or SSE).

2. **Tools**:
   - **KV**:
     - Cache the full member list and online/offline status for quick access.
   - **DO**:
     - Manage real-time activity timers for online members/fireteams.

---

### **B. Global Clan Stats**
1. **Data Flow**:
   - **Cron Job** (Every 5–10 minutes):
     - Aggregate stats from the cached member list in KV.
     - Cache the aggregated stats in KV with TTL.
   - **Frontend**:
     - Fetch global stats from KV.

2. **Tools**:
   - **KV**:
     - Store aggregated stats for fast reads.
   - **Cron Triggers**:
     - Periodically update stats.

---

### **C. Recent Clan Activities**
1. **Data Flow**:
   - **Cron Job** (Every 5 minutes):
     - Fetch the most recent activity for each member from Bungie API.
     - Deduplicate and sort to find the top 3 recent activities.
     - Pre-fetch PGCRs and cache in KV for 24 hours.
   - **Frontend**:
     - Fetch the top 3 activities from KV.
     - Display detailed PGCR data when a user clicks an activity.

2. **Tools**:
   - **KV**:
     - Cache top 3 activities and PGCRs for 24 hours.
   - **Cron Triggers**:
     - Automate periodic updates.

---

### **D. Clan Live Feed**
1. **Data Flow**:
   - **DO**:
     - Process real-time events like:
       - Member joins the clan.
       - Activity starts/ends.
     - Push updates to connected clients via WebSockets or SSE.
   - **KV**:
     - Backup live feed events for fallback access with a TTL.

2. **Tools**:
   - **DO**:
     - Push live feed updates in real time.
   - **KV**:
     - Serve as a fallback cache for recent events.

---

## **4. Setup Guide**

### **Backend Directory Structure**
```
/backend
├── /durable_objects
│   ├── FireteamTimer.js       # Tracks timers for online members/fireteams.
│   ├── LiveFeed.js            # Handles real-time live feed updates.
├── /routes
│   ├── memberList.js          # Handles member list fetching and caching.
│   ├── clanStats.js           # Aggregates and caches global clan stats.
│   ├── recentActivities.js    # Fetches and caches recent clan activities.
│   ├── liveFeed.js            # Serves live feed events via DO.
├── /utils
│   ├── bungieAPI.js           # Utility for Bungie API requests.
│   ├── kvHelpers.js           # Utility for KV operations.
│   ├── cronJobs.js            # Logic for periodic tasks.
├── index.js                   # Main entry point for Hono app.
├── wrangler.toml              # Cloudflare Worker configuration.
```

---

### **`wrangler.toml` Configuration**
```toml
name = "clan-homepage-app"
type = "javascript"

[[kv_namespaces]]
binding = "MEMBER_LIST_KV"
id = "kv-member-list-id"

[[kv_namespaces]]
binding = "CLAN_STATS_KV"
id = "kv-clan-stats-id"

[[kv_namespaces]]
binding = "RECENT_ACTIVITIES_KV"
id = "kv-recent-activities-id"

[[kv_namespaces]]
binding = "LIVE_FEED_KV"
id = "kv-live-feed-id"

[durable_objects]
bindings = [
  { name = "FireteamTimer", class_name = "FireteamTimer" },
  { name = "LiveFeed", class_name = "LiveFeed" }
]

[triggers]
crons = ["*/5 * * * *", "*/1 * * * *"]
```

---

### **Member List Implementation**

#### **1. Backend API (`routes/memberList.js`)**
```javascript
import { fetchFromBungieAPI } from '../utils/bungieAPI';

export const getMemberList = async (c) => {
  const cachedList = await c.env.MEMBER_LIST_KV.get('member_list', { type: 'json' });

  // If cached data exists, return it
  if (cachedList) return c.json(cachedList);

  // Fetch member list from Bungie API
  const response = await fetchFromBungieAPI(`/GroupV2/{groupId}/Members/`);
  const memberList = await response.json();

  // Cache the member list in KV
  await c.env.MEMBER_LIST_KV.put('member_list', JSON.stringify(memberList), { expirationTtl: 600 });

  return c.json(memberList);
};
```

#### **2. Cron Job (`utils/cronJobs.js`)**
```javascript
import { fetchFromBungieAPI } from './bungieAPI';

export const updateMemberList = async (env) => {
  const response = await fetchFromBungieAPI(`/GroupV2/{groupId}/Members/`);
  const memberList = await response.json();

  // Store the member list in KV
  await env.MEMBER_LIST_KV.put('member_list', JSON.stringify(memberList), { expirationTtl: 600 });
};
```

#### **3. Frontend Display (`MemberList.jsx`)**
```javascript
import { useEffect, useState } from 'react';

const MemberList = () => {
  const [members, setMembers] = useState([]);

  useEffect(() => {
    const fetchMembers = async () => {
      const response = await fetch('/api/member-list');
      const data = await response.json();
      setMembers(data.members);
    };

    fetchMembers();
  }, []);

  return (
    <div>
      <h2>Online Members</h2>
      <ul>
        {members.filter((m) => m.status === 'online').map((m) => (
          <li key={m.id}>{m.name}</li>
        ))}
      </ul>
      <h2>Offline Members</h2>
      <ul>
        {members.filter((m) => m.status === 'offline').map((m) => (
          <li key={m.id}>{m.name}</li>
        ))}
      </ul>
    </div>
  );
};

export default MemberList;
```

---

This plan ensures your app is **efficient, robust, and self-sufficient**. Let me know which part you'd like to implement next!