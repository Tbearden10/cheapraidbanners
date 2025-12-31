#!/usr/bin/env python3
"""
Single User Debug Test Script

This script mimics the /debug/user-completions endpoint logic for local testing
without making actual Bungie API calls. It uses mock data to simulate the entire
processing pipeline including:
- Character fetching
- Activity fetching with pagination
- Activity grouping by dungeon hash
- Deduplication logic (by instanceId, prefer completed)
- Live output with counts and statistics

Usage:
    python scripts/test_single_user_debug.py [--mock-file path/to/mock.json]

Mock Data Format:
    {
        "membershipId": "12345",
        "membershipType": 3,
        "clanId": "67890",
        "characters": [
            {
                "characterId": "char1",
                "deleted": false
            }
        ],
        "activities": {
            "char1": {
                "82": [  // mode 82 = Dungeon
                    {
                        "activityDetails": {
                            "referenceId": "2727361621",
                            "instanceId": "inst1"
                        },
                        "period": "2024-01-01T12:00:00Z",
                        "values": {
                            "completed": {
                                "basic": {
                                    "value": 1
                                }
                            }
                        }
                    }
                ],
                "2": []  // mode 2 = Story
            }
        }
    }
"""

import json
import sys
from datetime import datetime
from typing import Dict, List, Any, Set
from collections import defaultdict

# Activity Reference Map - mirrors the TypeScript version
ACTIVITY_REFERENCE_MAP = [
    {
        "hash": "2727361621",  # Equilibrium
        "displayName": "Equilibrium",
        "referenceIds": ["2727361621", "1754635208"]
    },
    {
        "hash": "3834447244",  # Sundered Doctrine - Normal
        "displayName": "Sundered Doctrine",
        "referenceIds": ["247869137", "3834447244", "3521648250"]
    },
    {
        "hash": "300092127",  # Vesper's Host - Normal
        "displayName": "Vesper's Host",
        "referenceIds": ["1915770060", "300092127", "4293676253", "3492566689"]
    },
    {
        "hash": "2004855007",  # Warlord's Ruin - Standard
        "displayName": "Warlord's Ruin",
        "referenceIds": ["2004855007", "2534833093"]
    },
    {
        "hash": "313828469",  # Ghosts of the Deep - Standard
        "displayName": "Ghosts of the Deep",
        "referenceIds": ["313828469", "1668217731"]
    },
    {
        "hash": "4078656646",  # Spire of the Watcher - Standard
        "displayName": "Spire of the Watcher",
        "referenceIds": ["4078656646", "1262462921"]
    },
    {
        "hash": "2136458560",  # Duality - Standard
        "displayName": "Duality",
        "referenceIds": ["2136458560", "1668889302"]
    },
    {
        "hash": "1878615566",  # Grasp of Avarice - Standard
        "displayName": "Grasp of Avarice",
        "referenceIds": ["1878615566", "4202569708"]
    },
    {
        "hash": "2032534090",  # Prophecy - Standard
        "displayName": "Prophecy",
        "referenceIds": ["2032534090", "1077850348"]
    },
    {
        "hash": "3456710851",  # Pit of Heresy - Standard
        "displayName": "Pit of Heresy",
        "referenceIds": ["3456710851", "1375089621"]
    },
    {
        "hash": "2588648629",  # Shattered Throne - Standard
        "displayName": "Shattered Throne",
        "referenceIds": ["2588648629", "134004582"]
    },
]

# Default mock data
DEFAULT_MOCK_DATA = {
    "membershipId": "12345678901234567",
    "membershipType": 3,
    "clanId": "9876543210",
    "characters": [
        {
            "characterId": "2305843009504575349",
            "deleted": False
        },
        {
            "characterId": "2305843009504575350",
            "deleted": False
        }
    ],
    "activities": {
        "2305843009504575349": {
            "82": [  # Dungeon mode
                # Equilibrium - 3 completions
                {
                    "activityDetails": {
                        "referenceId": "2727361621",
                        "instanceId": "12345678901"
                    },
                    "period": "2024-01-15T14:30:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                {
                    "activityDetails": {
                        "referenceId": "2727361621",
                        "instanceId": "12345678902"
                    },
                    "period": "2024-01-16T15:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                # Duplicate - same instanceId
                {
                    "activityDetails": {
                        "referenceId": "2727361621",
                        "instanceId": "12345678902"
                    },
                    "period": "2024-01-16T15:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                {
                    "activityDetails": {
                        "referenceId": "2727361621",
                        "instanceId": "12345678903"
                    },
                    "period": "2024-01-17T16:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                # Vesper's Host - 2 completions
                {
                    "activityDetails": {
                        "referenceId": "300092127",
                        "instanceId": "22345678901"
                    },
                    "period": "2024-02-10T10:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                {
                    "activityDetails": {
                        "referenceId": "300092127",
                        "instanceId": "22345678902"
                    },
                    "period": "2024-02-11T11:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                # Incomplete run
                {
                    "activityDetails": {
                        "referenceId": "300092127",
                        "instanceId": "22345678903"
                    },
                    "period": "2024-02-12T12:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 0}}
                    }
                },
                # Activity without instanceId
                {
                    "activityDetails": {
                        "referenceId": "2004855007"
                    },
                    "period": "2024-03-01T09:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                # Activity without referenceId
                {
                    "activityDetails": {},
                    "period": "2024-03-02T10:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
                # Unknown dungeon (not in reference map)
                {
                    "activityDetails": {
                        "referenceId": "9999999999",
                        "instanceId": "99999999999"
                    },
                    "period": "2024-03-03T11:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
            ],
            "2": []  # Story mode - empty
        },
        "2305843009504575350": {
            "82": [
                # Grasp of Avarice - 1 completion
                {
                    "activityDetails": {
                        "referenceId": "1878615566",
                        "instanceId": "32345678901"
                    },
                    "period": "2024-04-01T12:00:00Z",
                    "values": {
                        "completed": {"basic": {"value": 1}}
                    }
                },
            ],
            "2": []
        }
    }
}


class SingleUserDebugTester:
    def __init__(self, mock_data: Dict[str, Any]):
        self.mock_data = mock_data
        self.network_log: List[Dict[str, Any]] = []
        self.rate_limit_retries: List[Dict[str, Any]] = []
        self.total_pages_fetched = 0
        self.total_rate_limit_retries = 0
        
    def log(self, message: str):
        """Print with timestamp like the debug endpoint"""
        print(message, flush=True)
    
    def fetch_characters(self) -> List[Dict[str, Any]]:
        """Mock character fetching"""
        membership_id = self.mock_data.get("membershipId")
        membership_type = self.mock_data.get("membershipType")
        
        self.log(f"[Debug:Step 1] Fetching characters for membershipId={membership_id} membershipType={membership_type}")
        
        self.network_log.append({
            "type": "fetchCharacters",
            "url": f"Destiny2/{membership_type}/Account/{membership_id}/Stats/",
            "timestamp": datetime.now().isoformat()
        })
        
        characters = self.mock_data.get("characters", [])
        
        if not characters:
            self.log(f"[Debug:Step 1] No characters found for user {membership_id}")
            return []
        
        self.log(f"[Debug:Step 1] SUCCESS - Found {len(characters)} character(s)")
        for idx, char in enumerate(characters):
            self.log(f"  Character {idx + 1}: ID={char['characterId']}, Deleted={char.get('deleted', False)}")
        
        return characters
    
    def fetch_activities_for_all_characters(self, characters: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """Mock activity fetching with pagination simulation"""
        membership_id = self.mock_data.get("membershipId")
        membership_type = self.mock_data.get("membershipType")
        
        activities_by_char: Dict[str, List[Any]] = {}
        for char in characters:
            activities_by_char[char["characterId"]] = []
        
        self.log("\n[Debug:Step 2] Fetching activities for all characters")
        self.log("[Debug:Step 2] Will fetch modes: [82=Dungeon, 2=Story], PageSize: 250")
        
        modes = [82, 2]  # Dungeon, Story
        
        for mode in modes:
            mode_name = "Dungeon" if mode == 82 else "Story"
            self.log(f"\n[Debug:Step 2.{'1' if mode == 82 else '2'}] Starting fetch for mode {mode} ({mode_name})")
            
            for char in characters:
                char_id = char["characterId"]
                page = 0
                page_size = 250
                char_total_activities = 0
                
                self.log(f"  [CharID: {char_id}] Starting pagination...")
                
                # Get activities for this character and mode
                activities = self.mock_data.get("activities", {}).get(char_id, {}).get(str(mode), [])
                
                # Simulate pagination (even if we have all data, show it like pagination)
                total_activities = len(activities)
                
                while True:
                    self.total_pages_fetched += 1
                    activity_url = f"Destiny2/{membership_type}/Account/{membership_id}/Character/{char_id}/Stats/Activities/?mode={mode}&count={page_size}&page={page}"
                    
                    self.log(f"    Page {page}: Fetching (Request #{self.total_pages_fetched})...")
                    
                    log_entry = {
                        "type": "fetchActivities",
                        "requestNumber": self.total_pages_fetched,
                        "url": activity_url,
                        "characterId": char_id,
                        "mode": mode,
                        "modeName": mode_name,
                        "page": page,
                        "pageSize": page_size,
                        "timestamp": datetime.now().isoformat()
                    }
                    self.network_log.append(log_entry)
                    
                    # Simulate page of activities
                    start_idx = page * page_size
                    end_idx = start_idx + page_size
                    page_activities = activities[start_idx:end_idx]
                    
                    activities_count = len(page_activities)
                    char_total_activities += activities_count
                    self.log(f"    Page {page}: Retrieved {activities_count} activities")
                    log_entry["activitiesRetrieved"] = activities_count
                    
                    if page_activities:
                        activities_by_char[char_id].extend(page_activities)
                    
                    if not page_activities or activities_count < page_size:
                        self.log(f"  [CharID: {char_id}] Pagination complete - Total: {char_total_activities} activities for mode {mode}")
                        break
                    
                    page += 1
        
        total_activities_fetched = sum(len(acts) for acts in activities_by_char.values())
        
        self.log(f"\n[Debug:Step 2] COMPLETE - Fetched {total_activities_fetched} total activities across all characters and modes")
        self.log(f"[Debug:Step 2] Total pages fetched: {self.total_pages_fetched}")
        if self.total_rate_limit_retries > 0:
            self.log(f"[Debug:Step 2] ⚠️  Rate limit retries: {self.total_rate_limit_retries}")
        
        return activities_by_char
    
    def group_activities_by_dungeon(self, activities_by_char: Dict[str, List[Any]]) -> tuple:
        """Group activities by dungeon hash"""
        self.log("\n[Debug:Step 3] Per-character activity counts:")
        for char_id, activities in activities_by_char.items():
            self.log(f"  CharID {char_id}: {len(activities)} activities")
        
        self.log("\n[Debug:Step 4] Grouping activities by dungeon hash...")
        
        activities_by_dungeon: Dict[str, List[Any]] = {}
        ungrouped_activities: List[Dict[str, Any]] = []
        missing_ref_id_activities: List[Dict[str, Any]] = []
        total_processed = 0
        
        # Initialize dungeon categories
        for dungeon in ACTIVITY_REFERENCE_MAP:
            activities_by_dungeon[dungeon["hash"]] = []
        
        # Create set of known reference IDs for quick lookup
        known_reference_ids: Set[str] = set()
        for dungeon in ACTIVITY_REFERENCE_MAP:
            for ref_id in dungeon["referenceIds"]:
                known_reference_ids.add(ref_id)
        
        # Process all activities
        for char_id, activities in activities_by_char.items():
            for activity in activities:
                total_processed += 1
                ref_id = str(activity.get("activityDetails", {}).get("referenceId", ""))
                
                # Track activities without reference IDs
                if not ref_id:
                    missing_ref_id_activities.append({
                        "characterId": char_id,
                        "period": activity.get("period"),
                        "instanceId": activity.get("activityDetails", {}).get("instanceId") or activity.get("instanceId")
                    })
                    continue
                
                # Try to group by dungeon
                grouped = False
                for dungeon in ACTIVITY_REFERENCE_MAP:
                    if ref_id in dungeon["referenceIds"]:
                        activities_by_dungeon[dungeon["hash"]].append({
                            **activity,
                            "characterId": char_id
                        })
                        grouped = True
                        break
                
                # Track ungrouped activities
                if not grouped:
                    ungrouped_activities.append({
                        "referenceId": ref_id,
                        "characterId": char_id,
                        "period": activity.get("period"),
                        "instanceId": activity.get("activityDetails", {}).get("instanceId") or activity.get("instanceId")
                    })
        
        total_activities_fetched = sum(len(acts) for acts in activities_by_char.values())
        
        self.log(f"[Debug:Step 4] Processed {total_processed} activities (should match {total_activities_fetched})")
        if total_processed != total_activities_fetched:
            self.log(f"[Debug:Step 4] ⚠️  MISMATCH: Processed {total_processed} but fetched {total_activities_fetched} (diff: {total_activities_fetched - total_processed})")
        
        self.log(f"[Debug:Step 4] Grouped activities into {len(ACTIVITY_REFERENCE_MAP)} dungeon categories")
        self.log(f"[Debug:Step 4] Missing referenceId: {len(missing_ref_id_activities)} activities")
        self.log(f"[Debug:Step 4] Ungrouped (not matching any dungeon): {len(ungrouped_activities)} activities")
        
        return activities_by_dungeon, ungrouped_activities, missing_ref_id_activities
    
    def deduplicate_activities(self, activities_by_dungeon: Dict[str, List[Any]]) -> Dict[str, Dict[str, int]]:
        """Deduplicate activities per dungeon by instanceId (prefer completed)"""
        self.log("\n[Debug:Step 5] Deduplicating activities per dungeon...")
        
        deduplication_stats: Dict[str, Dict[str, int]] = {}
        total_before_dedup = 0
        total_after_dedup = 0
        total_missing_instance_ids = 0
        
        for dungeon_hash, activities in activities_by_dungeon.items():
            before_count = len(activities)
            total_before_dedup += before_count
            
            # Deduplicate by instanceId
            instance_map: Dict[str, Any] = {}
            activities_without_instance_id: List[Dict[str, Any]] = []
            
            for act in activities:
                instance_id = act.get("activityDetails", {}).get("instanceId") or act.get("instanceId")
                
                if not instance_id:
                    activities_without_instance_id.append({
                        "characterId": act.get("characterId"),
                        "period": act.get("period"),
                        "referenceId": act.get("activityDetails", {}).get("referenceId")
                    })
                    continue
                
                existing = instance_map.get(instance_id)
                if not existing:
                    instance_map[instance_id] = act
                else:
                    # Prefer completed activities
                    existing_completed = existing.get("values", {}).get("completed", {}).get("basic", {}).get("value") == 1
                    new_completed = act.get("values", {}).get("completed", {}).get("basic", {}).get("value") == 1
                    if not existing_completed and new_completed:
                        instance_map[instance_id] = act
            
            # Update activities list with deduplicated activities
            activities_by_dungeon[dungeon_hash] = list(instance_map.values())
            after_count = len(activities_by_dungeon[dungeon_hash])
            total_after_dedup += after_count
            total_missing_instance_ids += len(activities_without_instance_id)
            
            deduplication_stats[dungeon_hash] = {
                "before": before_count,
                "after": after_count,
                "removed": before_count - after_count - len(activities_without_instance_id),
                "missingInstanceId": len(activities_without_instance_id)
            }
            
            if activities_without_instance_id:
                dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d["hash"] == dungeon_hash), None)
                dungeon_name = dungeon["displayName"] if dungeon else dungeon_hash
                self.log(f"[Debug:Step 5] ⚠️  {dungeon_name}: {len(activities_without_instance_id)} activities missing instanceId (EXCLUDED from results)")
        
        self.log(f"[Debug:Step 5] Deduplication summary: {total_before_dedup} → {total_after_dedup} (removed {total_before_dedup - total_after_dedup - total_missing_instance_ids} duplicates, {total_missing_instance_ids} missing instanceId)")
        
        return deduplication_stats
    
    def print_activity_flow_check(self, total_activities_fetched: int, total_in_dungeons: int, 
                                   ungrouped_count: int, missing_ref_id_count: int, 
                                   total_missing_instance_ids: int):
        """Print activity flow sanity check"""
        total_accounted = total_in_dungeons + ungrouped_count + missing_ref_id_count + total_missing_instance_ids
        
        self.log("[Debug:Step 5] Activity flow check:")
        self.log(f"  Fetched: {total_activities_fetched}")
        self.log(f"  In dungeons (after dedup): {total_in_dungeons}")
        self.log(f"  Ungrouped (has refId, no match): {ungrouped_count}")
        self.log(f"  Missing refId: {missing_ref_id_count}")
        self.log(f"  Missing instanceId (excluded): {total_missing_instance_ids}")
        self.log(f"  Total accounted: {total_accounted}")
        
        if total_accounted != total_activities_fetched:
            self.log(f"[Debug:Step 5] ⚠️  ACCOUNTING MISMATCH: Difference of {total_activities_fetched - total_accounted} activities!")
    
    def build_dungeon_statistics(self, activities_by_dungeon: Dict[str, List[Any]], 
                                  deduplication_stats: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
        """Build per-dungeon statistics"""
        self.log("\n[Debug:Step 6] Building per-dungeon statistics...")
        
        dungeon_results = []
        
        for dungeon in ACTIVITY_REFERENCE_MAP:
            dungeon_hash = dungeon["hash"]
            activities = activities_by_dungeon.get(dungeon_hash, [])
            
            # Count by reference ID
            counts_by_ref_id: Dict[str, int] = {}
            for ref_id in dungeon["referenceIds"]:
                counts_by_ref_id[ref_id] = 0
            
            for act in activities:
                ref_id = str(act.get("activityDetails", {}).get("referenceId", ""))
                if ref_id in counts_by_ref_id:
                    counts_by_ref_id[ref_id] += 1
            
            # Filter to completed only
            completed = [a for a in activities if a.get("values", {}).get("completed", {}).get("basic", {}).get("value") == 1]
            incomplete = [a for a in activities if a.get("values", {}).get("completed", {}).get("basic", {}).get("value") != 1]
            
            # Sort by period
            completed.sort(key=lambda a: datetime.fromisoformat(a.get("period", "1970-01-01T00:00:00Z").replace("Z", "+00:00")))
            
            dedupe_info = deduplication_stats.get(dungeon_hash, {"before": 0, "after": 0, "removed": 0, "missingInstanceId": 0})
            
            self.log(f"  {dungeon['displayName']} ({dungeon_hash}):")
            self.log(f"    Raw activities: {dedupe_info['before']}")
            self.log(f"    After dedup: {dedupe_info['after']} (removed {dedupe_info['removed']} duplicates)")
            self.log(f"    Completed: {len(completed)}, Incomplete: {len(incomplete)}")
            self.log(f"    Counts by Reference ID:")
            for ref_id, count in counts_by_ref_id.items():
                if count > 0:
                    self.log(f"      RefID {ref_id}: {count} activities")
            
            dungeon_results.append({
                "dungeonName": dungeon["displayName"],
                "dungeonHash": dungeon_hash,
                "referenceIds": dungeon["referenceIds"],
                "deduplication": dedupe_info,
                "countsByReferenceId": counts_by_ref_id,
                "bungie": {
                    "totalActivities": len(activities),
                    "completedActivities": len(completed),
                    "incompleteActivities": len(incomplete),
                    "oldestCompletion": completed[0].get("period") if completed else None,
                    "newestCompletion": completed[-1].get("period") if completed else None,
                },
                "recentCompletions": [
                    {
                        "instanceId": a.get("activityDetails", {}).get("instanceId") or a.get("instanceId"),
                        "period": a.get("period"),
                        "characterId": a.get("characterId"),
                        "referenceId": a.get("activityDetails", {}).get("referenceId"),
                    }
                    for a in completed[-5:]
                ]
            })
        
        return dungeon_results
    
    def print_ungrouped_activities(self, ungrouped_activities: List[Dict[str, Any]]):
        """Print ungrouped activities summary"""
        if ungrouped_activities:
            self.log(f"\n[Debug:Step 7] Ungrouped Activities ({len(ungrouped_activities)} total):")
            
            # Group by reference ID
            ungrouped_by_ref_id: Dict[str, int] = defaultdict(int)
            for act in ungrouped_activities:
                ref_id = act["referenceId"]
                ungrouped_by_ref_id[ref_id] += 1
            
            for ref_id, count in ungrouped_by_ref_id.items():
                self.log(f"  RefID {ref_id}: {count} activities")
            
            # Show samples
            self.log("  Sample ungrouped activities (first 5):")
            for idx, act in enumerate(ungrouped_activities[:5], 1):
                self.log(f"    {idx}. RefID: {act['referenceId']}, CharID: {act['characterId']}, Period: {act['period']}")
        else:
            self.log("\n[Debug:Step 7] No ungrouped activities found - all activities matched known dungeons")
    
    def print_final_summary(self, total_activities_fetched: int, dungeon_results: List[Dict[str, Any]], 
                            total_processed: int, missing_ref_id_count: int, 
                            total_missing_instance_ids: int, ungrouped_count: int,
                            total_before_dedup: int, total_after_dedup: int):
        """Print final summary"""
        total_bungie_completions = sum(d["bungie"]["completedActivities"] for d in dungeon_results)
        
        self.log("\n========================================")
        self.log("[Debug] SUMMARY")
        self.log("========================================")
        self.log(f"Total activities fetched from Bungie: {total_activities_fetched}")
        self.log(f"Total pages fetched: {self.total_pages_fetched}")
        self.log(f"Total API requests: {len(self.network_log)}")
        self.log(f"Rate limit retries: {self.total_rate_limit_retries}")
        self.log("---")
        self.log(f"Activities processed: {total_processed}")
        self.log(f"Missing referenceId: {missing_ref_id_count}")
        self.log(f"Missing instanceId (excluded from dedup): {total_missing_instance_ids}")
        self.log(f"Ungrouped activities (has refId, no match): {ungrouped_count}")
        self.log("---")
        self.log(f"Before deduplication: {total_before_dedup}")
        self.log(f"After deduplication: {total_after_dedup}")
        self.log(f"Duplicates removed: {total_before_dedup - total_after_dedup - total_missing_instance_ids}")
        self.log("---")
        self.log(f"Total Bungie completions (all dungeons): {total_bungie_completions}")
        self.log("========================================\n")
    
    def run(self):
        """Run the complete debug test"""
        membership_id = self.mock_data.get("membershipId")
        membership_type = self.mock_data.get("membershipType")
        clan_id = self.mock_data.get("clanId")
        
        self.log("\n========================================")
        self.log(f"[Debug] START - Fetching completions for user {membership_id}")
        self.log(f"[Debug] MembershipType: {membership_type}, ClanId: {clan_id}")
        self.log("========================================\n")
        
        # Step 1: Fetch characters
        characters = self.fetch_characters()
        if not characters:
            self.log("\n[Debug] ERROR: No characters found")
            return
        
        # Step 2: Fetch activities
        activities_by_char = self.fetch_activities_for_all_characters(characters)
        total_activities_fetched = sum(len(acts) for acts in activities_by_char.values())
        
        # Step 3-4: Group by dungeon
        activities_by_dungeon, ungrouped_activities, missing_ref_id_activities = self.group_activities_by_dungeon(activities_by_char)
        
        # Step 5: Deduplicate
        deduplication_stats = self.deduplicate_activities(activities_by_dungeon)
        
        total_after_dedup = sum(len(acts) for acts in activities_by_dungeon.values())
        total_before_dedup = sum(stats["before"] for stats in deduplication_stats.values())
        total_missing_instance_ids = sum(stats["missingInstanceId"] for stats in deduplication_stats.values())
        
        # Activity flow check
        self.print_activity_flow_check(
            total_activities_fetched, 
            total_after_dedup,
            len(ungrouped_activities),
            len(missing_ref_id_activities),
            total_missing_instance_ids
        )
        
        # Step 6: Build statistics
        dungeon_results = self.build_dungeon_statistics(activities_by_dungeon, deduplication_stats)
        
        # Step 7: Print ungrouped
        self.print_ungrouped_activities(ungrouped_activities)
        
        # Final summary
        self.print_final_summary(
            total_activities_fetched,
            dungeon_results,
            total_activities_fetched,  # total_processed
            len(missing_ref_id_activities),
            total_missing_instance_ids,
            len(ungrouped_activities),
            total_before_dedup,
            total_after_dedup
        )
        
        # Return results as JSON
        return {
            "membershipId": membership_id,
            "membershipType": membership_type,
            "clanId": clan_id,
            "characters": [
                {
                    "characterId": c["characterId"],
                    "deleted": c.get("deleted", False),
                    "activitiesFetched": len(activities_by_char.get(c["characterId"], []))
                }
                for c in characters
            ],
            "totalActivitiesFetched": total_activities_fetched,
            "networkSummary": {
                "totalPagesFetched": self.total_pages_fetched,
                "totalRequests": len(self.network_log),
                "totalRateLimitRetries": self.total_rate_limit_retries,
            },
            "dungeons": dungeon_results,
            "summary": {
                "totalBungieCompletions": sum(d["bungie"]["completedActivities"] for d in dungeon_results),
            }
        }


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test single user debug logic with mock data")
    parser.add_argument("--mock-file", type=str, help="Path to JSON file with mock data")
    parser.add_argument("--output", type=str, help="Path to output JSON file")
    
    args = parser.parse_args()
    
    # Load mock data
    if args.mock_file:
        try:
            with open(args.mock_file, 'r') as f:
                mock_data = json.load(f)
            print(f"Loaded mock data from {args.mock_file}")
        except Exception as e:
            print(f"Error loading mock file: {e}")
            sys.exit(1)
    else:
        print("Using default mock data")
        mock_data = DEFAULT_MOCK_DATA
    
    # Run the test
    tester = SingleUserDebugTester(mock_data)
    result = tester.run()
    
    # Output results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
