#!/usr/bin/env python3
"""
Bungie API Activity Processor

This script processes activities from the Bungie API following the JavaScript pipeline logic.
It fetches activities, groups them by dungeon type, deduplicates, sorts, filters, validates,
creates batches, and provides a summary.

Usage:
    python process_bungie_activities.py --membership-id <ID> --membership-type <TYPE> --api-key <KEY>
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
import aiohttp


# ============================================================================
# CONSTANTS - Ported from activityReferenceMap.ts
# ============================================================================

ACTIVITY_REFERENCE_MAP = [
    {
        "hash": "2727361621",
        "displayName": "Equilibrium",
        "referenceIds": ["2727361621", "1754635208"],
    },
    {
        "hash": "3834447244",
        "displayName": "Sundered Doctrine",
        "referenceIds": ["247869137", "3834447244", "3521648250"],
    },
    {
        "hash": "300092127",
        "displayName": "Vesper's Host",
        "referenceIds": ["1915770060", "300092127", "4293676253", "3492566689"],
    },
    {
        "hash": "2004855007",
        "displayName": "Warlord's Ruin",
        "referenceIds": ["2004855007", "2534833093"],
    },
    {
        "hash": "313828469",
        "displayName": "Ghosts of the Deep",
        "referenceIds": ["313828469", "124340010", "4190119662", "1094262727", "2961030534", "2716998124"],
    },
    {
        "hash": "1262462921",
        "displayName": "Spire of the Watcher",
        "referenceIds": ["1262462921", "3339002067", "1225969316", "943878085", "4046934917", "2296818662"],
    },
    {
        "hash": "2823159265",
        "displayName": "Duality",
        "referenceIds": ["2823159265", "3012587626", "1668217731"],
    },
    {
        "hash": "4078656646",
        "displayName": "Grasp of Avarice",
        "referenceIds": ["4078656646", "1112917203", "3774021532"],
    },
    {
        "hash": "1077850348",
        "displayName": "Prophecy",
        "referenceIds": ["715153594", "3637651331", "1077850348", "3193125350", "1788465402", "4148187374"],
    },
    {
        "hash": "2582501063",
        "displayName": "Pit of Heresy",
        "referenceIds": ["2582501063", "1375089621"],
    },
    {
        "hash": "2032534090",
        "displayName": "The Shattered Throne",
        "referenceIds": ["2032534090"],
    },
]

# Activity modes to fetch
ACTIVITY_MODES = [82, 2]  # Dungeon (82), Story (2)
PAGE_SIZE = 250
MAX_BATCH_SIZE = 30


# ============================================================================
# BUNGIE API CLIENT
# ============================================================================

class BungieAPIClient:
    """Client for interacting with the Bungie API"""
    
    BASE_URL = "https://www.bungie.net/Platform"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers={
            'X-API-Key': self.api_key,
            'Accept': 'application/json'
        })
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    async def _fetch_with_retry(self, url: str, retries: int = 2) -> Optional[Dict[str, Any]]:
        """Fetch URL with retry logic for rate limits and server errors"""
        for attempt in range(retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    
                    # Retry on 429 (rate limit) or 5xx (server errors)
                    if response.status == 429:
                        retry_after = response.headers.get('Retry-After', '2')
                        try:
                            wait_seconds = float(retry_after)
                        except (ValueError, TypeError):
                            wait_seconds = 2
                        
                        if attempt < retries:
                            print(f"Rate limited, waiting {wait_seconds}s...")
                            await asyncio.sleep(wait_seconds)
                            continue
                    
                    if 500 <= response.status < 600:
                        if attempt < retries:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
                    
                    # Non-retriable error
                    print(f"HTTP {response.status} for {url}")
                    return None
                    
            except Exception as e:
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                print(f"Error fetching {url}: {e}")
                return None
        
        return None
    
    async def fetch_characters(self, membership_id: str, membership_type: int) -> List[Dict[str, Any]]:
        """Fetch characters for a member"""
        url = f"{self.BASE_URL}/Destiny2/{membership_type}/Account/{membership_id}/Stats/"
        
        data = await self._fetch_with_retry(url)
        
        if not data or data.get('ErrorCode') != 1:
            error_msg = data.get('Message', 'Unknown error') if data else 'No response'
            raise Exception(f"Bungie API error: {error_msg}")
        
        characters = data.get('Response', {}).get('characters', [])
        
        # Handle both array and object formats
        characters_arr = []
        if isinstance(characters, list):
            characters_arr = characters
        elif isinstance(characters, dict):
            characters_arr = list(characters.values())
        
        result = []
        for char in characters_arr:
            if isinstance(char, dict):
                char_id = char.get('characterId')
                if char_id:
                    result.append({
                        'characterId': char_id,
                        'membershipId': membership_id,
                        'membershipType': membership_type,
                        'deleted': bool(char.get('deleted', False))
                    })
        
        return result
    
    async def fetch_activities_for_character(
        self, 
        membership_type: int,
        membership_id: str, 
        character_id: str, 
        page: int, 
        mode: int
    ) -> List[Dict[str, Any]]:
        """Fetch activities for a specific character"""
        url = (f"{self.BASE_URL}/Destiny2/{membership_type}/Account/{membership_id}/"
               f"Character/{character_id}/Stats/Activities/?mode={mode}&count={PAGE_SIZE}&page={page}")
        
        data = await self._fetch_with_retry(url)
        
        if not data:
            return []
        
        return data.get('Response', {}).get('activities', [])


# ============================================================================
# ACTIVITY PROCESSING PIPELINE
# ============================================================================

def is_activity_completed(activity: Dict[str, Any]) -> bool:
    """Check if an activity is completed"""
    return activity.get('values', {}).get('completed', {}).get('basic', {}).get('value') == 1


async def fetch_all_activities(
    client: BungieAPIClient,
    membership_type: int,
    membership_id: str,
    character_ids: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all activities for all characters across multiple modes"""
    
    activities_by_char: Dict[str, List[Dict[str, Any]]] = {char_id: [] for char_id in character_ids}
    
    print(f"\n[Fetch] Starting activity fetch for {len(character_ids)} character(s)")
    for idx, char_id in enumerate(character_ids, 1):
        print(f"  Character {idx}: {char_id}")
    
    async def fetch_all_pages_for_character(char_id: str, mode: int):
        """Fetch all pages of activities for a character and mode"""
        page = 0
        char_total_activities = 0
        
        while True:
            activities = await client.fetch_activities_for_character(
                membership_type, membership_id, char_id, page, mode
            )
            
            if activities:
                activities_by_char[char_id].extend(activities)
                char_total_activities += len(activities)
            
            if not activities or len(activities) < PAGE_SIZE:
                if char_total_activities > 0:
                    mode_name = "Dungeon" if mode == 82 else "Story" if mode == 2 else f"Mode-{mode}"
                    print(f"[Fetch] CharID {char_id} Mode {mode} ({mode_name}): {char_total_activities} activities")
                break
            
            page += 1
            await asyncio.sleep(0.2)  # Small delay between pages
    
    # Fetch activities for all modes
    for mode in ACTIVITY_MODES:
        mode_name = "Dungeon" if mode == 82 else "Story" if mode == 2 else f"Mode-{mode}"
        print(f"[Fetch] Fetching mode {mode} ({mode_name}) for all characters...")
        
        # Fetch all characters concurrently for this mode
        await asyncio.gather(*[
            fetch_all_pages_for_character(char_id, mode)
            for char_id in character_ids
        ])
        
        await asyncio.sleep(0.25)  # Small delay between modes
    
    total_activities = sum(len(acts) for acts in activities_by_char.values())
    print(f"[Fetch] Complete: {total_activities} total activities fetched")
    
    return activities_by_char


def group_activities_by_dungeon(
    activities_by_char: Dict[str, List[Dict[str, Any]]]
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Group activities by dungeon hash, track ungrouped and missing refId activities"""
    
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]] = {}
    ungrouped_activities = []
    missing_ref_id_activities = []
    total_processed = 0
    
    # Initialize dungeon buckets
    for dungeon in ACTIVITY_REFERENCE_MAP:
        activities_by_dungeon[dungeon['hash']] = []
    
    # Process all activities
    for char_id, activities in activities_by_char.items():
        for activity in activities:
            total_processed += 1
            
            # Extract reference ID
            activity_details = activity.get('activityDetails', {})
            ref_id = str(activity_details.get('referenceId', ''))
            
            # Track activities without reference IDs
            if not ref_id:
                missing_ref_id_activities.append({
                    'characterId': char_id,
                    'period': activity.get('period'),
                    'instanceId': activity_details.get('instanceId') or activity.get('instanceId'),
                })
                continue
            
            # Try to match to a dungeon
            grouped = False
            for dungeon in ACTIVITY_REFERENCE_MAP:
                if ref_id in dungeon['referenceIds']:
                    activity_copy = activity.copy()
                    activity_copy['characterId'] = char_id
                    activities_by_dungeon[dungeon['hash']].append(activity_copy)
                    grouped = True
                    break
            
            # Track ungrouped activities
            if not grouped:
                ungrouped_activities.append({
                    'referenceId': ref_id,
                    'characterId': char_id,
                    'period': activity.get('period'),
                })
    
    total_fetched = sum(len(acts) for acts in activities_by_char.values())
    print(f"\n[Grouping] Processed {total_processed} activities (should match {total_fetched})")
    
    if total_processed != total_fetched:
        print(f"[Grouping] ⚠️  MISMATCH: Processed {total_processed} but fetched {total_fetched} "
              f"(diff: {total_fetched - total_processed})")
    
    if missing_ref_id_activities:
        print(f"[Grouping] ⚠️  {len(missing_ref_id_activities)} activities missing referenceId (skipped)")
    
    if ungrouped_activities:
        print(f"[Grouping] ⚠️  {len(ungrouped_activities)} ungrouped activities (not matching any dungeon)")
        
        # Count by refId
        ungrouped_by_ref_id: Dict[str, int] = defaultdict(int)
        for act in ungrouped_activities:
            ungrouped_by_ref_id[act['referenceId']] += 1
        print(f"[Grouping] Ungrouped by RefID: {dict(ungrouped_by_ref_id)}")
    
    return activities_by_dungeon, ungrouped_activities, missing_ref_id_activities


def deduplicate_activities(
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """Deduplicate activities by instanceId, preferring completed ones"""
    
    print("\n[Deduplication] Starting deduplication...")
    
    total_before_dedup = 0
    total_after_dedup = 0
    total_duplicates_removed = 0
    
    for dungeon_hash, activities in activities_by_dungeon.items():
        before_count = len(activities)
        total_before_dedup += before_count
        
        activity_map: Dict[str, Dict[str, Any]] = {}
        activities_without_id = []
        
        for activity in activities:
            activity_details = activity.get('activityDetails', {})
            instance_id = activity_details.get('instanceId') or activity.get('instanceId')
            
            if not instance_id:
                # Track but still include activities without instanceId
                # They cannot be deduplicated but should remain in the output
                activities_without_id.append(activity)
                continue
            
            existing = activity_map.get(instance_id)
            
            if not existing:
                activity_map[instance_id] = activity
            else:
                # Prefer completed activities
                existing_completed = is_activity_completed(existing)
                new_completed = is_activity_completed(activity)
                
                if not existing_completed and new_completed:
                    activity_map[instance_id] = activity
        
        # Include both deduplicated activities and activities without instanceId
        activities_by_dungeon[dungeon_hash] = list(activity_map.values()) + activities_without_id
        
        after_count = len(activities_by_dungeon[dungeon_hash])
        total_after_dedup += after_count
        removed = before_count - after_count
        total_duplicates_removed += removed
        
        if before_count > 0:
            dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d['hash'] == dungeon_hash), None)
            dungeon_name = dungeon['displayName'] if dungeon else dungeon_hash
            
            msg = f"[Deduplication] {dungeon_name}: {after_count} unique activities ({removed} duplicates removed"
            if activities_without_id:
                msg += f", {len(activities_without_id)} missing instanceId"
            msg += ")"
            print(msg)
            
            if activities_without_id:
                print(f"[Deduplication] ⚠️  {dungeon_name} has {len(activities_without_id)} activities "
                      f"without instanceId - these were EXCLUDED from deduplication")
    
    print(f"[Deduplication] Summary: {total_before_dedup} → {total_after_dedup} "
          f"(removed {total_duplicates_removed} duplicates)")
    
    return activities_by_dungeon


def sort_activities_by_date(activities_by_dungeon: Dict[str, List[Dict[str, Any]]]) -> None:
    """Sort activities by period (earliest date first) - in place"""
    
    print("\n[Sorting] Sorting activities by earliest date (period)...")
    
    for dungeon_hash, activities in activities_by_dungeon.items():
        if not activities:
            continue
        
        # Sort by period ascending (oldest first)
        activities.sort(key=lambda a: datetime.fromisoformat(
            a.get('period', '1970-01-01T00:00:00Z').replace('Z', '+00:00')
        ).timestamp() if a.get('period') else 0)
        
        dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d['hash'] == dungeon_hash), None)
        dungeon_name = dungeon['displayName'] if dungeon else dungeon_hash
        
        if activities:
            earliest = activities[0].get('period', 'unknown')
            latest = activities[-1].get('period', 'unknown')
            print(f"[Sorting] {dungeon_name}: {len(activities)} activities sorted "
                  f"(earliest: {earliest}, latest: {latest})")


def filter_completed_activities(
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """Filter to only completed activities"""
    
    print("\n[Filtering] Filtering to completed activities only...")
    
    filtered_by_dungeon: Dict[str, List[Dict[str, Any]]] = {}
    
    for dungeon_hash, activities in activities_by_dungeon.items():
        completed = [a for a in activities if is_activity_completed(a)]
        
        filtered_by_dungeon[dungeon_hash] = completed
        
        if activities:
            dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d['hash'] == dungeon_hash), None)
            dungeon_name = dungeon['displayName'] if dungeon else dungeon_hash
            
            filtered_count = len(activities) - len(completed)
            print(f"[Filtering] {dungeon_name}: {len(completed)} completed "
                  f"(filtered out {filtered_count} incomplete)")
    
    total_completed = sum(len(acts) for acts in filtered_by_dungeon.values())
    print(f"[Filtering] Total completed activities: {total_completed}")
    
    return filtered_by_dungeon


def validate_activity_counts(
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]],
    ungrouped_count: int,
    missing_ref_id_count: int,
    total_fetched: int
) -> None:
    """Validate that activity counts match expected totals"""
    
    print("\n[Validation] Validating activity counts...")
    
    total_after_dedup = sum(len(acts) for acts in activities_by_dungeon.values())
    total_grouped = total_after_dedup + ungrouped_count + missing_ref_id_count
    
    print(f"[Validation] Fetched: {total_fetched}")
    print(f"[Validation] After dedup: {total_after_dedup}")
    print(f"[Validation] Ungrouped: {ungrouped_count}")
    print(f"[Validation] Missing refId: {missing_ref_id_count}")
    print(f"[Validation] Total accounted: {total_grouped}")
    
    if total_grouped != total_fetched:
        print(f"[Validation] ⚠️  Activity accounting mismatch!")
        print(f"[Validation]   Difference: {total_fetched - total_grouped}")
    else:
        print(f"[Validation] ✓ Activity counts validated successfully")


def create_batches(
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[List[Dict[str, Any]]]]:
    """Create batches of activities for processing"""
    
    print(f"\n[Batching] Creating batches (max size: {MAX_BATCH_SIZE})...")
    
    batches_by_dungeon: Dict[str, List[List[Dict[str, Any]]]] = {}
    total_batches = 0
    
    for dungeon_hash, activities in activities_by_dungeon.items():
        if not activities:
            batches_by_dungeon[dungeon_hash] = []
            continue
        
        # Create batches
        batches = []
        for i in range(0, len(activities), MAX_BATCH_SIZE):
            batch = activities[i:i + MAX_BATCH_SIZE]
            batches.append(batch)
        
        batches_by_dungeon[dungeon_hash] = batches
        total_batches += len(batches)
        
        dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d['hash'] == dungeon_hash), None)
        dungeon_name = dungeon['displayName'] if dungeon else dungeon_hash
        
        print(f"[Batching] {dungeon_name}: {len(batches)} batch(es) for {len(activities)} activities")
    
    print(f"[Batching] Total batches created: {total_batches}")
    
    return batches_by_dungeon


def queue_batches(
    batches_by_dungeon: Dict[str, List[List[Dict[str, Any]]]],
    membership_id: str,
    membership_type: int
) -> Tuple[int, int]:
    """Simulate queueing batches for processing (no actual queue in this script)"""
    
    print(f"\n[Queueing] Simulating batch queueing...")
    
    total_queued = 0
    batches_queued = 0
    
    for dungeon_hash, batches in batches_by_dungeon.items():
        if not batches:
            continue
        
        dungeon = next((d for d in ACTIVITY_REFERENCE_MAP if d['hash'] == dungeon_hash), None)
        dungeon_name = dungeon['displayName'] if dungeon else dungeon_hash
        
        for batch_index, batch in enumerate(batches):
            # Prepare activities payload (extract key fields)
            activities_payload = [
                {
                    'instanceId': a.get('activityDetails', {}).get('instanceId') or a.get('instanceId'),
                    'date': a.get('period'),
                    'characterId': a.get('characterId'),
                }
                for a in batch
            ]
            
            # Create job ID
            job_id = f"{membership_id}-{dungeon_hash}-{batch_index}"
            
            # In a real implementation, this would send to a queue
            # Here we just simulate it
            total_queued += len(activities_payload)
            batches_queued += 1
        
        print(f"[Queueing] Queued {len(batches)} batch(es) for {dungeon_name} "
              f"({sum(len(b) for b in batches)} activities)")
    
    return total_queued, batches_queued


def print_final_summary(
    membership_id: str,
    membership_type: int,
    activities_by_dungeon: Dict[str, List[Dict[str, Any]]],
    batches_by_dungeon: Dict[str, List[List[Dict[str, Any]]]],
    total_queued: int,
    batches_queued: int,
    duration_seconds: float
):
    """Print final summary of the pipeline processing"""
    
    print("\n" + "="*80)
    print("FINAL PIPELINE SUMMARY")
    print("="*80)
    
    print(f"\nMembership ID: {membership_id}")
    print(f"Membership Type: {membership_type}")
    print(f"Processing Duration: {duration_seconds:.1f}s")
    
    print("\n--- Activities Processed by Dungeon ---")
    for dungeon in ACTIVITY_REFERENCE_MAP:
        dungeon_hash = dungeon['hash']
        activities = activities_by_dungeon.get(dungeon_hash, [])
        batches = batches_by_dungeon.get(dungeon_hash, [])
        
        if activities:
            print(f"  {dungeon['displayName']:30s}: {len(activities):4d} activities, {len(batches):3d} batch(es)")
    
    total_activities = sum(len(acts) for acts in activities_by_dungeon.values())
    print(f"\n  {'TOTAL':30s}: {total_activities:4d} activities, {batches_queued:3d} batch(es)")
    
    print("\n--- Batching Information ---")
    print(f"  Max Batch Size: {MAX_BATCH_SIZE}")
    print(f"  Total Batches: {batches_queued}")
    print(f"  Activities per Batch (avg): {total_queued / batches_queued:.1f}" if batches_queued > 0 else "  N/A")
    
    print("\n--- Queueing Outcomes ---")
    print(f"  Total Activities Queued: {total_queued}")
    print(f"  Total Batches Queued: {batches_queued}")
    print(f"  Status: Simulated (no actual queue used)")
    
    print("\n" + "="*80)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

async def main():
    """Main entry point for the script"""
    
    parser = argparse.ArgumentParser(
        description='Process Bungie API activities following the JavaScript pipeline logic',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python process_bungie_activities.py --membership-id 4611686018467765462 --membership-type 3 --api-key YOUR_API_KEY
        """
    )
    
    parser.add_argument(
        '--membership-id',
        required=True,
        help='Bungie membership ID to process'
    )
    
    parser.add_argument(
        '--membership-type',
        type=int,
        required=True,
        help='Bungie membership type (1=Xbox, 2=PSN, 3=Steam, 4=Blizzard, 5=Stadia, 6=Epic, 10=Demon)'
    )
    
    parser.add_argument(
        '--api-key',
        required=True,
        help='Bungie API key for authentication'
    )
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    
    print("="*80)
    print("BUNGIE API ACTIVITY PROCESSOR")
    print("="*80)
    print(f"\nStarting at: {start_time.isoformat()}")
    print(f"Membership ID: {args.membership_id}")
    print(f"Membership Type: {args.membership_type}")
    print()
    
    try:
        # Initialize Bungie API client
        async with BungieAPIClient(args.api_key) as client:
            
            # Step 1: Fetch characters
            print("[Pipeline] Step 1: Fetching characters...")
            characters = await client.fetch_characters(args.membership_id, args.membership_type)
            
            if not characters:
                print("No characters found for this membership ID")
                return 1
            
            print(f"[Pipeline] Found {len(characters)} character(s)")
            for idx, char in enumerate(characters, 1):
                deleted_str = " (deleted)" if char.get('deleted') else ""
                print(f"  Character {idx}: {char['characterId']}{deleted_str}")
            
            # Step 2: Fetch all activities
            print("\n[Pipeline] Step 2: Fetching all activities...")
            character_ids = [char['characterId'] for char in characters]
            activities_by_char = await fetch_all_activities(
                client, args.membership_type, args.membership_id, character_ids
            )
            
            total_fetched = sum(len(acts) for acts in activities_by_char.values())
            print(f"[Pipeline] Total activities fetched: {total_fetched}")
            
            # Step 3: Group by dungeon type
            print("\n[Pipeline] Step 3: Grouping activities by dungeon type...")
            activities_by_dungeon, ungrouped, missing_ref_id = group_activities_by_dungeon(activities_by_char)
            
            # Step 4: Deduplicate activities
            print("\n[Pipeline] Step 4: Deduplicating activities...")
            activities_by_dungeon = deduplicate_activities(activities_by_dungeon)
            
            # Step 5: Sort by earliest date
            print("\n[Pipeline] Step 5: Sorting by earliest date...")
            sort_activities_by_date(activities_by_dungeon)
            
            # Step 6: Filter to completed activities
            print("\n[Pipeline] Step 6: Filtering to completed activities...")
            activities_by_dungeon = filter_completed_activities(activities_by_dungeon)
            
            # Step 7: Validate counts
            print("\n[Pipeline] Step 7: Validating activity counts...")
            validate_activity_counts(
                activities_by_dungeon, len(ungrouped), len(missing_ref_id), total_fetched
            )
            
            # Step 8: Create batches
            print("\n[Pipeline] Step 8: Creating batches...")
            batches_by_dungeon = create_batches(activities_by_dungeon)
            
            # Step 9: Queue batches
            print("\n[Pipeline] Step 9: Queueing batches...")
            total_queued, batches_queued = queue_batches(
                batches_by_dungeon, args.membership_id, args.membership_type
            )
            
            # Step 10: Print final summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print_final_summary(
                args.membership_id,
                args.membership_type,
                activities_by_dungeon,
                batches_by_dungeon,
                total_queued,
                batches_queued,
                duration
            )
            
            print(f"\nCompleted at: {end_time.isoformat()}")
            print(f"Total duration: {duration:.1f}s")
            
            return 0
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
