# Test Scripts

## test_single_user_debug.py

A Python script that mimics the `/debug/user-completions` endpoint for local testing without making actual Bungie API calls.

### Purpose

This script simulates the entire single-user debug processing pipeline including:
- Character fetching
- Activity fetching with pagination
- Activity grouping by dungeon hash
- Deduplication logic (by instanceId, prefer completed)
- Live console output with counts and statistics
- Same logic as the TypeScript debug endpoint

### Usage

**With default mock data:**
```bash
python scripts/test_single_user_debug.py
```

**With custom mock data:**
```bash
python scripts/test_single_user_debug.py --mock-file path/to/mock.json
```

**Save output to JSON file:**
```bash
python scripts/test_single_user_debug.py --output results.json
```

**Combined:**
```bash
python scripts/test_single_user_debug.py --mock-file scripts/sample_mock_data.json --output /tmp/results.json
```

### Mock Data Format

Create a JSON file with the following structure:

```json
{
  "membershipId": "12345678901234567",
  "membershipType": 3,
  "clanId": "9876543210",
  "characters": [
    {
      "characterId": "2305843009504575349",
      "deleted": false
    }
  ],
  "activities": {
    "2305843009504575349": {
      "82": [
        {
          "activityDetails": {
            "referenceId": "2727361621",
            "instanceId": "12345678901"
          },
          "period": "2024-01-15T14:30:00Z",
          "values": {
            "completed": {
              "basic": {
                "value": 1
              }
            }
          }
        }
      ],
      "2": []
    }
  }
}
```

**Key fields:**
- `membershipId`: Destiny membership ID
- `membershipType`: Platform (3 = Steam, 1 = Xbox, 2 = PlayStation)
- `clanId`: Clan identifier
- `characters`: Array of character objects with `characterId` and `deleted` flag
- `activities`: Nested object keyed by characterId, then by mode (82 = Dungeon, 2 = Story)
  - Each activity must have:
    - `activityDetails.referenceId`: Dungeon reference ID from ACTIVITY_REFERENCE_MAP
    - `activityDetails.instanceId`: Unique instance ID (for deduplication)
    - `period`: ISO timestamp
    - `values.completed.basic.value`: 1 for completed, 0 for incomplete

### Sample Mock Data

See `scripts/sample_mock_data.json` for a working example with 3 dungeon completions.

### What It Tests

1. **Character Fetching**: Simulates API call to get character list
2. **Activity Pagination**: Simulates paginated fetching of activities per character
3. **Activity Grouping**: Groups activities by known dungeon hashes
4. **Deduplication**: 
   - Removes duplicate activities with same instanceId
   - Prefers completed activities over incomplete ones
   - Excludes activities without instanceId
5. **Statistics**: Calculates per-dungeon completion counts
6. **Edge Cases**:
   - Activities without referenceId
   - Activities without instanceId
   - Unknown dungeons (not in reference map)
   - Duplicate instanceIds with different completion status

### Output

The script provides:
- **Console output**: Live processing logs matching the debug endpoint format
- **JSON output** (if `--output` specified): Complete results including:
  - Character information
  - Network statistics
  - Per-dungeon statistics and completion counts
  - Deduplication details

### Supported Dungeons

The script includes all dungeons from ACTIVITY_REFERENCE_MAP:
- Equilibrium
- Sundered Doctrine
- Vesper's Host
- Warlord's Ruin
- Ghosts of the Deep
- Spire of the Watcher
- Duality
- Grasp of Avarice
- Prophecy
- Pit of Heresy
- Shattered Throne

### Testing Scenarios

Use this script to test:
- Deduplication logic with duplicate instanceIds
- Handling of incomplete runs
- Activities without instanceId or referenceId
- Unknown/ungrouped activities
- Multiple characters with different activities
- Pagination behavior with large datasets

### Requirements

- Python 3.6+
- No external dependencies (uses only standard library)
