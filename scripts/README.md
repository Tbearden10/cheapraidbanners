# Bungie API Activity Processor

This Python script processes activities from the Bungie API, following the exact pipeline logic implemented in the JavaScript/TypeScript codebase.

## Overview

The script implements a comprehensive pipeline for processing Destiny 2 dungeon activities:

1. **Fetch Activities** - Retrieves all activities from the Bungie API for a given membership ID and type
2. **Group by Dungeon Type** - Organizes activities using the ACTIVITY_REFERENCE_MAP
3. **Deduplicate** - Removes duplicate activities by instanceId, preferring completed ones
4. **Sort by Earliest Date** - Orders activities chronologically using the `period` field
5. **Filter to Completed** - Retains only completed activities (values.completed.basic.value === 1)
6. **Validate Counts** - Ensures activity counts match expected totals
7. **Create Batches** - Splits activities into batches of 30 (MAX_BATCH_SIZE)
8. **Queue Batches** - Simulates queueing for further processing
9. **Final Summary** - Provides comprehensive logging of the entire pipeline

## Requirements

- Python 3.7+
- `aiohttp` library for async HTTP requests

Install dependencies:

```bash
pip install aiohttp
```

## Usage

```bash
python scripts/process_bungie_activities.py \
  --membership-id <MEMBERSHIP_ID> \
  --membership-type <MEMBERSHIP_TYPE> \
  --api-key <BUNGIE_API_KEY>
```

### Parameters

- `--membership-id`: Bungie membership ID to process (required)
- `--membership-type`: Bungie membership type (required)
  - 1 = Xbox
  - 2 = PlayStation Network (PSN)
  - 3 = Steam
  - 4 = Blizzard
  - 5 = Stadia
  - 6 = Epic Games
  - 10 = Demon
- `--api-key`: Your Bungie API key (required)

### Example

```bash
python scripts/process_bungie_activities.py \
  --membership-id 4611686018467765462 \
  --membership-type 3 \
  --api-key your_api_key_here
```

## How to Get a Bungie API Key

1. Go to [Bungie.net Applications](https://www.bungie.net/en/Application)
2. Sign in with your Bungie account
3. Create a new application
4. Copy your API key from the application details

## Pipeline Structure

The script follows the exact same structure as `src/processors/memberJobProcessor.ts`:

### 1. Character Fetching
Fetches all characters for the given membership ID using the `/Stats/` endpoint.

### 2. Activity Fetching
- Fetches activities for modes 82 (Dungeon) and 2 (Story)
- Paginates through all available activities (250 per page)
- Processes all characters concurrently for efficiency

### 3. Grouping
- Groups activities by dungeon hash using ACTIVITY_REFERENCE_MAP
- Tracks ungrouped activities (activities not matching any known dungeon)
- Tracks activities missing referenceId

### 4. Deduplication
- Uses instanceId as the unique key
- Prefers completed activities over incomplete ones when duplicates exist
- Logs activities missing instanceId

### 5. Sorting
- Sorts activities by `period` field in ascending order (oldest first)
- Uses ISO 8601 date parsing for accurate chronological ordering

### 6. Filtering
- Filters to only completed activities
- Checks `values.completed.basic.value === 1`

### 7. Validation
- Validates that total activities match expected counts
- Accounts for grouped, ungrouped, and missing refId activities

### 8. Batching
- Creates batches of maximum 30 activities each
- Matches the MAX_BATCH_SIZE constant from the JavaScript code

### 9. Queueing
- Simulates queueing batches for processing
- In a production environment, this would send to an actual message queue

### 10. Summary Logging
- Comprehensive final summary including:
  - Activities processed per dungeon
  - Batching information (count, size, distribution)
  - Queueing outcomes
  - Processing duration

## Output

The script provides detailed logging at each stage:

```
================================================================================
BUNGIE API ACTIVITY PROCESSOR
================================================================================

Starting at: 2024-01-01T12:00:00.000000
Membership ID: 4611686018467765462
Membership Type: 3

[Pipeline] Step 1: Fetching characters...
[Pipeline] Found 3 character(s)
  Character 1: 2305843009876543210
  Character 2: 2305843009876543211
  Character 3: 2305843009876543212

[Pipeline] Step 2: Fetching all activities...
[Fetch] Starting activity fetch for 3 character(s)
...

[Pipeline] Total activities fetched: 1523

[Pipeline] Step 3: Grouping activities by dungeon type...
...

================================================================================
FINAL PIPELINE SUMMARY
================================================================================

Membership ID: 4611686018467765462
Membership Type: 3
Processing Duration: 45.2s

--- Activities Processed by Dungeon ---
  Prophecy                      :  234 activities,   8 batch(es)
  Grasp of Avarice             :  156 activities,   6 batch(es)
  Duality                       :   89 activities,   3 batch(es)
  ...

  TOTAL                         : 1234 activities,  42 batch(es)

--- Batching Information ---
  Max Batch Size: 30
  Total Batches: 42
  Activities per Batch (avg): 29.4

--- Queueing Outcomes ---
  Total Activities Queued: 1234
  Total Batches Queued: 42
  Status: Simulated (no actual queue used)

================================================================================
```

## Key Differences from JavaScript Implementation

While the script follows the same pipeline logic, there are a few differences:

1. **No Database Integration**: The script doesn't connect to a database (no cutoff date filtering)
2. **No Actual Queue**: The queueing step is simulated for demonstration
3. **No MemberCoordinator**: Batch coordination is not implemented
4. **Python Async**: Uses Python's `asyncio` and `aiohttp` instead of JavaScript Promises

## Integration Notes

This script can be integrated into a larger system by:

1. Adding database connectivity to check `last_processed_date` and filter activities
2. Implementing actual message queue integration (e.g., Redis, RabbitMQ, AWS SQS)
3. Adding batch coordination and tracking similar to MemberCoordinator
4. Implementing PGCR fetching for detailed activity statistics

## Testing

The script includes unit tests to validate the core pipeline logic:

```bash
python scripts/test_processor.py
```

This will test:
- Activity reference map loading
- Activity grouping by dungeon type
- Deduplication logic
- Filtering to completed activities
- Batching logic
- Constant values

All tests run without requiring API access or credentials.

## Code Quality

The implementation follows these principles:
- **Deterministic**: Same inputs always produce same outputs
- **Reliable**: Comprehensive error handling and retry logic
- **Observable**: Detailed logging at each pipeline stage
- **Tested**: Unit tests for all core functions
- **Documented**: Clear code comments and documentation

## License

This script is part of the cheapraidbanners project.
