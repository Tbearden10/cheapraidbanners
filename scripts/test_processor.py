#!/usr/bin/env python3
"""
Unit tests for the Bungie API Activity Processor

These tests validate the core logic without making actual API calls.
"""

import sys
import os
from datetime import datetime

# Add parent directory to path to import the script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.process_bungie_activities import (
    ACTIVITY_REFERENCE_MAP,
    group_activities_by_dungeon,
    deduplicate_activities,
    filter_completed_activities,
    create_batches,
    MAX_BATCH_SIZE
)


def test_activity_reference_map():
    """Test that ACTIVITY_REFERENCE_MAP is properly loaded"""
    print("Testing ACTIVITY_REFERENCE_MAP...")
    
    assert len(ACTIVITY_REFERENCE_MAP) > 0, "ACTIVITY_REFERENCE_MAP should not be empty"
    
    # Verify structure
    for dungeon in ACTIVITY_REFERENCE_MAP:
        assert 'hash' in dungeon, "Each dungeon should have a hash"
        assert 'displayName' in dungeon, "Each dungeon should have a displayName"
        assert 'referenceIds' in dungeon, "Each dungeon should have referenceIds"
        assert len(dungeon['referenceIds']) > 0, "Each dungeon should have at least one referenceId"
    
    print(f"  ✓ Found {len(ACTIVITY_REFERENCE_MAP)} dungeons")
    print(f"  ✓ All dungeons have required fields")


def test_grouping():
    """Test activity grouping logic"""
    print("\nTesting activity grouping...")
    
    # Create test activities
    activities_by_char = {
        'char1': [
            {
                'activityDetails': {'referenceId': '2032534090'},  # Shattered Throne
                'period': '2024-01-01T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
            {
                'activityDetails': {'referenceId': '1077850348'},  # Prophecy
                'period': '2024-01-02T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
            {
                'activityDetails': {'referenceId': '999999999'},  # Unknown (ungrouped)
                'period': '2024-01-03T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
        ],
        'char2': [
            {
                'activityDetails': {},  # Missing referenceId
                'period': '2024-01-04T12:00:00Z',
            }
        ]
    }
    
    activities_by_dungeon, ungrouped, missing_ref_id = group_activities_by_dungeon(activities_by_char)
    
    # Verify Shattered Throne
    assert '2032534090' in activities_by_dungeon
    assert len(activities_by_dungeon['2032534090']) == 1
    
    # Verify Prophecy
    assert '1077850348' in activities_by_dungeon
    assert len(activities_by_dungeon['1077850348']) == 1
    
    # Verify ungrouped
    assert len(ungrouped) == 1
    assert ungrouped[0]['referenceId'] == '999999999'
    
    # Verify missing refId
    assert len(missing_ref_id) == 1
    
    print("  ✓ Activities grouped correctly")
    print("  ✓ Ungrouped activities tracked")
    print("  ✓ Missing refId activities tracked")


def test_deduplication():
    """Test deduplication logic"""
    print("\nTesting deduplication...")
    
    activities_by_dungeon = {
        '2032534090': [  # Shattered Throne
            {
                'activityDetails': {'instanceId': 'instance1'},
                'period': '2024-01-01T12:00:00Z',
                'values': {'completed': {'basic': {'value': 0}}}
            },
            {
                'activityDetails': {'instanceId': 'instance1'},
                'period': '2024-01-01T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}  # Completed - should be preferred
            },
            {
                'activityDetails': {'instanceId': 'instance2'},
                'period': '2024-01-02T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
        ]
    }
    
    deduplicated = deduplicate_activities(activities_by_dungeon)
    
    # Should have 2 unique activities
    assert len(deduplicated['2032534090']) == 2
    
    # The instance1 activity should be the completed one
    instance1_activities = [a for a in deduplicated['2032534090'] 
                           if a.get('activityDetails', {}).get('instanceId') == 'instance1']
    assert len(instance1_activities) == 1
    assert instance1_activities[0]['values']['completed']['basic']['value'] == 1
    
    print("  ✓ Duplicates removed correctly")
    print("  ✓ Completed activities preferred")


def test_filtering():
    """Test filtering to completed activities"""
    print("\nTesting filtering to completed activities...")
    
    activities_by_dungeon = {
        '2032534090': [
            {
                'activityDetails': {'instanceId': 'instance1'},
                'period': '2024-01-01T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
            {
                'activityDetails': {'instanceId': 'instance2'},
                'period': '2024-01-02T12:00:00Z',
                'values': {'completed': {'basic': {'value': 0}}}
            },
            {
                'activityDetails': {'instanceId': 'instance3'},
                'period': '2024-01-03T12:00:00Z',
                'values': {'completed': {'basic': {'value': 1}}}
            },
        ]
    }
    
    filtered = filter_completed_activities(activities_by_dungeon)
    
    # Should have 2 completed activities
    assert len(filtered['2032534090']) == 2
    
    # All should be completed
    for activity in filtered['2032534090']:
        assert activity['values']['completed']['basic']['value'] == 1
    
    print("  ✓ Incomplete activities filtered out")
    print("  ✓ Completed activities retained")


def test_batching():
    """Test batching logic"""
    print("\nTesting batching...")
    
    # Create activities (more than MAX_BATCH_SIZE)
    activities = []
    for i in range(75):
        activities.append({
            'activityDetails': {'instanceId': f'instance{i}'},
            'period': f'2024-01-01T{i:02d}:00:00Z',
            'values': {'completed': {'basic': {'value': 1}}}
        })
    
    activities_by_dungeon = {'2032534090': activities}
    
    batches = create_batches(activities_by_dungeon)
    
    # Should have 3 batches (75 / 30 = 2.5 -> 3)
    assert len(batches['2032534090']) == 3
    
    # First two batches should be full
    assert len(batches['2032534090'][0]) == MAX_BATCH_SIZE
    assert len(batches['2032534090'][1]) == MAX_BATCH_SIZE
    
    # Last batch should have remainder
    assert len(batches['2032534090'][2]) == 15
    
    # Total should match original
    total = sum(len(b) for b in batches['2032534090'])
    assert total == 75
    
    print(f"  ✓ 75 activities split into 3 batches")
    print(f"  ✓ Batch sizes: {[len(b) for b in batches['2032534090']]}")


def test_constants():
    """Test that constants match the JavaScript implementation"""
    print("\nTesting constants...")
    
    # Test specific dungeons that should exist
    dungeon_names = [d['displayName'] for d in ACTIVITY_REFERENCE_MAP]
    
    expected_dungeons = [
        'Equilibrium',
        'Vesper\'s Host',
        'Prophecy',
        'Pit of Heresy',
        'The Shattered Throne',
        'Grasp of Avarice',
        'Duality',
        'Spire of the Watcher',
        'Ghosts of the Deep',
        'Warlord\'s Ruin',
        'Sundered Doctrine'
    ]
    
    for dungeon in expected_dungeons:
        assert dungeon in dungeon_names, f"{dungeon} should be in ACTIVITY_REFERENCE_MAP"
    
    print(f"  ✓ All expected dungeons present")
    print(f"  ✓ MAX_BATCH_SIZE = {MAX_BATCH_SIZE}")


def run_all_tests():
    """Run all tests"""
    print("="*80)
    print("RUNNING UNIT TESTS FOR BUNGIE API ACTIVITY PROCESSOR")
    print("="*80)
    
    tests = [
        test_activity_reference_map,
        test_constants,
        test_grouping,
        test_deduplication,
        test_filtering,
        test_batching,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n  ❌ FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*80)
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(run_all_tests())
