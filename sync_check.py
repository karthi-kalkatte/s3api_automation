#!/usr/bin/env python3
"""Check if all S3 operations have corresponding tests and vice versa."""

import re
from pathlib import Path

# Read s3_operations.py
ops_file = Path('s3_operations.py').read_text()
# Extract all method names (except __init__)
ops_methods = set(re.findall(r'def (\w+)\(self', ops_file))
ops_methods.discard('__init__')

# Read test_suite.py
test_file = Path('test_suite.py').read_text()
# Extract all test method names
test_methods = set(re.findall(r'def (test_\w+)\(self', test_file))

# Extract operations that have test equivalents
ops_with_tests = set()
for test_name in test_methods:
    op_name = test_name.replace('test_', '')
    if op_name in ops_methods:
        ops_with_tests.add(op_name)

# Find missing tests for operations
missing_tests = ops_methods - ops_with_tests

# Find orphaned tests (tests without operations)
# Exclude large file performance tests which are scenarios, not separate operations
performance_test_prefix = ('put_object_5mb', 'get_object_5mb', 'put_get_5mb_immediate', 
                          'put_delete_5mb_immediate', 'put_get_1kb_immediate', 'put_delete_1kb_immediate')
orphaned_tests = set()
for test_name in test_methods:
    op_name = test_name.replace('test_', '')
    if op_name not in ops_methods and op_name not in performance_test_prefix:
        orphaned_tests.add(test_name)

print("=" * 70)
print("S3 API OPERATIONS & TESTS SYNC CHECK")
print("=" * 70)

print(f"\n✓ Operations with Tests: {len(ops_with_tests)}")
print(f"✓ Total Test Methods: {len(test_methods)}")
print(f"✓ Total Operations: {len(ops_methods)}")

if missing_tests:
    print(f"\n✗ OPERATIONS WITHOUT TESTS ({len(missing_tests)}):")
    for op in sorted(missing_tests):
        print(f"  - {op}")
else:
    print(f"\n✓ All operations have corresponding tests!")

if orphaned_tests:
    print(f"\n✗ ORPHANED TESTS ({len(orphaned_tests)}):")
    for test in sorted(orphaned_tests):
        print(f"  - {test}")
else:
    print(f"\n✓ All tests have corresponding operations!")

# Check test_methods dictionary
test_methods_in_dict = set(re.findall(r"'(\w+)':\s*self\.test_", test_file))
missing_from_dict = test_methods - {f"test_{m}" for m in test_methods_in_dict}

print(f"\n✓ Test Methods in Dictionary: {len(test_methods_in_dict)}")

if missing_from_dict:
    print(f"\n✗ TEST METHODS MISSING FROM DICTIONARY ({len(missing_from_dict)}):")
    for test in sorted(missing_from_dict):
        print(f"  - {test}")
else:
    print(f"\n✓ All test methods registered in test_methods dictionary!")

# Check run_all_tests() calls
run_all_calls = set(re.findall(r'self\.(test_\w+)\(\)', test_file))
test_calls_in_run_all = run_all_calls & test_methods

print(f"\n✓ Tests Called in run_all_tests(): {len(test_calls_in_run_all)}")

missing_from_run_all = test_methods - test_calls_in_run_all

if missing_from_run_all:
    print(f"\n✗ TEST METHODS NOT CALLED IN run_all_tests() ({len(missing_from_run_all)}):")
    for test in sorted(missing_from_run_all):
        print(f"  - {test}")
else:
    print(f"\n✓ All test methods are called in run_all_tests()!")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
total_issues = len(missing_tests) + len(orphaned_tests) + len(missing_from_dict) + len(missing_from_run_all)
if total_issues == 0:
    print("✓ FULLY SYNCED - All operations and tests are properly synchronized!")
else:
    print(f"✗ ISSUES FOUND - {total_issues} synchronization issues detected")

print("=" * 70)
