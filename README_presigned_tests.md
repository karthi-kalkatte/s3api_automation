# Presigned URL Conditional PUT Tests

This is a standalone test suite extracted from the main S3 API automation suite, focusing specifically on presigned URLs with conditional headers (If-Match and If-None-Match).

## Purpose

These tests are designed to validate the behavior of presigned URLs when used with conditional HTTP headers, which is crucial for:

- **Distributed job queue systems**
- **Atomic operations (compare-and-swap)**
- **Race condition prevention**
- **Exclusive resource creation**

## Important Findings

⚠️ **Key Discovery**: Many S3-compatible services (including IDrive E2) do NOT properly enforce conditional headers on presigned URLs.

✅ **Recommended**: Use direct boto3 calls with conditional headers for atomic operations  
❌ **Avoid**: Presigned URLs for critical atomic operations

## Tests Included

1. **`test_presigned_put_if_match_success`** - Successful compare-and-swap operation
2. **`test_presigned_put_if_match_412_failure`** - Expected 412 Precondition Failed (race condition prevention)
3. **`test_aws_cli_presigned_if_match_412_failure`** - Same test using AWS CLI generated URLs
4. **`test_presigned_put_if_none_match_success`** - Successful exclusive resource creation
5. **`test_presigned_put_if_none_match_409_failure`** - Expected 409 Conflict (duplicate prevention)

## Prerequisites

Ensure you have the following files in the same directory:
- `config.py` - S3 configuration
- `s3_operations.py` - S3 operations wrapper
- `credentials.json` - Your S3 credentials

## Installation

Install required dependencies:

```bash
pip install boto3 requests
```

For AWS CLI tests, also install:
```bash
# Windows (if not already installed)
pip install awscli

# Or download from: https://aws.amazon.com/cli/
```

## Usage

### Run All Tests
```bash
python presigned_conditional_tests.py
```

### Run from Python
```python
from presigned_conditional_tests import PresignedConditionalTestSuite

suite = PresignedConditionalTestSuite()
suite.run_all_tests()
```

## Expected Behavior

### ✅ Ideal S3-Compatible Service
- Returns **412 Precondition Failed** for wrong If-Match headers
- Returns **409 Conflict** or **412** for If-None-Match violations
- Properly enforces conditional logic on presigned URLs

### ❌ Limited S3-Compatible Service (like IDrive E2)
- Ignores conditional headers on presigned URLs
- Always returns **200 OK** regardless of conditions
- Conditional headers work only with direct boto3 calls

## Output Example

```
🚀 Starting Presigned URL Conditional PUT Tests
============================================================

[TEST] Presigned PUT with If-Match header (success case)...
  └─ Creating initial job: presigned-job-if-match-success.json
  └─ Current ETag: d41d8cd98f00b204e9800998ecf8427e
  └─ Generating presigned PUT URL...
  └─ Sending PUT with If-Match HTTP header: d41d8cd98f00b204e9800998ecf8427e
✅ presigned_put_if_match_success: ✓ Presigned PUT with If-Match success!

[TEST] Presigned PUT with If-Match header (412 failure case)...
  └─ Attempting presigned PUT with If-Match HTTP header: wrong-etag-12345
  └─ DEBUG: status=200
❌ presigned_put_if_match_412_failure: Expected 412, got 200
```

## Integration with Job Queues

For distributed systems, use these patterns:

### ✅ Reliable Pattern (Direct API)
```python
# Compare-and-swap for job updates
s3_client.put_object(
    Bucket=bucket,
    Key=job_key,
    Body=updated_job_data,
    IfMatch=current_etag  # Ensures atomic update
)
```

### ❌ Unreliable Pattern (Presigned URL)
```python
# May not work on all S3-compatible services
presigned_url = s3_client.generate_presigned_url('put_object', {...})
requests.put(presigned_url, data=job_data, headers={'If-Match': etag})
```