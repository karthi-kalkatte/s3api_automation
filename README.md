# S3 API Automation Test Suite

A comprehensive automation test suite for S3 API operations covering both bucket-level and object-level operations.

## Features

- Run all tests at once or individual tests
- **30+ API operations** covering all major S3 functionality
- Real-time test execution status
- Detailed error reporting and summary

## Bucket Level Operations

| Operation | Tests |
|-----------|-------|
| **Basic** | CreateBucket, DeleteBucket, HeadBucket, GetBucketLocation |
| **Access Control** | PutBucketAcl, GetBucketAcl, PutPublicAccessBlock, GetPublicAccessBlock |
| **Tagging** | PutBucketTagging, GetBucketTagging, DeleteBucketTagging |
| **CORS** | PutBucketCors, GetBucketCors, DeleteBucketCors |
| **Versioning** | EnableBucketVersioning, GetBucketVersioning, SuspendBucketVersioning |

## Object Level Operations

| Operation | Tests |
|-----------|-------|
| **Basic** | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects |
| **Copy** | CopyObject |
| **Listing** | ListObjects, ListObjectVersions |
| **Access Control** | PutObjectAcl, GetObjectAcl |
| **Tagging** | PutObjectTagging, GetObjectTagging, DeleteObjectTagging |
| **Multipart Upload** | InitiateMultipartUpload, ListMultipartUploads |

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Update `credentials.json` with your AWS credentials:
   ```json
   {
     "access_key": "YOUR_ACCESS_KEY",
     "secret_key": "YOUR_SECRET_KEY",
     "region": "us-east-1",
     "endpoint_url": "https://s3.amazonaws.com"
   }
   ```

## Usage

### Run All Tests
```bash
python main.py --all
```

### Run Specific Test
```bash
# Bucket operations
python main.py --test create_bucket
python main.py --test put_bucket_tagging
python main.py --test enable_bucket_versioning
python main.py --test put_public_access_block

# Object operations
python main.py --test put_object
python main.py --test copy_object
python main.py --test put_object_tagging
python main.py --test initiate_multipart_upload
```

### View Help
```bash
python main.py --help
```

## All Available Tests

### Bucket Level Tests
- `create_bucket` - Create a new bucket
- `delete_bucket` - Delete a bucket
- `head_bucket` - Check bucket existence
- `get_bucket_location` - Get bucket region
- `put_bucket_acl` - Set bucket ACL
- `get_bucket_acl` - Get bucket ACL
- `put_bucket_tagging` - Add tags to bucket
- `get_bucket_tagging` - Retrieve bucket tags
- `delete_bucket_tagging` - Remove bucket tags
- `put_bucket_cors` - Configure CORS
- `get_bucket_cors` - Get CORS configuration
- `delete_bucket_cors` - Delete CORS configuration
- `enable_bucket_versioning` - Enable bucket versioning
- `get_bucket_versioning` - Get versioning status
- `suspend_bucket_versioning` - Suspend versioning
- `put_public_access_block` - Block public access
- `get_public_access_block` - Get public access block config

### Object Level Tests
- `put_object` - Upload an object
- `get_object` - Download an object
- `head_object` - Check object metadata
- `copy_object` - Copy an object
- `delete_object` - Delete an object
- `delete_objects` - Delete multiple objects
- `list_objects` - List bucket objects
- `list_object_versions` - List object versions
- `put_object_acl` - Set object ACL
- `get_object_acl` - Get object ACL
- `put_object_tagging` - Add tags to object
- `get_object_tagging` - Retrieve object tags
- `delete_object_tagging` - Remove object tags
- `initiate_multipart_upload` - Start multipart upload
- `list_multipart_uploads` - List ongoing uploads

## Test Flow - All Tests

1. **Setup** - Create test bucket and files
2. **Bucket Tests**
   - HeadBucket, GetLocation
   - ACL operations
   - Tagging operations
   - CORS operations
   - Public access block
   - Versioning operations
3. **Object Tests**
   - Put, Get, Head
   - Copy object
   - Object tagging and ACL
   - Multipart upload and listing
   - Version listing
4. **Cleanup** - Delete test resources

## Output Example

```
============================================================
Running S3 API Automation Test Suite - ALL TESTS
============================================================
✓ Setup complete - Test files created

[TEST] Creating bucket...
✓ create_bucket: Bucket test-bucket-xxxxx created successfully

[TEST] Checking bucket existence...
✓ head_bucket: Bucket test-bucket-xxxxx exists and is accessible

[TEST] Adding bucket tags...
✓ put_bucket_tagging: Tags added to bucket test-bucket-xxxxx

... more tests ...

============================================================
TEST SUMMARY
============================================================
Total Tests: 32
Passed: 32
Failed: 0
============================================================
```

## File Structure

```
s3api_automation/
├── credentials.json      # AWS credentials (update with your values)
├── config.py            # Configuration management
├── s3_operations.py     # S3 API operations (30+ methods)
├── test_suite.py        # Test cases (30+ tests)
├── main.py              # CLI entry point
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Configuration

### Environment Support
- **AWS S3**: `"endpoint_url": "https://s3.amazonaws.com"`
- **S3-Compatible (MinIO, etc.)**: Update endpoint URL accordingly

### Regions
Update the `region` in credentials.json:
- `us-east-1`, `us-west-2`, `eu-west-1`, etc.

## Notes

- Test bucket names are randomly generated to avoid conflicts
- Test files are created temporarily during execution
- Credentials should never be committed to version control
- Some operations depend on prerequisites (bucket must exist, etc.)
- The test suite handles dependencies automatically

## Requirements

- Python 3.6+
- boto3 >= 1.26
- botocore >= 1.29
- AWS credentials with S3 permissions
