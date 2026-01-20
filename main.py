"""Main script to run S3 automation tests."""
import sys
import argparse
from test_suite import S3TestSuite


def main():
    parser = argparse.ArgumentParser(
        description='S3 API Automation Test Suite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --all                              # Run all tests
  python main.py --test create_bucket               # Run specific test
  python main.py --test put_bucket_tagging          # Run put_bucket_tagging test
  python main.py --test put_object                  # Run put_object test
  python main.py --test copy_object                 # Run copy_object test
  python main.py --test put_get_5mb_immediate       # Put 5MB and get immediately

BUCKET LEVEL OPERATIONS:
  - create_bucket, delete_bucket, head_bucket
  - get_bucket_location
  - put_bucket_acl, get_bucket_acl
  - put_bucket_tagging, get_bucket_tagging, delete_bucket_tagging
  - put_bucket_cors, get_bucket_cors, delete_bucket_cors
  - enable_bucket_versioning, get_bucket_versioning, suspend_bucket_versioning
  - put_public_access_block, get_public_access_block

OBJECT LEVEL OPERATIONS:
  - put_object, get_object, head_object, delete_object, delete_objects
  - copy_object
  - list_objects, list_object_versions
  - put_object_acl, get_object_acl
  - put_object_tagging, get_object_tagging, delete_object_tagging
  - initiate_multipart_upload, list_multipart_uploads

LARGE FILE OPERATIONS:
  - put_object_5mb (Upload 5MB file)
  - get_object_5mb (Download 5MB file)
  - put_get_5mb_immediate (Upload and download 5MB immediately)
  - put_delete_5mb_immediate (Upload 5MB and delete immediately)
  - put_get_1kb_immediate (Upload 1KB and download immediately)
  - put_delete_1kb_immediate (Upload 1KB and delete immediately)
  - put_object_50mb (Upload 50MB file)
  - get_object_50mb_multipart (Download 50MB with 10MB parts)
  - put_get_50mb_multipart_immediate (Upload 50MB and download immediately with 10MB parts)

OBJECT LOCK OPERATIONS:
  - create_bucket_with_object_lock (Create bucket with WORM protection)
  - get_object_lock_configuration (Get Object Lock configuration)
  - put_object_lock_configuration (Set default retention)
  - put_object_retention (Set retention on specific object)
  - get_object_retention (Get object retention status)
  - put_object_legal_hold (Place legal hold on object)
  - get_object_legal_hold (Get legal hold status)

SSE (SERVER-SIDE ENCRYPTION) OPERATIONS:
  - put_bucket_encryption (Enable AES256 encryption on bucket)
  - get_bucket_encryption (Get bucket encryption configuration)
  - put_object_with_sse (Upload object with encryption)
  - get_object_with_sse (Download encrypted object)
  - delete_bucket_encryption (Remove bucket encryption)

LIFECYCLE RULES OPERATIONS:
  - put_bucket_lifecycle_configuration (Set lifecycle rules)
  - get_bucket_lifecycle_configuration (Get lifecycle rules)
  - delete_bucket_lifecycle_configuration (Delete lifecycle rules)
        """
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all tests'
    )
    
    parser.add_argument(
        '--test',
        type=str,
        help='Run a specific test'
    )
    
    args = parser.parse_args()
    
    # Initialize test suite
    test_suite = S3TestSuite()
    
    # Run tests based on arguments
    if args.all:
        test_suite.run_all_tests()
    elif args.test:
        test_suite.run_specific_test(args.test)
    else:
        print("Error: Please specify --all or --test <test_name>")
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
