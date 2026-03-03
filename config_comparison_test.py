#!/usr/bin/env python3
"""
Comparison Test: Original vs New IDrive E2 Configuration
=======================================================

This test compares the old configuration method vs new configuration method
to identify why IDrive E2 conditional headers now work when they didn't before.
"""

import os
import sys
import json
import tempfile
import requests
import boto3
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_original_config_method():
    """Test using the original config.py method (single endpoint)."""
    print("🔍 Testing ORIGINAL config method...")
    
    # Create old-style credentials for IDrive E2
    old_creds = {
        "access_key": "5lqHUNZl18xBfszKifpB",
        "secret_key": "Q7lk4k4YrwMdvN2vXt29R0WNfdrcRpPetFFaqwVz", 
        "region": "us-east-1",
        "endpoint_url": "https://s3.us-east-1.idrivee2.com"
    }
    
    # Save temporarily
    with open('temp_old_creds.json', 'w') as f:
        json.dump(old_creds, f, indent=2)
    
    try:
        # Import using original config
        from config import S3Config
        config = S3Config('temp_old_creds.json')
        
        print(f"    📡 Endpoint: {config.endpoint_url}")
        print(f"    🌍 Region: {config.region}")
        
        # Create boto3 client the old way
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            endpoint_url=config.endpoint_url
        )
        
        return run_conditional_test(s3_client, "ORIGINAL_CONFIG", config.endpoint_url)
        
    finally:
        # Cleanup
        if os.path.exists('temp_old_creds.json'):
            os.remove('temp_old_creds.json')


def test_new_config_method():
    """Test using the new multi-endpoint method."""
    print("🔍 Testing NEW config method...")
    
    # Load new-style credentials
    with open('credentials.json', 'r') as f:
        creds = json.load(f)
    
    e2_config = creds['e2']
    
    print(f"    📡 Endpoint: {e2_config['endpoint_url']}")  
    print(f"    🌍 Region: {e2_config['region']}")
    
    # Create boto3 client the new way
    s3_client = boto3.client(
        's3',
        aws_access_key_id=e2_config['access_key'],
        aws_secret_access_key=e2_config['secret_key'],
        region_name=e2_config['region'],
        endpoint_url=e2_config['endpoint_url']
    )
    
    return run_conditional_test(s3_client, "NEW_CONFIG", e2_config['endpoint_url'])


def run_conditional_test(s3_client, method_name, endpoint_url):
    """Run a conditional header test and return results."""
    # Use simpler bucket name for IDrive E2 compatibility
    test_bucket = f"comparetest{method_name.lower().replace('_', '')}{os.urandom(3).hex()}"
    
    try:
        print(f"    📦 Creating bucket: {test_bucket}")
        
        # Create bucket
        s3_client.create_bucket(Bucket=test_bucket)
        
        # Test If-Match failure scenario  
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        
        # Create initial object
        with open(temp_file, 'w') as f:
            f.write(f'{method_name}_initial_content')
        
        job_key = f'{method_name}-conditional-test.json'
        
        with open(temp_file, 'rb') as f:
            resp = s3_client.put_object(
                Bucket=test_bucket,
                Key=job_key,
                Body=f
            )
        
        current_etag = resp['ETag'].strip('"')
        print(f"    🏷️  Object ETag: {current_etag}")
        
        # Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': test_bucket, 'Key': job_key},
            ExpiresIn=300
        )
        
        print(f"    🔗 Generated presigned URL")
        print(f"       {presigned_url[:80]}...")
        
        # Test with WRONG If-Match header (should fail with 412)
        with open(temp_file, 'w') as f:
            f.write(f'{method_name}_updated_should_fail')
        
        wrong_etag = 'wrong-etag-test-12345'
        print(f"    🚫 Testing wrong If-Match: {wrong_etag}")
        
        with open(temp_file, 'rb') as f:
            http_response = requests.put(
                presigned_url,
                data=f.read(),
                headers={'If-Match': wrong_etag}
            )
        
        status = http_response.status_code
        print(f"    📊 HTTP Status: {status}")
        
        if status == 412:
            result = {
                'method': method_name,
                'endpoint': endpoint_url,
                'status': 'SUCCESS',
                'message': '✅ Conditional headers WORKING - Got 412 as expected',
                'http_status': status,
                'conditional_support': True
            }
        elif status == 200:
            result = {
                'method': method_name,
                'endpoint': endpoint_url,
                'status': 'FAILURE', 
                'message': '❌ Conditional headers IGNORED - Got 200 when should be 412',
                'http_status': status,
                'conditional_support': False
            }
        else:
            result = {
                'method': method_name,
                'endpoint': endpoint_url,
                'status': 'UNEXPECTED',
                'message': f'⚠️  Unexpected status {status}',
                'http_status': status,
                'conditional_support': False
            }
        
        # Cleanup temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return result
        
    except Exception as e:
        print(f"    ❌ Test failed: {str(e)}")
        return {
            'method': method_name,
            'endpoint': endpoint_url,
            'status': 'ERROR',
            'message': f'Test failed: {str(e)}',
            'conditional_support': False
        }
    
    finally:
        # Cleanup bucket
        try:
            print(f"    🧹 Cleaning up {test_bucket}...")
            # List and delete objects
            objects = s3_client.list_objects_v2(Bucket=test_bucket)
            if 'Contents' in objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                s3_client.delete_objects(
                    Bucket=test_bucket,
                    Delete={'Objects': delete_keys}
                )
            # Delete bucket
            s3_client.delete_bucket(Bucket=test_bucket)
            print(f"    ✓ Cleanup complete")
        except Exception as e:
            print(f"    ⚠️  Cleanup failed: {e}")


def main():
    """Main comparison test."""
    print("🔬 IDrive E2 Configuration Comparison Test")
    print("=" * 60)
    print("Comparing old vs new config methods to identify what changed...")
    print()
    
    # Test both methods
    print("1️⃣  TESTING ORIGINAL CONFIGURATION METHOD")
    print("-" * 40)
    original_result = test_original_config_method()
    
    print("\n2️⃣  TESTING NEW CONFIGURATION METHOD") 
    print("-" * 40)
    new_result = test_new_config_method()
    
    # Compare results
    print("\n" + "=" * 60)
    print("🔍 COMPARISON RESULTS")
    print("=" * 60)
    
    print(f"Original Method: {original_result['message']}")
    print(f"New Method:      {new_result['message']}")
    
    print(f"\nEndpoint URLs:")
    print(f"  Original: {original_result['endpoint']}")
    print(f"  New:      {new_result['endpoint']}")
    
    # Determine what changed
    if original_result['conditional_support'] != new_result['conditional_support']:
        print(f"\n🎯 KEY FINDING:")
        if new_result['conditional_support'] and not original_result['conditional_support']:
            print("✅ NEW method works, ORIGINAL method failed!")
            print("   This suggests either:")
            print("   1. IDrive E2 service was improved/updated")
            print("   2. Different endpoint URLs have different behavior")  
            print("   3. Different boto3 client configuration affects behavior")
        elif original_result['conditional_support'] and not new_result['conditional_support']:
            print("⚠️  ORIGINAL method worked, NEW method fails!")
    else:
        if both_support := original_result['conditional_support'] and new_result['conditional_support']:
            print("\n✅ BOTH methods work! IDrive E2 supports conditional headers consistently.")
        else:
            print("\n❌ BOTH methods fail! IDrive E2 does not support conditional headers.")
    
    # Check for URL differences
    if original_result['endpoint'] != new_result['endpoint']:
        print(f"\n📡 ENDPOINT DIFFERENCE DETECTED:")
        print(f"   Original: {original_result['endpoint']}")
        print(f"   New:      {new_result['endpoint']}")
        print("   This could explain different behavior!")
    else:
        print(f"\n📡 Same endpoint URL used in both tests")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())