#!/usr/bin/env python3
"""
IDrive E2 Conditional Header Verification Test
=============================================

This test specifically verifies that IDrive E2 REALLY supports conditional headers
and isn't just giving false positives. We'll do more rigorous testing.
"""

import os
import sys
import json
import tempfile
import requests
import boto3
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def load_e2_config():
    """Load IDrive E2 configuration."""
    with open('credentials.json', 'r') as f:
        creds = json.load(f)
    return creds['e2']

def create_s3_client():
    """Create boto3 S3 client for IDrive E2."""
    config = load_e2_config()
    return boto3.client(
        's3',
        aws_access_key_id=config['access_key'],
        aws_secret_access_key=config['secret_key'],
        region_name=config['region'],
        endpoint_url=config['endpoint_url']
    )

def rigorous_if_match_test():
    """More rigorous If-Match test to verify conditional logic is actually working."""
    print("🔍 RIGOROUS If-Match Test")
    print("=" * 40)
    
    s3_client = create_s3_client()
    bucket = f"verify-test-{os.urandom(6).hex()}"
    
    try:
        # Create bucket
        print(f"📦 Creating test bucket: {bucket}")
        s3_client.create_bucket(Bucket=bucket)
        
        # Test 1: Upload initial object
        key = "conditional-test.json"
        initial_content = "version_1_content"
        
        print("📝 Step 1: Upload initial object")
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write(initial_content)
            temp_file = f.name
        
        with open(temp_file, 'rb') as f:
            response = s3_client.put_object(Bucket=bucket, Key=key, Body=f)
        
        etag_v1 = response['ETag'].strip('"')
        print(f"  ✓ Initial ETag: {etag_v1}")
        
        # Test 2: Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=300
        )
        print(f"  ✓ Generated presigned URL")
        
        # Test 3: Modify object directly to change ETag
        print("📝 Step 2: Modify object directly (bypass presigned URL)")
        modified_content = "version_2_content_modified_directly"
        response = s3_client.put_object(
            Bucket=bucket, 
            Key=key, 
            Body=modified_content.encode()
        )
        etag_v2 = response['ETag'].strip('"')
        print(f"  ✓ New ETag after direct modification: {etag_v2}")
        print(f"  ✓ ETag changed: {etag_v1} → {etag_v2}")
        
        # Test 4: Try to use presigned URL with OLD ETag (should fail)
        print("📝 Step 3: Try presigned PUT with STALE ETag (should get 412)")
        stale_content = "version_3_using_stale_etag"
        
        response = requests.put(
            presigned_url,
            data=stale_content,
            headers={'If-Match': etag_v1}  # OLD ETag!
        )
        
        print(f"  📊 Response status: {response.status_code}")
        print(f"  📊 Response headers: {dict(response.headers)}")
        
        if response.status_code == 412:
            print("  ✅ SUCCESS: Got 412 - Conditional headers ARE working!")
            print("  🎯 IDrive E2 correctly detected stale ETag and prevented overwrite")
            return True
        elif response.status_code == 200:
            print("  ❌ FAILURE: Got 200 - Conditional headers are IGNORED!")
            print("  💥 IDrive E2 allowed overwrite with stale ETag - NOT SAFE for atomic operations")
            return False
        else:
            print(f"  ❓ UNEXPECTED: Got {response.status_code} - Need to investigate")
            print(f"  📄 Response body: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"  ❌ Test failed with exception: {e}")
        return False
    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects_v2(Bucket=bucket)
            if 'Contents' in objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            s3_client.delete_bucket(Bucket=bucket)
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass

def rigorous_if_none_match_test():
    """More rigorous If-None-Match test."""
    print("\n🔍 RIGOROUS If-None-Match Test")
    print("=" * 40)
    
    s3_client = create_s3_client()
    bucket = f"verify-test2-{os.urandom(6).hex()}"
    
    try:
        # Create bucket
        print(f"📦 Creating test bucket: {bucket}")
        s3_client.create_bucket(Bucket=bucket)
        
        # Test 1: Create object directly
        key = "existing-object.json"
        existing_content = "object_already_exists"
        
        print("📝 Step 1: Create object directly")
        s3_client.put_object(Bucket=bucket, Key=key, Body=existing_content.encode())
        print(f"  ✓ Object created: {key}")
        
        # Test 2: Generate presigned URL for same key
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=300
        )
        
        # Test 3: Try to "create" existing object with If-None-Match: *
        print("📝 Step 2: Try to 'create' existing object with If-None-Match: *")
        duplicate_content = "attempted_duplicate_creation"
        
        response = requests.put(
            presigned_url,
            data=duplicate_content,
            headers={'If-None-Match': '*'}  # Should fail - object exists!
        )
        
        print(f"  📊 Response status: {response.status_code}")
        print(f"  📊 Response headers: {dict(response.headers)}")
        
        if response.status_code == 412:
            print("  ✅ SUCCESS: Got 412 - Duplicate prevention working!")
            print("  🎯 IDrive E2 correctly prevented duplicate creation")
            return True
        elif response.status_code == 200:
            print("  ❌ FAILURE: Got 200 - Duplicate prevention BROKEN!")
            print("  💥 IDrive E2 allowed overwrite of existing object - NOT SAFE!")
            return False
        else:
            print(f"  ❓ UNEXPECTED: Got {response.status_code}")
            return False
            
    except Exception as e:
        print(f"  ❌ Test failed with exception: {e}")
        return False
    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects_v2(Bucket=bucket)
            if 'Contents' in objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            s3_client.delete_bucket(Bucket=bucket)
        except:
            pass

def cross_reference_test():
    """Cross-reference test: Compare behavior when conditions should succeed vs fail."""
    print("\n🔍 CROSS-REFERENCE Test")
    print("=" * 40)
    print("Comparing what SHOULD work vs what SHOULD fail...")
    
    s3_client = create_s3_client()
    bucket = f"crossref-test-{os.urandom(6).hex()}"
    
    try:
        s3_client.create_bucket(Bucket=bucket)
        key = "cross-ref-object.json"
        
        # Create initial object
        initial_content = "cross_ref_initial"
        response = s3_client.put_object(Bucket=bucket, Key=key, Body=initial_content.encode())
        correct_etag = response['ETag'].strip('"')
        
        presigned_url = s3_client.generate_presigned_url(
            'put_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=300
        )
        
        # Test A: Correct ETag (should succeed)
        print("📝 Test A: Using CORRECT ETag (should get 200)")
        response_a = requests.put(
            presigned_url,
            data="updated_with_correct_etag",
            headers={'If-Match': correct_etag}
        )
        print(f"  📊 Correct ETag result: {response_a.status_code}")
        
        # Get new ETag after successful update
        if response_a.status_code == 200:
            new_etag = response_a.headers.get('ETag', '').strip('"')
            print(f"  ✓ New ETag: {new_etag}")
        
        # Test B: Wrong ETag (should fail)
        print("📝 Test B: Using WRONG ETag (should get 412)")
        wrong_etag = "definitely-wrong-etag-123"
        response_b = requests.put(
            presigned_url,
            data="should_not_work_wrong_etag", 
            headers={'If-Match': wrong_etag}
        )
        print(f"  📊 Wrong ETag result: {response_b.status_code}")
        
        # Analysis
        if response_a.status_code == 200 and response_b.status_code == 412:
            print("  ✅ PERFECT: Correct ETag worked (200), Wrong ETag failed (412)")
            print("  🎯 This confirms conditional logic is working correctly!")
            return True
        elif response_a.status_code == 200 and response_b.status_code == 200:
            print("  ❌ BROKEN: Both requests succeeded - conditionals are ignored!")
            return False
        else:
            print(f"  ❓ Unexpected pattern: Correct={response_a.status_code}, Wrong={response_b.status_code}")
            return False
            
    except Exception as e:
        print(f"  ❌ Cross-reference test failed: {e}")
        return False
    finally:
        try:
            objects = s3_client.list_objects_v2(Bucket=bucket)
            if 'Contents' in objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            s3_client.delete_bucket(Bucket=bucket)
        except:
            pass

def main():
    """Run verification tests."""
    print("🚨 IDrive E2 Conditional Header Verification")
    print("=" * 50)
    print("These tests will definitively answer whether IDrive E2")
    print("REALLY supports conditional headers or if we're getting false positives.")
    print()
    
    config = load_e2_config()
    print(f"🔗 Testing endpoint: {config['endpoint_url']}")
    print()
    
    # Run all verification tests
    results = []
    
    results.append(("If-Match Rigorous Test", rigorous_if_match_test()))
    results.append(("If-None-Match Rigorous Test", rigorous_if_none_match_test()))  
    results.append(("Cross-Reference Test", cross_reference_test()))
    
    # Summary
    print("\n" + "=" * 50)
    print("🏁 VERIFICATION RESULTS")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    if passed == total:
        print(f"\n🎉 CONCLUSIVE: IDrive E2 DOES support conditional headers!")
        print(f"✅ All {total}/{total} verification tests passed")
        print(f"🚀 Safe for production atomic operations")
    elif passed == 0:
        print(f"\n💥 CONCLUSIVE: IDrive E2 does NOT support conditional headers!")
        print(f"❌ All {total}/{total} verification tests failed")  
        print(f"🚫 NOT safe for atomic operations")
    else:
        print(f"\n⚠️  INCONCLUSIVE: Mixed results ({passed}/{total} passed)")
        print(f"🔍 Requires further investigation")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())