#!/usr/bin/env python3
"""
Presigned URL Conditional PUT Tests
=====================================

This standalone test suite focuses on testing presigned URLs with conditional 
headers (If-Match, If-None-Match) for distributed job queue systems.

IMPORTANT FINDINGS:
✅ Direct boto3 calls with conditional headers (fully supported)  
❌ Presigned URLs for atomic operations (not reliable on many S3-compatible services)

Tests included:
- Presigned PUT + If-Match (success case)
- Presigned PUT + If-Match (412 failure case) 
- AWS CLI presigned PUT + If-Match (412 failure case)
- Presigned PUT + If-None-Match (success case)
- Presigned PUT + If-None-Match (409/412 failure case)
"""

import os
import sys
import json
import tempfile
import subprocess
import requests
from datetime import datetime

# Add the current directory to Python path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_operations import S3Operations


class PresignedConditionalTestSuite:
    """Focused test suite for presigned URL conditional operations."""

    def __init__(self, service='e2'):
        self.service = service
        self.config = self._load_service_config(service)
        self.s3_ops = S3Operations(self.config)
        self.test_results = []
        self.test_bucket = f"presigned-test-bucket-{service}-{os.urandom(4).hex()}"
        
    def _load_service_config(self, service):
        """Load configuration for specified service."""
        try:
            with open('credentials.json', 'r') as f:
                creds = json.load(f)
            
            if service not in creds:
                raise KeyError(f"Service '{service}' not found in credentials.json. Available: {list(creds.keys())}")
            
            service_config = creds[service]
            
            # Create config object that S3Operations expects
            class ServiceConfig:
                def __init__(self, config):
                    self.access_key = config['access_key']
                    self.secret_key = config['secret_key']
                    self.region = config['region']
                    self.endpoint_url = config['endpoint_url']
                    self.service_name = config['name']
            
            config = ServiceConfig(service_config)
            print(f"✓ Loaded {config.service_name} configuration")
            return config
            
        except Exception as e:
            raise Exception(f"Failed to load {service} credentials: {str(e)}")
        
    def _log_result(self, test_name, result):
        """Log test result."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.test_results.append({
            'test': test_name,
            'timestamp': timestamp,
            'status': result['status'],
            'message': result.get('message', ''),
            'details': {k: v for k, v in result.items() if k not in ['status', 'message']}
        })
        
        status_symbol = "✅" if result['status'] == 'success' else "❌"
        print(f"{status_symbol} {test_name}: {result.get('message', result['status'])}")
        
        # Print additional details if available
        if result.get('details'):
            for key, value in result['details'].items():
                print(f"   {key}: {value}")

    def setup(self):
        """Setup test environment."""
        print(f"\n🔧 Setting up presigned conditional test environment...")
        print(f"� Testing service: {self.config.service_name}")
        print(f"🌐 Endpoint: {self.config.endpoint_url}")
        print(f"�📦 Test bucket: {self.test_bucket}")
        
        # Create test bucket
        result = self.s3_ops.create_bucket(self.test_bucket)
        if result['status'] != 'success':
            raise Exception(f"Failed to create test bucket: {result['message']}")
        print("✓ Test bucket created successfully")
        
        return True

    def teardown(self):
        """Cleanup test environment."""
        print(f"\n🧹 Cleaning up test environment...")
        
        # List and delete all objects first
        try:
            list_result = self.s3_ops.list_objects(self.test_bucket)
            if list_result.get('objects'):
                object_keys = [obj['Key'] for obj in list_result['objects']]
                print(f"  └─ Deleting {len(object_keys)} objects...")
                self.s3_ops.delete_objects(self.test_bucket, object_keys)
        except Exception as e:
            print(f"  └─ Warning: Could not clean objects: {e}")
        
        # Delete bucket
        try:
            result = self.s3_ops.delete_bucket(self.test_bucket)
            if result['status'] == 'success':
                print("✓ Test bucket deleted successfully")
            else:
                print(f"⚠️  Warning: {result['message']}")
        except Exception as e:
            print(f"⚠️  Warning: Could not delete bucket: {e}")

    def test_presigned_put_if_match_success(self):
        """Test: Presigned PUT URL + If-Match header (success case)"""
        print("\n[TEST] Presigned PUT with If-Match header (success case)...")
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = 'presigned_job_v1'
        with open(temp_file, 'w') as f:
            f.write(original_content)
        
        try:
            # Upload initial object to get ETag
            job_key = 'presigned-job-if-match-success.json'
            print(f"  └─ Creating initial job: {job_key}")
            
            with open(temp_file, 'rb') as f:
                response = self.s3_ops.s3_client.put_object(
                    Bucket=self.test_bucket,
                    Key=job_key,
                    Body=f
                )
            
            current_etag = response['ETag'].strip('"')
            print(f"  └─ Current ETag: {current_etag}")
            
            # Create updated content
            updated_content = 'presigned_job_v2_updated'
            with open(temp_file, 'w') as f:
                f.write(updated_content)
            
            # Generate presigned URL (without conditional parameters)
            print(f"  └─ Generating presigned PUT URL...")
            presigned_url = self.s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.test_bucket,
                    'Key': job_key
                },
                ExpiresIn=300  # 5 minutes
            )
            print(f"  └─ Generated Presigned URL:")
            print(f"     {presigned_url}")
            
            # Use presigned URL with If-Match HTTP header
            print(f"  └─ Sending PUT with If-Match HTTP header: {current_etag}")
            with open(temp_file, 'rb') as f:
                http_response = requests.put(
                    presigned_url, 
                    data=f.read(),
                    headers={'If-Match': current_etag}
                )
            
            if http_response.status_code == 200:
                new_etag = http_response.headers.get('ETag', '').strip('"')
                result = {
                    'status': 'success',
                    'message': f'✓ Presigned PUT with If-Match success! Old ETag: {current_etag}, New ETag: {new_etag}',
                    'old_etag': current_etag,
                    'new_etag': new_etag,
                    'http_status': 200,
                    'presigned_url_conditional': True,
                    'compare_and_swap': 'success'
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Presigned PUT failed with status {http_response.status_code}: {http_response.text}'
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Presigned If-Match success test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('presigned_put_if_match_success', result)
        return result['status'] == 'success'
    
    def test_presigned_put_if_match_412_failure(self):
        """Test: Presigned PUT URL + If-Match header (expect 412)"""
        print("\n[TEST] Presigned PUT with If-Match header (412 failure case)...")
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = "presigned_job_initial"
        with open(temp_file, "w") as f:
            f.write(original_content)
        
        try:
            job_key = "presigned-job-if-match-412.json"
            print(f"  └─ Creating initial job: {job_key}")
            
            # 1) Create initial object
            with open(temp_file, "rb") as f:
                resp = self.s3_ops.s3_client.put_object(
                    Bucket=self.test_bucket,
                    Key=job_key,
                    Body=f,
                )
            
            current_etag = resp["ETag"].strip('"')
            print(f"  └─ Current ETag: {current_etag}")
            
            # 2) Presign PUT (NO IfMatch in Params - it's an HTTP header, not a URL parameter)
            presigned_url = self.s3_ops.s3_client.generate_presigned_url(
                "put_object",
                Params={"Bucket": self.test_bucket, "Key": job_key},
                ExpiresIn=300,
            )
            print("  └─ Presigned URL generated (without conditional parameters).")
            print(f"     {presigned_url}")
            
            # 3) Write updated content locally
            updated_content = "presigned_job_should_fail"
            with open(temp_file, "w") as f:
                f.write(updated_content)
            
            # 4) PUT with WRONG If-Match header -> expect 412
            wrong_etag = "wrong-etag-12345"
            print(f"  └─ Attempting presigned PUT with If-Match HTTP header: {wrong_etag}")
            
            with open(temp_file, "rb") as f:
                http_resp = requests.put(
                    presigned_url,
                    data=f.read(),
                    headers={"If-Match": wrong_etag},
                )
            
            print(f"  └─ DEBUG: status={http_resp.status_code}")
            print(f"  └─ DEBUG: headers={dict(http_resp.headers)}")
            print(f"  └─ DEBUG: body={http_resp.text[:200]}")
            
            if http_resp.status_code == 412:
                result = {
                    "status": "success",
                    "message": "✓ Got expected 412 Precondition Failed for wrong If-Match",
                    "http_status": 412,
                    "error_response": "PreconditionFailed",
                    "presigned_url_conditional": True,
                }
            else:
                result = {
                    "status": "error",
                    "message": f"Expected 412, got {http_resp.status_code}: {http_resp.text}",
                    "debug_info": {
                        "note": "Your S3-compatible endpoint may not enforce If-Match on PUT, or may require it signed/handled differently.",
                        "actual_status": http_resp.status_code,
                        "response_headers": dict(http_resp.headers),
                    },
                }
            
        except Exception as e:
            result = {"status": "error", "message": f"Presigned If-Match 412 test failed: {e}"}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result("presigned_put_if_match_412_failure", result)
        return result["status"] == "success"
    
    def test_aws_cli_presigned_if_match_412_failure(self):
        """Test: AWS CLI presigned PUT with If-Match header (expect 412)"""
        print("\n[TEST] AWS CLI presigned PUT with If-Match header (412 failure case)...")
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = "aws_cli_job_initial"
        with open(temp_file, "w") as f:
            f.write(original_content)
        
        try:
            job_key = "aws-cli-presigned-job-if-match-412.json"
            print(f"  └─ Creating initial job: {job_key}")
            
            # Set environment variables for AWS CLI
            env = os.environ.copy()
            env['AWS_ACCESS_KEY_ID'] = self.config.access_key
            env['AWS_SECRET_ACCESS_KEY'] = self.config.secret_key
            env['AWS_DEFAULT_REGION'] = self.config.region
            
            # 1) Upload initial object using AWS CLI
            upload_cmd = [
                "aws", "s3", "cp", temp_file, 
                f"s3://{self.test_bucket}/{job_key}",
                "--endpoint-url", self.config.endpoint_url
            ]
            print(f"  └─ Running AWS CLI upload: {' '.join(upload_cmd)}")
            result_upload = subprocess.run(upload_cmd, capture_output=True, text=True, env=env)
            print(f"  └─ Upload return code: {result_upload.returncode}")
            print(f"  └─ Upload stdout: {result_upload.stdout}")
            print(f"  └─ Upload stderr: {result_upload.stderr}")
            if result_upload.returncode != 0:
                result = {"status": "error", "message": f"AWS CLI upload failed: {result_upload.stderr}"}
                print(f"  └─ FAILED: {result}")
                self._log_result("aws_cli_presigned_if_match_412_failure", result)
                return False
            
            # 2) Get object metadata to retrieve ETag
            head_cmd = [
                "aws", "s3api", "head-object",
                "--bucket", self.test_bucket,
                "--key", job_key,
                "--endpoint-url", self.config.endpoint_url
            ]
            print(f"  └─ Running head-object: {' '.join(head_cmd)}")
            result_head = subprocess.run(head_cmd, capture_output=True, text=True, env=env)
            print(f"  └─ Head return code: {result_head.returncode}")
            print(f"  └─ Head stdout: {result_head.stdout}")
            print(f"  └─ Head stderr: {result_head.stderr}")
            if result_head.returncode != 0:
                result = {"status": "error", "message": f"AWS CLI head-object failed: {result_head.stderr}"}
                print(f"  └─ FAILED: {result}")
                self._log_result("aws_cli_presigned_if_match_412_failure", result)
                return False
            
            metadata = json.loads(result_head.stdout)
            current_etag = metadata['ETag'].strip('"')
            print(f"  └─ Current ETag: {current_etag}")
            
            # 3) Generate presigned URL using AWS CLI
            presign_cmd = [
                "aws", "s3", "presign",
                f"s3://{self.test_bucket}/{job_key}",
                "--expires-in", "300",
                "--endpoint-url", self.config.endpoint_url
            ]
            result_presign = subprocess.run(presign_cmd, capture_output=True, text=True, env=env)
            if result_presign.returncode != 0:
                result = {"status": "error", "message": f"Failed to generate presigned URL: {result_presign.stderr}"}
            else:
                presigned_url = result_presign.stdout.strip()
                print("  └─ AWS CLI presigned URL generated.")
                print(f"     {presigned_url}")
                
                # 4) Write updated content locally
                updated_content = "aws_cli_job_should_fail"
                with open(temp_file, "w") as f:
                    f.write(updated_content)
                
                # 5) Try PUT with wrong If-Match header using requests
                wrong_etag = "wrong-etag-12345"
                print(f"  └─ Attempting presigned PUT via requests with If-Match: {wrong_etag}")
                
                # Read content to send
                with open(temp_file, 'r') as f:
                    body_content = f.read()
                
                try:
                    headers = {
                        'If-Match': wrong_etag,
                        'Content-Type': 'text/plain'
                    }
                    
                    response = requests.put(presigned_url, data=body_content, headers=headers)
                    print(f"  └─ DEBUG: AWS CLI + requests status={response.status_code}")
                    print(f"  └─ DEBUG: Response headers: {dict(response.headers)}")
                    
                    if response.status_code == 412:
                        result = {
                            "status": "success",
                            "message": "✓ AWS CLI + requests returned expected 412 Precondition Failed!",
                            "http_status": 412,
                            "method": "aws_cli_requests",
                            "presigned_url_conditional": True,
                        }
                    else:
                        result = {
                            "status": "error", 
                            "message": f"✗ AWS CLI + requests method: Expected 412, got {response.status_code}. IDrive E2 ignores conditional headers in presigned URLs!",
                            "http_status": response.status_code,
                            "method": "aws_cli_requests",
                            "presigned_url_conditional": False,
                        }
                
                except Exception as e:
                    result = {"status": "error", "message": f"Requests failed: {str(e)}"}
            
        except Exception as e:
            result = {"status": "error", "message": f"AWS CLI presigned If-Match test failed: {e}"}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result("aws_cli_presigned_if_match_412_failure", result)
        return result["status"] == "success"
    
    def test_presigned_put_if_none_match_success(self):
        """Test: Presigned PUT URL + If-None-Match: * header (success case)"""
        print("\n[TEST] Presigned PUT with If-None-Match: * header (success case)...")
        
        # Create test content for new job
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        job_content = 'presigned_exclusive_job_creation'
        with open(temp_file, 'w') as f:
            f.write(job_content)
        
        try:
            # Ensure object doesn't exist
            new_job_key = f'presigned-exclusive-job-{os.urandom(4).hex()}.json'
            print(f"  └─ Creating new exclusive job via presigned URL: {new_job_key}")
            
            # Generate presigned URL (without conditional parameters)
            presigned_url = self.s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.test_bucket,
                    'Key': new_job_key
                },
                ExpiresIn=300  # 5 minutes
            )
            print(f"  └─ Generated Presigned URL:")
            print(f"     {presigned_url}")
            
            # Use presigned URL with If-None-Match HTTP header
            print(f"  └─ Sending PUT with If-None-Match: * HTTP header")
            with open(temp_file, 'rb') as f:
                http_response = requests.put(
                    presigned_url, 
                    data=f.read(),
                    headers={'If-None-Match': '*'}
                )
            
            if http_response.status_code == 200:
                etag = http_response.headers.get('ETag', '').strip('"')
                result = {
                    'status': 'success',
                    'message': f'✓ Presigned PUT If-None-Match success! Exclusive job created. ETag: {etag}',
                    'job_key': new_job_key,
                    'etag': etag,
                    'http_status': 200,
                    'presigned_url_conditional': True,
                    'exclusive_create': True
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Presigned PUT If-None-Match failed with status {http_response.status_code}: {http_response.text}'
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Presigned If-None-Match success test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('presigned_put_if_none_match_success', result)
        return result['status'] == 'success'
    
    def test_presigned_put_if_none_match_409_failure(self):
        """Test: Presigned PUT URL + If-None-Match: * header (expect 409/412)"""
        print("\n[TEST] Presigned PUT with If-None-Match: * header (409 Conflict case)...")
        
        # Create test content
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        initial_content = 'presigned_existing_job'
        with open(temp_file, 'w') as f:
            f.write(initial_content)
        
        try:
            # Upload initial object (ensure it exists)
            existing_job_key = 'presigned-existing-job-conflict.json'
            result_upload = self.s3_ops.put_object(self.test_bucket, existing_job_key, temp_file)
            if result_upload['status'] != 'success':
                return False
                
            print(f"  └─ Object exists. Attempting exclusive create via presigned URL with If-None-Match: *")
            
            # Generate presigned URL (without conditional parameters)
            presigned_url = self.s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.test_bucket,
                    'Key': existing_job_key
                },
                ExpiresIn=300  # 5 minutes
            )
            print(f"  └─ Generated Presigned URL:")
            print(f"     {presigned_url}")
            
            # Try to create with If-None-Match HTTP header (should fail)
            duplicate_content = 'presigned_attempted_duplicate'
            with open(temp_file, 'w') as f:
                f.write(duplicate_content)
            
            print(f"  └─ Sending PUT with If-None-Match: * HTTP header")
            with open(temp_file, 'rb') as f:
                http_response = requests.put(
                    presigned_url, 
                    data=f.read(),
                    headers={'If-None-Match': '*'}
                )
            
            print(f"  └─ DEBUG: status={http_response.status_code}")
            print(f"  └─ DEBUG: headers={dict(http_response.headers)}")
            print(f"  └─ DEBUG: body={http_response.text[:200]}")
            
            # Should get 409 Conflict or 412 Precondition Failed
            if http_response.status_code in [409, 412]:
                error_name = 'Conflict' if http_response.status_code == 409 else 'PreconditionFailed'
                result = {
                    'status': 'success',
                    'message': f'✓ Presigned PUT If-None-Match returned expected {http_response.status_code} {error_name}!',
                    'http_status': http_response.status_code,
                    'error_response': error_name,
                    'presigned_url_conditional': True,
                    'duplicate_prevention': 'working'
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Expected 409 Conflict or 412 Precondition Failed, got {http_response.status_code}: {http_response.text}',
                    'debug_info': {
                        'note': 'Your S3-compatible endpoint may not enforce If-None-Match on PUT',
                        'actual_status': http_response.status_code,
                        'response_headers': dict(http_response.headers)
                    }
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Presigned If-None-Match 409/412 test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('presigned_put_if_none_match_409_failure', result)
        return result['status'] == 'success'

    def run_all_tests(self):
        """Run all presigned conditional PUT tests."""
        print("🚀 Starting Presigned URL Conditional PUT Tests")
        print("=" * 60)
        
        tests = [
            ('setup', self.setup),
            ('presigned_put_if_match_success', self.test_presigned_put_if_match_success),
            ('presigned_put_if_match_412_failure', self.test_presigned_put_if_match_412_failure),
            ('aws_cli_presigned_if_match_412_failure', self.test_aws_cli_presigned_if_match_412_failure),
            ('presigned_put_if_none_match_success', self.test_presigned_put_if_none_match_success),
            ('presigned_put_if_none_match_409_failure', self.test_presigned_put_if_none_match_409_failure),
        ]
        
        passed = 0
        failed = 0
        
        try:
            for test_name, test_func in tests:
                if test_name == 'setup':
                    if not test_func():
                        print("❌ Setup failed, aborting tests")
                        return
                    continue
                
                try:
                    success = test_func()
                    if success:
                        passed += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"❌ {test_name}: Exception occurred: {str(e)}")
                    failed += 1
        
        finally:
            # Always run teardown
            self.teardown()
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"📝 Total: {passed + failed}")
        
        if failed == 0:
            print("\n🎉 All presigned conditional PUT tests PASSED!")
        else:
            print(f"\n⚠️  {failed} test(s) failed. Check the output above for details.")
        
        # Print detailed results
        if self.test_results:
            print(f"\n📋 DETAILED RESULTS")
            print("-" * 60)
            for result in self.test_results:
                status_icon = "✅" if result['status'] == 'success' else "❌"
                print(f"{status_icon} {result['test']}")
                print(f"   Time: {result['timestamp']}")
                print(f"   Message: {result['message']}")
                if result.get('details'):
                    for key, value in result['details'].items():
                        print(f"   {key}: {value}")
                print()


def main():
    """Main function to run the presigned conditional PUT tests."""
    print("Presigned URL Conditional PUT Test Suite")
    print("========================================")
    print()
    print("This test suite focuses on testing presigned URLs with conditional headers")
    print("for distributed job queue systems and atomic operations.")
    print()
    
    # Default to IDrive E2, but allow override via environment variable
    service = os.environ.get('S3_SERVICE', 'e2')
    
    try:
        test_suite = PresignedConditionalTestSuite(service=service)
        test_suite.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"\n\n💥 Fatal error: {str(e)}")
        print("\nAvailable services in credentials.json:")
        try:
            with open('credentials.json', 'r') as f:
                creds = json.load(f)
            for service_key, config in creds.items():
                print(f"  - {service_key}: {config.get('name', 'Unknown')}")
            print(f"\nTo test a different service, set S3_SERVICE environment variable:")
            print(f"  $env:S3_SERVICE='aws'; python presigned_conditional_tests.py")
        except:
            pass
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())