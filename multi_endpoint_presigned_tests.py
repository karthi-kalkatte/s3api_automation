#!/usr/bin/env python3
"""
Multi-Endpoint Presigned URL Conditional PUT Tests
==================================================

This enhanced test suite runs presigned URL conditional tests against multiple
S3 endpoints (IDrive E2 vs Amazon S3) and compares their behavior.

Tests both services to validate:
- Compare-and-swap operations (If-Match)
- Exclusive resource creation (If-None-Match)
- Error handling consistency
- Conditional header enforcement
"""

import os
import sys
import json
import tempfile
import subprocess
import requests
from datetime import datetime
from typing import Dict, List

# Add the current directory to Python path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_operations import S3Operations
import boto3


class MultiEndpointConfig:
    """Configuration for multiple S3 endpoints."""
    
    def __init__(self, access_key, secret_key, region, endpoint_url, name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.endpoint_url = endpoint_url
        self.name = name


class MultiEndpointPresignedTestSuite:
    """Enhanced test suite for comparing presigned URL behavior across multiple S3 services."""

    def __init__(self):
        self.endpoints = self._load_endpoints()
        self.test_results = {}
        self.comparison_results = []
        
    def _load_endpoints(self) -> Dict[str, MultiEndpointConfig]:
        """Load endpoint configurations from credentials.json."""
        endpoints = {}
        
        try:
            with open('credentials.json', 'r') as f:
                creds = json.load(f)
            
            for service_key, config in creds.items():
                endpoints[service_key] = MultiEndpointConfig(
                    access_key=config['access_key'],
                    secret_key=config['secret_key'],
                    region=config['region'],
                    endpoint_url=config['endpoint_url'],
                    name=config['name']
                )
                print(f"✓ Loaded {config['name']} configuration")
                
        except Exception as e:
            raise Exception(f"Failed to load credentials: {str(e)}")
        
        return endpoints

    def _create_s3_operations(self, config: MultiEndpointConfig) -> S3Operations:
        """Create S3Operations instance for a specific endpoint."""
        # Create a temporary config object that S3Operations expects
        class TempConfig:
            def __init__(self, config):
                self.access_key = config.access_key
                self.secret_key = config.secret_key
                self.region = config.region
                self.endpoint_url = config.endpoint_url
        
        return S3Operations(TempConfig(config))

    def _log_result(self, endpoint_key: str, test_name: str, result: Dict):
        """Log test result for a specific endpoint."""
        if endpoint_key not in self.test_results:
            self.test_results[endpoint_key] = []
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.test_results[endpoint_key].append({
            'test': test_name,
            'timestamp': timestamp,
            'status': result['status'],
            'message': result.get('message', ''),
            'details': {k: v for k, v in result.items() if k not in ['status', 'message']}
        })
        
        endpoint_name = self.endpoints[endpoint_key].name
        status_symbol = "✅" if result['status'] == 'success' else "❌"
        print(f"    {status_symbol} [{endpoint_name}] {test_name}: {result.get('message', result['status'])}")

    def test_presigned_put_if_match_success(self, endpoint_key: str, config: MultiEndpointConfig, s3_ops: S3Operations, test_bucket: str):
        """Test presigned PUT with If-Match header (success case) for a specific endpoint."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = f'{endpoint_key}_job_v1'
        
        with open(temp_file, 'w') as f:
            f.write(original_content)
        
        try:
            job_key = f'{endpoint_key}-presigned-if-match-success.json'
            
            # Upload initial object
            with open(temp_file, 'rb') as f:
                response = s3_ops.s3_client.put_object(
                    Bucket=test_bucket,
                    Key=job_key,
                    Body=f
                )
            
            current_etag = response['ETag'].strip('"')
            
            # Create updated content
            updated_content = f'{endpoint_key}_job_v2_updated'
            with open(temp_file, 'w') as f:
                f.write(updated_content)
            
            # Generate presigned URL
            presigned_url = s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': test_bucket, 'Key': job_key},
                ExpiresIn=300
            )
            
            # Use presigned URL with If-Match HTTP header
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
                    'message': f'✓ Compare-and-swap successful! ETag: {current_etag} → {new_etag}',
                    'old_etag': current_etag,
                    'new_etag': new_etag,
                    'http_status': 200,
                    'conditional_support': True
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Failed with status {http_response.status_code}',
                    'http_status': http_response.status_code
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result(endpoint_key, 'presigned_put_if_match_success', result)
        return result

    def test_presigned_put_if_match_412_failure(self, endpoint_key: str, config: MultiEndpointConfig, s3_ops: S3Operations, test_bucket: str):
        """Test presigned PUT with If-Match header (expect 412) for a specific endpoint."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = f'{endpoint_key}_job_initial'
        
        with open(temp_file, 'w') as f:
            f.write(original_content)
        
        try:
            job_key = f'{endpoint_key}-presigned-if-match-412.json'
            
            # Create initial object
            with open(temp_file, 'rb') as f:
                resp = s3_ops.s3_client.put_object(
                    Bucket=test_bucket,
                    Key=job_key,
                    Body=f
                )
            
            # Update content locally
            updated_content = f'{endpoint_key}_job_should_fail'
            with open(temp_file, 'w') as f:
                f.write(updated_content)
            
            # Generate presigned URL
            presigned_url = s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': test_bucket, 'Key': job_key},
                ExpiresIn=300
            )
            
            # Try PUT with wrong If-Match header
            wrong_etag = 'wrong-etag-12345'
            with open(temp_file, 'rb') as f:
                http_resp = requests.put(
                    presigned_url,
                    data=f.read(),
                    headers={'If-Match': wrong_etag}
                )
            
            if http_resp.status_code == 412:
                result = {
                    'status': 'success',
                    'message': '✓ Correctly returned 412 Precondition Failed',
                    'http_status': 412,
                    'conditional_support': True,
                    'race_protection': True
                }
            elif http_resp.status_code == 200:
                result = {
                    'status': 'error',
                    'message': '✗ IGNORED conditional header! Overwrote without checking ETag',
                    'http_status': 200,
                    'conditional_support': False,
                    'race_protection': False
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Unexpected status {http_resp.status_code}',
                    'http_status': http_resp.status_code,
                    'conditional_support': False
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result(endpoint_key, 'presigned_put_if_match_412_failure', result)
        return result

    def test_presigned_put_if_none_match_success(self, endpoint_key: str, config: MultiEndpointConfig, s3_ops: S3Operations, test_bucket: str):
        """Test presigned PUT with If-None-Match (success case) for a specific endpoint."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        job_content = f'{endpoint_key}_exclusive_job_creation'
        
        with open(temp_file, 'w') as f:
            f.write(job_content)
        
        try:
            new_job_key = f'{endpoint_key}-exclusive-job-{os.urandom(4).hex()}.json'
            
            # Generate presigned URL
            presigned_url = s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': test_bucket, 'Key': new_job_key},
                ExpiresIn=300
            )
            
            # Use presigned URL with If-None-Match header
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
                    'message': f'✓ Exclusive creation successful! ETag: {etag}',
                    'job_key': new_job_key,
                    'etag': etag,
                    'http_status': 200,
                    'conditional_support': True,
                    'exclusive_create': True
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Failed with status {http_response.status_code}',
                    'http_status': http_response.status_code
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result(endpoint_key, 'presigned_put_if_none_match_success', result)
        return result

    def test_presigned_put_if_none_match_409_failure(self, endpoint_key: str, config: MultiEndpointConfig, s3_ops: S3Operations, test_bucket: str):
        """Test presigned PUT with If-None-Match (expect 409/412) for a specific endpoint."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        initial_content = f'{endpoint_key}_existing_job'
        
        with open(temp_file, 'w') as f:
            f.write(initial_content)
        
        try:
            existing_job_key = f'{endpoint_key}-existing-job-conflict.json'
            
            # Upload initial object (ensure it exists)
            with open(temp_file, 'rb') as f:
                s3_ops.s3_client.put_object(
                    Bucket=test_bucket,
                    Key=existing_job_key,
                    Body=f
                )
            
            # Generate presigned URL
            presigned_url = s3_ops.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': test_bucket, 'Key': existing_job_key},
                ExpiresIn=300
            )
            
            # Try to create with If-None-Match (should fail)
            duplicate_content = f'{endpoint_key}_attempted_duplicate'
            with open(temp_file, 'w') as f:
                f.write(duplicate_content)
            
            with open(temp_file, 'rb') as f:
                http_response = requests.put(
                    presigned_url, 
                    data=f.read(),
                    headers={'If-None-Match': '*'}
                )
            
            if http_response.status_code in [409, 412]:
                error_name = 'Conflict' if http_response.status_code == 409 else 'PreconditionFailed'
                result = {
                    'status': 'success',
                    'message': f'✓ Correctly returned {http_response.status_code} {error_name}',
                    'http_status': http_response.status_code,
                    'conditional_support': True,
                    'duplicate_prevention': True
                }
            elif http_response.status_code == 200:
                result = {
                    'status': 'error',
                    'message': '✗ IGNORED If-None-Match! Allowed duplicate creation',
                    'http_status': 200,
                    'conditional_support': False,
                    'duplicate_prevention': False
                }
            else:
                result = {
                    'status': 'error',
                    'message': f'Unexpected status {http_response.status_code}',
                    'http_status': http_response.status_code,
                    'conditional_support': False
                }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'Test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result(endpoint_key, 'presigned_put_if_none_match_409_failure', result)
        return result

    def run_endpoint_tests(self, endpoint_key: str, config: MultiEndpointConfig):
        """Run all tests for a specific endpoint."""
        print(f"\n🔧 Testing {config.name} ({config.endpoint_url})")
        print("=" * 60)
        
        s3_ops = self._create_s3_operations(config)
        test_bucket = f"multi-test-bucket-{endpoint_key}-{os.urandom(4).hex()}"
        
        try:
            # Setup
            print(f"  📦 Creating test bucket: {test_bucket}")
            result = s3_ops.create_bucket(test_bucket)
            if result['status'] != 'success':
                raise Exception(f"Failed to create test bucket: {result['message']}")
            
            # Run tests
            tests = [
                ('if_match_success', self.test_presigned_put_if_match_success),
                ('if_match_412_failure', self.test_presigned_put_if_match_412_failure),
                ('if_none_match_success', self.test_presigned_put_if_none_match_success),
                ('if_none_match_409_failure', self.test_presigned_put_if_none_match_409_failure),
            ]
            
            endpoint_results = {}
            for test_name, test_func in tests:
                print(f"\n  🧪 Running {test_name}...")
                try:
                    result = test_func(endpoint_key, config, s3_ops, test_bucket)
                    endpoint_results[test_name] = result
                except Exception as e:
                    print(f"    ❌ Test failed with exception: {str(e)}")
                    endpoint_results[test_name] = {'status': 'error', 'message': str(e)}
            
            return endpoint_results
            
        finally:
            # Cleanup
            try:
                print(f"\n  🧹 Cleaning up {test_bucket}...")
                # List and delete all objects first
                list_result = s3_ops.list_objects(test_bucket)
                if list_result.get('objects'):
                    object_keys = [obj['Key'] for obj in list_result['objects']]
                    s3_ops.delete_objects(test_bucket, object_keys)
                
                # Delete bucket
                s3_ops.delete_bucket(test_bucket)
                print(f"    ✓ Cleanup complete")
            except Exception as e:
                print(f"    ⚠️  Cleanup warning: {e}")

    def compare_results(self):
        """Compare test results across endpoints and generate comparison report."""
        print("\n" + "="*80)
        print("🔍 MULTI-ENDPOINT COMPARISON REPORT")
        print("="*80)
        
        # Get all endpoints and test names
        endpoints = list(self.test_results.keys())
        if len(endpoints) < 2:
            print("❌ Need at least 2 endpoints to compare")
            return
        
        test_names = set()
        for results in self.test_results.values():
            test_names.update([r['test'] for r in results])
        
        # Compare each test across endpoints
        for test_name in sorted(test_names):
            print(f"\n📋 Test: {test_name}")
            print("-" * 50)
            
            results_by_endpoint = {}
            for endpoint in endpoints:
                endpoint_results = self.test_results.get(endpoint, [])
                test_result = next((r for r in endpoint_results if r['test'] == test_name), None)
                results_by_endpoint[endpoint] = test_result
            
            # Compare results
            statuses = [r['status'] if r else 'missing' for r in results_by_endpoint.values()]
            all_success = all(s == 'success' for s in statuses if s != 'missing')
            all_same = len(set(statuses)) <= 1
            
            if all_same and all_success:
                print("✅ CONSISTENT SUCCESS - All endpoints behave identically")
            elif all_same:
                print(f"⚠️  CONSISTENT FAILURE - All endpoints failed similarly")
            else:
                print("❌ INCONSISTENT BEHAVIOR - Endpoints behave differently!")
            
            for endpoint, result in results_by_endpoint.items():
                endpoint_name = self.endpoints[endpoint].name
                if result:
                    status_icon = "✅" if result['status'] == 'success' else "❌"
                    print(f"  {status_icon} {endpoint_name:15}: {result['message']}")
                    
                    # Show important differences
                    if result.get('details'):
                        for key, value in result['details'].items():
                            if key in ['conditional_support', 'race_protection', 'duplicate_prevention']:
                                print(f"     └─ {key}: {value}")
                else:
                    print(f"  ❓ {endpoint_name:15}: Test not run")
        
        # Overall comparison summary
        print(f"\n🎯 OVERALL ASSESSMENT")
        print("-" * 50)
        
        for endpoint in endpoints:
            endpoint_name = self.endpoints[endpoint].name
            results = self.test_results.get(endpoint, [])
            
            passed = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'error')
            total = len(results)
            
            # Check conditional support
            conditional_tests = [r for r in results if 'conditional_support' in r.get('details', {})]
            conditional_support = all(r['details']['conditional_support'] for r in conditional_tests if r['status'] == 'success')
            
            print(f"{endpoint_name:15}: {passed}/{total} tests passed")
            if conditional_tests:
                support_icon = "✅" if conditional_support else "❌"
                print(f"                 {support_icon} Conditional headers: {'SUPPORTED' if conditional_support else 'NOT SUPPORTED'}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS")
        print("-" * 50)
        
        aws_results = self.test_results.get('aws', [])
        e2_results = self.test_results.get('e2', [])
        
        if aws_results and e2_results:
            aws_conditional = any(r.get('details', {}).get('conditional_support') for r in aws_results if r['status'] == 'success')
            e2_conditional = any(r.get('details', {}).get('conditional_support') for r in e2_results if r['status'] == 'success')
            
            if aws_conditional and e2_conditional:
                print("✅ Both services support conditional headers - Use presigned URLs safely!")
            elif aws_conditional and not e2_conditional:
                print("⚠️  AWS supports conditionals, E2 does not - Use direct API calls for E2")
            elif not aws_conditional and e2_conditional:
                print("⚠️  E2 supports conditionals, AWS does not - Use direct API calls for AWS")
            else:
                print("❌ Neither service supports conditional headers reliably - Use direct API calls!")

    def run_all_tests(self):
        """Run tests for all configured endpoints and compare results."""
        print("🚀 Multi-Endpoint Presigned URL Conditional Tests")
        print("=" * 60)
        print(f"📡 Testing {len(self.endpoints)} S3 endpoints")
        
        # Run tests for each endpoint
        for endpoint_key, config in self.endpoints.items():
            try:
                self.run_endpoint_tests(endpoint_key, config)
            except Exception as e:
                print(f"❌ Failed to test {config.name}: {str(e)}")
        
        # Compare results
        self.compare_results()


def main():
    """Main function."""
    print("Multi-Endpoint Presigned URL Conditional Test Suite")
    print("==================================================")
    print()
    print("Comparing presigned URL conditional header behavior across multiple S3 services")
    print()
    
    try:
        test_suite = MultiEndpointPresignedTestSuite()
        test_suite.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"\n\n💥 Fatal error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())