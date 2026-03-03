"""S3 API test suite."""
import os
import tempfile
from config import S3Config
from s3_operations import S3Operations

class S3TestSuite:
    """Test cases for S3 API operations."""

    def __init__(self):
        self.config = S3Config()
        self.s3_ops = S3Operations(self.config)
        self.test_results = []
        self.test_bucket = f"test-bucket-{os.urandom(4).hex()}"
        self.test_bucket_lock = f"test-bucket-lock-{os.urandom(4).hex()}"
        self.test_object_key = "test-object.txt"
        self.test_object_key2 = "test-object-copy.txt"
        self.test_object_lock_key = "test-object-lock-retention.txt"
        self.test_object_5mb = "test-object-5mb.bin"
        self.test_object_10mb = "test-object-10mb.bin"
        self.test_object_1kb = "test-object-1kb.bin"
        self.test_object_50mb = "test-object-50mb.bin"
        self.test_file_path = None
        self.test_file_path2 = None
        self.test_file_path_5mb = None
        self.test_file_path_10mb = None
        self.test_file_path_1kb = None
        self.test_file_path_50mb = None

    # ...existing code...

    def test_put_object_tagging(self):
        """Test: Put object tagging"""
        print("\n[TEST] Adding object tags...")
        tags = {'Version': '1.0', 'Type': 'Test'}
        result = self.s3_ops.put_object_tagging(self.test_bucket, self.test_object_key, tags)
        self._log_result('put_object_tagging', result)
        return result['status'] == 'success'

    def test_put_object_if_match(self):
        """Test: Put object with If-Match (simulate conditional put by ETag check)"""
        print("\n[TEST] Putting object with If-Match...")
        
        # Create a temporary file with content "a"
        temp_file_a = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        with open(temp_file_a, 'w') as f:
            f.write('a')
        
        try:
            # Upload object with content "a" first
            print("  └─ Uploading object with content 'a'...")
            result_upload = self.s3_ops.put_object(self.test_bucket, self.test_object_key, temp_file_a)
            if result_upload['status'] != 'success':
                return False
                
            # Get ETag of the uploaded object
            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key=self.test_object_key)
            original_etag = meta['ETag'].strip('"')
            print(f"  └─ Original ETag: {original_etag}")
            
            # Create another file with different content 
            temp_file_b = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
            with open(temp_file_b, 'w') as f:
                f.write('b')  # Different content
            
            try:
                # Try to upload with If-Match using the original ETag (should succeed since ETag matches)
                print("  └─ Attempting PUT with If-Match condition...")
                result = self.s3_ops.put_object_with_conditions(self.test_bucket, self.test_object_key, temp_file_b, if_match=original_etag)
                self._log_result('put_object_if_match', result)
                return result['status'] == 'success'
            finally:
                # Clean up second temp file
                if os.path.exists(temp_file_b):
                    os.remove(temp_file_b)
        finally:
            # Clean up first temp file
            if os.path.exists(temp_file_a):
                os.remove(temp_file_a)

    def test_put_object_if_match_fails(self):
        """Test: Put object with If-Match fails when ETag doesn't match"""
        print("\n[TEST] Putting object with If-Match (should fail with wrong ETag)...")
        
        # Create and upload initial file
        temp_file_a = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        with open(temp_file_a, 'w') as f:
            f.write('initial content')
        
        try:
            # Upload initial object
            result_upload = self.s3_ops.put_object(self.test_bucket, self.test_object_key, temp_file_a)
            if result_upload['status'] != 'success':
                return False
            
            # Create another file with different content 
            temp_file_b = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
            with open(temp_file_b, 'w') as f:
                f.write('modified content')
            
            try:
                # Try to upload with wrong/fake ETag (should fail with 412)
                fake_etag = "wrong-etag-12345"
                print(f"  └─ Attempting PUT with wrong ETag: {fake_etag}")
                result = self.s3_ops.put_object_with_conditions(self.test_bucket, self.test_object_key, temp_file_b, if_match=fake_etag)
                
                # For If-Match failure test, 'error' status with 412/precondition failed is the expected success
                if result['status'] == 'error' and ('412' in result.get('message', '') or 'PreconditionFailed' in result.get('message', '') or 'does not match' in result.get('message', '')):
                    result['status'] = 'success'
                    result['message'] = f"✓ If-Match correctly failed with wrong ETag: {result.get('message', '')}"
                
                self._log_result('put_object_if_match_fails', result)
                return result['status'] == 'success'
            finally:
                if os.path.exists(temp_file_b):
                    os.remove(temp_file_b)
        finally:
            if os.path.exists(temp_file_a):
                os.remove(temp_file_a)

    def test_put_object_if_not_match(self):
        """Test: Put object with If-None-Match (simulate conditional put by ETag check)"""
        print("\n[TEST] Putting object with If-None-Match...")
        # Upload object first
        self.s3_ops.put_object(self.test_bucket, self.test_object_key, self.test_file_path)
        # Get ETag
        meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key=self.test_object_key)
        etag = meta['ETag'].strip('"')
        # Try to upload with If-None-Match (should NOT overwrite, so 'error' is expected and is a success for this test)
        result = self.s3_ops.put_object_with_conditions(self.test_bucket, self.test_object_key, self.test_file_path, if_none_match=etag)
        # Treat ETag match prevention as a success
        if result['status'] == 'error' and 'ETag matches' in result.get('message', ''):
            result['status'] = 'success'
        self._log_result('put_object_if_not_match', result)
        return result['status'] == 'success'

    def test_get_object_tagging(self):
        """Test: Get object tagging"""
        print("\n[TEST] Getting object tags...")
        result = self.s3_ops.get_object_tagging(self.test_bucket, self.test_object_key)
        self._log_result('get_object_tagging', result)
        return result['status'] == 'success'
    
    def setup(self):
        """Setup test environment."""
        # Create temporary test files
        self.test_file_path = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        with open(self.test_file_path, 'w') as f:
            f.write('This is a test file for S3 automation testing.')
        
        self.test_file_path2 = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        with open(self.test_file_path2, 'w') as f:
            f.write('This is a second test file.')
        
        # Create 1KB test file
        self.test_file_path_1kb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        with open(self.test_file_path_1kb, 'wb') as f:
            # Write 1KB of data
            f.write(b'X' * 1024)
        
        # Create 5MB test file
        self.test_file_path_5mb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        with open(self.test_file_path_5mb, 'wb') as f:
            # Write 5MB of data (5 * 1024 * 1024 bytes)
            chunk = b'0' * (1024 * 1024)  # 1MB chunk
            for _ in range(5):
                f.write(chunk)
        
        # Create 10MB test file
        self.test_file_path_10mb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        with open(self.test_file_path_10mb, 'wb') as f:
            # Write 10MB of data (10 * 1024 * 1024 bytes)
            chunk = b'A' * (1024 * 1024)  # 1MB chunk
            for _ in range(10):
                f.write(chunk)
        
        # Create 50MB test file
        self.test_file_path_50mb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        with open(self.test_file_path_50mb, 'wb') as f:
            # Write 50MB of data (50 * 1024 * 1024 bytes)
            chunk = b'X' * (1024 * 1024)  # 1MB chunk
            for _ in range(50):
                f.write(chunk)
        
        print(f"✓ Setup complete - Test files created (1KB, 5MB, 10MB, and 50MB files)")
    
    def teardown(self):
        """Cleanup test environment."""
        for file_path in [self.test_file_path, self.test_file_path2, self.test_file_path_5mb, self.test_file_path_10mb, self.test_file_path_1kb, self.test_file_path_50mb]:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
        print("✓ Cleanup complete - Test files removed")
    
    def test_create_bucket(self):
        """Test: Create bucket"""
        print("\n[TEST] Creating bucket...")
        result = self.s3_ops.create_bucket(self.test_bucket)
        self._log_result('create_bucket', result)
        return result['status'] == 'success'
    
    def test_put_object(self):
        """Test: Put object"""
        print("\n[TEST] Putting object...")
        result = self.s3_ops.put_object(self.test_bucket, self.test_object_key, self.test_file_path)
        self._log_result('put_object', result)
        return result['status'] == 'success'
    
    def test_head_object(self):
        """Test: Head object"""
        print("\n[TEST] Checking object metadata...")
        result = self.s3_ops.head_object(self.test_bucket, self.test_object_key)
        self._log_result('head_object', result)
        return result['status'] == 'success'
    
    def test_list_objects(self):
        """Test: List objects"""
        print("\n[TEST] Listing objects...")
        result = self.s3_ops.list_objects(self.test_bucket)
        self._log_result('list_objects', result)
        return result['status'] == 'success' and result['count'] > 0
    
    def test_delete_object(self):
        """Test: Delete object"""
        print("\n[TEST] Deleting object...")
        result = self.s3_ops.delete_object(self.test_bucket, self.test_object_key)
        self._log_result('delete_object', result)
        return result['status'] == 'success'
    
    def test_delete_bucket(self):
        """Test: Delete bucket"""
        print("\n[TEST] Deleting bucket...")
        result = self.s3_ops.delete_bucket(self.test_bucket)
        self._log_result('delete_bucket', result)
        return result['status'] == 'success'
    
    def test_enable_bucket_versioning(self):
        """Test: Enable bucket versioning"""
        print("\n[TEST] Enabling bucket versioning...")
        result = self.s3_ops.enable_bucket_versioning(self.test_bucket)
        self._log_result('enable_bucket_versioning', result)
        return result['status'] == 'success'
    
    def test_get_bucket_versioning(self):
        """Test: Get bucket versioning status"""
        print("\n[TEST] Getting bucket versioning status...")
        result = self.s3_ops.get_bucket_versioning(self.test_bucket)
        self._log_result('get_bucket_versioning', result)
        return result['status'] == 'success' and result.get('versioning_status') == 'Enabled'
    
    def test_suspend_bucket_versioning(self):
        """Test: Suspend bucket versioning"""
        print("\n[TEST] Suspending bucket versioning...")
        result = self.s3_ops.suspend_bucket_versioning(self.test_bucket)
        self._log_result('suspend_bucket_versioning', result)
        return result['status'] == 'success'
    
    # ============ BUCKET LEVEL TESTS ============
    
    def test_head_bucket(self):
        """Test: Head bucket"""
        print("\n[TEST] Checking bucket existence...")
        result = self.s3_ops.head_bucket(self.test_bucket)
        self._log_result('head_bucket', result)
        return result['status'] == 'success'
    
    def test_get_bucket_location(self):
        """Test: Get bucket location"""
        print("\n[TEST] Getting bucket location...")
        result = self.s3_ops.get_bucket_location(self.test_bucket)
        self._log_result('get_bucket_location', result)
        return result['status'] == 'success'
    
    def test_put_bucket_acl(self):
        """Test: Put bucket ACL"""
        print("\n[TEST] Setting bucket ACL...")
        result = self.s3_ops.put_bucket_acl(self.test_bucket, 'private')
        self._log_result('put_bucket_acl', result)
        return result['status'] == 'success'
    
    def test_get_bucket_acl(self):
        """Test: Get bucket ACL"""
        print("\n[TEST] Getting bucket ACL...")
        result = self.s3_ops.get_bucket_acl(self.test_bucket)
        self._log_result('get_bucket_acl', result)
        return result['status'] == 'success'
    
    def test_put_bucket_tagging(self):
        """Test: Put bucket tagging"""
        print("\n[TEST] Adding bucket tags...")
        tags = {'Environment': 'Test', 'Project': 'S3Automation'}
        result = self.s3_ops.put_bucket_tagging(self.test_bucket, tags)
        self._log_result('put_bucket_tagging', result)
        return result['status'] == 'success'
    
    def test_get_bucket_tagging(self):
        """Test: Get bucket tagging"""
        print("\n[TEST] Getting bucket tags...")
        result = self.s3_ops.get_bucket_tagging(self.test_bucket)
        self._log_result('get_bucket_tagging', result)
        return result['status'] == 'success'
    
    def test_delete_bucket_tagging(self):
        """Test: Delete bucket tagging"""
        print("\n[TEST] Deleting bucket tags...")
        result = self.s3_ops.delete_bucket_tagging(self.test_bucket)
        self._log_result('delete_bucket_tagging', result)
        return result['status'] == 'success'
    
    def test_put_bucket_cors(self):
        """Test: Put bucket CORS"""
        print("\n[TEST] Adding CORS configuration...")
        cors_rules = [{
            'AllowedMethods': ['GET', 'PUT'],
            'AllowedOrigins': ['*'],
            'AllowedHeaders': ['*']
        }]
        result = self.s3_ops.put_bucket_cors(self.test_bucket, cors_rules)
        self._log_result('put_bucket_cors', result)
        return result['status'] == 'success'
    
    def test_get_bucket_cors(self):
        """Test: Get bucket CORS"""
        print("\n[TEST] Getting CORS configuration...")
        result = self.s3_ops.get_bucket_cors(self.test_bucket)
        self._log_result('get_bucket_cors', result)
        return result['status'] == 'success'
    
    def test_delete_bucket_cors(self):
        """Test: Delete bucket CORS"""
        print("\n[TEST] Deleting CORS configuration...")
        result = self.s3_ops.delete_bucket_cors(self.test_bucket)
        self._log_result('delete_bucket_cors', result)
        return result['status'] == 'success'
    
    def test_put_public_access_block(self):
        """Test: Put public access block"""
        print("\n[TEST] Blocking public access...")
        result = self.s3_ops.put_public_access_block(self.test_bucket, block_all=True)
        self._log_result('put_public_access_block', result)
        return result['status'] == 'success'
    
    def test_get_public_access_block(self):
        """Test: Get public access block configuration..."""
        print("\n[TEST] Getting public access block configuration...")
        result = self.s3_ops.get_public_access_block(self.test_bucket)
        self._log_result('get_public_access_block', result)
        return result['status'] == 'success'
    
    # ============ BUCKET POLICY TESTS ============
    
    def test_put_bucket_policy(self):
        """Test: Put bucket policy"""
        print("\n[TEST] Applying bucket policy...")
        result = self.s3_ops.put_bucket_policy(self.test_bucket)
        self._log_result('put_bucket_policy', result)
        return result['status'] == 'success'
    
    def test_get_bucket_policy(self):
        """Test: Get bucket policy"""
        print("\n[TEST] Getting bucket policy...")
        result = self.s3_ops.get_bucket_policy(self.test_bucket)
        self._log_result('get_bucket_policy', result)
        return result['status'] == 'success'
    
    def test_delete_bucket_policy(self):
        """Test: Delete bucket policy"""
        print("\n[TEST] Deleting bucket policy...")
        result = self.s3_ops.delete_bucket_policy(self.test_bucket)
        self._log_result('delete_bucket_policy', result)
        return result['status'] == 'success'
    
    # ============ OBJECT LEVEL TESTS ============
    
    def test_get_object(self):
        """Test: Get object"""
        print("\n[TEST] Downloading object...")
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.txt').name
        result = self.s3_ops.get_object(self.test_bucket, self.test_object_key, download_path)
        self._log_result('get_object', result)
        if os.path.exists(download_path):
            os.remove(download_path)
        return result['status'] == 'success'
    
    def test_copy_object(self):
        """Test: Copy object"""
        print("\n[TEST] Copying object...")
        result = self.s3_ops.copy_object(
            self.test_bucket, self.test_object_key,
            self.test_bucket, self.test_object_key2
        )
        self._log_result('copy_object', result)
        return result['status'] == 'success'
    
    def test_put_object_acl(self):
        """Test: Put object ACL"""
        print("\n[TEST] Setting object ACL...")
        result = self.s3_ops.put_object_acl(self.test_bucket, self.test_object_key, 'private')
        self._log_result('put_object_acl', result)
        return result['status'] == 'success'
    
    def test_get_object_acl(self):
        """Test: Get object ACL"""
        print("\n[TEST] Getting object ACL...")
        result = self.s3_ops.get_object_acl(self.test_bucket, self.test_object_key)
        self._log_result('get_object_acl', result)
        return result['status'] == 'success'
    
    def test_put_object_tagging(self):
        """Test: Put object tagging"""
        print("\n[TEST] Adding object tags...")
        tags = {'Version': '1.0', 'Type': 'Test'}
        result = self.s3_ops.put_object_tagging(self.test_bucket, self.test_object_key, tags)
        self._log_result('put_object_tagging', result)
        return result['status'] == 'success'
    
    def test_get_object_tagging(self):
        """Test: Get object tagging"""
        print("\n[TEST] Getting object tags...")
        result = self.s3_ops.get_object_tagging(self.test_bucket, self.test_object_key)
        self._log_result('get_object_tagging', result)
        return result['status'] == 'success'
    
    def test_delete_object_tagging(self):
        """Test: Delete object tagging"""
        print("\n[TEST] Deleting object tags...")
        result = self.s3_ops.delete_object_tagging(self.test_bucket, self.test_object_key)
        self._log_result('delete_object_tagging', result)
        return result['status'] == 'success'
    
    def test_delete_objects(self):
        """Test: Delete multiple objects"""
        print("\n[TEST] Deleting multiple objects...")
        result = self.s3_ops.delete_objects(self.test_bucket, [self.test_object_key, self.test_object_key2])
        self._log_result('delete_objects', result)
        return result['status'] == 'success'
    
    def test_list_object_versions(self):
        """Test: List object versions"""
        print("\n[TEST] Listing object versions...")
        result = self.s3_ops.list_object_versions(self.test_bucket)
        self._log_result('list_object_versions', result)
        return result['status'] == 'success'
    
    def test_initiate_multipart_upload(self):
        """Test: Initiate multipart upload"""
        print("\n[TEST] Initiating multipart upload...")
        result = self.s3_ops.initiate_multipart_upload(self.test_bucket, 'multipart-object.txt')
        self._log_result('initiate_multipart_upload', result)
        return result['status'] == 'success'
    
    def test_list_multipart_uploads(self):
        """Test: List multipart uploads"""
        print("\n[TEST] Listing multipart uploads...")
        result = self.s3_ops.list_multipart_uploads(self.test_bucket)
        self._log_result('list_multipart_uploads', result)
        return result['status'] == 'success'
    
    def test_put_object_http_if_match_success(self):
        """Test: PUT with If-Match HTTP header (success case for job queue)"""
        print("\n[TEST] PUT with HTTP If-Match header (success case)...")
        
        # Create test content
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = 'job_queue_task_v1'
        with open(temp_file, 'w') as f:
            f.write(original_content)
        
        try:
            # Upload initial object
            result_upload = self.s3_ops.put_object(self.test_bucket, 'job-queue-task.json', temp_file)
            if result_upload['status'] != 'success':
                return False
                
            # Get current ETag
            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key='job-queue-task.json')
            current_etag = meta['ETag']
            print(f"  └─ Current ETag: {current_etag}")
            
            # Create updated content
            updated_content = 'job_queue_task_v2_updated'
            with open(temp_file, 'w') as f:
                f.write(updated_content)
            
            # PUT with If-Match header (should succeed - compare-and-swap)
            print(f"  └─ Attempting compare-and-swap with ETag: {current_etag}")
            with open(temp_file, 'rb') as f:
                response = self.s3_ops.s3_client.put_object(
                    Bucket=self.test_bucket,
                    Key='job-queue-task.json',
                    Body=f,
                    IfMatch=current_etag
                )
            
            result = {
                'status': 'success',
                'message': f'HTTP If-Match success! Compare-and-swap completed. New ETag: {response["ETag"]}',
                'original_etag': current_etag,
                'new_etag': response['ETag'],
                'http_status': 200
            }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'HTTP If-Match test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('put_object_http_if_match_success', result)
        return result['status'] == 'success'
    
    def test_put_object_http_if_match_412_failure(self):
        """Test: PUT with If-Match HTTP header (412 Precondition Failed)"""
        print("\n[TEST] PUT with HTTP If-Match header (412 failure case)...")
        
        # Create test content
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = 'job_queue_initial_state'
        with open(temp_file, 'w') as f:
            f.write(original_content)
        
        try:
            # Upload initial object
            result_upload = self.s3_ops.put_object(self.test_bucket, 'job-queue-cas-test.json', temp_file)
            if result_upload['status'] != 'success':
                return False
            
            # Create updated content
            updated_content = 'job_queue_attempted_update'
            with open(temp_file, 'w') as f:
                f.write(updated_content)
            
            # Use wrong/outdated ETag (simulate race condition in job queue)
            wrong_etag = '"wrong-etag-simulation"'
            print(f"  └─ Attempting compare-and-swap with WRONG ETag: {wrong_etag}")
            
            try:
                with open(temp_file, 'rb') as f:
                    response = self.s3_ops.s3_client.put_object(
                        Bucket=self.test_bucket,
                        Key='job-queue-cas-test.json',
                        Body=f,
                        IfMatch=wrong_etag
                    )
                
                # If we get here, the test failed (should have thrown exception)
                result = {
                    'status': 'error',
                    'message': 'Expected 412 Precondition Failed but operation succeeded!'
                }
            
            except self.s3_ops.s3_client.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                http_status = e.response['ResponseMetadata']['HTTPStatusCode']
                
                if error_code == 'PreconditionFailed' and http_status == 412:
                    result = {
                        'status': 'success',
                        'message': f'✓ Expected 412 Precondition Failed returned! Error: {error_code}',
                        'http_status': http_status,
                        'error_code': error_code,
                        'cas_protection': 'working'
                    }
                else:
                    result = {
                        'status': 'error',
                        'message': f'Wrong error! Expected 412/PreconditionFailed, got {http_status}/{error_code}'
                    }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'HTTP If-Match 412 test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('put_object_http_if_match_412_failure', result)
        return result['status'] == 'success'
    
    def test_put_object_http_if_none_match_success(self):
        """Test: PUT with If-None-Match: * HTTP header (success - object doesn't exist)"""
        print("\n[TEST] PUT with HTTP If-None-Match: * header (success case)...")
        
        # Create test content for new job
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        job_content = 'new_exclusive_job_initialization'
        with open(temp_file, 'w') as f:
            f.write(job_content)
        
        try:
            # Ensure object doesn't exist
            new_job_key = f'exclusive-job-{os.urandom(4).hex()}.json'
            print(f"  └─ Creating new exclusive job: {new_job_key}")
            
            # PUT with If-None-Match: * (should succeed - object doesn't exist)
            with open(temp_file, 'rb') as f:
                response = self.s3_ops.s3_client.put_object(
                    Bucket=self.test_bucket,
                    Key=new_job_key,
                    Body=f,
                    IfNoneMatch='*'
                )
            
            result = {
                'status': 'success',
                'message': f'✓ HTTP If-None-Match success! Exclusive job created. ETag: {response["ETag"]}',
                'job_key': new_job_key,
                'etag': response['ETag'],
                'http_status': 200,
                'exclusive_create': True
            }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'HTTP If-None-Match success test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('put_object_http_if_none_match_success', result)
        return result['status'] == 'success'
    
    def test_put_object_http_if_none_match_409_failure(self):
        """Test: PUT with If-None-Match: * HTTP header (409 Conflict - object exists)"""
        print("\n[TEST] PUT with HTTP If-None-Match: * header (409 Conflict case)...")
        
        # Create test content
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        initial_content = 'existing_job_state'
        with open(temp_file, 'w') as f:
            f.write(initial_content)
        
        try:
            # Upload initial object (ensure it exists)
            existing_job_key = 'existing-job-conflict-test.json'
            result_upload = self.s3_ops.put_object(self.test_bucket, existing_job_key, temp_file)
            if result_upload['status'] != 'success':
                return False
                
            print(f"  └─ Object exists. Attempting exclusive create with If-None-Match: *")
            
            # Try to create with If-None-Match: * (should fail with 409 Conflict or 412 Precondition Failed)
            duplicate_content = 'attempted_duplicate_job'
            with open(temp_file, 'w') as f:
                f.write(duplicate_content)
            
            try:
                with open(temp_file, 'rb') as f:
                    response = self.s3_ops.s3_client.put_object(
                        Bucket=self.test_bucket,
                        Key=existing_job_key,
                        Body=f,
                        IfNoneMatch='*'
                    )
                
                # If we get here, the test failed
                result = {
                    'status': 'error',
                    'message': 'Expected 409 Conflict or 412 Precondition Failed but operation succeeded!'
                }
            
            except self.s3_ops.s3_client.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                http_status = e.response['ResponseMetadata']['HTTPStatusCode']
                
                # Check for either 409 Conflict or 412 Precondition Failed (S3 implementations vary)
                if (error_code == 'Conflict' and http_status == 409) or (error_code == 'PreconditionFailed' and http_status == 412):
                    result = {
                        'status': 'success',
                        'message': f'✓ Expected {http_status} {error_code} returned! Duplicate prevention working.',
                        'http_status': http_status,
                        'error_code': error_code,
                        'duplicate_prevention': 'working',
                        's3_behavior': f'{http_status}_{error_code}'
                    }
                else:
                    result = {
                        'status': 'error',
                        'message': f'Wrong error! Expected 409/Conflict or 412/PreconditionFailed, got {http_status}/{error_code}'
                    }
            
        except Exception as e:
            result = {'status': 'error', 'message': f'HTTP If-None-Match 409/412 test failed: {str(e)}'}
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('put_object_http_if_none_match_409_failure', result)
        return result['status'] == 'success'
    
    def test_multipart_repeat_same_part(self):
        """Test: Repeat upload of same part before completing multipart"""
        print("\n[TEST] Multipart upload with repeated part...")
        import hashlib
        
        # Create test content for two parts (5MB each to meet S3 minimum part size)
        part1_content = b'A' * (5 * 1024 * 1024)  # 5MB
        part2_content = b'B' * (5 * 1024 * 1024)  # 5MB
        total_content = part1_content + part2_content
        
        # Create temporary file with the content
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        with open(temp_file, 'wb') as f:
            f.write(total_content)
        
        try:
            # Calculate expected MD5 of final content
            expected_md5 = hashlib.md5(total_content).hexdigest()
            print(f"  └─ Expected final MD5: {expected_md5} (10MB total: 5MB + 5MB)")
            
            # Initiate multipart upload
            init_result = self.s3_ops.s3_client.create_multipart_upload(
                Bucket=self.test_bucket,
                Key='multipart-repeat-test.bin'
            )
            upload_id = init_result['UploadId']
            print(f"  └─ Initiated multipart upload: {upload_id}")
            
            parts = []
            
            # Upload Part 1 (first attempt)
            print("  └─ Uploading Part 1 - 5MB (first attempt)...")
            part1_response = self.s3_ops.s3_client.upload_part(
                Bucket=self.test_bucket,
                Key='multipart-repeat-test.bin',
                PartNumber=1,
                UploadId=upload_id,
                Body=part1_content
            )
            first_etag = part1_response['ETag']
            print(f"    └─ Part 1 first upload ETag: {first_etag}")
            
            # Upload Part 1 again (repeat/retry scenario)
            print("  └─ Re-uploading Part 1 - 5MB (repeat attempt)...")
            part1_repeat_response = self.s3_ops.s3_client.upload_part(
                Bucket=self.test_bucket,
                Key='multipart-repeat-test.bin',
                PartNumber=1,
                UploadId=upload_id,
                Body=part1_content
            )
            repeat_etag = part1_repeat_response['ETag']
            print(f"    └─ Part 1 repeat upload ETag: {repeat_etag}")
            
            # Use the latest (repeat) ETag for part 1
            parts.append({
                'ETag': repeat_etag,
                'PartNumber': 1
            })
            
            # Upload Part 2
            print("  └─ Uploading Part 2 - 5MB...")
            part2_response = self.s3_ops.s3_client.upload_part(
                Bucket=self.test_bucket,
                Key='multipart-repeat-test.bin',
                PartNumber=2,
                UploadId=upload_id,
                Body=part2_content
            )
            parts.append({
                'ETag': part2_response['ETag'],
                'PartNumber': 2
            })
            print(f"    └─ Part 2 ETag: {part2_response['ETag']}")
            
            # Complete multipart upload
            print("  └─ Completing multipart upload...")
            self.s3_ops.s3_client.complete_multipart_upload(
                Bucket=self.test_bucket,
                Key='multipart-repeat-test.bin',
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            # Verify the final object
            print("  └─ Verifying final object...")
            download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
            get_result = self.s3_ops.get_object(self.test_bucket, 'multipart-repeat-test.bin', download_path)
            
            if get_result['status'] == 'success':
                # Verify content matches expected
                with open(download_path, 'rb') as f:
                    downloaded_content = f.read()
                
                downloaded_md5 = hashlib.md5(downloaded_content).hexdigest()
                
                if downloaded_md5 == expected_md5 and len(downloaded_content) == len(total_content):
                    result = {
                        'status': 'success',
                        'message': f'Multipart upload with repeated part successful! Final size: {len(downloaded_content) / (1024*1024):.1f}MB, ETags: {first_etag} -> {repeat_etag}',
                        'first_part_etag': first_etag,
                        'repeat_part_etag': repeat_etag,
                        'final_size_mb': len(downloaded_content) / (1024*1024),
                        'md5_match': True
                    }
                else:
                    result = {
                        'status': 'error',
                        'message': f'Content verification failed. Expected MD5: {expected_md5}, Got: {downloaded_md5}'
                    }
            else:
                result = get_result
            
            # Cleanup download file
            if os.path.exists(download_path):
                os.remove(download_path)
            
        except Exception as e:
            # Abort multipart upload on error
            try:
                self.s3_ops.s3_client.abort_multipart_upload(
                    Bucket=self.test_bucket,
                    Key='multipart-repeat-test.bin',
                    UploadId=upload_id
                )
            except:
                pass
            result = {'status': 'error', 'message': f'Multipart repeat test failed: {str(e)}'}
        finally:
            # Cleanup temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        self._log_result('multipart_repeat_same_part', result)
        return result['status'] == 'success'
    
    # ============ PRESIGNED URL CONDITIONAL TESTS ============
    # 
    # IMPORTANT FINDING: IDrive E2's presigned URLs do NOT properly enforce 
    # conditional headers (If-Match, If-None-Match). While the URLs are generated
    # correctly with these parameters, the actual PUT operations ignore the 
    # conditional logic and succeed even when conditions should fail.
    #
    # For distributed job queue systems:
    # ✅ USE: Direct boto3 calls with conditional headers (fully supported)
    # ❌ AVOID: Presigned URLs for atomic operations (not reliable)
    #
    # This is a common limitation across S3-compatible services.
    # 
    def test_presigned_put_if_match_success(self):
        """Test: Presigned PUT URL + If-Match header (success case)"""
        print("\n[TEST] Presigned PUT with If-Match header (success case)...")
        import requests
        
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
        import requests
        
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
        import subprocess
        import json
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        original_content = "aws_cli_job_initial"
        with open(temp_file, "w") as f:
            f.write(original_content)
        
        try:
            job_key = "aws-cli-presigned-job-if-match-412.json"
            print(f"  └─ Creating initial job: {job_key}")
            
            # Set environment variables for AWS CLI
            import os
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
                    import requests
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
        import requests
        
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
        import requests
        
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
    
    # ============ LARGE FILE OPERATIONS ============
    
    def test_put_object_5mb(self):
        """Test: Put 5MB object"""
        print("\n[TEST] Uploading 5MB object...")
        result = self.s3_ops.put_object(self.test_bucket, self.test_object_5mb, self.test_file_path_5mb)
        self._log_result('put_object_5mb', result)
        return result['status'] == 'success'
    
    def test_get_object_5mb(self):
        """Test: Get 5MB object immediately after upload"""
        print("\n[TEST] Downloading 5MB object immediately...")
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        result = self.s3_ops.get_object(self.test_bucket, self.test_object_5mb, download_path)
        
        if result['status'] == 'success':
            # Verify file sizes match
            original_size = os.path.getsize(self.test_file_path_5mb)
            downloaded_size = os.path.getsize(download_path)
            
            if original_size == downloaded_size:
                result['message'] = f'Downloaded 5MB object successfully (Size: {downloaded_size / (1024*1024):.2f} MB)'
                result['original_size'] = original_size
                result['downloaded_size'] = downloaded_size
            else:
                result['status'] = 'error'
                result['message'] = f'Size mismatch! Original: {original_size} bytes, Downloaded: {downloaded_size} bytes'
        
        self._log_result('get_object_5mb', result)
        if os.path.exists(download_path):
            os.remove(download_path)
        return result['status'] == 'success'
    
    def test_put_get_5mb_immediate(self):
        """Test: Put 5MB object and get immediately"""
        print("\n[TEST] Putting 5MB object and getting immediately...")
        
        # Put object
        put_result = self.s3_ops.put_object(self.test_bucket, self.test_object_5mb, self.test_file_path_5mb)
        if put_result['status'] != 'success':
            self._log_result('put_get_5mb_immediate', put_result)
            return False
        
        print("  └─ Object uploaded, downloading immediately...")
        
        # Get object immediately
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        get_result = self.s3_ops.get_object(self.test_bucket, self.test_object_5mb, download_path)
        
        if get_result['status'] == 'success':
            # Verify file sizes match
            original_size = os.path.getsize(self.test_file_path_5mb)
            downloaded_size = os.path.getsize(download_path)
            
            if original_size == downloaded_size:
                get_result['message'] = f'Put and Get 5MB successful! Size: {downloaded_size / (1024*1024):.2f} MB'
                get_result['original_size'] = original_size
                get_result['downloaded_size'] = downloaded_size
            else:
                get_result['status'] = 'error'
                get_result['message'] = f'Size mismatch! Original: {original_size} bytes, Downloaded: {downloaded_size} bytes'
        
        self._log_result('put_get_5mb_immediate', get_result)
        if os.path.exists(download_path):
            os.remove(download_path)
        return get_result['status'] == 'success'
    
    def test_put_delete_5mb_immediate(self):
        """Test: Put 5MB object and delete immediately"""
        print("\n[TEST] Putting 5MB object and deleting immediately...")
        
        # Put object
        put_result = self.s3_ops.put_object(self.test_bucket, self.test_object_5mb, self.test_file_path_5mb)
        if put_result['status'] != 'success':
            self._log_result('put_delete_5mb_immediate', put_result)
            return False
        
        print("  └─ Object uploaded, deleting immediately...")
        
        # Delete object immediately
        delete_result = self.s3_ops.delete_object(self.test_bucket, self.test_object_5mb)
        
        if delete_result['status'] == 'success':
            delete_result['message'] = f'Put and Delete 5MB successful! Object deleted immediately after upload'
        
        self._log_result('put_delete_5mb_immediate', delete_result)
        return delete_result['status'] == 'success'
    
    def test_put_get_1kb_immediate(self):
        """Test: Put 1KB object and get immediately after upload"""
        print("\n[TEST] Putting 1KB object and getting immediately...")
        
        # Put object
        put_result = self.s3_ops.put_object(self.test_bucket, self.test_object_1kb, self.test_file_path_1kb)
        if put_result['status'] != 'success':
            self._log_result('put_get_1kb_immediate', put_result)
            return False
        
        print("  └─ Object uploaded, downloading immediately...")
        
        # Get object immediately
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        get_result = self.s3_ops.get_object(self.test_bucket, self.test_object_1kb, download_path)
        
        if get_result['status'] == 'success':
            # Verify file sizes match
            original_size = os.path.getsize(self.test_file_path_1kb)
            downloaded_size = os.path.getsize(download_path)
            
            if original_size == downloaded_size:
                get_result['message'] = f'Put and Get 1KB successful! Size: {downloaded_size / 1024:.2f} KB'
                get_result['original_size'] = original_size
                get_result['downloaded_size'] = downloaded_size
            else:
                get_result['status'] = 'error'
                get_result['message'] = f'Size mismatch! Original: {original_size} bytes, Downloaded: {downloaded_size} bytes'
        
        self._log_result('put_get_1kb_immediate', get_result)
        if os.path.exists(download_path):
            os.remove(download_path)
        return get_result['status'] == 'success'
    
    def test_put_delete_1kb_immediate(self):
        """Test: Put 1KB object and delete immediately"""
        print("\n[TEST] Putting 1KB object and deleting immediately...")
        
        # Put object
        put_result = self.s3_ops.put_object(self.test_bucket, self.test_object_1kb, self.test_file_path_1kb)
        if put_result['status'] != 'success':
            self._log_result('put_delete_1kb_immediate', put_result)
            return False
        
        print("  └─ Object uploaded, deleting immediately...")
        
        # Delete object immediately
        delete_result = self.s3_ops.delete_object(self.test_bucket, self.test_object_1kb)
        
        if delete_result['status'] == 'success':
            delete_result['message'] = f'Put and Delete 1KB successful! Object deleted immediately after upload'
        
        self._log_result('put_delete_1kb_immediate', delete_result)
        return delete_result['status'] == 'success'
    
    # ============ MULTIPART DOWNLOAD TESTS (50MB) ============
    
    def test_put_object_50mb(self):
        """Test: Upload 50MB object"""
        print("\n[TEST] Uploading 50MB object...")
        result = self.s3_ops.put_object_50mb(self.test_bucket, self.test_object_50mb, self.test_file_path_50mb)
        self._log_result('put_object_50mb', result)
        return result['status'] == 'success'
    
    def test_get_object_50mb_multipart(self):
        """Test: Download 50MB object with multipart (10MB parts)"""
        print("\n[TEST] Downloading 50MB object with multipart (10MB parts)...")
        
        # Upload first if needed
        put_result = self.s3_ops.put_object_50mb(self.test_bucket, self.test_object_50mb, self.test_file_path_50mb)
        if put_result['status'] != 'success':
            self._log_result('get_object_50mb_multipart', put_result)
            return False
        
        # Download with multipart
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        result = self.s3_ops.get_object_50mb_multipart(self.test_bucket, self.test_object_50mb, download_path, part_size=10*1024*1024)
        self._log_result('get_object_50mb_multipart', result)
        
        if os.path.exists(download_path):
            os.remove(download_path)
        
        return result['status'] == 'success'
    
    def test_put_get_50mb_multipart_immediate(self):
        """Test: Put 50MB object and download immediately with multipart"""
        print("\n[TEST] Putting 50MB object and downloading immediately with multipart...")
        
        # Put object
        put_result = self.s3_ops.put_object_50mb(self.test_bucket, self.test_object_50mb, self.test_file_path_50mb)
        if put_result['status'] != 'success':
            self._log_result('put_get_50mb_multipart_immediate', put_result)
            return False
        
        print("  └─ Object uploaded, downloading immediately with 10MB parts...")
        
        # Download with multipart immediately
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        get_result = self.s3_ops.get_object_50mb_multipart(self.test_bucket, self.test_object_50mb, download_path, part_size=10*1024*1024)
        
        if get_result['status'] == 'success':
            get_result['message'] = f'Put and Get 50MB with multipart successful! Downloaded in {get_result["parts_downloaded"]} parts ({get_result["part_size_mb"]:.1f}MB each)'
        
        self._log_result('put_get_50mb_multipart_immediate', get_result)
        
        if os.path.exists(download_path):
            os.remove(download_path)
        
        return get_result['status'] == 'success'
    
    # ============ OBJECT LOCK TESTS ============
    
    def test_create_bucket_with_object_lock(self):
        """Test: Create bucket with Object Lock"""
        print("\n[TEST] Creating bucket with Object Lock...")
        result = self.s3_ops.create_bucket_with_object_lock(self.test_bucket_lock)
        self._log_result('create_bucket_with_object_lock', result)
        return result['status'] == 'success'
    
    def test_get_object_lock_configuration(self):
        """Test: Get Object Lock configuration"""
        print("\n[TEST] Getting Object Lock configuration...")
        result = self.s3_ops.get_object_lock_configuration(self.test_bucket_lock)
        self._log_result('get_object_lock_configuration', result)
        return result['status'] == 'success'
    
    def test_put_object_lock_configuration(self):
        """Test: Put Object Lock default retention"""
        print("\n[TEST] Setting Object Lock default retention...")
        result = self.s3_ops.put_object_lock_configuration(self.test_bucket_lock, mode='GOVERNANCE', days=30)
        self._log_result('put_object_lock_configuration', result)
        return result['status'] == 'success'
    
    def test_put_object_retention(self):
        """Test: Put Object retention on object"""
        print("\n[TEST] Uploading object to lock bucket first...")
        # First upload object to the lock bucket
        upload_result = self.s3_ops.put_object(self.test_bucket_lock, self.test_object_lock_key, self.test_file_path)
        if upload_result['status'] != 'success':
            self._log_result('put_object_retention', upload_result)
            return False
        
        print("  └─ Setting retention on object...")
        result = self.s3_ops.put_object_retention(self.test_bucket_lock, self.test_object_lock_key, mode='GOVERNANCE', days=7)
        self._log_result('put_object_retention', result)
        return result['status'] == 'success'
    
    def test_get_object_retention(self):
        """Test: Get Object retention"""
        print("\n[TEST] Getting object retention...")
        result = self.s3_ops.get_object_retention(self.test_bucket_lock, self.test_object_lock_key)
        self._log_result('get_object_retention', result)
        return result['status'] == 'success'
    
    def test_put_object_legal_hold(self):
        """Test: Put legal hold on object"""
        print("\n[TEST] Setting legal hold on object...")
        result = self.s3_ops.put_object_legal_hold(self.test_bucket_lock, self.test_object_lock_key, status='ON')
        self._log_result('put_object_legal_hold', result)
        return result['status'] == 'success'
    
    def test_get_object_legal_hold(self):
        """Test: Get legal hold status"""
        print("\n[TEST] Getting legal hold status...")
        result = self.s3_ops.get_object_legal_hold(self.test_bucket_lock, self.test_object_lock_key)
        self._log_result('get_object_legal_hold', result)
        return result['status'] == 'success'
    
    # ============ SSE (SERVER-SIDE ENCRYPTION) TESTS ============
    
    def test_put_bucket_encryption(self):
        """Test: Enable bucket encryption"""
        print("\n[TEST] Enabling bucket encryption...")
        result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        self._log_result('put_bucket_encryption', result)
        return result['status'] == 'success'
    
    def test_get_bucket_encryption(self):
        """Test: Get bucket encryption configuration"""
        print("\n[TEST] Getting bucket encryption configuration...")
        result = self.s3_ops.get_bucket_encryption(self.test_bucket)
        self._log_result('get_bucket_encryption', result)
        return result['status'] == 'success'
    
    def test_put_object_with_sse(self):
        """Test: Upload object with SSE encryption"""
        print("\n[TEST] Uploading object with SSE...")
        result = self.s3_ops.put_object_with_sse(self.test_bucket, 'sse-object.txt', self.test_file_path, sse_algorithm='AES256')
        self._log_result('put_object_with_sse', result)
        return result['status'] == 'success'
    
    def test_get_object_with_sse(self):
        """Test: Download encrypted object and verify encryption"""
        print("\n[TEST] Downloading encrypted object...")
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.txt').name
        result = self.s3_ops.get_object_with_sse(self.test_bucket, 'sse-object.txt', download_path)
        self._log_result('get_object_with_sse', result)
        if os.path.exists(download_path):
            os.remove(download_path)
        return result['status'] == 'success'
    
    def test_delete_bucket_encryption(self):
        """Test: Remove bucket encryption"""
        print("\n[TEST] Removing bucket encryption...")
        result = self.s3_ops.delete_bucket_encryption(self.test_bucket)
        self._log_result('delete_bucket_encryption', result)
        return result['status'] == 'success'
    
    def test_put_object_sse_bucket_single_part(self):
        """Test: Put single part object on SSE encryption enabled bucket"""
        print("\n[TEST] Single part upload on SSE enabled bucket...")
        
        # Enable bucket encryption first
        encrypt_result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        if encrypt_result['status'] != 'success':
            self._log_result('put_object_sse_bucket_single_part', encrypt_result)
            return False
        
        print("  └─ Bucket encryption enabled, uploading single part object...")
        
        # Upload object (should inherit bucket encryption)
        put_result = self.s3_ops.put_object(self.test_bucket, 'sse-bucket-single.txt', self.test_file_path)
        
        if put_result['status'] == 'success':
            put_result['message'] = f'Single part upload on SSE bucket successful! Object: sse-bucket-single.txt'
            put_result['encryption_inherited'] = True
        
        self._log_result('put_object_sse_bucket_single_part', put_result)
        return put_result['status'] == 'success'
    
    def test_put_object_sse_bucket_multipart(self):
        """Test: Put multipart object on SSE encryption enabled bucket"""
        print("\n[TEST] Multipart upload on SSE enabled bucket...")
        
        # Enable bucket encryption first
        encrypt_result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        if encrypt_result['status'] != 'success':
            self._log_result('put_object_sse_bucket_multipart', encrypt_result)
            return False
        
        print("  └─ Bucket encryption enabled, uploading 50MB object with multipart...")
        
        # Upload 50MB object using multipart (should inherit bucket encryption)
        put_result = self.s3_ops.put_object_50mb(self.test_bucket, 'sse-bucket-multipart.bin', self.test_file_path_50mb)
        
        if put_result['status'] == 'success':
            put_result['message'] = f'Multipart upload on SSE bucket successful! Object: sse-bucket-multipart.bin (50MB)'
            put_result['encryption_inherited'] = True
        
        self._log_result('put_object_sse_bucket_multipart', put_result)
        return put_result['status'] == 'success'
    
    def test_get_object_sse_bucket_single_part(self):
        """Test: Get single part object from SSE encryption enabled bucket"""
        print("\n[TEST] Single part download from SSE enabled bucket...")
        
        # Enable bucket encryption first
        encrypt_result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        if encrypt_result['status'] != 'success':
            self._log_result('get_object_sse_bucket_single_part', encrypt_result)
            return False
        
        print("  └─ Bucket encryption enabled, downloading single part object...")
        
        # Download single part object (assumes sse-bucket-single.txt exists)
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.txt').name
        get_result = self.s3_ops.get_object(self.test_bucket, 'sse-bucket-single.txt', download_path)
        
        if get_result['status'] == 'success':
            downloaded_size = os.path.getsize(download_path)
            get_result['message'] = f'Single part download from SSE bucket successful! Size: {downloaded_size} bytes'
            get_result['encryption_enabled'] = True
        
        self._log_result('get_object_sse_bucket_single_part', get_result)
        
        # Cleanup
        if os.path.exists(download_path):
            os.remove(download_path)
        
        return get_result['status'] == 'success'
    
    def test_get_object_sse_bucket_multipart(self):
        """Test: Get multipart object from SSE encryption enabled bucket"""
        print("\n[TEST] Downloading multipart object from SSE enabled bucket...")
        
        # Enable bucket encryption first
        encrypt_result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        if encrypt_result['status'] != 'success':
            self._log_result('get_object_sse_bucket_multipart', encrypt_result)
            return False
        
        print("  └─ Bucket encryption enabled, downloading multipart object (50MB)...")
        
        # Download multipart object (assumes sse-bucket-multipart.bin exists)
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        get_result = self.s3_ops.get_object(self.test_bucket, 'sse-bucket-multipart.bin', download_path)
        
        if get_result['status'] == 'success':
            downloaded_size = os.path.getsize(download_path)
            get_result['message'] = f'Multipart object download from SSE bucket successful! Size: {downloaded_size / (1024*1024):.2f} MB'
            get_result['encryption_enabled'] = True
            get_result['downloaded_size'] = downloaded_size
        
        self._log_result('get_object_sse_bucket_multipart', get_result)
        
        # Cleanup
        if os.path.exists(download_path):
            os.remove(download_path)
        
        return get_result['status'] == 'success'
    
    def test_get_object_multipart_sse_bucket(self):
        """Test: Get object as multipart download from SSE encryption enabled bucket"""
        print("\n[TEST] Multipart download from SSE enabled bucket...")
        
        # Enable bucket encryption first
        encrypt_result = self.s3_ops.put_bucket_encryption(self.test_bucket, sse_algorithm='AES256')
        if encrypt_result['status'] != 'success':
            self._log_result('get_object_multipart_sse_bucket', encrypt_result)
            return False
        
        print("  └─ Bucket encryption enabled, downloading object with multipart (10MB parts)...")
        
        # Download with multipart (assumes sse-bucket-multipart.bin exists)
        download_path = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
        get_result = self.s3_ops.get_object_50mb_multipart(self.test_bucket, 'sse-bucket-multipart.bin', download_path, part_size=10*1024*1024)
        
        if get_result['status'] == 'success':
            downloaded_size = os.path.getsize(download_path)
            get_result['message'] = f'Multipart download from SSE bucket successful! Size: {downloaded_size / (1024*1024):.2f} MB in {get_result.get("parts_downloaded", "N/A")} parts'
            get_result['encryption_enabled'] = True
            get_result['downloaded_size'] = downloaded_size
        
        self._log_result('get_object_multipart_sse_bucket', get_result)
        
        # Cleanup
        if os.path.exists(download_path):
            os.remove(download_path)
        
        return get_result['status'] == 'success'
    
    # ============ LIFECYCLE RULES TESTS ============
    
    def test_put_bucket_lifecycle_configuration(self):
        """Test: Set lifecycle rules"""
        print("\n[TEST] Setting lifecycle configuration...")
        rules = [
            {
                'ID': 'ExpireCurrentAndOld1Day',
                'Status': 'Enabled',
                'Prefix': '',
                'Expiration': {'Days': 1},
                'NoncurrentVersionExpiration': {'NoncurrentDays': 1}
            }
        ]
        result = self.s3_ops.put_bucket_lifecycle_configuration(self.test_bucket, rules=rules)
        self._log_result('put_bucket_lifecycle_configuration', result)
        return result['status'] == 'success'
    
    def test_get_bucket_lifecycle_configuration(self):
        """Test: Get lifecycle rules"""
        print("\n[TEST] Getting lifecycle configuration...")
        result = self.s3_ops.get_bucket_lifecycle_configuration(self.test_bucket)
        self._log_result('get_bucket_lifecycle_configuration', result)
        return result['status'] == 'success'
    
    def test_delete_bucket_lifecycle_configuration(self):
        """Test: Delete lifecycle rules"""
        print("\n[TEST] Deleting lifecycle configuration...")
        result = self.s3_ops.delete_bucket_lifecycle_configuration(self.test_bucket)
        self._log_result('delete_bucket_lifecycle_configuration', result)
        return result['status'] == 'success'
    
    def _log_result(self, test_name: str, result: dict):
        """Log test result."""
        status = result.get('status', 'unknown')
        message = result.get('message', str(result))
        status_symbol = '✓' if status == 'success' else '✗'
        print(f"{status_symbol} {test_name}: {message}")
        self.test_results.append({'test': test_name, 'status': status, 'result': result})
    
    def run_all_tests(self):
        """Run all test cases."""
        print("=" * 60)
        print("Running S3 API Automation Test Suite - ALL TESTS")
        print("=" * 60)
        
        self.setup()
        
        try:
            # Basic Operations
            self.test_create_bucket()
            self.test_head_bucket()
            self.test_get_bucket_location()
            
            # Versioning
            self.test_enable_bucket_versioning()
            self.test_get_bucket_versioning()
            
            # ACL Operations
            self.test_put_bucket_acl()
            self.test_get_bucket_acl()
            
            # Tagging Operations
            self.test_put_bucket_tagging()
            self.test_get_bucket_tagging()
            
            # CORS Operations
            self.test_put_bucket_cors()
            self.test_get_bucket_cors()
            
            # Public Access Block
            self.test_put_public_access_block()
            self.test_get_public_access_block()
            
            # Bucket Policy
            self.test_put_bucket_policy()
            self.test_get_bucket_policy()
            self.test_delete_bucket_policy()
            
            # Object Operations
            self.test_put_object()
            self.test_head_object()
            self.test_get_object()
            self.test_list_objects()
            
            # Object Tagging
            self.test_put_object_tagging()
            self.test_get_object_tagging()
            
            # Object ACL
            self.test_put_object_acl()
            self.test_get_object_acl()
            
            # Conditional PUT Operations
            self.test_put_object_if_match()
            self.test_put_object_if_match_fails()
            self.test_put_object_if_not_match()
            
            # Copy and Multipart
            self.test_copy_object()
            self.test_initiate_multipart_upload()
            self.test_list_multipart_uploads()
            self.test_multipart_repeat_same_part()
            
            # HTTP Header-based Conditional PUTs (for job queue systems)
            self.test_put_object_http_if_match_success()
            self.test_put_object_http_if_match_412_failure()
            self.test_put_object_http_if_none_match_success()
            self.test_put_object_http_if_none_match_409_failure()
            
            # Presigned URL Conditional PUTs (for job queue systems)
            self.test_presigned_put_if_match_success()
            self.test_presigned_put_if_match_412_failure()
            self.test_aws_cli_presigned_if_match_412_failure()  # AWS CLI comparison test
            self.test_presigned_put_if_none_match_success()
            self.test_presigned_put_if_none_match_409_failure()
            
            self.test_list_object_versions()
            
            # Large File Operations
            self.test_put_object_5mb()
            self.test_get_object_5mb()
            self.test_put_get_5mb_immediate()
            self.test_put_delete_5mb_immediate()
            self.test_put_get_1kb_immediate()
            self.test_put_delete_1kb_immediate()
            
            # Multipart Download Operations (50MB)
            self.test_put_object_50mb()
            self.test_get_object_50mb_multipart()
            self.test_put_get_50mb_multipart_immediate()
            
            # SSE (Server-Side Encryption) Operations
            self.test_put_bucket_encryption()
            self.test_get_bucket_encryption()
            self.test_put_object_with_sse()
            self.test_get_object_with_sse()
            
            # SSE Bucket-level Encryption Tests
            self.test_put_object_sse_bucket_single_part()
            self.test_put_object_sse_bucket_multipart()
            self.test_get_object_sse_bucket_single_part()
            self.test_get_object_sse_bucket_multipart()
            self.test_get_object_multipart_sse_bucket()
            
            # MD5/ETag Verification Tests
            self.test_single_part_put_verify_md5_etag()
            self.test_multipart_put_verify_md5_etag()
            self.test_single_part_put_sse_verify_md5_etag()
            self.test_multipart_put_sse_verify_md5_etag()
            
            self.test_delete_bucket_encryption()
            
            # Lifecycle Rules Operations
            self.test_put_bucket_lifecycle_configuration()
            self.test_get_bucket_lifecycle_configuration()
            self.test_delete_bucket_lifecycle_configuration()
            
            # Object Lock Operations (requires separate bucket)
            self.test_create_bucket_with_object_lock()
            self.test_get_object_lock_configuration()
            # Upload object BEFORE setting retention policies
            self.test_put_object_retention()  # This uploads object first, then sets retention
            self.test_get_object_retention()
            self.test_put_object_legal_hold()
            self.test_get_object_legal_hold()
            # Set default retention AFTER object operations
            self.test_put_object_lock_configuration()
            
            # Cleanup
            self.test_delete_bucket_tagging()
            self.test_delete_bucket_cors()
            self.test_delete_object_tagging()
            self.test_delete_objects()
            self.test_suspend_bucket_versioning()
            self.test_delete_bucket()
        finally:
            self.teardown()
        
        self._print_summary()
    
    def run_specific_test(self, test_name: str):
        """Run a specific test case."""
        test_methods = {
            # Bucket Level
            'create_bucket': self.test_create_bucket,
            'delete_bucket': self.test_delete_bucket,
            'head_bucket': self.test_head_bucket,
            'get_bucket_location': self.test_get_bucket_location,
            'put_bucket_acl': self.test_put_bucket_acl,
            'get_bucket_acl': self.test_get_bucket_acl,
            'put_bucket_tagging': self.test_put_bucket_tagging,
            'get_bucket_tagging': self.test_get_bucket_tagging,
            'delete_bucket_tagging': self.test_delete_bucket_tagging,
            'put_bucket_cors': self.test_put_bucket_cors,
            'get_bucket_cors': self.test_get_bucket_cors,
            'delete_bucket_cors': self.test_delete_bucket_cors,
            'enable_bucket_versioning': self.test_enable_bucket_versioning,
            'get_bucket_versioning': self.test_get_bucket_versioning,
            'suspend_bucket_versioning': self.test_suspend_bucket_versioning,
            'put_public_access_block': self.test_put_public_access_block,
            'get_public_access_block': self.test_get_public_access_block,
            'put_bucket_policy': self.test_put_bucket_policy,
            'get_bucket_policy': self.test_get_bucket_policy,
            'delete_bucket_policy': self.test_delete_bucket_policy,
            # Object Level
            'put_object': self.test_put_object,
            'get_object': self.test_get_object,
            'head_object': self.test_head_object,
            'copy_object': self.test_copy_object,
            'delete_object': self.test_delete_object,
            'delete_objects': self.test_delete_objects,
            'list_objects': self.test_list_objects,
            'list_object_versions': self.test_list_object_versions,
            'put_object_acl': self.test_put_object_acl,
            'get_object_acl': self.test_get_object_acl,
            'put_object_tagging': self.test_put_object_tagging,
            'get_object_tagging': self.test_get_object_tagging,
            'delete_object_tagging': self.test_delete_object_tagging,
            'initiate_multipart_upload': self.test_initiate_multipart_upload,
            'list_multipart_uploads': self.test_list_multipart_uploads,
            'multipart_repeat_same_part': self.test_multipart_repeat_same_part,
            # HTTP Header-based Conditional PUTs (for distributed job queues)
            'put_object_http_if_match_success': self.test_put_object_http_if_match_success,
            'put_object_http_if_match_412_failure': self.test_put_object_http_if_match_412_failure,
            'put_object_http_if_none_match_success': self.test_put_object_http_if_none_match_success,
            'put_object_http_if_none_match_409_failure': self.test_put_object_http_if_none_match_409_failure,
            # Presigned URL Conditional PUTs (for distributed job queues)
            'presigned_put_if_match_success': self.test_presigned_put_if_match_success,
            'presigned_put_if_match_412_failure': self.test_presigned_put_if_match_412_failure,
            'aws_cli_presigned_if_match_412_failure': self.test_aws_cli_presigned_if_match_412_failure,
            'presigned_put_if_none_match_success': self.test_presigned_put_if_none_match_success,
            'presigned_put_if_none_match_409_failure': self.test_presigned_put_if_none_match_409_failure,
            # Legacy simulated conditional PUTs
            'put_object_if_match': self.test_put_object_if_match,
            'put_object_if_match_fails': self.test_put_object_if_match_fails,
            'put_object_if_not_match': self.test_put_object_if_not_match,
            # Large File Operations
            'put_object_5mb': self.test_put_object_5mb,
            'get_object_5mb': self.test_get_object_5mb,
            'put_get_5mb_immediate': self.test_put_get_5mb_immediate,
            'put_delete_5mb_immediate': self.test_put_delete_5mb_immediate,
            'put_get_1kb_immediate': self.test_put_get_1kb_immediate,
            'put_delete_1kb_immediate': self.test_put_delete_1kb_immediate,
            # Multipart Download Operations (50MB)
            'put_object_50mb': self.test_put_object_50mb,
            'get_object_50mb_multipart': self.test_get_object_50mb_multipart,
            'put_get_50mb_multipart_immediate': self.test_put_get_50mb_multipart_immediate,
            # Object Lock Operations
            'create_bucket_with_object_lock': self.test_create_bucket_with_object_lock,
            'get_object_lock_configuration': self.test_get_object_lock_configuration,
            'put_object_lock_configuration': self.test_put_object_lock_configuration,
            'put_object_retention': self.test_put_object_retention,
            'get_object_retention': self.test_get_object_retention,
            'put_object_legal_hold': self.test_put_object_legal_hold,
            'get_object_legal_hold': self.test_get_object_legal_hold,
            # SSE Operations
            'put_bucket_encryption': self.test_put_bucket_encryption,
            'get_bucket_encryption': self.test_get_bucket_encryption,
            'put_object_with_sse': self.test_put_object_with_sse,
            'get_object_with_sse': self.test_get_object_with_sse,
            'delete_bucket_encryption': self.test_delete_bucket_encryption,
            'put_object_sse_bucket_single_part': self.test_put_object_sse_bucket_single_part,
            'put_object_sse_bucket_multipart': self.test_put_object_sse_bucket_multipart,
            'get_object_sse_bucket_single_part': self.test_get_object_sse_bucket_single_part,
            'get_object_sse_bucket_multipart': self.test_get_object_sse_bucket_multipart,
            'get_object_multipart_sse_bucket': self.test_get_object_multipart_sse_bucket,
            # New MD5/ETag verification tests
            'single_part_put_verify_md5_etag': self.test_single_part_put_verify_md5_etag,
            'multipart_put_verify_md5_etag': self.test_multipart_put_verify_md5_etag,
            'single_part_put_sse_verify_md5_etag': self.test_single_part_put_sse_verify_md5_etag,
            'test_single_part_put_sse_verify_md5_etag': self.test_single_part_put_sse_verify_md5_etag,
            'multipart_put_sse_verify_md5_etag': self.test_multipart_put_sse_verify_md5_etag,
            # Lifecycle Rules Operations
            'put_bucket_lifecycle_configuration': self.test_put_bucket_lifecycle_configuration,
            'get_bucket_lifecycle_configuration': self.test_get_bucket_lifecycle_configuration,
            'delete_bucket_lifecycle_configuration': self.test_delete_bucket_lifecycle_configuration,
        }
        
        if test_name not in test_methods:
            print(f"✗ Test '{test_name}' not found.")
            print(f"\nAvailable Bucket Tests:")
            for key in sorted(test_methods.keys()):
                if key in ['create_bucket', 'delete_bucket', 'head_bucket', 'get_bucket_location', 
                           'put_bucket_acl', 'get_bucket_acl', 'put_bucket_tagging', 'get_bucket_tagging',
                           'delete_bucket_tagging', 'put_bucket_cors', 'get_bucket_cors', 'delete_bucket_cors',
                           'enable_bucket_versioning', 'get_bucket_versioning', 'suspend_bucket_versioning',
                           'put_public_access_block', 'get_public_access_block']:
                    print(f"  - {key}")
            print(f"\nAvailable Object Tests:")
            for key in sorted(test_methods.keys()):
                if key in ['put_object', 'get_object', 'head_object', 'copy_object', 'delete_object',
                           'delete_objects', 'list_objects', 'list_object_versions', 'put_object_acl',
                           'get_object_acl', 'put_object_tagging', 'get_object_tagging', 'delete_object_tagging',
                           'initiate_multipart_upload', 'list_multipart_uploads', 'multipart_repeat_same_part',
                           'put_object_http_if_match_success', 'put_object_http_if_match_412_failure',
                           'put_object_http_if_none_match_success', 'put_object_http_if_none_match_409_failure',
                           'put_object_if_match', 'put_object_if_match_fails', 'put_object_if_not_match']:
                    print(f"  - {key}")
            return
        
        print("=" * 60)
        print(f"Running S3 API Test - {test_name.upper()}")
        print("=" * 60)
        
        self.setup()
        
        try:
            # Pre-setup based on test requirements
            if test_name not in ['create_bucket']:
                self.test_create_bucket()
            
            if test_name in ['put_object', 'head_object', 'get_object', 'copy_object', 'delete_object',
                           'list_objects', 'put_object_acl', 'get_object_acl', 'put_object_tagging',
                           'get_object_tagging', 'delete_object_tagging', 'delete_objects', 'put_object_if_match_fails']:
                self.test_put_object()
            
            if test_name in ['copy_object', 'delete_objects']:
                self.test_put_object()  # Ensure we have second object
            
            if test_name in ['get_object_5mb', 'put_get_5mb_immediate']:
                self.test_put_object_5mb()  # Upload 5MB file first
            
            test_methods[test_name]()
            
            # Cleanup
            if test_name != 'delete_bucket':
                # Try to cleanup objects if they might exist
                try:
                    if test_name in ['put_object', 'copy_object']:
                        self.test_delete_objects()
                    elif test_name in ['head_object', 'get_object', 'list_objects', 'put_object_if_match', 'put_object_if_match_fails', 'put_object_if_not_match']:
                        self.test_delete_object()
                except:
                    pass
                
                try:
                    self.test_delete_bucket()
                except:
                    pass
        finally:
            self.teardown()
        
        self._print_summary()
    
    def _print_summary(self):
        """Print test execution summary."""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for r in self.test_results if r['status'] == 'success')
        failed = sum(1 for r in self.test_results if r['status'] == 'error')
        total = len(self.test_results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        
        if failed > 0:
            print("\nFailed Tests:")
            for result in self.test_results:
                if result['status'] == 'error':
                    print(f"  - {result['test']}: {result['result'].get('message', 'Unknown error')}")
        
        print("=" * 60)

    def test_single_part_put_verify_md5_etag(self):
        """Test: Single part PUT, verifying MD5 and ETag."""
        import hashlib
        print("\n[TEST] Single part PUT, verifying MD5 and ETag...")
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        test_content = 'test content for MD5 validation'
        with open(temp_file, 'w') as f:
            f.write(test_content)
        
        try:
            # Calculate expected MD5 hash
            with open(temp_file, 'rb') as f:
                expected_md5 = hashlib.md5(f.read()).hexdigest()
            print(f"  └─ Expected MD5: {expected_md5}")
            
            result = self.s3_ops.put_object(self.test_bucket, self.test_object_key, temp_file)
            if result['status'] != 'success':
                return False

            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key=self.test_object_key)
            etag = meta['ETag'].strip('"')
            print(f"  └─ Received ETag: {etag}")
            
            # Validate MD5 matches ETag for single part uploads
            if expected_md5 == etag:
                result['message'] = f"✓ MD5 and ETag validation successful! MD5: {expected_md5}"
                result['md5_validated'] = True
            else:
                result['status'] = 'error'
                result['message'] = f"✗ MD5 mismatch! Expected: {expected_md5}, Got: {etag}"
                result['md5_validated'] = False
            
            self._log_result('single_part_put_verify_md5_etag', result)
            return result['status'] == 'success'
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def test_multipart_put_verify_md5_etag(self):
        """Test: Multipart PUT, verifying MD5 and ETag."""
        import hashlib
        print("\n[TEST] Multipart PUT, verifying MD5 and ETag...")
        
        try:
            # Calculate expected MD5 hash of 10MB file
            with open(self.test_file_path_10mb, 'rb') as f:
                expected_md5 = hashlib.md5(f.read()).hexdigest()
            print(f"  └─ Expected MD5 of 10MB file: {expected_md5}")
            
            # Upload 10MB file using FORCED multipart
            result = self.s3_ops.put_object_multipart_force(self.test_bucket, self.test_object_10mb, self.test_file_path_10mb)
            if result['status'] != 'success':
                return False

            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key=self.test_object_10mb)
            etag = meta['ETag'].strip('"')
            print(f"  └─ Received ETag: {etag}")
            print(f"  └─ Upload Type: {result.get('upload_type', 'unknown')} ({result.get('parts_count', 0)} parts)")
            
            # For forced multipart uploads, ETag should have format: <hash>-<part_count>
            if '-' in etag:
                parts_count = etag.split('-')[1]
                result['message'] = f"✓ Forced Multipart ETag validation successful! ETag: {etag} (Parts: {parts_count})"
                result['etag_validated'] = True
                result['multipart_etag'] = etag
                result['parts_count'] = parts_count
                result['upload_type'] = 'multipart'
            else:
                result['status'] = 'error'
                result['message'] = f"✗ Expected multipart ETag format but got single-part: {etag}"
                result['etag_validated'] = False
            
            self._log_result('multipart_put_verify_md5_etag', result)
            return result['status'] == 'success'
        except Exception as e:
            result = {'status': 'error', 'message': f'Multipart MD5/ETag validation failed: {str(e)}'}
            self._log_result('multipart_put_verify_md5_etag', result)
            return False

    def test_single_part_put_sse_verify_md5_etag(self):
        """Test: Single part PUT with SSE, verifying MD5 and ETag."""
        import hashlib
        print("\n[TEST] Single part PUT with SSE, verifying MD5 and ETag...")
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        test_content = 'test content for SSE MD5 validation'
        with open(temp_file, 'w') as f:
            f.write(test_content)

        try:
            # Calculate expected MD5 hash
            with open(temp_file, 'rb') as f:
                expected_md5 = hashlib.md5(f.read()).hexdigest()
            print(f"  └─ Expected MD5: {expected_md5}")
            
            result = self.s3_ops.put_object_with_sse(self.test_bucket, self.test_object_key, temp_file, sse_algorithm='AES256')
            if result['status'] != 'success':
                return False

            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key=self.test_object_key)
            etag = meta['ETag'].strip('"')
            server_side_encryption = meta.get('ServerSideEncryption', 'None')
            print(f"  └─ Received ETag: {etag}")
            print(f"  └─ Server-side encryption: {server_side_encryption}")
            
            # Validate MD5 matches ETag for single part uploads with SSE
            if expected_md5 == etag:
                result['message'] = f"✓ SSE MD5 and ETag validation successful! MD5: {expected_md5}, SSE: {server_side_encryption}"
                result['md5_validated'] = True
                result['sse_enabled'] = server_side_encryption != 'None'
            else:
                result['status'] = 'error'
                result['message'] = f"✗ SSE MD5 mismatch! Expected: {expected_md5}, Got: {etag}"
                result['md5_validated'] = False
            
            self._log_result('single_part_put_sse_verify_md5_etag', result)
            return result['status'] == 'success'
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def test_multipart_put_sse_verify_md5_etag(self):
        """Test: Multipart PUT with SSE, verifying MD5 and ETag."""
        import hashlib
        print("\n[TEST] Multipart PUT with SSE, verifying MD5 and ETag...")
        
        try:
            # Calculate expected MD5 hash of 10MB file
            with open(self.test_file_path_10mb, 'rb') as f:
                expected_md5 = hashlib.md5(f.read()).hexdigest()
            print(f"  └─ Expected MD5 of 10MB file: {expected_md5}")
            
            # Upload 10MB file using FORCED multipart with SSE
            result = self.s3_ops.put_object_multipart_sse_force(self.test_bucket, 'sse-multipart-test.bin', self.test_file_path_10mb)
            if result['status'] != 'success':
                return False

            meta = self.s3_ops.s3_client.head_object(Bucket=self.test_bucket, Key='sse-multipart-test.bin')
            etag = meta['ETag'].strip('"')
            server_side_encryption = meta.get('ServerSideEncryption', 'None')
            print(f"  └─ Received ETag: {etag}")
            print(f"  └─ Server-side encryption: {server_side_encryption}")
            print(f"  └─ Upload Type: {result.get('upload_type', 'unknown')} ({result.get('parts_count', 0)} parts)")
            
            # For forced multipart uploads with SSE, ETag should have format: <hash>-<part_count>
            if '-' in etag:
                parts_count = etag.split('-')[1]
                result['message'] = f"✓ Forced SSE Multipart ETag validation successful! ETag: {etag} (Parts: {parts_count}), SSE: {server_side_encryption}"
                result['etag_validated'] = True
                result['sse_enabled'] = server_side_encryption != 'None'
                result['multipart_etag'] = etag
                result['parts_count'] = parts_count
                result['upload_type'] = 'multipart'
            else:
                result['status'] = 'error'
                result['message'] = f"✗ Expected multipart ETag format with SSE but got single-part: {etag}"
                result['etag_validated'] = False
                result['sse_enabled'] = server_side_encryption != 'None'
            
            self._log_result('multipart_put_sse_verify_md5_etag', result)
            return result['status'] == 'success'
        except Exception as e:
            result = {'status': 'error', 'message': f'SSE Multipart MD5/ETag validation failed: {str(e)}'}
            self._log_result('multipart_put_sse_verify_md5_etag', result)
            return False

if __name__ == "__main__":
    import sys
    
    # Initialize test suite
    suite = S3TestSuite()
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        print(f"Running specific test: {test_name}")
        if not suite.run_specific_test(test_name):
            sys.exit(1)
    else:
        print("Running all tests...")
        if not suite.run_all_tests():
            sys.exit(1)
