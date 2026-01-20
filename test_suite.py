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
        self.test_object_1kb = "test-object-1kb.bin"
        self.test_file_path = None
        self.test_file_path2 = None
        self.test_file_path_5mb = None
        self.test_file_path_1kb = None
        self.test_file_path_5mb = None
    
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
        
        print(f"✓ Setup complete - Test files created (1KB and 5MB files)")
    
    def teardown(self):
        """Cleanup test environment."""
        for file_path in [self.test_file_path, self.test_file_path2, self.test_file_path_5mb, self.test_file_path_1kb]:
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
        """Test: Get public access block"""
        print("\n[TEST] Getting public access block configuration...")
        result = self.s3_ops.get_public_access_block(self.test_bucket)
        self._log_result('get_public_access_block', result)
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
            
            # Copy and Multipart
            self.test_copy_object()
            self.test_initiate_multipart_upload()
            self.test_list_multipart_uploads()
            self.test_list_object_versions()
            
            # Large File Operations
            self.test_put_object_5mb()
            self.test_get_object_5mb()
            self.test_put_get_5mb_immediate()
            self.test_put_delete_5mb_immediate()
            self.test_put_get_1kb_immediate()
            self.test_put_delete_1kb_immediate()
            
            # SSE (Server-Side Encryption) Operations
            self.test_put_bucket_encryption()
            self.test_get_bucket_encryption()
            self.test_put_object_with_sse()
            self.test_get_object_with_sse()
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
            # Large File Operations
            'put_object_5mb': self.test_put_object_5mb,
            'get_object_5mb': self.test_get_object_5mb,
            'put_get_5mb_immediate': self.test_put_get_5mb_immediate,
            'put_delete_5mb_immediate': self.test_put_delete_5mb_immediate,
            'put_get_1kb_immediate': self.test_put_get_1kb_immediate,
            'put_delete_1kb_immediate': self.test_put_delete_1kb_immediate,
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
                           'initiate_multipart_upload', 'list_multipart_uploads']:
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
                           'get_object_tagging', 'delete_object_tagging', 'delete_objects']:
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
                    elif test_name in ['head_object', 'get_object', 'list_objects']:
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
