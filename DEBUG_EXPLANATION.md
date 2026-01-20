# How `put_object_50mb` Test Works (Isolated Execution)

## Command
```bash
python main.py --test put_object_50mb
```

---

## Code Execution Flow

### **Step 1: main.py Entry Point**
```python
# main.py
test_suite = S3TestSuite()
test_suite.run_specific_test('put_object_50mb')  # Called with test name
```

---

### **Step 2: run_specific_test() Method**
Location: `test_suite.py` (lines 751-902)

```python
def run_specific_test(self, test_name: str):
    """Run a specific test case."""
    test_methods = {
        'put_object_50mb': self.test_put_object_50mb,
        # ... other tests ...
    }
    
    # VALIDATION: Check if test exists
    if test_name not in test_methods:
        print(f"✗ Test '{test_name}' not found.")
        return  # Exit if test doesn't exist
    
    # === SETUP PHASE ===
    print("=" * 60)
    print(f"Running S3 API Test - PUT_OBJECT_50MB")
    print("=" * 60)
    
    self.setup()  # ⬅️ CREATE TEMP FILES (including 50MB file)
```

---

### **Step 3: setup() Method**
Location: `test_suite.py` (lines 31-55)

**Creates temporary test files:**

```python
def setup(self):
    """Setup test environment."""
    # Creates 1KB test file
    self.test_file_path_1kb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
    with open(self.test_file_path_1kb, 'wb') as f:
        f.write(b'X' * 1024)  # 1024 bytes
    
    # Creates 5MB test file
    self.test_file_path_5mb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
    with open(self.test_file_path_5mb, 'wb') as f:
        chunk = b'0' * (1024 * 1024)  # 1MB chunk
        for _ in range(5):
            f.write(chunk)  # Write 5 times = 5MB
    
    # ⬅️ Creates 50MB test file
    self.test_file_path_50mb = tempfile.NamedTemporaryFile(delete=False, suffix='.bin').name
    with open(self.test_file_path_50mb, 'wb') as f:
        chunk = b'X' * (1024 * 1024)  # 1MB chunk
        for _ in range(50):
            f.write(chunk)  # Write 50 times = 50MB
    
    print(f"✓ Setup complete - Test files created (1KB, 5MB, and 50MB files)")
```

**Result:**
- Creates temporary file with 50MB of data
- Stores path in `self.test_file_path_50mb`

---

### **Step 4: Automatic Bucket Creation**
Back in `run_specific_test()`:

```python
try:
    # Pre-setup based on test requirements
    # put_object_50mb is NOT in ['create_bucket'], so bucket IS created
    if test_name not in ['create_bucket']:
        self.test_create_bucket()  # ⬅️ CREATES BUCKET
```

**test_create_bucket() method:**

```python
def test_create_bucket(self):
    """Test: Create bucket"""
    print("\n[TEST] Creating bucket...")
    result = self.s3_ops.create_bucket(self.test_bucket)
    self._log_result('create_bucket', result)
    return result['status'] == 'success'
```

**In s3_operations.py:**

```python
def create_bucket(self, bucket_name: str) -> dict:
    """Create an S3 bucket."""
    try:
        if self.config.region == 'us-east-1':
            self.s3_client.create_bucket(Bucket=bucket_name)
        else:
            self.s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': self.config.region}
            )
        return {'status': 'success', 'message': f'Bucket {bucket_name} created successfully'}
    except ClientError as e:
        return {'status': 'error', 'message': str(e)}
```

**Result:**
- Bucket created with name like: `test-bucket-a1b2c3d4` (random hex suffix)

---

### **Step 5: Prerequisites Check**
Still in `run_specific_test()`:

```python
if test_name in ['get_object_5mb', 'put_get_5mb_immediate']:
    self.test_put_object_5mb()  # Upload 5MB file first

# put_object_50mb NOT in this list, so NO automatic upload
```

**Result:**
- No automatic prerequisites needed for `put_object_50mb`

---

### **Step 6: Execute the Test**
Back in `run_specific_test()`:

```python
test_methods[test_name]()  # ⬅️ Calls: self.test_put_object_50mb()
```

**test_put_object_50mb() method:**

```python
def test_put_object_50mb(self):
    """Test: Upload 50MB object"""
    print("\n[TEST] Uploading 50MB object...")
    
    # Calls S3 operation to upload
    result = self.s3_ops.put_object_50mb(
        self.test_bucket,           # Bucket name: "test-bucket-a1b2c3d4"
        self.test_object_50mb,      # Object key: "test-object-50mb.bin"
        self.test_file_path_50mb    # Local file path: temp file with 50MB data
    )
    
    self._log_result('put_object_50mb', result)
    return result['status'] == 'success'
```

**In s3_operations.py:**

```python
def put_object_50mb(self, bucket_name: str, object_key: str, file_path: str) -> dict:
    """Upload a 50MB object."""
    try:
        with open(file_path, 'rb') as f:  # ⬅️ Opens local temp file
            self.s3_client.put_object(
                Bucket=bucket_name,        # "test-bucket-a1b2c3d4"
                Key=object_key,            # "test-object-50mb.bin"
                Body=f                     # 50MB data stream
            )
        return {'status': 'success', 'message': f'Object {object_key} (50MB) uploaded successfully'}
    except FileNotFoundError:
        return {'status': 'error', 'message': f'File not found: {file_path}'}
    except ClientError as e:
        return {'status': 'error', 'message': str(e)}
```

**Result:**
- 50MB object uploaded to S3 bucket
- Object named: `test-object-50mb.bin`

---

### **Step 7: Cleanup (teardown)**
Back in `run_specific_test()`:

```python
finally:
    self.teardown()  # ⬅️ CLEANUP
```

**teardown() method:**

```python
def teardown(self):
    """Cleanup test environment."""
    for file_path in [self.test_file_path, self.test_file_path2, 
                      self.test_file_path_5mb, self.test_file_path_1kb, 
                      self.test_file_path_50mb]:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)  # ⬅️ DELETE TEMP 50MB FILE
    print("✓ Cleanup complete - Test files removed")
```

**Result:**
- Temporary 50MB file deleted from disk
- Test bucket remains (user can manually delete)

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ python main.py --test put_object_50mb                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │ run_specific_test('put_object_50mb')│
        └────────────────┬───────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │ setup()                             │
        │ - Create 50MB temp file in disk    │
        │ - File path: C:\Users\...\tmp...   │
        └────────────────┬───────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │ test_create_bucket()                │
        │ - Create S3 bucket                 │
        │ - Bucket name: test-bucket-xxxxx   │
        └────────────────┬───────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │ test_put_object_50mb()              │
        │ - Read 50MB file from disk         │
        │ - Upload to S3 bucket              │
        │ - Object: test-object-50mb.bin     │
        └────────────────┬───────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │ teardown()                          │
        │ - Delete temp 50MB file from disk  │
        └────────────────────────────────────┘
```

---

## Key Points

✓ **Automatic Bucket Creation**: When running a specific test, bucket is automatically created (unless test_name is 'create_bucket')

✓ **File Creation**: 50MB temp file created during setup() with actual 50MB data

✓ **S3 Upload**: boto3's put_object() streams the file directly to S3

✓ **Cleanup**: Temporary file deleted after test, but S3 object remains

✓ **Object Key**: `test-object-50mb.bin` stored in S3 bucket

---

## Variables Used

| Variable | Value | Purpose |
|----------|-------|---------|
| `self.test_bucket` | `test-bucket-{random_hex}` | S3 bucket name |
| `self.test_object_50mb` | `test-object-50mb.bin` | S3 object key |
| `self.test_file_path_50mb` | `C:\Users\...\temp\xxx.bin` | Local temp file (50MB) |
| `self.config.endpoint_url` | `https://s3.us-east-1.idrivee2.com` | S3-compatible service endpoint |
