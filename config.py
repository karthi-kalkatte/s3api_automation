"""Configuration module for S3 API automation tests."""
import json
import os


class S3Config:
    """Load and manage S3 configuration."""
    
    def __init__(self, credentials_file="credentials.json"):
        self.credentials_file = credentials_file
        self.config = self._load_credentials()
    
    def _load_credentials(self):
        """Load credentials from JSON file."""
        if not os.path.exists(self.credentials_file):
            raise FileNotFoundError(f"Credentials file not found: {self.credentials_file}")
        
        with open(self.credentials_file, 'r') as f:
            return json.load(f)
    
    @property
    def access_key(self):
        return self.config.get('access_key')
    
    @property
    def secret_key(self):
        return self.config.get('secret_key')
    
    @property
    def region(self):
        return self.config.get('region', 'us-east-1')
    
    @property
    def endpoint_url(self):
        return self.config.get('endpoint_url', 'https://s3.amazonaws.com')
    
    @property
    def bucket_name(self):
        """Test bucket name - can be overridden."""
        return self.config.get('bucket_name', 'test-automation-bucket')
