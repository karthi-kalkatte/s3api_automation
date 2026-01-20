"""S3 API operations module."""
import boto3
from botocore.exceptions import ClientError
from config import S3Config


class S3Operations:
    """Handle S3 API operations."""
    
    def __init__(self, config: S3Config):
        self.config = config
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            endpoint_url=config.endpoint_url if config.endpoint_url != "https://s3.amazonaws.com" else None
        )
    
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
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def create_bucket_with_object_lock(self, bucket_name: str) -> dict:
        """Create an S3 bucket with Object Lock enabled."""
        try:
            if self.config.region == 'us-east-1':
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    ObjectLockEnabledForBucket=True
                )
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.config.region},
                    ObjectLockEnabledForBucket=True
                )
            return {'status': 'success', 'message': f'Bucket {bucket_name} created with Object Lock enabled'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object(self, bucket_name: str, object_key: str, file_path: str) -> dict:
        """Upload an object to S3."""
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)
            return {'status': 'success', 'message': f'Object {object_key} uploaded successfully'}
        except FileNotFoundError:
            return {'status': 'error', 'message': f'File not found: {file_path}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def head_object(self, bucket_name: str, object_key: str) -> dict:
        """Check if object exists and get its metadata."""
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return {
                'status': 'success',
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'content_type': response.get('ContentType', 'N/A')
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {'status': 'error', 'message': 'Object not found'}
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def list_objects(self, bucket_name: str, prefix: str = '') -> dict:
        """List objects in a bucket."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            objects = []
            if 'Contents' in response:
                objects = [{'Key': obj['Key'], 'Size': obj['Size']} for obj in response['Contents']]
            return {
                'status': 'success',
                'objects': objects,
                'count': len(objects)
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_object(self, bucket_name: str, object_key: str) -> dict:
        """Delete an object from S3."""
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            return {'status': 'success', 'message': f'Object {object_key} deleted successfully'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket(self, bucket_name: str) -> dict:
        """Delete an empty S3 bucket."""
        try:
            self.s3_client.delete_bucket(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Bucket {bucket_name} deleted successfully'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def enable_bucket_versioning(self, bucket_name: str) -> dict:
        """Enable versioning on an S3 bucket."""
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            return {'status': 'success', 'message': f'Versioning enabled for bucket {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_versioning(self, bucket_name: str) -> dict:
        """Get versioning status of an S3 bucket."""
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            status = response.get('Status', 'Not Set')
            return {
                'status': 'success',
                'versioning_status': status,
                'message': f'Bucket versioning status: {status}'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def suspend_bucket_versioning(self, bucket_name: str) -> dict:
        """Suspend versioning on an S3 bucket."""
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Suspended'}
            )
            return {'status': 'success', 'message': f'Versioning suspended for bucket {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    # ============ BUCKET LEVEL OPERATIONS ============
    
    def head_bucket(self, bucket_name: str) -> dict:
        """Check if bucket exists and is accessible."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Bucket {bucket_name} exists and is accessible'}
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {'status': 'error', 'message': 'Bucket not found'}
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_location(self, bucket_name: str) -> dict:
        """Get the region/location of a bucket."""
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            location = response.get('LocationConstraint', 'us-east-1')
            return {'status': 'success', 'location': location, 'message': f'Bucket location: {location}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_bucket_acl(self, bucket_name: str, acl: str = 'private') -> dict:
        """Set bucket ACL (Access Control List)."""
        try:
            self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL=acl)
            return {'status': 'success', 'message': f'Bucket ACL set to {acl}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_acl(self, bucket_name: str) -> dict:
        """Get bucket ACL."""
        try:
            response = self.s3_client.get_bucket_acl(Bucket=bucket_name)
            grants = len(response.get('Grants', []))
            owner = response.get('Owner', {}).get('DisplayName', 'Unknown')
            return {
                'status': 'success',
                'owner': owner,
                'grants_count': grants,
                'message': f'Bucket has {grants} grants'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_bucket_tagging(self, bucket_name: str, tags: dict) -> dict:
        """Add tags to a bucket."""
        try:
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self.s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': tag_set})
            return {'status': 'success', 'message': f'Tags added to bucket {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_tagging(self, bucket_name: str) -> dict:
        """Get bucket tags."""
        try:
            response = self.s3_client.get_bucket_tagging(Bucket=bucket_name)
            tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
            return {'status': 'success', 'tags': tags, 'message': f'Retrieved {len(tags)} tags'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket_tagging(self, bucket_name: str) -> dict:
        """Delete all tags from a bucket."""
        try:
            self.s3_client.delete_bucket_tagging(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Tags deleted from bucket {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_bucket_cors(self, bucket_name: str, cors_rules: list) -> dict:
        """Set CORS configuration for a bucket."""
        try:
            self.s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={'CORSRules': cors_rules})
            return {'status': 'success', 'message': f'CORS rules added to bucket {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_cors(self, bucket_name: str) -> dict:
        """Get CORS configuration of a bucket."""
        try:
            response = self.s3_client.get_bucket_cors(Bucket=bucket_name)
            rules_count = len(response.get('CORSRules', []))
            return {'status': 'success', 'rules_count': rules_count, 'message': f'Found {rules_count} CORS rules'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket_cors(self, bucket_name: str) -> dict:
        """Delete CORS configuration from a bucket."""
        try:
            self.s3_client.delete_bucket_cors(Bucket=bucket_name)
            return {'status': 'success', 'message': f'CORS configuration deleted from {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_public_access_block(self, bucket_name: str, block_all: bool = True) -> dict:
        """Block public access to a bucket."""
        try:
            self.s3_client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': block_all,
                    'IgnorePublicAcls': block_all,
                    'BlockPublicPolicy': block_all,
                    'RestrictPublicBuckets': block_all
                }
            )
            return {'status': 'success', 'message': f'Public access block applied to {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_public_access_block(self, bucket_name: str) -> dict:
        """Get public access block configuration."""
        try:
            response = self.s3_client.get_public_access_block(Bucket=bucket_name)
            config = response['PublicAccessBlockConfiguration']
            return {
                'status': 'success',
                'block_public_acls': config['BlockPublicAcls'],
                'ignore_public_acls': config['IgnorePublicAcls'],
                'block_public_policy': config['BlockPublicPolicy'],
                'restrict_public_buckets': config['RestrictPublicBuckets'],
                'message': 'Public access block configuration retrieved'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_bucket_policy(self, bucket_name: str, policy: dict = None) -> dict:
        """Set bucket policy."""
        try:
            if policy is None:
                # Default policy allowing all actions on bucket
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:*",
                            "Resource": [
                                f"arn:aws:s3:::{bucket_name}",
                                f"arn:aws:s3:::{bucket_name}/*"
                            ]
                        }
                    ]
                }
            
            import json
            policy_str = json.dumps(policy)
            self.s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)
            return {'status': 'success', 'message': f'Bucket policy applied to {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_policy(self, bucket_name: str) -> dict:
        """Get bucket policy."""
        try:
            response = self.s3_client.get_bucket_policy(Bucket=bucket_name)
            policy = response.get('Policy', {})
            return {
                'status': 'success',
                'policy': policy,
                'message': f'Bucket policy retrieved for {bucket_name}'
            }
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                return {'status': 'error', 'message': 'No bucket policy found'}
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket_policy(self, bucket_name: str) -> dict:
        """Delete bucket policy."""
        try:
            self.s3_client.delete_bucket_policy(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Bucket policy deleted from {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    # ============ OBJECT LEVEL OPERATIONS ============
    
    def get_object(self, bucket_name: str, object_key: str, file_path: str = None) -> dict:
        """Download an object from S3."""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            if file_path:
                with open(file_path, 'wb') as f:
                    f.write(response['Body'].read())
            return {
                'status': 'success',
                'size': response['ContentLength'],
                'message': f'Object {object_key} downloaded successfully'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> dict:
        """Copy an object from one location to another."""
        try:
            self.s3_client.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Bucket=dest_bucket,
                Key=dest_key
            )
            return {'status': 'success', 'message': f'Object copied to {dest_bucket}/{dest_key}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_acl(self, bucket_name: str, object_key: str, acl: str = 'private') -> dict:
        """Set object ACL."""
        try:
            self.s3_client.put_object_acl(Bucket=bucket_name, Key=object_key, ACL=acl)
            return {'status': 'success', 'message': f'Object ACL set to {acl}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_acl(self, bucket_name: str, object_key: str) -> dict:
        """Get object ACL."""
        try:
            response = self.s3_client.get_object_acl(Bucket=bucket_name, Key=object_key)
            grants = len(response.get('Grants', []))
            return {'status': 'success', 'grants_count': grants, 'message': f'Object has {grants} grants'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_tagging(self, bucket_name: str, object_key: str, tags: dict) -> dict:
        """Add tags to an object."""
        try:
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self.s3_client.put_object_tagging(Bucket=bucket_name, Key=object_key, Tagging={'TagSet': tag_set})
            return {'status': 'success', 'message': f'Tags added to object {object_key}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_tagging(self, bucket_name: str, object_key: str) -> dict:
        """Get object tags."""
        try:
            response = self.s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
            tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
            return {'status': 'success', 'tags': tags, 'message': f'Retrieved {len(tags)} tags'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_object_tagging(self, bucket_name: str, object_key: str) -> dict:
        """Delete all tags from an object."""
        try:
            self.s3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)
            return {'status': 'success', 'message': f'Tags deleted from object {object_key}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_objects(self, bucket_name: str, object_keys: list) -> dict:
        """Delete multiple objects in batch."""
        try:
            objects_to_delete = [{'Key': key} for key in object_keys]
            response = self.s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
            deleted_count = len(response.get('Deleted', []))
            error_count = len(response.get('Errors', []))
            return {
                'status': 'success',
                'deleted': deleted_count,
                'errors': error_count,
                'message': f'Deleted {deleted_count} objects with {error_count} errors'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def list_object_versions(self, bucket_name: str) -> dict:
        """List all object versions in a versioned bucket."""
        try:
            response = self.s3_client.list_object_versions(Bucket=bucket_name)
            versions = len(response.get('Versions', []))
            return {
                'status': 'success',
                'versions_count': versions,
                'message': f'Found {versions} object versions'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def initiate_multipart_upload(self, bucket_name: str, object_key: str) -> dict:
        """Initiate a multipart upload."""
        try:
            response = self.s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
            upload_id = response['UploadId']
            return {
                'status': 'success',
                'upload_id': upload_id,
                'message': f'Multipart upload initiated with ID: {upload_id}'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def list_multipart_uploads(self, bucket_name: str) -> dict:
        """List all ongoing multipart uploads."""
        try:
            response = self.s3_client.list_multipart_uploads(Bucket=bucket_name)
            uploads = len(response.get('Uploads', []))
            return {
                'status': 'success',
                'uploads_count': uploads,
                'message': f'Found {uploads} ongoing multipart uploads'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    # ============ OBJECT LOCK OPERATIONS ============
    
    def get_object_lock_configuration(self, bucket_name: str) -> dict:
        """Get Object Lock configuration for a bucket."""
        try:
            response = self.s3_client.get_object_lock_configuration(Bucket=bucket_name)
            config = response.get('ObjectLockConfiguration', {})
            return {
                'status': 'success',
                'enabled': config.get('ObjectLockEnabled') == 'Enabled',
                'rule': config.get('Rule', {}),
                'message': f'Object Lock configuration retrieved'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_lock_configuration(self, bucket_name: str, mode: str = 'GOVERNANCE', days: int = 30) -> dict:
        """Set default retention for Object Lock."""
        try:
            self.s3_client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    'ObjectLockEnabled': 'Enabled',
                    'Rule': {
                        'DefaultRetention': {
                            'Mode': mode,  # GOVERNANCE or COMPLIANCE
                            'Days': days
                        }
                    }
                }
            )
            return {'status': 'success', 'message': f'Object Lock default retention set to {mode} mode for {days} days'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_retention(self, bucket_name: str, object_key: str, mode: str = 'GOVERNANCE', days: int = 30) -> dict:
        """Set retention on a specific object."""
        try:
            from datetime import datetime, timedelta
            retain_until = datetime.utcnow() + timedelta(days=days)
            
            self.s3_client.put_object_retention(
                Bucket=bucket_name,
                Key=object_key,
                Retention={
                    'Mode': mode,
                    'RetainUntilDate': retain_until
                }
            )
            return {'status': 'success', 'message': f'Object retention set to {mode} mode until {retain_until.isoformat()}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_retention(self, bucket_name: str, object_key: str) -> dict:
        """Get retention settings for an object."""
        try:
            response = self.s3_client.get_object_retention(Bucket=bucket_name, Key=object_key)
            retention = response.get('Retention', {})
            return {
                'status': 'success',
                'mode': retention.get('Mode'),
                'retain_until': retention.get('RetainUntilDate'),
                'message': f'Object retention mode: {retention.get("Mode")}'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_legal_hold(self, bucket_name: str, object_key: str, status: str = 'ON') -> dict:
        """Set legal hold on an object."""
        try:
            self.s3_client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=object_key,
                LegalHold={'Status': status}  # ON or OFF
            )
            return {'status': 'success', 'message': f'Legal hold {status} for object {object_key}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_legal_hold(self, bucket_name: str, object_key: str) -> dict:
        """Get legal hold status for an object."""
        try:
            response = self.s3_client.get_object_legal_hold(Bucket=bucket_name, Key=object_key)
            legal_hold = response.get('LegalHold', {})
            return {
                'status': 'success',
                'legal_hold_status': legal_hold.get('Status'),
                'message': f'Legal hold status: {legal_hold.get("Status")}'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}    
    # ============ SSE (SERVER-SIDE ENCRYPTION) OPERATIONS ============
    
    def put_bucket_encryption(self, bucket_name: str, sse_algorithm: str = 'AES256') -> dict:
        """Enable server-side encryption for a bucket."""
        try:
            if sse_algorithm == 'aws:kms':
                encryption_config = {
                    'Rules': [{
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms'
                        }
                    }]
                }
            else:
                encryption_config = {
                    'Rules': [{
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }]
                }
            
            self.s3_client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=encryption_config
            )
            return {'status': 'success', 'message': f'Bucket encryption enabled with {sse_algorithm}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_encryption(self, bucket_name: str) -> dict:
        """Get bucket encryption configuration."""
        try:
            response = self.s3_client.get_bucket_encryption(Bucket=bucket_name)
            config = response.get('ServerSideEncryptionConfiguration', {})
            rules = config.get('Rules', [])
            sse_algo = rules[0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] if rules else 'None'
            return {
                'status': 'success',
                'sse_algorithm': sse_algo,
                'message': f'Bucket encryption: {sse_algo}'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def put_object_with_sse(self, bucket_name: str, object_key: str, file_path: str, sse_algorithm: str = 'AES256') -> dict:
        """Upload an object with server-side encryption."""
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=f,
                    ServerSideEncryption=sse_algorithm
                )
            return {'status': 'success', 'message': f'Object {object_key} uploaded with {sse_algorithm} encryption'}
        except FileNotFoundError:
            return {'status': 'error', 'message': f'File not found: {file_path}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_with_sse(self, bucket_name: str, object_key: str, file_path: str = None) -> dict:
        """Download an encrypted object and verify encryption."""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            sse_algo = response.get('ServerSideEncryption', 'None')
            
            if file_path:
                with open(file_path, 'wb') as f:
                    f.write(response['Body'].read())
            else:
                response['Body'].read()
            
            return {
                'status': 'success',
                'encryption': sse_algo,
                'size': response['ContentLength'],
                'message': f'Object downloaded with {sse_algo} encryption'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket_encryption(self, bucket_name: str) -> dict:
        """Remove encryption from a bucket."""
        try:
            self.s3_client.delete_bucket_encryption(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Bucket encryption removed'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    # ============ LIFECYCLE RULES OPERATIONS ============
    
    def put_bucket_lifecycle_configuration(self, bucket_name: str, rules: list = None) -> dict:
        """Set lifecycle configuration for a bucket."""
        try:
            if rules is None:
                rules = [
                    {
                        'ID': 'ExpireCurrentAndOld1Day',
                        'Status': 'Enabled',
                        'Prefix': '',
                        'Expiration': {'Days': 1},
                        'NoncurrentVersionExpiration': {'NoncurrentDays': 1}
                    }
                ]
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={'Rules': rules}
            )
            return {'status': 'success', 'message': f'Lifecycle rules configured for {bucket_name}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_bucket_lifecycle_configuration(self, bucket_name: str) -> dict:
        """Get lifecycle configuration for a bucket."""
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            rules = response.get('Rules', [])
            return {
                'status': 'success',
                'rules_count': len(rules),
                'rules': [{'Id': r.get('Id'), 'Status': r.get('Status')} for r in rules],
                'message': f'Found {len(rules)} lifecycle rules'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def delete_bucket_lifecycle_configuration(self, bucket_name: str) -> dict:
        """Delete lifecycle configuration from a bucket."""
        try:
            self.s3_client.delete_bucket_lifecycle(Bucket=bucket_name)
            return {'status': 'success', 'message': f'Lifecycle configuration deleted'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    # ============ LARGE FILE MULTIPART DOWNLOAD OPERATIONS ============
    
    def put_object_50mb(self, bucket_name: str, object_key: str, file_path: str) -> dict:
        """Upload a 50MB object."""
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)
            return {'status': 'success', 'message': f'Object {object_key} (50MB) uploaded successfully'}
        except FileNotFoundError:
            return {'status': 'error', 'message': f'File not found: {file_path}'}
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}
    
    def get_object_50mb_multipart(self, bucket_name: str, object_key: str, file_path: str = None, part_size: int = 10 * 1024 * 1024) -> dict:
        """Download a 50MB object in multipart (default 10MB parts)."""
        try:
            # Get object metadata to determine size
            head_response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            total_size = head_response['ContentLength']
            
            # Download in parts
            downloaded_size = 0
            parts_downloaded = 0
            
            if file_path:
                with open(file_path, 'wb') as f:
                    for start_byte in range(0, total_size, part_size):
                        end_byte = min(start_byte + part_size - 1, total_size - 1)
                        range_header = f'bytes={start_byte}-{end_byte}'
                        
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key, Range=range_header)
                        part_data = response['Body'].read()
                        f.write(part_data)
                        
                        downloaded_size += len(part_data)
                        parts_downloaded += 1
            else:
                for start_byte in range(0, total_size, part_size):
                    end_byte = min(start_byte + part_size - 1, total_size - 1)
                    range_header = f'bytes={start_byte}-{end_byte}'
                    
                    response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key, Range=range_header)
                    part_data = response['Body'].read()
                    
                    downloaded_size += len(part_data)
                    parts_downloaded += 1
            
            size_mb = total_size / (1024 * 1024)
            return {
                'status': 'success',
                'total_size': total_size,
                'parts_downloaded': parts_downloaded,
                'part_size_mb': part_size / (1024 * 1024),
                'message': f'Downloaded 50MB object in {parts_downloaded} parts ({size_mb:.2f} MB total)'
            }
        except ClientError as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': f'Unexpected error: {str(e)}'}