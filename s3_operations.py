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
