"""
Storage module for saving processed data
Supports local filesystem and S3-compatible storage (MinIO)
"""
import json
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

class DataStorage:
    def __init__(self, 
                 storage_type='local',
                 bucket_name='schema-data',
                 endpoint_url='http://localhost:9000',
                 access_key='minioadmin',
                 secret_key='minioadmin'):
        """
        Initialize storage
        storage_type: 'local' or 's3'
        """
        self.storage_type = storage_type
        self.bucket_name = bucket_name
        
        if storage_type == 's3':
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            self._ensure_bucket_exists()
            print(f"✅ S3 storage initialized (bucket: {bucket_name})")
        else:
            self.local_dir = 'storage'
            os.makedirs(self.local_dir, exist_ok=True)
            print(f"✅ Local storage initialized (directory: {self.local_dir})")
    
    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"📦 Created bucket: {self.bucket_name}")
            except ClientError as e:
                print(f"❌ Failed to create bucket: {e}")
    
    def save_record(self, record, schema, record_id=None):
        """
        Save record with its inferred schema
        """
        if not record_id:
            record_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        
        data_to_save = {
            'record_id': record_id,
            'timestamp': datetime.utcnow().isoformat(),
            'record': record,
            'schema': schema
        }
        
        filename = f"record_{record_id}.json"
        
        if self.storage_type == 's3':
            return self._save_to_s3(filename, data_to_save)
        else:
            return self._save_to_local(filename, data_to_save)
    
    def _save_to_s3(self, filename, data):
        """Save to S3/MinIO"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            print(f"  💾 Saved to S3: s3://{self.bucket_name}/{filename}")
            return True
        except ClientError as e:
            print(f"  ❌ Failed to save to S3: {e}")
            return False
    
    def _save_to_local(self, filename, data):
        """Save to local filesystem"""
        try:
            filepath = os.path.join(self.local_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"  💾 Saved locally: {filepath}")
            return True
        except Exception as e:
            print(f"  ❌ Failed to save locally: {e}")
            return False
    
    def list_records(self, limit=10):
        """List stored records"""
        if self.storage_type == 's3':
            return self._list_s3_records(limit)
        else:
            return self._list_local_records(limit)
    
    def _list_s3_records(self, limit):
        """List records from S3"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=limit
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            print(f"❌ Failed to list S3 records: {e}")
            return []
    
    def _list_local_records(self, limit):
        """List records from local storage"""
        try:
            files = os.listdir(self.local_dir)
            json_files = [f for f in files if f.endswith('.json')]
            return sorted(json_files, reverse=True)[:limit]
        except Exception as e:
            print(f"❌ Failed to list local records: {e}")
            return []


# Test the storage module
if __name__ == "__main__":
    print("Testing Storage Module\n")
    
    # Test local storage
    storage = DataStorage(storage_type='local')
    
    test_record = {"product_id": "123", "price": "$99.99"}
    test_schema = {
        "fields": {
            "product_id": {"type": "string", "confidence": 1.0},
            "price": {"type": "currency", "confidence": 0.95}
        }
    }
    
    storage.save_record(test_record, test_schema, record_id="test_001")
    
    print("\n📋 Stored records:")
    records = storage.list_records()
    for r in records:
        print(f"  - {r}")