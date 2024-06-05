
from abc import ABC, abstractmethod
import os
import boto3
from botocore.config import Config


from cloud_services.env_vars import AWS_KEY, AWS_REGION, AWS_SECRET, AWS_URL

class AbstractStorageService(ABC):
    @abstractmethod
    def get_file(self, bucket_name, file_path):
        ...
    
    @abstractmethod
    def upload_file(self, data, bucket_name, file_path):
        ...
    
    @abstractmethod
    def delete_file(self, bucket_name, file_path):
        ...

    @abstractmethod
    def dowload_file(self, bucket_name):
        ...

class S3Service(AbstractStorageService):
    s3_default = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "endpoint_url": AWS_URL,
    }
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            config=Config(region_name=AWS_REGION),
            **self.s3_default
        )
    
    def get_file(self, bucket_name, file_path):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response["Body"]

    def upload_file(self, data, bucket_name, file_path):    
        return self.s3_client.upload_file(data, bucket_name, file_path)
    
    def delete_file(self, bucket_name, file_path):
        return self.s3_client.delete_object(Bucket=bucket_name, Key=file_path)
    
    def dowload_file(self, bucket_name, download_location, path_prefix=""):
        bucket_objects=self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)
        for s3_key in bucket_objects["Contents"]:
            relative_path = os.path.relpath(s3_key["Key"], start=path_prefix)
            local_file_path = os.path.join(download_location, relative_path)
            local_dir_path = os.path.dirname(local_file_path)
            os.makedirs(local_dir_path, exist_ok=True)
            self.s3_client.download_file(bucket_name, s3_key["Key"], local_file_path)