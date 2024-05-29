
from abc import ABC, abstractmethod
import boto3
from botocore.config import Config


from cloud_services.env_vars import AWS_KEY, AWS_REGION, AWS_SECRET

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

class S3Service(AbstractStorageService):
    s3_default = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
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