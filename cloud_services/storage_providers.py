
from abc import ABC, abstractmethod
import hashlib
import os
import tempfile
import boto3
import botocore
from botocore.config import Config
from azure.storage.blob import BlobServiceClient

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
    
    @abstractmethod
    def upload_bites_file(self, data, bucket_name, file_path):
        ...
    
    @abstractmethod
    def download_bites_file(self, bucket_name, download_location):
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
    
    async def files_discovery(self, bucket_name, ingested_paths, latest_created_at, max_file_size_mb=500, use_hash=False, prefix = ""):
        seen_identifiers = set()
        discovered_paths = []

        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                object_key = obj['Key']
                if object_key.endswith('/') and obj['Size'] == 0:
                    continue
                s3_path = f"s3://{bucket_name}/{object_key}"

                if s3_path in ingested_paths:
                    continue

                last_modified = int(obj['LastModified'].timestamp())
                if last_modified <= latest_created_at:
                    continue

                try:
                    file_size = obj['Size']
                    if file_size > max_file_size_mb * 1024 * 1024:
                        continue

                    if use_hash:
                        hasher = hashlib.sha256()
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                        with response['Body'] as file_obj:
                            while chunk := file_obj.read(8192):
                                hasher.update(chunk)
                        file_identifier = hasher.hexdigest()

                        if file_identifier in seen_identifiers:
                            continue
                        seen_identifiers.add(file_identifier)

                    discovered_paths.append(s3_path)

                except botocore.exceptions.BotoCoreError:
                    continue
                except Exception:
                    continue

        return discovered_paths
    
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
    
    def upload_bites_file(self, data, bucket_name, file_path):
        return self.s3_client.upload_fileobj(data, bucket_name, file_path)
    
    def download_bites_file(self, bucket_name, file_location):
        fp = tempfile.TemporaryFile()
        self.s3_client.download_fileobj(Bucket=bucket_name, Key= file_location, Fileobj=fp)
        fp.seek(0)
        return fp
    

class AzureBlobService(AbstractStorageService):
    def __init__(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    async def files_discovery(self, container_name, ingested_paths, latest_created_at, max_file_size_mb=500, use_hash=False, prefix=""):
        seen_identifiers = set()
        discovered_paths = []

        container_client = self.blob_service_client.get_container_client(container_name)

        async for blob in container_client.list_blobs(name_starts_with=prefix):
            blob_path = f"azure://{container_name}/{blob.name}"

            if blob_path in ingested_paths:
                continue

            if not blob.last_modified:
                continue

            blob_ts = int(blob.last_modified.timestamp())
            if blob_ts <= latest_created_at:
                continue

            blob_size = blob.size or 0
            if blob_size > max_file_size_mb * 1024 * 1024:
                continue

            try:
                if use_hash:
                    hasher = hashlib.sha256()
                    downloader = await container_client.download_blob(blob.name)
                    async for chunk in downloader.chunks():
                        hasher.update(chunk)
                    file_identifier = hasher.hexdigest()

                    if file_identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(file_identifier)

                discovered_paths.append(blob_path)

            except Exception:
                continue

        return discovered_paths


    def get_file(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        return blob_client.download_blob().readall()

    def upload_file(self, file_path, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    def delete_file(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()

    def dowload_file(self, container_name, download_location, path_prefix=""):
        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=path_prefix)
        for blob in blobs:
            rel_path = os.path.relpath(blob.name, start=path_prefix)
            local_path = os.path.join(download_location, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as file:
                data = container_client.download_blob(blob.name)
                file.write(data.readall())

    def upload_bites_file(self, data, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(data, overwrite=True)

    def download_bites_file(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        fp = tempfile.TemporaryFile()
        data = blob_client.download_blob()
        fp.write(data.readall())
        fp.seek(0)
        return fp