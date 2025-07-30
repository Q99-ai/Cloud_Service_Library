from abc import ABC, abstractmethod
from datetime import UTC, datetime
import boto3
from botocore.config import Config

from cloud_services.env_vars import AWS_KEY, AWS_REGION, AWS_SECRET, AWS_URL

class AbstractLogService(ABC):
    @abstractmethod
    def create_log_stream(self, log_group, log_stream):
        ...
    
    @abstractmethod
    def create_log_group(self, log_group):
        ...
    
    @abstractmethod
    def emit_log(self, log_group, log_stream, log_message):
        ...

class CloudWachService(AbstractLogService):
    s3_default = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "endpoint_url": AWS_URL,
    }
    def __init__(self):
        self.logs_client = boto3.client(
            'logs',
            config=Config(region_name=AWS_REGION),
            **self.s3_default
        )
    
    def create_log_stream(self, log_group, log_stream):
        self.logs_client.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    
    def create_log_group(self, log_group):
        self.logs_client.create_log_group(logGroupName=log_group)

    def emit_log(self, log_group, log_stream, log_message):
        timestamp = int(datetime.now(UTC).timestamp() * 1000)  # AWS uses milliseconds

        log_event = {
            'timestamp': timestamp,
            'message': log_message
        }
																															
        kwargs = {
            'logGroupName': log_group,
            'logStreamName': log_stream,
            'logEvents': [log_event]
        }
        
        return self.logs_client.put_log_events(**kwargs)
    
