
from datetime import UTC, datetime
from cloud_services import get_cloud_service
from cloud_services.logs_providers import CloudWachService
from cloud_services.storage_providers import S3Service
from moto import mock_aws
import os

def test_aws_download_file():
    # s3_provider = get_cloud_service("aws", "storage")
    # s3_provider.dowload_file("test-q99-data", "../qdrant_pdf", "gold/default/pdf/qdrant_pdf/")

    # assert os.path.exists("../qdrant_pdf/.lock")
    # assert os.path.exists("../qdrant_pdf/meta.json")
    # assert os.path.exists("../qdrant_pdf/collection/default_winshare/storage.sqlite")
    mock = mock_aws()
    mock.start()

    file_path = 'test_upload.txt'
    with open(file_path, 'w+') as f:
        f.write('test content')

    s3_provider = get_cloud_service("aws", "storage")
    s3_provider.s3_client.create_bucket(Bucket='my_bucket')
    s3_provider.upload_file(data=file_path,bucket_name="my_bucket",file_path="my_bucket/test.txt")
    s3_provider.dowload_file("my_bucket", "../texts")

    assert os.path.exists("../texts/my_bucket/test.txt")

    if os.path.exists(file_path):
        os.remove(file_path)
    mock.stop()

def test_aws_storage_service():
    mock = mock_aws()
    mock.start()
    
    s3_provider = get_cloud_service("aws", "storage")
    assert isinstance(s3_provider, S3Service)
    s3_provider.s3_client.create_bucket(Bucket='my_bucket')

    file_path = 'test_upload.txt'
    with open(file_path, 'w+') as f:
        f.write('test content')
    
    s3_provider.upload_file(data=file_path,bucket_name="my_bucket",file_path="my_bucket/test.txt")

    test_file = s3_provider.get_file(bucket_name="my_bucket",file_path="my_bucket/test.txt")

    assert test_file.read().decode('utf-8') == "test content"

    if os.path.exists(file_path):
        os.remove(file_path)
    mock.stop()

def test_aws_logging_service():
    mock = mock_aws()
    mock.start()

    cloudwach_provider = get_cloud_service("aws", "logging")
    assert isinstance(cloudwach_provider, CloudWachService)

    cloudwach_provider.create_log_group("log_group")
    cloudwach_provider.create_log_stream("log_group", "logstream")

    log_event = {
            'timestamp': int(datetime.now(UTC).timestamp() * 1000),
            'message': "test log"
        }
																															
    data = {
            'logGroupName': "log_group",
            'logStreamName': "logstream",
            'logEvents': [log_event]
        }
    
    cloudwach_provider.emit_log("log_group", "logstream", "test log")

    response = cloudwach_provider.logs_client.get_log_events(logGroupName="log_group", logStreamName="logstream")
    assert len(response['events']) == 1
    assert response['events'][0]['message'] == 'test log'

    mock.stop()