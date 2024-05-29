# Cloud_Service_Library
This library provides an interfase to connect to different cloud providers services

## how to use it

import the methoh get_cloud_service() and pass as parameter the cloud provider and the service you want to use

from cloud_service import get_cloud_service

log_service = get_cloud_service("aws", "logging")

log_service.create_log_group("log_group_name")

## List of services avilables

### Storage Service (storage)

#### Available methods

* get_file
* upload_file
* delete_file

### Logging Service (logging)

#### Available methods

* create_log_stream
* create_log_group
* emit_log 

## List of cloud providers

### Amazon web service (aws)

#### Required environment variables

* AWS_KEY: aws access key
* AWS_SECRET: aws secret key
* AWS_REGION: aws region 