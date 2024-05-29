
from cloud_service.logs_providers import CloudWachService
from cloud_service.storage_providers import S3Service


def get_cloud_service(cloud, service):
    providers = {
        "aws":{
            "storage": S3Service,
            "logging": CloudWachService
            },
        "azure":{},
        "gcp":{}
    }
    
    
    return providers[cloud][service]()


