import os
from gear.cloud_config import get_global_config
from hailtop.config import get_deploy_config

if get_deploy_config().location() == 'external':
    CLOUD = os.environ['CLOUD']
    DOCKER_PREFIX = os.environ['DOCKER_PREFIX']
    DOCKER_ROOT_IMAGE = os.environ['DOCKER_ROOT_IMAGE']
    DOMAIN = os.environ['DOMAIN']
    KUBERNETES_SERVER_URL = os.environ['KUBERNETES_SERVER_URL']
    DEFAULT_NAMESPACE = os.environ['HAIL_DEFAULT_NAMESPACE']
else:
    global_config = get_global_config()

    CLOUD = global_config['cloud']
    DOCKER_PREFIX = global_config['docker_prefix']
    DOCKER_ROOT_IMAGE = global_config['docker_root_image']
    DOMAIN = global_config['domain']
    KUBERNETES_SERVER_URL = global_config['kubernetes_server_url']
    DEFAULT_NAMESPACE = global_config['default_namespace']

CI_UTILS_IMAGE = os.environ['HAIL_CI_UTILS_IMAGE']
BUILDKIT_IMAGE = os.environ['HAIL_BUILDKIT_IMAGE']
BUCKET = os.environ['HAIL_CI_BUCKET_NAME']
