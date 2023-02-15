from typing import Optional, List, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
import configparser
from pathlib import Path
from botocore import UNSIGNED
import warnings

from ...config.user_config import configuration_of
from ..aioaws.fs import S3AsyncFS


class R2AsyncFS(S3AsyncFS):
    schemes: Set[str] = {'r2'}

    def __init__(self,
                 *,
                 thread_pool: Optional[ThreadPoolExecutor] = None,
                 max_workers: Optional[int] = None,
                 credentials_file: Optional[str] = None,
                 max_pool_connections: int = 10,
                 account_id: Optional[str] = None
                 ):
        default_r2_creds_file = str(Path.home() / '.r2' / 'credentials')
        credentials_file = configuration_of('aiocloudflare', 'credentials_file', credentials_file, default_r2_creds_file)
        account_id = configuration_of('aiocloudflare', 'account_id', account_id, None)
        if account_id is None:
            raise ValueError('must provide account_id')
        user_config = configparser.ConfigParser()
        user_config.read(credentials_file)
        endpoint_url = f'https://{account_id}.r2.cloudflarestorage.com'
        aws_access_key_id = user_config.get('default', 'aws_access_key_id', fallback=None)
        aws_secret_access_key = user_config.get('default', 'aws_secret_access_key', fallback=None)

        if aws_access_key_id is None and aws_secret_access_key is None:
            warnings.warn(f'using anonymous credentials for R2, account_id: {account_id}')
            botocore_config_kwargs = {'signature_version': UNSIGNED}
            s3_client_kwargs = {}
        elif aws_access_key_id is not None and aws_secret_access_key is not None:
            botocore_config_kwargs = {}
            s3_client_kwargs = {'endpoint_url': endpoint_url,
                                'aws_access_key_id': aws_access_key_id,
                                'aws_secret_access_key': aws_secret_access_key}
        else:
            raise ValueError('both aws_secret_access_key and aws_secret_key_id must be specified or unspecified')

        super().__init__(thread_pool=thread_pool,
                         max_workers=max_workers,
                         max_pool_connections=max_pool_connections,
                         botocore_config_kwargs=botocore_config_kwargs,
                         s3_client_kwargs=s3_client_kwargs,
                         protocol='r2')

    @staticmethod
    def get_bucket_and_name(url: str) -> Tuple[str, str]:
        colon_index = url.find(':')
        if colon_index == -1:
            raise ValueError(f'invalid URL: {url}')

        scheme = url[:colon_index]
        if scheme != 'r2':
            raise ValueError(f'invalid scheme, expected r2: {scheme}')

        rest = url[(colon_index + 1):]
        if not rest.startswith('//'):
            raise ValueError(f'r2 URI must be of the form: r2://bucket/key, found: {url}')

        end_of_bucket = rest.find('/', 2)
        bucket = rest[2:end_of_bucket]
        name = rest[(end_of_bucket + 1):]

        return (bucket, name)
