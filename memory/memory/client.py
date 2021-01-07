
import aiohttp
import concurrent
import google.api_core.exceptions

from hailtop.auth import service_auth_headers
from hailtop.config import get_deploy_config
from hailtop.google_storage import GCS
from hailtop import httpx


class MemoryClient:
    def __init__(self, gcs_project=None, fs=None, deploy_config=None, session=None,
                 headers=None, _token=None):
        if not deploy_config:
            self._deploy_config = get_deploy_config()
        else:
            self._deploy_config = deploy_config

        self.url = self._deploy_config.base_url('memory')
        self._session = session
        if fs is None:
            fs = GCS(blocking_pool=concurrent.futures.ThreadPoolExecutor(), project=gcs_project)
        self._fs = fs
        self._headers = {}
        if headers:
            self._headers.update(headers)
        if _token:
            self._headers['Authorization'] = f'Bearer {_token}'

    async def async_init(self):
        if self._session is None:
            self._session = httpx.client_session(
                timeout=aiohttp.ClientTimeout(total=60))
        if 'Authorization' not in self._headers:
            self._headers.update(service_auth_headers(self._deploy_config, 'memory'))

    async def _get_file_if_exists(self, filename):
        try:
            etag = await self._fs.get_etag(filename)
        except google.api_core.exceptions.NotFound:
            return None
        params = {'q': filename, 'etag': etag}
        try:
            url = f'{self.url}/api/v1alpha/objects'
            async with await self._session.get(url,
                                               params=params,
                                               headers=self._headers) as response:
                return await response.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            raise e

    async def read_file(self, filename):
        data = await self._get_file_if_exists(filename)
        if data is not None:
            return data
        return await self._fs.read_binary_gs_file(filename)

    async def close(self):
        await self._session.close()
        self._session = None
