import json
import logging
import asyncio
import pytest
import aiohttp

from hailtop.config import get_deploy_config
from hailtop.auth import service_auth_headers
from hailtop.httpx import client_session
import hailtop.utils as utils

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

sha = 'd626f793ad700c45a878d192652a0378818bbd8b'


async def test_update_commits():
    deploy_config = get_deploy_config()
    headers = service_auth_headers(deploy_config, 'benchmark')
    commit_benchmark_url = deploy_config.url('benchmark', f'/api/v1alpha/benchmark/commit/{sha}')

    async with client_session() as session:
        commit = None

        await session.delete(f'{commit_benchmark_url}', headers=headers, json={'sha': sha})

        resp_status = await session.get(
            f'{commit_benchmark_url}', headers=headers, json={'sha': sha})
        commit = await resp_status.json()
        assert commit['status'] is None, commit

        resp_commit = await session.post(
            f'{commit_benchmark_url}', headers=headers, json={'sha': sha})
        commit = await resp_commit.json()

        async def wait_forever():
            nonlocal commit
            while commit is None or not commit['status']['complete']:
                resp = await session.get(
                    f'{commit_benchmark_url}', headers=headers, json={'sha': sha})
                commit = await resp.json()
                await asyncio.sleep(5)
                print(commit['status'])
            return commit

        commit = await wait_forever()
        assert commit['status']['complete'] == True, commit
