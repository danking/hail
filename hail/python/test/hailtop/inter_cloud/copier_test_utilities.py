from typing import Tuple, Dict, AsyncIterator
import os
import secrets
from concurrent.futures import ThreadPoolExecutor
import asyncio
import functools
import pytest
from hailtop.utils import bounded_gather2
from hailtop.aiotools import LocalAsyncFS, AsyncFS
from hailtop.aiotools.router_fs import RouterAsyncFS
from hailtop.aiocloud.aiogoogle import GoogleStorageAsyncFS
from hailtop.aiocloud.aioaws import S3AsyncFS
from hailtop.aiocloud.aioazure import AzureAsyncFS


from .copy_test_specs import COPY_TEST_SPECS


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# This fixture is for test_copy_behavior.  It runs a series of copy
# test "specifications" by calling run_test_spec.  The set of
# specifications is enumerated by
# generate_copy_test_specs.py::copy_test_configurations which are then
# run against the local file system.  This tests that (1) that copy
# runs without expected error for each enumerated spec, and that the
# semantics of each filesystem agree.
@pytest.fixture(params=COPY_TEST_SPECS)
async def test_spec(request):
    return request.param


@pytest.fixture(params=['gs', 's3', 'azure-https'])
async def cloud_scheme(request):
    yield request.param

@pytest.fixture(scope='module')
async def router_filesystem(request) -> AsyncIterator[Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]]]:
    token = secrets.token_hex(16)

    with ThreadPoolExecutor() as thread_pool:
        async with RouterAsyncFS(
            filesystems=[
                LocalAsyncFS(thread_pool),
                GoogleStorageAsyncFS(),
                S3AsyncFS(thread_pool),
                AzureAsyncFS()
            ]
        ) as fs:
            file_base = f'/tmp/{token}/'
            await fs.mkdir(file_base)

            gs_bucket = os.environ['HAIL_TEST_GCS_BUCKET']
            gs_base = f'gs://{gs_bucket}/tmp/{token}/'

            s3_bucket = os.environ['HAIL_TEST_S3_BUCKET']
            s3_base = f's3://{s3_bucket}/tmp/{token}/'

            azure_account = os.environ['HAIL_TEST_AZURE_ACCOUNT']
            azure_container = os.environ['HAIL_TEST_AZURE_CONTAINER']
            azure_base = f'https://{azure_account}.blob.core.windows.net/{azure_container}/tmp/{token}/'

            bases = {
                'file': file_base,
                'gs': gs_base,
                's3': s3_base,
                'azure-https': azure_base
            }

            sema = asyncio.Semaphore(50)
            async with sema:
                yield (sema, fs, bases)
                await bounded_gather2(sema,
                                      functools.partial(fs.rmtree, sema, file_base),
                                      functools.partial(fs.rmtree, sema, gs_base),
                                      functools.partial(fs.rmtree, sema, s3_base),
                                      functools.partial(fs.rmtree, sema, azure_base))

            assert not await fs.isdir(file_base)
            assert not await fs.isdir(gs_base)
            assert not await fs.isdir(s3_base)
            assert not await fs.isdir(azure_base)


async def fresh_dir(fs, bases, scheme):
    token = secrets.token_hex(16)
    dir = f'{bases[scheme]}{token}/'
    await fs.mkdir(dir)
    return dir


@pytest.fixture(params=['file/file', 'file/gs', 'file/s3', 'file/azure-https',
                        'gs/file', 'gs/gs', 'gs/s3', 'gs/azure-https',
                        's3/file', 's3/gs', 's3/s3', 's3/azure-https',
                        'azure-https/file', 'azure-https/gs', 'azure-https/s3', 'azure-https/azure-https'])
async def copy_test_context(request, router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]]):
    sema, fs, bases = router_filesystem

    [src_scheme, dest_scheme] = request.param.split('/')

    src_base = await fresh_dir(fs, bases, src_scheme)
    dest_base = await fresh_dir(fs, bases, dest_scheme)

    # make sure dest_base exists
    async with await fs.create(f'{dest_base}keep'):
        pass

    yield sema, fs, src_base, dest_base
