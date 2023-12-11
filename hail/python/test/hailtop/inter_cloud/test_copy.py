from typing import Tuple, Dict, AsyncIterator, List
import secrets
import asyncio
import pytest
from hailtop.utils import url_scheme
from hailtop.aiotools.plan import plan
from hailtop.aiotools.sync import sync
from hailtop.aiotools import Transfer, FileAndDirectoryError, Copier, AsyncFS, FileListEntry


from .generate_copy_test_specs import run_test_spec, create_test_file, create_test_dir
from .copier_test_utilities import event_loop, test_spec, cloud_scheme, router_filesystem, copy_test_context


@pytest.mark.asyncio
async def test_copy_behavior(copy_test_context, test_spec):
    sema, fs, src_base, dest_base = copy_test_context

    result = await run_test_spec(sema, fs, test_spec, src_base, dest_base)
    try:
        expected = test_spec['result']

        dest_scheme = url_scheme(dest_base)
        if ((dest_scheme == 'gs' or dest_scheme == 's3' or dest_scheme == 'https')
                and (result is not None and 'files' in result)
                and expected.get('exception') in ('IsADirectoryError', 'NotADirectoryError')):
            return

        assert result == expected, (test_spec, result, expected)
    except Exception:
        print(test_spec)
        raise


async def expect_file(fs, path, expected):
    async with await fs.open(path) as f:
        actual = (await f.read()).decode('utf-8')
    assert actual == expected, (actual, expected)


async def copy_tool(fs, sema, transfer):
    await Copier.copy(fs, sema, transfer)


async def sync_tool(fs, sema, transfer):
    del fs
    max_parallelism = sema._value
    copies = [
        (transfer.src, transfer.dest)  # FIXME: what about treat_dest_as?
    ]
    await plan('plan1', copies, None, True, max_parallelism)
    await sync('plan1', None, True, max_parallelism)


@pytest.fixture(params=['remote', 'local'])
def copy_tool(request):
    if request.param == 'copy_tool':
        return copy_tool
    if request.param == 'sync_tool':
        return sync_tool
    raise ValueError('bad: ' + request.param)


class DidNotRaiseError(Exception):
    pass


class RaisedWrongExceptionError(Exception):
    pass


class RaisesOrObjectStore:
    def __init__(self, dest_base, expected_type):
        scheme = url_scheme(dest_base)
        self._object_store = (scheme == 'gs' or scheme == 's3' or scheme == 'https')
        self._expected_type = expected_type

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # object stores can succeed or throw
        if type is None:
            if not self._object_store:
                raise DidNotRaiseError()
        elif type != self._expected_type:
            raise RaisedWrongExceptionError(type)

        # suppress exception
        return True


@pytest.mark.asyncio
async def test_copy_doesnt_exist(copy_test_context, copy_tool):
    sema, fs, src_base, dest_base = copy_test_context

    with pytest.raises(FileNotFoundError):
        await copy_tool(fs, sema, Transfer(f'{src_base}a', dest_base))


@pytest.mark.asyncio
async def test_copy_file(copy_test_context, copy_tool):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await copy_tool(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))

    await expect_file(fs, f'{dest_base}a', 'src/a')


@pytest.mark.asyncio
async def test_copy_large_file(copy_test_context, copy_tool):
    sema, fs, src_base, dest_base = copy_test_context

    # mainly needs to be larger than the transfer block size (8K)
    contents = secrets.token_bytes(1_000_000)
    async with await fs.create(f'{src_base}a') as f:
        await f.write(contents)

    await copy_tool(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))

    async with await fs.open(f'{dest_base}a') as f:
        copy_contents = await f.read()
    assert copy_contents == contents


@pytest.mark.asyncio
async def test_copy_rename_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x'))

    await expect_file(fs, f'{dest_base}x', 'src/a')


@pytest.mark.asyncio
async def test_copy_rename_file_dest_target_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x', treat_dest_as=Transfer.DEST_IS_TARGET))

    await expect_file(fs, f'{dest_base}x', 'src/a')


@pytest.mark.asyncio
async def test_copy_file_dest_target_directory_doesnt_exist(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    # SourceCopier._copy_file creates destination directories as needed
    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x', treat_dest_as=Transfer.DEST_DIR))
    await expect_file(fs, f'{dest_base}x/a', 'src/a')


@pytest.mark.asyncio
async def test_overwrite_rename_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'dest', dest_base, 'x')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x'))

    await expect_file(fs, f'{dest_base}x', 'src/a')


@pytest.mark.asyncio
async def test_copy_rename_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x'))

    await expect_file(fs, f'{dest_base}x/file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}x/subdir/file2', 'src/a/subdir/file2')


@pytest.mark.asyncio
async def test_copy_rename_dir_dest_is_target(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x', treat_dest_as=Transfer.DEST_IS_TARGET))

    await expect_file(fs, f'{dest_base}x/file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}x/subdir/file2', 'src/a/subdir/file2')


@pytest.mark.asyncio
async def test_overwrite_rename_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')
    await create_test_dir(fs, 'dest', dest_base, 'x/')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}x', treat_dest_as=Transfer.DEST_IS_TARGET))

    await expect_file(fs, f'{dest_base}x/file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}x/subdir/file2', 'src/a/subdir/file2')
    await expect_file(fs, f'{dest_base}x/file3', 'dest/x/file3')


@pytest.mark.asyncio
async def test_copy_file_dest_trailing_slash_target_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base, treat_dest_as=Transfer.DEST_DIR))

    await expect_file(fs, f'{dest_base}a', 'src/a')


@pytest.mark.asyncio
async def test_copy_file_dest_target_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/'), treat_dest_as=Transfer.DEST_DIR))

    await expect_file(fs, f'{dest_base}a', 'src/a')


@pytest.mark.asyncio
async def test_copy_file_dest_target_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', f'{dest_base}a', treat_dest_as=Transfer.DEST_IS_TARGET))

    await expect_file(fs, f'{dest_base}a', 'src/a')


@pytest.mark.asyncio
async def test_copy_dest_target_file_is_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    with RaisesOrObjectStore(dest_base, IsADirectoryError):
        await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/'), treat_dest_as=Transfer.DEST_IS_TARGET))


@pytest.mark.asyncio
async def test_overwrite_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'dest', dest_base, 'a')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))

    await expect_file(fs, f'{dest_base}a', 'src/a')


@pytest.mark.asyncio
async def test_copy_file_src_trailing_slash(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    with pytest.raises(FileNotFoundError):
        await Copier.copy(fs, sema, Transfer(f'{src_base}a/', dest_base))


@pytest.mark.asyncio
async def test_copy_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))

    await expect_file(fs, f'{dest_base}a/file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}a/subdir/file2', 'src/a/subdir/file2')


@pytest.mark.asyncio
async def test_overwrite_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')
    await create_test_dir(fs, 'dest', dest_base, 'a/')

    await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))

    await expect_file(fs, f'{dest_base}a/file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}a/subdir/file2', 'src/a/subdir/file2')
    await expect_file(fs, f'{dest_base}a/file3', 'dest/a/file3')


@pytest.mark.asyncio
async def test_copy_multiple(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'src', src_base, 'b')

    await Copier.copy(fs, sema, Transfer([f'{src_base}a', f'{src_base}b'], dest_base.rstrip('/')))

    await expect_file(fs, f'{dest_base}a', 'src/a')
    await expect_file(fs, f'{dest_base}b', 'src/b')


@pytest.mark.asyncio
async def test_copy_multiple_dest_target_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'src', src_base, 'b')

    with RaisesOrObjectStore(dest_base, NotADirectoryError):
        await Copier.copy(fs, sema, Transfer([f'{src_base}a', f'{src_base}b'], dest_base.rstrip('/'), treat_dest_as=Transfer.DEST_IS_TARGET))


@pytest.mark.asyncio
async def test_copy_multiple_dest_file(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'src', src_base, 'b')
    await create_test_file(fs, 'dest', dest_base, 'x')

    with RaisesOrObjectStore(dest_base, NotADirectoryError):
        await Copier.copy(fs, sema, Transfer([f'{src_base}a', f'{src_base}b'], f'{dest_base}x'))


@pytest.mark.asyncio
async def test_file_overwrite_dir(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_file(fs, 'src', src_base, 'a')

    with RaisesOrObjectStore(dest_base, IsADirectoryError):
        await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/'), treat_dest_as=Transfer.DEST_IS_TARGET))


@pytest.mark.asyncio
async def test_file_and_directory_error(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]], cloud_scheme: str):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, cloud_scheme)
    dest_base = await fresh_dir(fs, bases, 'file')

    await create_test_file(fs, 'src', src_base, 'a')
    await create_test_file(fs, 'src', src_base, 'a/subfile')

    with pytest.raises(FileAndDirectoryError):
        await Copier.copy(fs, sema, Transfer(f'{src_base}a', dest_base.rstrip('/')))


@pytest.mark.asyncio
async def test_copy_src_parts(copy_test_context):
    sema, fs, src_base, dest_base = copy_test_context

    await create_test_dir(fs, 'src', src_base, 'a/')

    await Copier.copy(fs, sema, Transfer([f'{src_base}a/file1', f'{src_base}a/subdir'], dest_base.rstrip('/'), treat_dest_as=Transfer.DEST_DIR))

    await expect_file(fs, f'{dest_base}file1', 'src/a/file1')
    await expect_file(fs, f'{dest_base}subdir/file2', 'src/a/subdir/file2')


async def write_file(fs, url, data):
    async with await fs.create(url) as f:
        await f.write(data)


async def collect_files(it: AsyncIterator[FileListEntry]) -> List[str]:
    return [await x.url() async for x in it]


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_empty_file(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]], cloud_scheme: str):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, cloud_scheme)

    await write_file(fs, f'{src_base}empty/', '')
    await write_file(fs, f'{src_base}empty/foo', b'foo')

    await collect_files(await fs.listfiles(f'{src_base}'))
    await collect_files(await fs.listfiles(f'{src_base}', recursive=True))
    await collect_files(await fs.listfiles(f'{src_base}empty/'))
    await collect_files(await fs.listfiles(f'{src_base}empty/', recursive=True))

    for transfer_type in (Transfer.DEST_IS_TARGET, Transfer.DEST_DIR, Transfer.INFER_DEST):
        dest_base = await fresh_dir(fs, bases, cloud_scheme)

        await Copier.copy(fs, sema, Transfer(f'{src_base}', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        dest_base = await fresh_dir(fs, bases, cloud_scheme)

        await Copier.copy(fs, sema, Transfer(f'{src_base}empty/', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        await collect_files(await fs.listfiles(f'{dest_base}'))
        await collect_files(await fs.listfiles(f'{dest_base}', recursive=True))

        if transfer_type == Transfer.DEST_DIR:
            exp_dest = f'{dest_base}empty/foo'
            await expect_file(fs, exp_dest, 'foo')
            assert not await fs.isfile(f'{dest_base}empty/')
            assert await fs.isdir(f'{dest_base}empty/')
            await collect_files(await fs.listfiles(f'{dest_base}empty/'))
            await collect_files(await fs.listfiles(f'{dest_base}empty/', recursive=True))
        else:
            exp_dest = f'{dest_base}foo'
            await expect_file(fs, exp_dest, 'foo')

@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_non_empty_file_for_google_non_recursive(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]]):
    _, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, 'gs')

    await write_file(fs, f'{src_base}not-empty/', b'not-empty')
    await write_file(fs, f'{src_base}not-empty/bar', b'bar')

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}'))

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}not-empty/'))


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_non_empty_file(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]], cloud_scheme: str):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, cloud_scheme)

    await write_file(fs, f'{src_base}not-empty/', b'not-empty')
    await write_file(fs, f'{src_base}not-empty/bar', b'bar')

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}', recursive=True))

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}not-empty/', recursive=True))

    for transfer_type in (Transfer.DEST_IS_TARGET, Transfer.DEST_DIR, Transfer.INFER_DEST):
        dest_base = await fresh_dir(fs, bases, cloud_scheme)

        await Copier.copy(fs, sema, Transfer(f'{src_base}not-empty/bar', dest_base.rstrip('/'), treat_dest_as=transfer_type))
        if transfer_type == Transfer.DEST_DIR:
            exp_dest = f'{dest_base}bar'
            await expect_file(fs, exp_dest, 'bar')
            assert not await fs.isfile(f'{dest_base}not-empty/')
            assert not await fs.isdir(f'{dest_base}not-empty/')
            x = await collect_files(await fs.listfiles(f'{dest_base}'))
            assert x == [f'{dest_base}bar'], x
        else:
            await expect_file(fs, dest_base.rstrip('/'), 'bar')

        with pytest.raises(FileAndDirectoryError):
            dest_base = await fresh_dir(fs, bases, cloud_scheme)
            await Copier.copy(fs, sema, Transfer(f'{src_base}not-empty/', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        with pytest.raises(FileAndDirectoryError):
            dest_base = await fresh_dir(fs, bases, cloud_scheme)
            await Copier.copy(fs, sema, Transfer(f'{src_base}', dest_base.rstrip('/'), treat_dest_as=transfer_type))


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_non_empty_file_only_for_google_non_recursive(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]]):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, 'gs')

    await write_file(fs, f'{src_base}empty-only/', '')

    await collect_files(await fs.listfiles(f'{src_base}'))
    await collect_files(await fs.listfiles(f'{src_base}empty-only/'))

    for transfer_type in (Transfer.DEST_IS_TARGET, Transfer.DEST_DIR, Transfer.INFER_DEST):
        dest_base = await fresh_dir(fs, bases, 'gs')
        await Copier.copy(fs, sema, Transfer(f'{src_base}empty-only/', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        # We ignore empty directories when copying
        with pytest.raises(FileNotFoundError):
            await collect_files(await fs.listfiles(f'{dest_base}empty-only/'))


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_empty_file_only(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]], cloud_scheme: str):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, cloud_scheme)

    await write_file(fs, f'{src_base}empty-only/', '')

    await collect_files(await fs.listfiles(f'{src_base}', recursive=True))
    await collect_files(await fs.listfiles(f'{src_base}empty-only/', recursive=True))

    for transfer_type in (Transfer.DEST_IS_TARGET, Transfer.DEST_DIR, Transfer.INFER_DEST):
        dest_base = await fresh_dir(fs, bases, cloud_scheme)
        await Copier.copy(fs, sema, Transfer(f'{src_base}empty-only/', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        with pytest.raises(FileNotFoundError):
            await collect_files(await fs.listfiles(f'{dest_base}empty-only/', recursive=True))

        dest_base = await fresh_dir(fs, bases, cloud_scheme)
        await Copier.copy(fs, sema, Transfer(f'{src_base}', dest_base.rstrip('/'), treat_dest_as=transfer_type))


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_non_empty_file_only_google_non_recursive(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]]):
    _, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, 'gs')

    await write_file(fs, f'{src_base}not-empty-file-w-slash/', b'not-empty')

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}'))

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}not-empty-file-w-slash/'))


@pytest.mark.asyncio
async def test_file_and_directory_error_with_slash_non_empty_file_only(router_filesystem: Tuple[asyncio.Semaphore, AsyncFS, Dict[str, str]], cloud_scheme: str):
    sema, fs, bases = router_filesystem

    src_base = await fresh_dir(fs, bases, cloud_scheme)

    await write_file(fs, f'{src_base}not-empty-file-w-slash/', b'not-empty')

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}', recursive=True))

    with pytest.raises(FileAndDirectoryError):
        await collect_files(await fs.listfiles(f'{src_base}not-empty-file-w-slash/', recursive=True))

    for transfer_type in (Transfer.DEST_IS_TARGET, Transfer.DEST_DIR, Transfer.INFER_DEST):
        with pytest.raises(FileAndDirectoryError):
            dest_base = await fresh_dir(fs, bases, cloud_scheme)
            await Copier.copy(fs, sema, Transfer(f'{src_base}not-empty-file-w-slash/', dest_base.rstrip('/'), treat_dest_as=transfer_type))

        with pytest.raises(FileAndDirectoryError):
            dest_base = await fresh_dir(fs, bases, cloud_scheme)
            await Copier.copy(fs, sema, Transfer(f'{src_base}', dest_base.rstrip('/'), treat_dest_as=transfer_type))
