from typing import Tuple, Dict, AsyncIterator, List
import pytest
import os.path
import tempfile
import secrets
import asyncio
import pytest
from hailtop.utils import url_scheme, check_exec_output
from hailtop.aiotools import Transfer, FileAndDirectoryError, Copier, AsyncFS, FileListEntry


from .generate_copy_test_specs import run_test_spec, create_test_file, create_test_dir
from .copier_test_utilities import event_loop, test_spec, cloud_scheme, router_filesystem


@pytest.mark.asyncio
async def test_cli_file_and_dir(router_filesystem, cloud_scheme):
    sema, fs, bases = router_filesystem

    test_dir = await fresh_dir(fs, bases, cloud_scheme)

    fs.write(f"{test_dir}/file1", "hello world\n")

    check_exec_output(
        'hailctl', 'fs', 'sync', '--make-plan',  'plan1',
        '--copy', f'{test_dir}/file1', f'{test_dir}/file2'
        '--copy', f'{test_dir}/file1', f'{test_dir}/dir1',
    )

    check_exec_output(
        'hailctl', 'fs', 'sync', '--use-plan',  'plan1',
    )

    expected_files = [f"{test_dir}/file1", f"{test_dir}/file2", f"{test_dir}/dir1/file1"]
    for url in expected_files:
        assert fs.read(url) == "hello world\n"


@pytest.mark.asyncio
async def test_cli_dir(router_filesystem, cloud_scheme):
    sema, fs, bases = router_filesystem

    test_dir = await fresh_dir(fs, bases, cloud_scheme)

    fs.makedirs(f"{test_dir}/subdir1")
    fs.write(f"{test_dir}/subdir1/file1", "hello world\n")

    check_exec_output(
        'hailctl', 'fs', 'sync', '--make-plan',  'plan1',
        '--copy', f'{test_dir}/subdir1', f'{test_dir}/subdir2'
    )

    check_exec_output(
        'hailctl', 'fs', 'sync', '--use-plan',  'plan1',
    )

    assert fs.read(f"{test_dir}/subdir2/subdir1/file1") == "hello world\n"


@pytest.mark.asyncio
async def test_cli_already_synced(router_filesystem, cloud_scheme):
    sema, fs, bases = router_filesystem

    test_dir = await fresh_dir(fs, bases, cloud_scheme)

    fs.makedirs(f"{test_dir}/dir")
    fs.write(f"{test_dir}/dir/foo", "hello world\n")
    fs.write(f"{test_dir}/bar", "hello world\n")

    check_exec_output(
        'hailctl', 'fs', 'sync', '--make-plan',  'plan1',
        '--copy', f'{test_dir}/dir/foo', f'{test_dir}/bar'
    )

    assert fs.read('plan1/plan') == ''
    assert fs.read('plan1/summary') == '0\t0\n'
    assert fs.read('plan1/differs') == ''
    assert fs.read('plan1/dstonly') == ''
    assert fs.read('plan1/srconly') == ''
    assert fs.read('plan1/matches') == f'{test_dir}/dir/foo\t{test_dir}/bar\n'
