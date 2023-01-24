import argparse
import asyncio
import datetime
import functools
import logging
import os
import sys
from ..utils.rich_progress_bar import RichProgressBar, Progress

from .router_fs import RouterAsyncFS
from .fs import AsyncFS
from .fs.exceptions import UnexpectedEOFError
from ..utils import retry_transient_errors, bounded_gather2
from ..utils.utils import WithoutSemaphore

try:
    import uvloop
    uvloop_install = uvloop.install
except ImportError as e:
    if not sys.platform.startswith('win32'):
        raise e

    def uvloop_install():
        pass


DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024


async def main() -> None:
    parser = argparse.ArgumentParser(
        description='Hail copy 2',
        epilog='Create a plan first and run this program from within the plan directory.')
    parser.add_argument('--requester-pays-project', type=str,
                        help='the Google project to which to charge egress costs')
    parser.add_argument('--max-simultaneous-transfers', type=int,
                        help='The limit on the number of simultaneous transfers. A file can be uploaded in parallel in multiple transfers.')
    parser.add_argument('--max-simultaneous-files', type=int)
    parser.add_argument('--timeout', type=float, help='request timeout in seconds')
    parser.add_argument('--max-buffer-size', type=int,
                        help='The limit on the number of bytes per chunk. Expect to use at least three times the buffer size per part per file.')
    parser.add_argument('-v', '--verbose', action='store_const',
                        const=True, default=False,
                        help='show logging information')
    args = parser.parse_args()

    now = datetime.datetime.now()
    logging.basicConfig(
        filename='copy2-' + now.isoformat(timespec='seconds') + '.log',
        filemode='w',
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO)
    if args.verbose:
        logging.getLogger().addHandler(logging.StreamHandler())

    requester_pays_project = args.requester_pays_project
    max_simultaneous_transfers = args.max_simultaneous_transfers or 75
    max_simultaneous_files = args.max_simultaneous_files or args.max_simultaneous_transfers // 4
    gcs_kwargs = {'project': requester_pays_project, 'timeout': args.timeout}
    s3_kwargs = {'max_pool_connections': max_simultaneous_transfers * 5,
                 'timeout': args.timeout,
                 'max_workers': max_simultaneous_transfers}
    azure_kwargs = {'timeout': args.timeout}

    if any((not os.path.exists(x) for x in ('matches', 'differs', 'srconly', 'dstonly', 'plan', 'summary'))):
        print('Run hailtop.aiotools.plan first.')
        sys.exit(1)

    async with RouterAsyncFS(default_scheme='file',
                             # local_kwargs=local_kwargs,
                             gcs_kwargs=gcs_kwargs,
                             azure_kwargs=azure_kwargs,
                             s3_kwargs=s3_kwargs
                             ) as fs:
        with open('summary') as fobj:
            n_files, n_bytes = fobj.read().strip().split('\t')
        n_files = int(n_files)
        n_bytes = int(n_bytes)

        with RichProgressBar(transient=True) as progress:
            file_tid = progress.add_task(description='files', total=n_files, visible=args.verbose)
            bytes_tid = progress.add_task(description='bytes', total=n_bytes, visible=args.verbose)
            await copy2(
                fs,
                progress,
                file_tid,
                bytes_tid,
                max_simultaneous_transfers,
                max_simultaneous_files,
                args.max_buffer_size or DEFAULT_BUFFER_SIZE
            )

async def copy2(fs, progress: Progress, file_tid, bytes_tid, max_simultaneous_transfers: int, n_workers: int, buffer_size: int):
    sema = asyncio.Semaphore(max_simultaneous_transfers)
    work = asyncio.Queue(maxsize=n_workers * 3)
    remaining_workers = n_workers

    async def load_work():
        for src, dst in iterate_plan_file():
            src = src.strip()
            dst = dst.strip()
            await work.put((src, dst))
        for _ in range(n_workers):
            await work.put(None)

    async def worker():
        nonlocal remaining_workers

        while True:
            async with sema:
                w = await work.get()
                if w is None:
                    remaining_workers -= 1
                    return
                src, dst = w
                await copy_one(fs, src, dst, progress, file_tid, bytes_tid, buffer_size, sema)

    workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
    await asyncio.gather(load_work(), *workers)


async def copy_one(fs: AsyncFS, src: str, dst: str, progress: Progress, file_tid, bytes_tid, buffer_size: int, sema: asyncio.Semaphore):
    srcstat = await fs.statfile(src)
    srcsize = await srcstat.size()
    part_size = fs.copy_part_size(dst)

    if srcsize <= part_size:
        return await retry_transient_errors(copy_in_one_part, fs, src, dst, progress, file_tid, bytes_tid, buffer_size)

    await retry_transient_errors(copy_in_multiple_parts, fs, src, srcsize, dst, progress, file_tid, bytes_tid, buffer_size, sema)


async def copy_in_one_part(fs: AsyncFS, src: str, dst: str, progress, file_tid, bytes_tid, buffer_size: int):
    written = 0
    try:
        async with await fs.open(src) as srcf:
            try:
                dest_cm = await fs.create(dst, retry_writes=False)
            except FileNotFoundError:
                await fs.makedirs(os.path.dirname(dst), exist_ok=True)
                dest_cm = await fs.create(dst)

            async with dest_cm as destf:
                while True:
                    b = await srcf.read(buffer_size)
                    if not b:
                        progress.update(file_tid, advance=1)
                        return
                    written = await destf.write(b)
                    assert written == len(b)
                    progress.update(bytes_tid, advance=len(b))
    except:
        progress.update(bytes_tid, advance=-written)
        raise


async def copy_in_multiple_parts(fs: AsyncFS, src: str, srcsize: int, dst: str, progress, file_tid, bytes_tid, buffer_size: int, sema: asyncio.Semaphore):
    part_size = fs.copy_part_size(dst)
    n_parts, rem = divmod(srcsize, part_size)
    if rem:
        n_parts += 1

    try:
        part_creator = await fs.multi_part_create(sema, dst, n_parts)
    except FileNotFoundError:
        await fs.makedirs(os.path.dirname(dst), exist_ok=True)
        part_creator = await fs.multi_part_create(sema, dst, n_parts)

    async with part_creator:
        async def f(i):
            this_part_size = rem if i == n_parts - 1 and rem else part_size
            await retry_transient_errors(
                copy_part,
                fs, part_creator, src, part_size, i, this_part_size, progress, bytes_tid, buffer_size)

        async with WithoutSemaphore(sema):
            await bounded_gather2(sema, *[
                functools.partial(f, i)
                for i in range(n_parts)
            ], cancel_on_error=True)

    progress.update(file_tid, advance=1)


async def copy_part(fs: AsyncFS, part_creator, src, part_size, part_number, this_part_size, progress, bytes_tid, buffer_size: int):
    written = 0
    try:
        async with await fs.open_from(src, part_number * part_size, length=this_part_size) as srcf:
            async with await part_creator.create_part(part_number, part_number * part_size, size_hint=this_part_size) as destf:
                n = this_part_size
                while n > 0:
                    b = await srcf.read(min(buffer_size, n))
                    if len(b) == 0:
                        raise UnexpectedEOFError()
                    written = await destf.write(b)
                    assert written == len(b)
                    progress.update(bytes_tid, advance=len(b))
                    n -= len(b)
    except:
        progress.update(bytes_tid, advance=-written)
        raise


def iterate_plan_file():
    lineno = 0
    with open('plan') as plan:
        for line in plan.readlines():
            parts = line.split('\t')
            if len(parts) != 2:
                print(f'Malformed plan line, {lineno}, more than one tab.')
                sys.exit(1)
            yield parts


if __name__ == '__main__':
    uvloop_install()
    asyncio.run(main())
