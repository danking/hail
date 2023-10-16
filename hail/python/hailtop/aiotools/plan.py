from typing import List, Tuple, Dict, Awaitable
import aiomonitor
import argparse
import asyncio
import logging
import os
import sys
from io import TextIOWrapper

from .router_fs import RouterAsyncFS
from .fs import FileListEntry, AsyncFS
from ..utils.rich_progress_bar import RichProgressBar, Progress

try:
    import uvloop
    uvloop_install = uvloop.install
except ImportError as e:
    if not sys.platform.startswith('win32'):
        raise e

    def uvloop_install():
        pass


def removeprefix(s, prefix):
    n = len(prefix)
    if prefix == s[:n]:
        return s[n:]
    return s


async def main() -> None:
    parser = argparse.ArgumentParser(description='Hail copy planner')
    parser.add_argument('--requester-pays-project', type=str,
                        help='a JSON string indicating the Google project to which to charge egress costs')
    parser.add_argument('--copy', type=str, nargs=2, action='append',
                        help='Pairs of source and destination URL. May be specified multiple times.')
    parser.add_argument('-v', '--verbose', action='store_const',
                        const=True, default=False,
                        help='show logging information')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig()
        logging.root.setLevel(logging.INFO)

    requester_pays_project = args.requester_pays_project
    gcs_kwargs = {'gcs_requester_pays_configuration': requester_pays_project}

    total_n_files = 0
    total_n_bytes = 0

    async with RouterAsyncFS(# local_kwargs=local_kwargs,
                             gcs_kwargs=gcs_kwargs,
                             # azure_kwargs=azure_kwargs,
                             # s3_kwargs=s3_kwargs
                             ) as fs:
        with open('matches', 'w') as matches, \
             open('differs', 'w') as differs, \
             open('srconly', 'w') as srconly, \
             open('dstonly', 'w') as dstonly, \
             open('plan', 'w') as plan:
            with RichProgressBar(transient=True, disable=not args.verbose) as progress:
                for src, dst in args.copy:
                    n_files, n_bytes = await find_all_copy_pairs(
                        fs,
                        matches,
                        differs,
                        srconly,
                        dstonly,
                        plan,
                        src,
                        dst,
                        progress,
                        asyncio.Semaphore(4)
                    )
                    total_n_files += n_files
                    total_n_bytes += n_bytes
    with open('summary', 'w') as summary:
        summary.write(f'{total_n_files}\t{total_n_bytes}\n')


async def file_size(f: FileListEntry) -> int:
    return await (await f.status()).size()


async def extract(x: FileListEntry) -> Tuple[str, str, bool, int]:
    url, is_dir = await asyncio.gather(x.url(), x.is_dir())
    if is_dir:
        size = 0
    else:
        size = await file_size(x)
    return (x.name(), url, is_dir, size)


async def matching_files(fs: AsyncFS, x: str) -> List[Tuple[str, str, bool, int]]:
    try:
        it = await fs.listfiles(x)
        return [await extract(x) async for x in it]
    except FileNotFoundError:
        return []



async def find_all_copy_pairs(
    fs: AsyncFS,
    matches: TextIOWrapper,
    differs: TextIOWrapper,
    srconly: TextIOWrapper,
    dstonly: TextIOWrapper,
    plan: TextIOWrapper,
    src: str,
    dst: str,
    progress: Progress,
    sema: asyncio.Semaphore,
) -> Tuple[int, int]:
    async with sema:
        srcfiles, dstfiles = await asyncio.gather(matching_files(fs, src), matching_files(fs, dst))
        srcfiles.sort(key=lambda x: x[0])
        dstfiles.sort(key=lambda x: x[0])

        tid = progress.add_task(description=src, total=len(srcfiles) + len(dstfiles))

        srcidx = 0
        dstidx = 0

        n_files = 0
        n_bytes = 0

        # if len(srcfiles) > 100 or len(dstfiles) > 100:
        #     informed = True
        #     print(src)
        #     sys.stdout.flush()
        #     sys.stderr.flush()
        # else:
        #     informed = False

        child_directory_tasks: List[asyncio.Task] = []
        while srcidx < len(srcfiles) and dstidx < len(dstfiles):
            srcf = srcfiles[srcidx]
            dstf = dstfiles[dstidx]
            # print(f' {srcf}\n {dstf}\n')
            srcname, srcurl, srcisdir, srcsize = srcf
            dstname, dsturl, dstisdir, dstsize = dstf
            if srcname == dstname:
                if srcisdir and dstisdir:
                    # if not informed:
                    #     informed = True
                    #     print(src)
                    child_directory_tasks.append(
                        asyncio.create_task(find_all_copy_pairs(
                            fs, matches, differs, srconly, dstonly, plan, srcurl, dsturl, progress, sema
                        ))
                    )
                elif srcisdir and not dstisdir:
                    differs.write(srcurl + '\t' + dsturl + '\t' + 'dir' + '\t' + 'file' + '\n')
                elif not srcisdir and dstisdir:
                    differs.write(srcurl + '\t' + dsturl + '\t' + 'file' + '\t' + 'dir' + '\n')
                elif srcsize == dstsize:
                    matches.write(srcurl + '\t' + dsturl + '\n')
                else:
                    differs.write(srcurl + '\t' + dsturl + '\t' + str(srcsize) + '\t' + str(dstsize) + '\n')
                dstidx += 1
                srcidx += 1
                progress.update(tid, advance=2)
            elif srcname < dstname:
                if srcisdir:
                    if src[-1] != '/':
                        src += '/'
                    # if not informed:
                    #     informed = True
                    #     print(src)
                    child_directory_tasks.append(
                        asyncio.create_task(find_all_copy_pairs(
                            fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, removeprefix(srcurl, src)), progress, sema
                        ))
                    )
                else:
                    srconly.write(srcurl + '\n')
                    plan.write(srcurl + '\t' + os.path.join(dst, srcname) + '\n')
                    n_files += 1
                    n_bytes += srcsize
                srcidx += 1
                progress.update(tid, advance=1)
            else:
                assert srcname >= dstname
                dstidx += 1
                progress.update(tid, advance=1)
                dstonly.write(dsturl + '\n')
        while srcidx < len(srcfiles):
            srcf = srcfiles[srcidx]
            srcname, srcurl, srcisdir, srcsize = srcf

            if srcisdir:
                if src[-1] != '/':
                    src += '/'
                # if not informed:
                #     informed = True
                #     print(src)
                child_directory_tasks.append(
                    asyncio.create_task(find_all_copy_pairs(
                        fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, removeprefix(srcurl, src)), progress, sema
                    ))
                )
            else:
                srconly.write(srcurl + '\n')
                plan.write(srcurl + '\t' + os.path.join(dst, srcname) + '\n')
                n_files += 1
                n_bytes += srcsize
            srcidx += 1
            progress.update(tid, advance=1)
        while dstidx < len(dstfiles):
            dstf = dstfiles[dstidx]
            dstname, dsturl, dstisdir, dstsize = dstf

            dstonly.write(dsturl + '\n')
            dstidx += 1
            progress.update(tid, advance=1)

    await asyncio.sleep(0.3)
    progress.remove_task(tid)

    for t in child_directory_tasks:
        dir_n_files, dir_n_bytes = await t
        n_files += dir_n_files
        n_bytes += dir_n_bytes

    return n_files, n_bytes

if __name__ == '__main__':
    uvloop_install()
    loop = asyncio.new_event_loop()
    with aiomonitor.start_monitor(loop=loop) as monitor:
        print(f'aiomonitor available on {monitor.host} {monitor.port}')
        loop.run_until_complete(main())
