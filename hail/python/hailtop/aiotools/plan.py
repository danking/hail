from typing import List, Tuple, Optional
import asyncio
import os
import sys

from .router_fs import RouterAsyncFS
from .fs import FileListEntry, AsyncFS, WritableStream
from ..utils.rich_progress_bar import CopyToolProgressBar, Progress

try:
    import uvloop
    uvloop_install = uvloop.install
except ImportError as e:
    if not sys.platform.startswith('win32'):
        raise e

    def uvloop_install():
        pass


async def plan(
    folder: str,
    copy: List[Tuple[str, str]],
    gcs_requester_pays_project: Optional[str],
    verbose: bool,
    max_parallelism: int,
):
    if gcs_requester_pays_project:
        gcs_kwargs = {'gcs_requester_pays_configuration': gcs_requester_pays_project}
    else:
        gcs_kwargs = {}

    total_n_files = 0
    total_n_bytes = 0

    async with RouterAsyncFS(gcs_kwargs=gcs_kwargs) as fs:
        if any(await asyncio.gather(fs.isfile(folder), fs.isdir(folder.rstrip('/') + '/'))):
            print(f'plan folder already exists: {folder}')
            sys.exit(1)

        await fs.mkdir(folder)

        async with await fs.create(os.path.join(folder, 'matches')) as matches, \
             await fs.create(os.path.join(folder, 'differs')) as differs, \
             await fs.create(os.path.join(folder, 'srconly')) as srconly, \
             await fs.create(os.path.join(folder, 'dstonly')) as dstonly, \
             await fs.create(os.path.join(folder, 'plan')) as plan:
            with CopyToolProgressBar(transient=True, disable=not verbose) as progress:
                for src, dst in copy:
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
                        asyncio.Semaphore(max_parallelism)
                    )
                    total_n_files += n_files
                    total_n_bytes += n_bytes
        async with await fs.create(os.path.join(folder, 'summary')) as summary:
            await summary.write((f'{total_n_files}\t{total_n_bytes}\n').encode('utf-8'))


async def file_size(f: FileListEntry) -> int:
    return await (await f.status()).size()


async def extract(x: FileListEntry) -> Tuple[str, str, bool, int]:
    url, is_dir = await asyncio.gather(x.url(), x.is_dir())
    if is_dir:
        size = 0
    else:
        size = await file_size(x)
    return (x.name(), url, is_dir, size)


async def listfiles(fs: AsyncFS, x: str) -> List[Tuple[str, str, bool, int]]:
    try:
        it = await fs.listfiles(x)
        return [await extract(x) async for x in it]
    except FileNotFoundError:
        return []


async def find_all_copy_pairs(
    fs: AsyncFS,
    matches: WritableStream,
    differs: WritableStream,
    srconly: WritableStream,
    dstonly: WritableStream,
    plan: WritableStream,
    src: str,
    dst: str,
    progress: Progress,
    sema: asyncio.Semaphore,
) -> Tuple[int, int]:
    async with sema:
        srcfiles, dstfiles = await asyncio.gather(listfiles(fs, src), listfiles(fs, dst))
        srcfiles.sort(key=lambda x: x[0])
        dstfiles.sort(key=lambda x: x[0])

        tid = progress.add_task(description=src, total=len(srcfiles) + len(dstfiles))

        srcidx = 0
        dstidx = 0

        n_files = 0
        n_bytes = 0

        child_directory_tasks: List[asyncio.Task] = []
        while srcidx < len(srcfiles) and dstidx < len(dstfiles):
            srcf = srcfiles[srcidx]
            dstf = dstfiles[dstidx]
            srcname, srcurl, srcisdir, srcsize = srcf
            dstname, dsturl, dstisdir, dstsize = dstf
            if srcname == dstname:
                if srcisdir and dstisdir:
                    child_directory_tasks.append(
                        asyncio.create_task(find_all_copy_pairs(
                            fs, matches, differs, srconly, dstonly, plan, srcurl, dsturl, progress, sema
                        ))
                    )
                elif srcisdir and not dstisdir:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + 'dir' + '\t' + 'file' + '\n').encode('utf-8'))
                elif not srcisdir and dstisdir:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + 'file' + '\t' + 'dir' + '\n').encode('utf-8'))
                elif srcsize == dstsize:
                    await matches.write((srcurl + '\t' + dsturl + '\n').encode('utf-8'))
                else:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + str(srcsize) + '\t' + str(dstsize) + '\n').encode('utf-8'))
                dstidx += 1
                srcidx += 1
                progress.update(tid, advance=2)
            elif srcname < dstname:
                if srcisdir:
                    if src[-1] != '/':
                        src += '/'
                    child_directory_tasks.append(
                        asyncio.create_task(find_all_copy_pairs(
                            fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, srcurl.removeprefix(src)), progress, sema
                        ))
                    )
                else:
                    await srconly.write((srcurl + '\n').encode('utf-8'))
                    await plan.write((srcurl + '\t' + os.path.join(dst, srcname) + '\n').encode('utf-8'))
                    n_files += 1
                    n_bytes += srcsize
                srcidx += 1
                progress.update(tid, advance=1)
            else:
                assert srcname >= dstname
                dstidx += 1
                progress.update(tid, advance=1)
                await dstonly.write((dsturl + '\n').encode('utf-8'))
        while srcidx < len(srcfiles):
            srcf = srcfiles[srcidx]
            srcname, srcurl, srcisdir, srcsize = srcf

            if srcisdir:
                if src[-1] != '/':
                    src += '/'
                child_directory_tasks.append(
                    asyncio.create_task(find_all_copy_pairs(
                        fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, srcurl.removeprefix(src)), progress, sema
                    ))
                )
            else:
                await srconly.write((srcurl + '\n').encode('utf-8'))
                await plan.write((srcurl + '\t' + os.path.join(dst, srcname) + '\n').encode('utf-8'))
                n_files += 1
                n_bytes += srcsize
            srcidx += 1
            progress.update(tid, advance=1)
        while dstidx < len(dstfiles):
            dstf = dstfiles[dstidx]
            dstname, dsturl, dstisdir, dstsize = dstf

            await dstonly.write((dsturl + '\n').encode('utf-8'))
            dstidx += 1
            progress.update(tid, advance=1)

    # a short sleep ensures the progress bar is visible for a moment to the user
    await asyncio.sleep(0.150)
    progress.remove_task(tid)

    for t in child_directory_tasks:
        dir_n_files, dir_n_bytes = await t
        n_files += dir_n_files
        n_bytes += dir_n_bytes

    return n_files, n_bytes
