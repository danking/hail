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


class PlanError(ValueError):
    pass


async def plan(
    folder: str,
    copy_to: List[Tuple[str, str]],
    copy_into: List[Tuple[str, str]],
    gcs_requester_pays_project: Optional[str],
    verbose: bool,
    max_parallelism: int,
    overwrite_if_size_differs: bool,
):
    if gcs_requester_pays_project:
        gcs_kwargs = {'gcs_requester_pays_configuration': gcs_requester_pays_project}
    else:
        gcs_kwargs = {}

    total_n_files = 0
    total_n_bytes = 0

    async with RouterAsyncFS(gcs_kwargs=gcs_kwargs) as fs:
        def create_copy_into(copy_into_tuple: Tuple[str, str]) -> Tuple[str, str]:
            src, dest = copy_into_tuple
            src_url = fs.parse_url(src)
            dest_url = fs.parse_url(dest)
            src_basename = os.path.basename(src_url.path)
            destination_file = dest_url.with_new_path_component(src_basename)
            return (src, str(destination_file))

        copy = [*copy_to, *(create_copy_into(x) for x in copy_into)]

        if any(await asyncio.gather(fs.isfile(folder), fs.isdir(folder.rstrip('/') + '/'))):
            raise PlanError(f'plan folder already exists: {folder}', 1)

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
                        asyncio.Semaphore(max_parallelism),
                        overwrite_if_size_differs,
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
    except (FileNotFoundError, NotADirectoryError):
        return []

async def statfile(fs: AsyncFS, x: str) -> Optional[Tuple[str, str, bool, int]]:
    try:
        single_file_stat = await fs.statfile(x)
        return (
            single_file_stat.name(),
            single_file_stat.url(),
            False,
            await single_file_stat.size()
        )
    except FileNotFoundError:
        return None


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
    overwrite_if_size_differs: bool,
) -> Tuple[int, int]:
    async with sema:
        srcstat, srcfiles, dststat, dstfiles = await asyncio.gather(
            statfile(fs, src),
            listfiles(fs, src),
            statfile(fs, dst),
            listfiles(fs, dst),
        )

        if srcstat and srcfiles:
            raise PlanError(f'Source is both a directory and a file. This is not supported. {src}', 1)
        if dststat and dstfiles:
            raise PlanError(f'Destination is both a directory and a file. This is not supported. {dst}', 1)
        if srcstat and dstfiles:
            raise PlanError(f'Source is a file but destination is a directory. This is not supported. {src} -> {dst}', 1) from (
                IsADirectoryError(dst))
        if srcfiles and dststat:
            raise PlanError(f'Source is a directory but destination is a file. This is not supported. {src} -> {dst}', 1) from (
                IsADirectoryError(src))
        if srcstat:
            assert len(srcfiles) == 0
            assert len(dstfiles) == 0
            srcname, srcurl, srcisdir, srcsize = srcstat
            if dststat:
                dstname, dsturl, dstisdir, dstsize = dststat
                if srcsize == dstsize:
                    await matches.write((srcurl + '\t' + dsturl + '\n').encode('utf-8'))
                    return 0, 0
                elif overwrite_if_size_differs:
                    await plan.write((srcurl + '\t' + dsturl + '\n').encode('utf-8'))
                    return 1, srcsize
                else:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + str(srcsize) + '\t' + str(dstsize) + '\n').encode('utf-8'))
                    return 0, 0
            else:
                await srconly.write((srcurl + '\n').encode('utf-8'))
                await plan.write((srcurl + '\t' + dst + '\n').encode('utf-8'))
                return 1, srcsize
        elif not srcfiles:
            assert srcstat is None
            raise PlanError(f'Source is neither a folder nor a file: {src}', 1) from FileNotFoundError(src)

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
                            fs, matches, differs, srconly, dstonly, plan, srcurl, dsturl, progress, sema, overwrite_if_size_differs
                        ))
                    )
                elif srcisdir and not dstisdir:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + 'dir' + '\t' + 'file' + '\n').encode('utf-8'))
                elif not srcisdir and dstisdir:
                    await differs.write((srcurl + '\t' + dsturl + '\t' + 'file' + '\t' + 'dir' + '\n').encode('utf-8'))
                elif srcsize == dstsize:
                    await matches.write((srcurl + '\t' + dsturl + '\n').encode('utf-8'))
                else:
                    if overwrite_if_size_differs:
                        await plan.write((srcurl + '\t' + os.path.join(dst, srcname) + '\n').encode('utf-8'))
                        n_files += 1
                        n_bytes += srcsize
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
                            fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, srcurl.removeprefix(src)), progress, sema, overwrite_if_size_differs
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
                print((1, srcf, dstf, srcidx, dstidx, srcfiles, dstfiles))
                await dstonly.write((dsturl + '\n').encode('utf-8'))
        while srcidx < len(srcfiles):
            srcf = srcfiles[srcidx]
            srcname, srcurl, srcisdir, srcsize = srcf

            if srcisdir:
                if src[-1] != '/':
                    src += '/'
                child_directory_tasks.append(
                    asyncio.create_task(find_all_copy_pairs(
                        fs, matches, differs, srconly, dstonly, plan, srcurl, os.path.join(dst, srcurl.removeprefix(src)), progress, sema, overwrite_if_size_differs
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

            print((2, dstidx, dstf))
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
