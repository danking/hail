from typing import Any, AsyncIterator, Awaitable, Optional, List, Union, Dict, Callable, Tuple
import os
import os.path
import asyncio
import functools
import humanize
from concurrent.futures import Executor


from ...utils import (
    retry_transient_errors,
    url_basename,
    url_join,
    bounded_gather2,
    time_msecs,
    humanize_timedelta_msecs,
)
from ..weighted_semaphore import WeightedSemaphore
from .exceptions import UnexpectedEOFError
from .fs import MultiPartCreate, FileStatus, AsyncFS, FileListEntry


class Transfer:
    DEST_DIR = 'dest_dir'
    DEST_IS_TARGET = 'dest_is_target'
    INFER_DEST = 'infer_dest'

    def __init__(self, src: Union[str, List[str]], dest: str, *, treat_dest_as: str = INFER_DEST):
        if treat_dest_as not in (Transfer.DEST_DIR, Transfer.DEST_IS_TARGET, Transfer.INFER_DEST):
            raise ValueError(f'treat_dest_as invalid: {treat_dest_as}')

        if treat_dest_as == Transfer.DEST_IS_TARGET and isinstance(src, list):
            raise NotADirectoryError(dest)
        if treat_dest_as == Transfer.INFER_DEST and dest.endswith('/'):
            treat_dest_as = Transfer.DEST_DIR

        self.src = src
        self.dest = dest
        self.treat_dest_as = treat_dest_as


class SourceReport:
    def __init__(
        self,
        source,
        *,
        files_listener: Optional[Callable[[int], None]] = None,
        bytes_listener: Optional[Callable[[int], None]] = None,
    ):
        self._source = source
        self._files_listener = files_listener
        self._bytes_listener = bytes_listener
        self._source_type: Optional[str] = None
        self._files = 0
        self._bytes = 0
        self._errors = 0
        self._complete = 0
        self._first_file_error: Optional[Dict[str, Any]] = None
        self._exception: Optional[Exception] = None

    def start_files(self, n_files: int):
        self._files += n_files
        if self._files_listener:
            self._files_listener(n_files)

    def start_bytes(self, n_bytes: int):
        self._bytes += n_bytes
        if self._bytes_listener:
            self._bytes_listener(n_bytes)

    def finish_files(self, n_files: int, failed: bool = False):
        if failed:
            self._errors += n_files
        else:
            self._complete += n_files
        if self._files_listener:
            self._files_listener(-n_files)

    def finish_bytes(self, n_bytes: int):
        if self._bytes_listener:
            self._bytes_listener(-n_bytes)

    def set_exception(self, exception: Exception):
        assert not self._exception
        self._exception = exception

    def set_file_error(self, srcfile: str, destfile: str, exception: Exception):
        if self._first_file_error is None:
            self._first_file_error = {'srcfile': srcfile, 'destfile': destfile, 'exception': exception}


class TransferReport:
    _source_report: Union[SourceReport, List[SourceReport]]

    def __init__(
        self,
        transfer: Transfer,
        *,
        files_listener: Optional[Callable[[int], None]] = None,
        bytes_listener: Optional[Callable[[int], None]] = None,
    ):
        self._transfer = transfer
        if isinstance(transfer.src, str):
            self._source_report = SourceReport(
                transfer.src, files_listener=files_listener, bytes_listener=bytes_listener
            )
        else:
            self._source_report = [
                SourceReport(s, files_listener=files_listener, bytes_listener=bytes_listener) for s in transfer.src
            ]
        self._exception: Optional[Exception] = None

    def set_exception(self, exception: Exception):
        assert not self._exception
        self._exception = exception


class CopyReport:
    def __init__(
        self,
        transfer: Union[Transfer, List[Transfer]],
        *,
        files_listener: Optional[Callable[[int], None]] = None,
        bytes_listener: Optional[Callable[[int], None]] = None,
    ):
        self._start_time = time_msecs()
        self._end_time: Optional[int] = None
        self._duration: Optional[int] = None
        if isinstance(transfer, Transfer):
            self._transfer_report: Union[TransferReport, List[TransferReport]] = TransferReport(
                transfer, files_listener=files_listener, bytes_listener=bytes_listener
            )
        else:
            self._transfer_report = [
                TransferReport(t, files_listener=files_listener, bytes_listener=bytes_listener) for t in transfer
            ]
        self._exception: Optional[Exception] = None

    def set_exception(self, exception: Exception):
        assert not self._exception
        self._exception = exception

    def mark_done(self):
        self._end_time = time_msecs()
        self._duration = self._end_time - self._start_time

    def summarize(self, include_sources: bool = True):
        source_reports = []

        def add_source_reports(transfer_report):
            if isinstance(transfer_report._source_report, SourceReport):
                source_reports.append(transfer_report._source_report)
            else:
                source_reports.extend(transfer_report._source_report)

        if isinstance(self._transfer_report, TransferReport):
            total_transfers = 1
            add_source_reports(self._transfer_report)
        else:
            total_transfers = len(self._transfer_report)
            for t in self._transfer_report:
                add_source_reports(t)

        total_sources = len(source_reports)
        total_files = sum(sr._files for sr in source_reports)
        total_bytes = sum(sr._bytes for sr in source_reports)

        print('Transfer summary:')
        print(f'  Transfers: {total_transfers}')
        print(f'  Sources: {total_sources}')
        print(f'  Files: {total_files}')
        print(f'  Bytes: {humanize.naturalsize(total_bytes)}')
        print(f'  Time: {humanize_timedelta_msecs(self._duration)}')
        assert self._duration is not None
        if self._duration > 0:
            bandwidth = humanize.naturalsize(total_bytes / (self._duration / 1000))
            print(f'  Average bandwidth: {bandwidth}/s')
            file_rate = total_files / (self._duration / 1000)
            print(f'  Average file rate: {file_rate:,.1f}/s')

        if include_sources:
            print('Sources:')
            for sr in source_reports:
                print(f'  {sr._source}: {sr._files} files, {humanize.naturalsize(sr._bytes)}')


def _copy_file(srcfile: str, size: int, destfile: str) -> None:
    async def foo():
        print('_copy_file', srcfile)
        assert not destfile.endswith('/')

        from ..router_fs import RouterAsyncFS

        router_fs = RouterAsyncFS()
        total_written = 0

        try:
            async with await router_fs.open(srcfile) as srcf:
                try:
                    dest_cm = await router_fs.create(destfile, retry_writes=False)
                except FileNotFoundError:
                    await router_fs.makedirs(os.path.dirname(destfile), exist_ok=True)
                    dest_cm = await router_fs.create(destfile)

                async with dest_cm as destf:
                    while True:
                        b = await srcf.read(Copier.BUFFER_SIZE)
                        if not b:
                            return total_written
                        written = await destf.write(b)
                        assert written == len(b)
                        total_written += written
            return total_written
        finally:
            await router_fs.close()

    async def bar():
        return await retry_transient_errors(foo)

    return asyncio.run(bar())


class SourceCopier:
    """This class implements copy from a single source.  In general, a
    transfer will have multiple sources, and a SourceCopier will be
    created for each source.
    """

    def __init__(
        self,
        router_fs: AsyncFS,
        process_pool: Executor,
        xfer_sema: WeightedSemaphore,
        src: str,
        dest: str,
        treat_dest_as: str,
        dest_type,
    ):
        # self.router_fs = router_fs
        self.process_pool = process_pool
        # self.xfer_sema = xfer_sema
        self.src = src
        self.dest = dest
        self.treat_dest_as = treat_dest_as
        self.dest_type = dest_type

        self.src_is_file: Optional[bool] = None
        self.src_is_dir: Optional[bool] = None

        # self.pending = 2
        # self.barrier = asyncio.Event()

    def router_fs(self):
        from ..router_fs import RouterAsyncFS

        return RouterAsyncFS()

    # async def release_barrier(self):
    #     self.pending -= 1
    #     if self.pending == 0:
    #         self.barrier.set()

    async def _copy_file_multi_part_main(
        self,
        srcfile: str,
        srcstat: FileStatus,
        destfile: str,
        return_exceptions: bool,
    ):
        size = await srcstat.size()

        router_fs = self.router_fs()
        try:
            sema = asyncio.Semaphore(10)
            part_size = router_fs.copy_part_size(destfile)

            if size <= part_size:
                print('_copy_file_multi_part_main', srcfile)
                x = await asyncio.get_running_loop().run_in_executor(
                    self.process_pool, _copy_file, srcfile, size, destfile
                )
                assert isinstance(x, int), x
                print('_copy_file_multi_part_main', x)
                return x

            n_parts, rem = divmod(size, part_size)
            if rem:
                n_parts += 1

            async def foo():
                try:
                    part_creator = await router_fs.multi_part_create(sema, destfile, n_parts)
                except FileNotFoundError:
                    await router_fs.makedirs(os.path.dirname(destfile), exist_ok=True)
                    part_creator = await router_fs.multi_part_create(sema, destfile, n_parts)

                async with part_creator:

                    async def _copy_part(
                        part_size: int,
                        srcfile: str,
                        part_number: int,
                        this_part_size: int,
                        part_creator: MultiPartCreate,
                        return_exceptions: bool,
                    ) -> None:
                        print('_copy_part', srcfile)
                        total_written = 0
                        from ..router_fs import RouterAsyncFS

                        router_fs = RouterAsyncFS()
                        try:
                            # async with self.xfer_sema.acquire_manager(min(Copier.BUFFER_SIZE, this_part_size)):
                            async with await router_fs.open_from(
                                srcfile, part_number * part_size, length=this_part_size
                            ) as srcf:
                                async with await part_creator.create_part(
                                    part_number, part_number * part_size, size_hint=this_part_size
                                ) as destf:
                                    n = this_part_size
                                    while n > 0:
                                        b = await srcf.read(min(Copier.BUFFER_SIZE, n))
                                        if len(b) == 0:
                                            raise UnexpectedEOFError()
                                        written = await destf.write(b)
                                        assert written == len(b)
                                        total_written += written
                                        n -= len(b)
                            return total_written
                        finally:
                            await router_fs.close()
                            # if return_exceptions:
                            #     source_report.set_exception(e)
                            # else:
                            #     raise

                    async def f(i):
                        this_part_size = rem if i == n_parts - 1 and rem else part_size
                        print('f', srcfile)
                        x = await retry_transient_errors(
                            _copy_part,
                            part_size,
                            srcfile,
                            i,
                            this_part_size,
                            part_creator,
                            return_exceptions,
                        )
                        assert isinstance(x, int), x
                        print('f', x)
                        return x

                    return sum(
                        await bounded_gather2(
                            sema, *[functools.partial(f, i) for i in range(n_parts)], cancel_on_error=True
                        )
                    )

            def bar():
                return asyncio.run(foo())

            return await asyncio.get_running_loop().run_in_executor(bar)
        finally:
            await router_fs.close()

    async def _copy_file_multi_part(
        self,
        srcfile: str,
        srcstat: FileStatus,
        destfile: str,
        return_exceptions: bool,
    ) -> None:
        try:
            return await self._copy_file_multi_part_main(srcfile, srcstat, destfile, return_exceptions)
        # except Exception as e:
        #     if return_exceptions:
        #         source_report.set_file_error(srcfile, destfile, e)
        #     else:
        #         raise e
        finally:
            pass

    async def _full_dest(self):
        dest_type = self.dest_type

        if self.treat_dest_as == Transfer.DEST_DIR or (
            self.treat_dest_as == Transfer.INFER_DEST and dest_type == AsyncFS.DIR
        ):
            # We know dest is a dir, but we're copying to
            # dest/basename(src), and we don't know its type.
            return url_join(self.dest, url_basename(self.src.rstrip('/'))), None

        if self.treat_dest_as == Transfer.DEST_IS_TARGET and self.dest.endswith('/'):
            dest_type = AsyncFS.DIR

        return self.dest, dest_type

    async def copy_as_file(
        self,
        return_exceptions: bool,
    ):
        # try:
        #     src = self.src
        #     if src.endswith('/'):
        #         return
        #     try:
        #         srcstat = await self.router_fs.statfile(src)
        #     except FileNotFoundError:
        #         self.src_is_file = False
        #         return
        #     self.src_is_file = True
        # finally:
        #     await self.release_barrier()

        # await self.barrier.wait()

        # if self.src_is_dir:
        #     raise FileAndDirectoryError(self.src)

        src = self.src
        router_fs = self.router_fs()
        try:
            srcstat = await router_fs.statfile(src)

            full_dest, full_dest_type = await self._full_dest()
            if full_dest_type == AsyncFS.DIR:
                raise IsADirectoryError(full_dest)

            return await self._copy_file_multi_part(src, srcstat, full_dest, return_exceptions)
        finally:
            await router_fs.close()

    async def copy_as_dir(self, return_exceptions: bool):
        src = self.src
        router_fs = self.router_fs()

        try:

            async def files_iterator() -> AsyncIterator[FileListEntry]:
                return await router_fs.listfiles(src, recursive=True)

            srcentries: Optional[AsyncIterator[FileListEntry]] = await files_iterator()

            # try:
            #     if not src.endswith('/'):
            #         src = src + '/'

            #     try:
            #         srcentries: Optional[AsyncIterator[FileListEntry]] = await files_iterator()
            #     except (NotADirectoryError, FileNotFoundError):
            #         self.src_is_dir = False
            #         return
            #     self.src_is_dir = True
            # finally:
            #     await self.release_barrier()

            # await self.barrier.wait()

            # if self.src_is_file:
            #     raise FileAndDirectoryError(self.src)

            full_dest, full_dest_type = await self._full_dest()
            if full_dest_type == AsyncFS.FILE:
                raise NotADirectoryError(full_dest)

            async def copy_source(srcentry: FileListEntry) -> None:
                srcfile = await srcentry.url_maybe_trailing_slash()
                assert srcfile.startswith(src)

                # skip files with empty names
                if srcfile.endswith('/'):
                    return

                relsrcfile = srcfile[len(src) :]
                assert not relsrcfile.startswith('/')

                await self._copy_file_multi_part(
                    srcfile,
                    await srcentry.status(),
                    url_join(full_dest, relsrcfile),
                    return_exceptions,
                )

            async def create_copies() -> Tuple[List[Callable[[], Awaitable[None]]], int]:
                nonlocal srcentries
                bytes_to_copy = 0
                if srcentries is None:
                    srcentries = await files_iterator()
                try:
                    copy_thunks = []
                    async for srcentry in srcentries:
                        # In cloud FSes, status and size never make a network request. In local FS, they
                        # can make system calls on symlinks. This line will be fairly expensive if
                        # copying a tree with a lot of symlinks.
                        bytes_to_copy += await (await srcentry.status()).size()
                        copy_thunks.append(functools.partial(copy_source, srcentry))
                    return (copy_thunks, bytes_to_copy)
                finally:
                    srcentries = None

            copies, bytes_to_copy = await retry_transient_errors(create_copies)

            sema = asyncio.Semaphore(10)
            await bounded_gather2(sema, *copies, cancel_on_error=True)
        finally:
            await router_fs.close()

    async def copy(self, return_exceptions: bool):
        try:
            # gather with return_exceptions=True to make copy
            # deterministic with respect to exceptions
            try:
                return await self.copy_as_file(return_exceptions)
            except FileNotFoundError:
                pass

            # try:
            #     await self.copy_as_dir(return_exceptions)
            #     is_dir = True
            # except (NotADirectoryError, FileNotFoundError):
            #     is_dir = False
            # results = await asyncio.gather(
            #     self.copy_as_file(sema, source_report, return_exceptions),
            #     self.copy_as_dir(sema, source_report, return_exceptions),
            #     # return_exceptions=True,
            # )

            # assert self.pending == 0

            # for result in results:
            #     if isinstance(result, BaseException):
            #         raise result

            # assert (self.src_is_file is None) == self.src.endswith('/')
            # assert self.src_is_dir is not None, repr((
            #     results,
            #     self.src_is_file,
            #     self.src_is_dir,
            #     self.src,
            #     self.dest,
            #     # self.barrier,
            #     self.pending,
            # ))
            # if (is_file is False or self.src.endswith('/')) and not is_dir:
            #     raise FileNotFoundError(self.src)
        finally:
            pass

        # except Exception as e:
        #     if return_exceptions:
        #         source_report.set_exception(e)
        #     else:
        #         raise e


class Copier:
    """
    This class implements copy for a list of transfers.
    """

    BUFFER_SIZE = 8 * 1024 * 1024

    @staticmethod
    async def copy(
        fs: AsyncFS,
        sema: asyncio.Semaphore,
        transfer: Union[Transfer, List[Transfer]],
        process_pool: Executor,
        return_exceptions: bool = False,
        *,
        files_listener: Optional[Callable[[int], None]] = None,
        bytes_listener: Optional[Callable[[int], None]] = None,
    ) -> CopyReport:
        copier = Copier(fs, process_pool)
        copy_report = CopyReport(transfer, files_listener=files_listener, bytes_listener=bytes_listener)
        await copier._copy(sema, copy_report, transfer, return_exceptions)
        copy_report.mark_done()
        return copy_report

    def __init__(self, router_fs: AsyncFS, process_pool: Executor):
        self.router_fs = router_fs
        self.process_pool = process_pool
        # This is essentially a limit on amount of memory in temporary
        # buffers during copying.  We allow ~10 full-sized copies to
        # run concurrently.
        self.xfer_sema = WeightedSemaphore(100 * Copier.BUFFER_SIZE)

    async def _dest_type(self, transfer: Transfer):
        """Return the (real or assumed) type of `dest`.

        If the transfer assumes the type of `dest`, return that rather
        than the real type.  A return value of `None` mean `dest` does
        not exist.
        """
        assert transfer.treat_dest_as != Transfer.DEST_IS_TARGET

        if transfer.treat_dest_as == Transfer.DEST_DIR or isinstance(transfer.src, list) or transfer.dest.endswith('/'):
            return AsyncFS.DIR

        assert not transfer.dest.endswith('/')
        try:
            dest_type = await self.router_fs.staturl(transfer.dest)
        except FileNotFoundError:
            dest_type = None

        return dest_type

    async def copy_source(
        self,
        sema: asyncio.Semaphore,
        transfer: Transfer,
        src: str,
        dest_type_task,
        return_exceptions: bool,
    ):
        src_copier = SourceCopier(
            self.router_fs,
            self.process_pool,
            self.xfer_sema,
            src,
            transfer.dest,
            transfer.treat_dest_as,
            await dest_type_task if dest_type_task else None,
        )
        async with sema:
            print(sema)
            return await src_copier.copy(return_exceptions)

    async def _copy_one_transfer(
        self, sema: asyncio.Semaphore, transfer_report: TransferReport, transfer: Transfer, return_exceptions: bool
    ):
        try:
            if transfer.treat_dest_as == Transfer.INFER_DEST:
                dest_type_task: Optional[asyncio.Task] = asyncio.create_task(self._dest_type(transfer))
            else:
                dest_type_task = None

            try:
                src = transfer.src
                src_report = transfer_report._source_report
                if isinstance(src, str):
                    assert isinstance(src_report, SourceReport)
                    written = await self.copy_source(sema, transfer, src, dest_type_task, return_exceptions)
                    src_report.finish_bytes(written)
                    src_report.finish_files(1, failed=False)
                else:
                    assert isinstance(src_report, list)
                    if transfer.treat_dest_as == Transfer.DEST_IS_TARGET:
                        raise NotADirectoryError(transfer.dest)

                    await bounded_gather2(
                        sema,
                        *[
                            functools.partial(self.copy_source, sema, transfer, s, dest_type_task, return_exceptions)
                            for r, s in zip(src_report, src)
                        ],
                        cancel_on_error=True,
                    )

                # raise potential exception
                if dest_type_task:
                    await dest_type_task
            finally:
                if dest_type_task:
                    await asyncio.wait([dest_type_task])
        except Exception as e:
            if return_exceptions:
                transfer_report.set_exception(e)
            else:
                raise e

    async def _copy(
        self,
        sema: asyncio.Semaphore,
        copy_report: CopyReport,
        transfer: Union[Transfer, List[Transfer]],
        return_exceptions: bool,
    ):
        transfer_report = copy_report._transfer_report
        try:
            print('_copy', len(transfer))
            if isinstance(transfer, Transfer):
                assert isinstance(transfer_report, TransferReport)
                await self._copy_one_transfer(sema, transfer_report, transfer, return_exceptions)
                return

            assert isinstance(transfer_report, list)
            idx = 0
            while sema._value > 0 and idx < len(transfer):
                await asyncio.gather(*[
                    self._copy_one_transfer(sema, r, t, return_exceptions)
                    for r, t in zip(transfer_report[idx : (idx + 10)], transfer[idx : (idx + 10)])
                ])
                idx += 10
        finally:
            pass
