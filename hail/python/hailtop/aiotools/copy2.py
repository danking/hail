import argparse
import aiomonitor
import asyncio
import datetime
import functools
import logging
import os
import sys
from ..utils.rich_progress_bar import RichProgressBar, Progress

from .router_fs import RouterAsyncFS
from .fs import AsyncFS
from .fs.copier import Transfer
from .fs.exceptions import UnexpectedEOFError
from ..utils import retry_transient_errors, bounded_gather2
from ..utils.utils import WithoutSemaphore
from .copy import copy

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
    max_simultaneous_files = args.max_simultaneous_files or max_simultaneous_transfers // 4
    gcs_kwargs = {'gcs_requester_pays_configuration': requester_pays_project}
    s3_kwargs = {'max_pool_connections': max_simultaneous_transfers * 5,
                 'timeout': args.timeout,
                 'max_workers': max_simultaneous_transfers}
    azure_kwargs = {'timeout': args.timeout}

    if any((not os.path.exists(x) for x in ('matches', 'differs', 'srconly', 'dstonly', 'plan', 'summary'))):
        print('Run hailtop.aiotools.plan first.')
        sys.exit(1)

    await copy(
        max_simultaneous_transfers=max_simultaneous_transfers,
        local_kwargs=None,
        gcs_kwargs=gcs_kwargs,
        azure_kwargs=azure_kwargs,
        s3_kwargs=s3_kwargs,
        transfers=[Transfer(src, dst, treat_dest_as=Transfer.DEST_IS_TARGET) for src, dst in iterate_plan_file()],
        verbose=args.verbose
    )


def iterate_plan_file():
    lineno = 0
    with open('plan') as plan:
        for line in plan.readlines():
            parts = line.strip().split('\t')
            if len(parts) != 2:
                print(f'Malformed plan line, {lineno}, more than one tab.')
                sys.exit(1)
            yield parts


if __name__ == '__main__':
    uvloop_install()
    loop = asyncio.new_event_loop()
    with aiomonitor.start_monitor(loop=loop) as monitor:
        print(f'aiomonitor available on {monitor.host} {monitor.port}')
        loop.run_until_complete(main())
