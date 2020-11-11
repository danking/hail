from types import TracebackType
from typing import Optional, Type
import asyncio
import logging
import weakref


log = logging.getLogger('aiotools.tasks')


class BackgroundTaskManager:
    def __init__(self):
        self.tasks: weakref.WeakSet = weakref.WeakSet()

    def ensure_future(self, coroutine):
        fut = asyncio.ensure_future(coroutine)
        self.tasks.add(fut)
        return fut

    def shutdown(self):
        for task in self.tasks:
            try:
                task.cancel()
            except Exception:
                log.warning(f'encountered an exception while cancelling background task: {task}', exc_info=True)

    def __enter__(self):
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
        self.shutdown()
