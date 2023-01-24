from typing import Optional, Callable, Tuple
from rich import filesize
from rich.progress import MofNCompleteColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, Progress, ProgressColumn, TaskProgressColumn, TransferSpeedColumn, Task
from rich.text import Text


class SimpleRichProgressBarTask:
    def __init__(self, progress: Progress, tid):
        self._progress = progress
        self.tid = tid

    def total(self) -> int:
        assert len(self._progress.tasks) == 1
        return int(self._progress.tasks[0].total or 0)

    def update(self, delta_n: int, *, total: Optional[int] = None):
        self._progress.update(self.tid, advance=delta_n, total=total)

    def make_listener(self) -> Callable[[int], None]:
        return make_listener(self._progress, self.tid)


class SimpleRichProgressBar:
    def __init__(self, *args, description: Optional[str] = None, total: int, visible: bool = True, **kwargs):
        self.description = description
        self.total = total
        self.visible = visible
        if len(args) == 0:
            args = RichProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    def __enter__(self) -> SimpleRichProgressBarTask:
        self._progress.start()
        tid = self._progress.add_task(self.description or '', total=self.total, visible=self.visible)
        return SimpleRichProgressBarTask(self._progress, tid)

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()


def make_listener(progress: Progress, tid) -> Callable[[int], None]:
    total = 0

    def listen(delta: int):
        nonlocal total
        if delta > 0:
            total += delta
            progress.update(tid, total=total)
        else:
            progress.update(tid, advance=-delta)
    return listen


class BytesOrCount(ProgressColumn):
    def __init__(
        self, table_column = None
    ) -> None:

        super().__init__(table_column=table_column)

    def render(self, task: "Task") -> Text:
        completed = int(task.completed)

        unit_and_suffix_calculation_base = (
            int(task.total) if task.total is not None else completed
        )

        if task.description == 'files':
            units = ["files", "K files", "M files", "G files", "T files", "P files", "E files", "Z files", "Y files"]
            magnitude = 1000
        else:
            units = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
            magnitude = 1024

        unit, suffix = filesize.pick_unit_and_suffix(
            unit_and_suffix_calculation_base,
            units,
            magnitude
        )

        precision = 0 if unit == 1 else 1

        completed_ratio = completed / unit
        completed_str = f"{completed_ratio:,.{precision}f}"

        if task.total is not None:
            total = int(task.total)
            total_ratio = total / unit
            total_str = f"{total_ratio:,.{precision}f}"
        else:
            total_str = "?"

        download_status = f"{completed_str}/{total_str} {suffix}"
        download_text = Text(download_status, style="progress.download")
        return download_text


class RichProgressBar:
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            args = RichProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    @staticmethod
    def get_default_columns() -> Tuple[ProgressColumn, ...]:
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="bar.finished"),
            TaskProgressColumn(),
            BytesOrCount(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn()
        )

    def __enter__(self) -> Progress:
        self._progress.start()
        return self._progress

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()


class BatchProgressBar:
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            args = BatchProgressBar.get_default_columns()
        self._progress = Progress(*args, **kwargs)

    @staticmethod
    def get_default_columns() -> Tuple[ProgressColumn, ...]:
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="bar.finished"),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn()
        )

    def __enter__(self) -> 'BatchProgressBar':
        self._progress.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        try:
            self._progress.refresh()
        finally:
            self._progress.stop()

    def with_task(self, description: str, *, total: int = 0, disable: bool = False, transient: bool = False) -> 'BatchProgressBarTask':
        tid = self._progress.add_task(description, total=total, visible=not disable)
        return BatchProgressBarTask(self._progress, tid, transient)


class BatchProgressBarTask:
    def __init__(self, progress: Progress, tid, transient: bool):
        self._progress = progress
        self.tid = tid
        self.transient = transient

    def total(self) -> int:
        assert len(self._progress.tasks) == 1
        return int(self._progress.tasks[0].total or 0)

    def __enter__(self) -> 'BatchProgressBarTask':
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type
        del exc_value
        del traceback
        if self.transient:
            self._progress.remove_task(self.tid)

    def update(self, advance: Optional[int] = None, **kwargs):
        self._progress.update(self.tid, advance=advance, **kwargs)
