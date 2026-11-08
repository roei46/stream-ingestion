from types import TracebackType
from typing import Self

from models import BatchStats, Event


class RecoverableBatchError(Exception):
    pass


class FatalBatchError(Exception):
    pass


class FakeConnection:
    def __init__(self, *, fail_on: frozenset[str] = frozenset()) -> None:
        self.fail_on = fail_on
        self.pending: list[Event] = []
        self.committed: list[Event] = []
        self.is_open = False
        self.close_calls = 0

    def _maybe_fail(self, step: str) -> None:
        if step in self.fail_on:
            raise ConnectionError(f"{step} failed")

    def open(self) -> None:
        self.is_open = True
        self._maybe_fail("open")

    def begin(self) -> None:
        self.pending = []

    def write(self, events: list[Event]) -> None:
        self.pending.extend(events)

    def commit(self) -> None:
        self._maybe_fail("commit")
        self.committed.extend(self.pending)
        self.pending = []

    def rollback(self) -> None:
        self._maybe_fail("rollback")
        self.pending = []

    def close(self) -> None:
        self.close_calls += 1
        self._maybe_fail("close")
        self.is_open = False


class BatchTransaction:
    def __init__(self, connection: FakeConnection, stats: BatchStats, size: int) -> None:
        self.connection = connection
        self.stats = stats
        self.size = size

    def __enter__(self) -> Self:
        self.connection.begin()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if exc is None:
            try:
                self.connection.commit()
            except Exception as commit_error:
                try:
                    self.connection.rollback()
                except Exception as rollback_error:  # noqa: BLE001
                    raise BaseExceptionGroup(
                        "commit failed and rollback failed",
                        [commit_error, rollback_error],
                    )
                raise
            self.stats.committed += self.size
            return False
        try:
            self.connection.rollback()
        except Exception as rollback_error:  # noqa: BLE001
            raise BaseExceptionGroup(
                "batch failed and rollback failed", [exc, rollback_error]
            )
        if isinstance(exc, RecoverableBatchError):
            self.stats.failed_batches += 1
            self.stats.failed_records += self.size
            return True
        return False


class ManagedBatchSink:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.is_open = False

    def __enter__(self) -> Self:
        try:
            self.connection.open()
        except Exception as open_error:
            try:
                self.connection.close()
            except Exception as close_error:  # noqa: BLE001
                raise BaseExceptionGroup(
                    "open failed and close failed", [open_error, close_error]
                )
            raise
        self.is_open = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self.is_open:
            self.is_open = False
            try:
                self.connection.close()
            except Exception as close_error:
                if exc is None:
                    raise
                raise BaseExceptionGroup(
                    "run failed and close failed", [exc, close_error]
                )

    def transaction(self, stats: BatchStats, size: int) -> BatchTransaction:
        if not self.is_open:
            raise RuntimeError("sink is not open")
        return BatchTransaction(self.connection, stats, size)
