from datetime import UTC, datetime

import pytest

from models import BatchStats, Event, Status
from sink import (
    FakeConnection,
    FatalBatchError,
    ManagedBatchSink,
    RecoverableBatchError,
)

T = datetime(2026, 11, 8, 10, 0, tzinfo=UTC)


def make_events(count: int) -> list[Event]:
    return [
        Event(
            event_id=f"e{i}",
            shipment_id="s1",
            occurred_at=T,
            status=Status.CREATED,
            tags=(),
        )
        for i in range(count)
    ]


def test_successful_batch_is_committed() -> None:
    connection = FakeConnection()
    stats = BatchStats()
    events = make_events(4)
    with ManagedBatchSink(connection) as sink, sink.transaction(stats, len(events)):
        connection.write(events)
    assert connection.committed == events
    assert stats.committed == 4
    assert connection.is_open is False


def test_recoverable_error_rolls_back_and_is_swallowed() -> None:
    connection = FakeConnection()
    stats = BatchStats()
    events = make_events(4)
    with ManagedBatchSink(connection) as sink:
        with sink.transaction(stats, len(events)):
            connection.write(events)
            raise RecoverableBatchError("bad batch")
        continued = True
    assert continued is True
    assert connection.pending == []
    assert connection.committed == []
    assert stats.committed == 0
    assert stats.failed_batches == 1
    assert stats.failed_records == 4


def test_fatal_error_rolls_back_and_is_raised() -> None:
    connection = FakeConnection()
    stats = BatchStats()
    events = make_events(4)
    with (
        pytest.raises(FatalBatchError),
        ManagedBatchSink(connection) as sink,
        sink.transaction(stats, len(events)),
    ):
        connection.write(events)
        raise FatalBatchError("disk broken")
    assert connection.pending == []
    assert connection.committed == []
    assert stats.failed_batches == 0
    assert connection.is_open is False


def test_failed_open_closes_connection_and_raises() -> None:
    connection = FakeConnection(fail_on=frozenset({"open"}))
    body_ran = False
    with pytest.raises(ConnectionError), ManagedBatchSink(connection):
        body_ran = True
    assert body_ran is False
    assert connection.is_open is False


def test_sink_closes_connection_only_once() -> None:
    connection = FakeConnection()
    sink = ManagedBatchSink(connection)
    with sink:
        pass
    sink.__exit__(None, None, None)
    assert connection.close_calls == 1


def test_rollback_failure_keeps_both_errors() -> None:
    connection = FakeConnection(fail_on=frozenset({"rollback"}))
    stats = BatchStats()
    with (
        pytest.RaisesGroup(FatalBatchError, ConnectionError),
        ManagedBatchSink(connection) as sink,
        sink.transaction(stats, 4),
    ):
        raise FatalBatchError("disk broken")
    assert connection.is_open is False


def test_failed_commit_rolls_back_and_raises() -> None:
    connection = FakeConnection(fail_on=frozenset({"commit"}))
    stats = BatchStats()
    events = make_events(4)
    with (
        pytest.raises(ConnectionError),
        ManagedBatchSink(connection) as sink,
        sink.transaction(stats, len(events)),
    ):
        connection.write(events)
    assert connection.pending == []
    assert connection.committed == []
    assert stats.committed == 0
