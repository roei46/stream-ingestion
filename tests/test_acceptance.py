import json
from pathlib import Path

import pytest

from engine import event_batches, run
from models import BatchStats, Event, Rejection, RejectionCategory
from sink import FakeConnection, FatalBatchError, RecoverableBatchError


def make_line(number: int, **changes: object) -> str:
    payload = {
        "event_id": f"e{number}",
        "shipment_id": "s1",
        "occurred_at": "2026-11-08T12:00:00+02:00",
        "status": "created",
        "schema_version": 1,
    } | changes
    return json.dumps(payload)


def write_file(tmp_path: Path, lines: list[str]) -> Path:
    file = tmp_path / "events.jsonl"
    file.write_text("".join(line + "\n" for line in lines))
    return file


def baseline_lines() -> list[str]:
    # 12 lines; lines 4 and 9 are invalid
    return [
        make_line(n, status="lost") if n in (4, 9) else make_line(n)
        for n in range(1, 13)
    ]


class RecordingConnection(FakeConnection):
    def __init__(
        self,
        *,
        fail_on_write: int = 0,
        error: Exception | None = None,
        fail_on: frozenset[str] = frozenset(),
    ) -> None:
        super().__init__(fail_on=fail_on)
        self.write_sizes: list[int] = []
        self.fail_on_write = fail_on_write
        self.error = error

    def write(self, events: list[Event]) -> None:
        self.write_sizes.append(len(events))
        if len(self.write_sizes) == self.fail_on_write and self.error is not None:
            raise self.error
        super().write(events)


def run_file(
    file: Path, connection: FakeConnection, stats: BatchStats, batch_size: int = 4
) -> list[Rejection]:
    rejections: list[Rejection] = []
    run(
        file,
        connection,
        stats,
        batch_size=batch_size,
        max_batch_size=10,
        max_line_bytes=1000,
        reject=rejections.append,
    )
    return rejections


def committed_ids(connection: FakeConnection) -> list[str]:
    return [event.event_id for event in connection.committed]


# 1. Baseline (also covers the final partial batch)
def test_baseline(tmp_path: Path) -> None:
    connection = RecordingConnection()
    stats = BatchStats()
    rejections = run_file(write_file(tmp_path, baseline_lines()), connection, stats)
    assert connection.write_sizes == [4, 4, 2]
    assert stats.read == 12
    assert stats.accepted == 10
    assert stats.rejected == 2
    assert stats.committed == 10
    assert [r.line_number for r in rejections] == [4, 9]
    assert connection.is_open is False


# 2. Recoverable batch
def test_recoverable_batch(tmp_path: Path) -> None:
    connection = RecordingConnection(
        fail_on_write=2, error=RecoverableBatchError("batch 2 failed")
    )
    stats = BatchStats()
    run_file(write_file(tmp_path, baseline_lines()), connection, stats)
    assert connection.write_sizes == [4, 4, 2]
    assert stats.committed == 6
    assert stats.failed_records == 4
    assert stats.failed_batches == 1
    assert committed_ids(connection) == ["e1", "e2", "e3", "e5", "e11", "e12"]
    assert connection.is_open is False


# 3. Fatal batch
def test_fatal_batch(tmp_path: Path) -> None:
    connection = RecordingConnection(
        fail_on_write=2, error=FatalBatchError("batch 2 failed")
    )
    stats = BatchStats()
    with pytest.raises(FatalBatchError):
        run_file(write_file(tmp_path, baseline_lines()), connection, stats)
    assert connection.write_sizes == [4, 4]
    assert committed_ids(connection) == ["e1", "e2", "e3", "e5"]
    assert connection.pending == []
    assert connection.is_open is False


# 4. Early stop
def test_early_stop_after_first_batch(tmp_path: Path) -> None:
    rejections: list[Rejection] = []
    pipeline = event_batches(
        write_file(tmp_path, baseline_lines()),
        batch_size=4,
        max_batch_size=10,
        max_line_bytes=1000,
        reject=rejections.append,
    )
    first = next(pipeline)
    pipeline.close()
    assert [event.event_id for event in first] == ["e1", "e2", "e3", "e5"]
    assert [r.line_number for r in rejections] == [4]
    with pytest.raises(StopIteration):
        next(pipeline)


# 5. Boundaries
def test_empty_input(tmp_path: Path) -> None:
    connection = RecordingConnection()
    stats = BatchStats()
    run_file(write_file(tmp_path, []), connection, stats)
    assert connection.write_sizes == []
    assert stats == BatchStats()


def test_all_invalid_input(tmp_path: Path) -> None:
    connection = RecordingConnection()
    stats = BatchStats()
    lines = [make_line(n, status="lost") for n in range(1, 4)]
    run_file(write_file(tmp_path, lines), connection, stats)
    assert connection.write_sizes == []
    assert stats.read == 3
    assert stats.rejected == 3
    assert stats.committed == 0


def test_overlong_line_and_unknown_field_are_rejected(tmp_path: Path) -> None:
    connection = RecordingConnection()
    stats = BatchStats()
    lines = [
        make_line(1),
        make_line(2, shipment_id="x" * 2000),
        make_line(3, color="red"),
        make_line(4),
    ]
    rejections = run_file(write_file(tmp_path, lines), connection, stats)
    assert [(r.line_number, r.category) for r in rejections] == [
        (2, RejectionCategory.LINE_TOO_LONG),
        (3, RejectionCategory.VALIDATION),
    ]
    assert committed_ids(connection) == ["e1", "e4"]


def test_invalid_batch_size_raises_and_closes(tmp_path: Path) -> None:
    connection = RecordingConnection()
    with pytest.raises(ValueError):
        run_file(
            write_file(tmp_path, baseline_lines()), connection, BatchStats(), batch_size=0
        )
    assert connection.is_open is False


def test_cleanup_error_keeps_the_original_error(tmp_path: Path) -> None:
    connection = RecordingConnection(
        fail_on_write=1,
        error=FatalBatchError("batch 1 failed"),
        fail_on=frozenset({"close"}),
    )
    with pytest.RaisesGroup(FatalBatchError, ConnectionError):
        run_file(write_file(tmp_path, baseline_lines()), connection, BatchStats())
