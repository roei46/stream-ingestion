import io
import json
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from engine import PipelineCancelled, batches, event_batches, read_lines
from models import Event, Rejection, RejectionCategory, Status

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
        for i in range(1, count + 1)
    ]


def ids(batch: list[Event]) -> list[str]:
    return [event.event_id for event in batch]


@dataclass
class Counter:
    pulled: int = 0
    closed: bool = False


def counting_source(events: list[Event], counter: Counter) -> Generator[Event]:
    try:
        for event in events:
            counter.pulled += 1
            yield event
    finally:
        counter.closed = True


# --- line reader ---


def test_overlong_line_is_rejected_once_and_reading_continues() -> None:
    rejections: list[Rejection] = []
    data = b"ok\n" + b"x" * 50 + b"\nalso ok"
    lines = list(
        read_lines(io.BytesIO(data), max_line_bytes=10, reject=rejections.append)
    )
    assert lines == [(1, b"ok"), (3, b"also ok")]
    assert [r.line_number for r in rejections] == [2]
    assert rejections[0].category is RejectionCategory.LINE_TOO_LONG


# --- batch generator: send / sizes ---


def test_send_changes_the_next_batch_size() -> None:
    g = batches(iter(make_events(7)), batch_size=3, max_batch_size=10)
    assert ids(next(g)) == ["e1", "e2", "e3"]
    assert ids(g.send(2)) == ["e4", "e5"]
    assert ids(next(g)) == ["e6", "e7"]
    with pytest.raises(StopIteration):
        next(g)


@pytest.mark.parametrize("bad_size", [0, 11, True])
def test_invalid_sent_size_raises_and_ends_generator(bad_size: int) -> None:
    g = batches(iter(make_events(7)), batch_size=3, max_batch_size=10)
    next(g)
    with pytest.raises(ValueError):
        g.send(bad_size)
    with pytest.raises(StopIteration):
        next(g)


def test_invalid_starting_size_is_rejected() -> None:
    g = batches(iter(make_events(3)), batch_size=0, max_batch_size=10)
    with pytest.raises(ValueError):
        next(g)


def test_starting_size_is_capped_at_max() -> None:
    g = batches(iter(make_events(7)), batch_size=50, max_batch_size=4)
    assert len(next(g)) == 4


def test_send_before_priming_is_a_type_error() -> None:
    g = batches(iter(make_events(3)), batch_size=3, max_batch_size=10)
    with pytest.raises(TypeError):
        g.send(2)


def test_empty_input_gives_no_batches() -> None:
    assert list(batches(iter([]), batch_size=3, max_batch_size=10)) == []


# --- laziness, cancel, close ---


def test_batches_are_lazy() -> None:
    counter = Counter()
    g = batches(counting_source(make_events(10), counter), batch_size=3, max_batch_size=10)
    assert counter.pulled == 0
    next(g)
    assert counter.pulled == 3


def test_throw_cancels_and_closes_the_source() -> None:
    counter = Counter()
    g = batches(counting_source(make_events(10), counter), batch_size=3, max_batch_size=10)
    next(g)
    with pytest.raises(PipelineCancelled):
        g.throw(PipelineCancelled())
    assert counter.pulled == 3
    assert counter.closed is True


def test_close_stops_and_closes_the_source() -> None:
    counter = Counter()
    g = batches(counting_source(make_events(10), counter), batch_size=3, max_batch_size=10)
    next(g)
    g.close()
    assert counter.pulled == 3
    assert counter.closed is True


# --- yield from: event_batches forwards send / throw / close to batches ---


def write_valid_file(tmp_path: Path, count: int) -> Path:
    file = tmp_path / "events.jsonl"
    lines = [
        json.dumps(
            {
                "event_id": f"e{i}",
                "shipment_id": "s1",
                "occurred_at": "2026-11-08T12:00:00+02:00",
                "status": "created",
                "schema_version": 1,
            }
        )
        for i in range(1, count + 1)
    ]
    file.write_text("".join(line + "\n" for line in lines))
    return file


def open_pipeline(file: Path) -> Generator[list[Event], int | None, None]:
    return event_batches(
        file, batch_size=3, max_batch_size=10, max_line_bytes=1000, reject=print
    )


def test_yield_from_forwards_send(tmp_path: Path) -> None:
    g = open_pipeline(write_valid_file(tmp_path, 7))
    assert ids(next(g)) == ["e1", "e2", "e3"]
    assert ids(g.send(2)) == ["e4", "e5"]
    g.close()


def test_yield_from_forwards_throw(tmp_path: Path) -> None:
    g = open_pipeline(write_valid_file(tmp_path, 7))
    next(g)
    with pytest.raises(PipelineCancelled):
        g.throw(PipelineCancelled())
    with pytest.raises(StopIteration):
        next(g)


def test_yield_from_forwards_close(tmp_path: Path) -> None:
    g = open_pipeline(write_valid_file(tmp_path, 7))
    next(g)
    g.close()
    with pytest.raises(StopIteration):
        next(g)
