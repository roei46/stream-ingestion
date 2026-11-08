from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import closing
from itertools import islice
from pathlib import Path
from typing import BinaryIO

from pydantic import ValidationError

from models import (
    BatchStats,
    Event,
    EventPayload,
    Rejection,
    RejectionCategory,
    to_domain,
)
from sink import FakeConnection, ManagedBatchSink


def read_lines(
    source: BinaryIO,
    *,
    max_line_bytes: int,
    reject: Callable[[Rejection], None],
) -> Generator[tuple[int, bytes]]:
    line_number = 0
    while True:
        chunk = source.readline(max_line_bytes + 1)
        if not chunk:
            return
        line_number += 1
        if chunk.endswith(b"\n") or len(chunk) <= max_line_bytes:
            yield line_number, chunk.removesuffix(b"\n")
            continue
        while chunk and not chunk.endswith(b"\n"):
            chunk = source.readline(max_line_bytes + 1)
        reject(
            Rejection(
                line_number=line_number,
                category=RejectionCategory.LINE_TOO_LONG,
                message=f"line is longer than {max_line_bytes} bytes",
            )
        )


def parse_events(
    lines: Iterable[tuple[int, bytes]],
    *,
    reject: Callable[[Rejection], None],
) -> Generator[Event]:
    for line_number, line in lines:
        try:
            event = to_domain(EventPayload.model_validate_json(line))
        except ValidationError as error:
            first = error.errors(include_input=False)[0]
            category = (
                RejectionCategory.INVALID_JSON
                if first["type"] == "json_invalid"
                else RejectionCategory.VALIDATION
            )
            reject(
                Rejection(
                    line_number=line_number,
                    category=category,
                    message=first["msg"],
                )
            )
            continue
        yield event


class PipelineCancelled(Exception):
    pass


def _valid_size(size: int, max_batch_size: int) -> int:
    if isinstance(size, bool) or not 1 <= size <= max_batch_size:
        raise ValueError(f"batch size must be 1..{max_batch_size}, got {size!r}")
    return size


def batches(
    events: Iterator[Event],
    *,
    batch_size: int,
    max_batch_size: int,
) -> Generator[list[Event], int | None, None]:
    size = _valid_size(min(batch_size, max_batch_size), max_batch_size)
    try:
        while True:
            batch = list(islice(events, size))
            if not batch:
                return
            new_size = yield batch
            if new_size is not None:
                size = _valid_size(new_size, max_batch_size)
    finally:
        if isinstance(events, Generator):
            events.close()


def event_batches(
    path: Path,
    *,
    batch_size: int,
    max_batch_size: int,
    max_line_bytes: int,
    reject: Callable[[Rejection], None],
) -> Generator[list[Event], int | None, None]:
    with path.open("rb") as source:
        lines = read_lines(source, max_line_bytes=max_line_bytes, reject=reject)
        events = parse_events(lines, reject=reject)
        yield from batches(events, batch_size=batch_size, max_batch_size=max_batch_size)


def run(
    path: Path,
    connection: FakeConnection,
    stats: BatchStats,
    *,
    batch_size: int,
    max_batch_size: int,
    max_line_bytes: int,
    reject: Callable[[Rejection], None],
) -> None:
    def count_and_reject(rejection: Rejection) -> None:
        stats.read += 1
        stats.rejected += 1
        reject(rejection)

    pipeline = event_batches(
        path,
        batch_size=batch_size,
        max_batch_size=max_batch_size,
        max_line_bytes=max_line_bytes,
        reject=count_and_reject,
    )
    with ManagedBatchSink(connection) as sink, closing(pipeline):
        for batch in pipeline:
            stats.read += len(batch)
            stats.accepted += len(batch)
            with sink.transaction(stats, len(batch)):
                connection.write(batch)
