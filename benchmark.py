

import hashlib
import json
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from engine import run
from models import BatchStats, Event, Rejection, Status
from sink import FakeConnection


@dataclass(frozen=True, kw_only=True)
class PlainEvent:
    event_id: str
    shipment_id: str
    occurred_at: datetime
    status: Status
    tags: tuple[str, ...]
    fingerprint: str = field(init=False, compare=False)

    def __post_init__(self) -> None:
        if self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("occurred_at must be timezone-aware UTC")
        canonical = json.dumps(
            {
                "event_id": self.event_id,
                "shipment_id": self.shipment_id,
                "occurred_at": self.occurred_at.isoformat(),
                "status": self.status,
                "tags": list(self.tags),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        object.__setattr__(self, "fingerprint", digest)

    def __repr__(self) -> str:
        return (
            f"Event(event_id={self.event_id!r}, shipment_id={self.shipment_id!r}, "
            f"status={self.status.value!r}, fingerprint={self.fingerprint[:12]}...)"
        )


class PydanticEvent(BaseModel):
    event_id: str
    shipment_id: str
    occurred_at: datetime
    status: Status
    tags: tuple[str, ...]
    fingerprint: str = ""

    def model_post_init(self, context: Any, /) -> None:
        if self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("occurred_at must be timezone-aware UTC")
        canonical = json.dumps(
            {
                "event_id": self.event_id,
                "shipment_id": self.shipment_id,
                "occurred_at": self.occurred_at.isoformat(),
                "status": self.status,
                "tags": list(self.tags),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        self.fingerprint = digest

    def __repr__(self) -> str:
        return (
            f"Event(event_id={self.event_id!r}, shipment_id={self.shipment_id!r}, "
            f"status={self.status.value!r}, fingerprint={self.fingerprint[:12]}...)"
        )




def make_lines(n: int) -> list[str]:
    lines: list[str] = []
    for i in range(n):
        canonical = json.dumps(
               {
                   "event_id":  f"e{i}",
                    "shipment_id": "s1", 
                    "occurred_at": "2026-11-10T18:00:00+00:00",
                    "status": "created",
                    "tags": ["a"]
                })
        lines.append(canonical)
    return lines    


def parse_plain(line: str) -> PlainEvent:
    dic = json.loads(line)

    event = PlainEvent( 
            event_id=dic["event_id"],
            shipment_id=dic["shipment_id"],
            occurred_at=datetime.fromisoformat(dic["occurred_at"]),
            status=Status(dic["status"]),
            tags=tuple(dic["tags"]))
    return event



def parse_slotted(line: str) -> Event:
    dic = json.loads(line)

    event = Event( 
            event_id=dic["event_id"],
            shipment_id=dic["shipment_id"],
            occurred_at=datetime.fromisoformat(dic["occurred_at"]),
            status=Status(dic["status"]),
            tags=tuple(dic["tags"]))
    return event



def run_once(parse: Callable[[Any], object], lines: list[Any]) -> float:
    start = time.perf_counter() 

    for line in lines:
        _ = parse(line)


    return time.perf_counter() - start    



def measure(parse: Callable[[Any], object], lines: list[Any]) -> tuple[float, float]:
    times: list[float] = []
    run_once(parse, lines)

    for i in range(5):
        times.append(run_once(parse, lines))
    
    return statistics.median(times), max(times) - min(times)

def main() -> None:
    lines = make_lines(100_000) 
    print(measure(parse_plain, lines))
    print(measure(parse_slotted, lines))
    print(measure(PydanticEvent.model_validate_json, lines))

    values = make_values(100_000)
    print(measure(build_plain, values))
    print(measure(build_slotted, values))
    print(measure(build_pydantic, values))



def make_values(n: int) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for i in range(n):
        new_event = {"event_id": f"e{i}", "shipment_id": "s1",
                   "occurred_at": datetime(2026, 11, 10, 18, 0, tzinfo=UTC),
                   "status": Status.CREATED, "tags": ("a",)}
        lines.append(new_event)
                
    return lines    


def build_plain(values: dict[str, Any]) -> PlainEvent:    
    return PlainEvent(**values)

def build_slotted(values: dict[str, Any]) -> Event:

    return Event(**values)

def build_pydantic(values: dict[str, Any]) -> PydanticEvent:
    return PydanticEvent(**values)


def measure_memory(build: Callable[[Any], object], values: list[Any]) -> tuple[int, int]:
    tracemalloc.start()
    objects: list[object] = []
    for v in values:
        objects.append(build(v))

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return current, peak    


class CountingConnection(FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.count = 0

    def commit(self) -> None:
        self.count += len(self.pending)
        self.pending = []


def write_file(path: Path, n: int) -> None:      
    with open(path, "w") as f:
        f.writelines(line + "\n" for line in make_lines(n))
            

def measure_streaming(n: int) -> int:
    path = Path("stream.jsonl")
    write_file(path, n)
    tracemalloc.start()
    run(path, CountingConnection(), BatchStats(), batch_size=100, max_batch_size=1000, max_line_bytes=1024, reject=ignore)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return peak

def ignore(rejection: Rejection) -> None:
    pass

if __name__ == "__main__":
    command = sys.argv[1]
    if command == "timing":
        main()
    elif command == "streaming":
        for n in (10_000, 100_000, 1_000_000):
            print(n, measure_streaming(n))
    else:
        builders = {"plain": build_plain, "slotted": build_slotted, "pydantic": build_pydantic}
        build = builders[command]
        values = make_values(100_000)
        current, peak = measure_memory(build, values)
        print(command, peak)





