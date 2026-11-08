from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock


@dataclass
class LockTiming:
    elapsed: float = 0.0


@contextmanager
def timed_lock(lock: Lock, *, clock: Callable[[], float]) -> Generator[LockTiming]:
    timing = LockTiming()
    start = clock()
    lock.acquire()
    try:
        yield timing
    finally:
        lock.release()
        timing.elapsed = clock() - start
