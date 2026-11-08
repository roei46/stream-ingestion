from threading import Lock

import pytest

from locks import timed_lock


def test_timed_lock_releases_lock_after_error() -> None:
    lock = Lock()
    times = iter([10.0, 12.5])
    with (
        pytest.raises(ValueError),
        timed_lock(lock, clock=lambda: next(times)) as timing,
    ):
        assert lock.locked()
        raise ValueError("boom")
    assert not lock.locked()
    assert timing.elapsed == 2.5
