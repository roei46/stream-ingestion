from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from models import BatchStats, Event

T = datetime(2026, 11, 7, 10, 0, tzinfo=UTC)


def make_event(event_id: str = "e1") -> Event:
    return Event(
        event_id=event_id,
        shipment_id="s1",
        occurred_at=T,
        status="created",
        tags=("a",),
    )


def test_equal_events_are_equal() -> None:
    assert make_event() == make_event()


def test_different_events_are_not_equal() -> None:
    assert make_event("e1") != make_event("e2")


def test_equal_events_have_equal_hash() -> None:
    assert hash(make_event()) == hash(make_event())


def test_event_works_as_dict_key() -> None:
    lookup = {make_event(): "found"}
    assert lookup[make_event()] == "found"


def test_batch_stats_do_not_share_errors_list() -> None:
    a = BatchStats()
    b = BatchStats()
    a.errors.append("bad line")
    assert b.errors == []


def test_fingerprint_comes_from_fields() -> None:
    assert make_event().fingerprint == make_event().fingerprint
    assert make_event("e1").fingerprint != make_event("e2").fingerprint
    assert len(make_event().fingerprint) == 64


def test_non_utc_time_is_rejected() -> None:
    with pytest.raises(ValueError):
        Event(
            event_id="e1",
            shipment_id="s1",
            occurred_at=datetime(2026, 11, 7, 10, 0),
            status="created",
            tags=(),
        )


def test_changing_a_field_fails() -> None:
    event = make_event()
    with pytest.raises(FrozenInstanceError):
        event.status = "delivered"  # type: ignore[misc]


def test_event_has_no_dict_and_rejects_new_attributes() -> None:
    event = make_event()
    assert not hasattr(event, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        event.extra = 1  # type: ignore[attr-defined]
