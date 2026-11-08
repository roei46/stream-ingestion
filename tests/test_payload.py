import json

import pytest
from pydantic import ValidationError

from models import EventPayload, Status, to_domain

VALID = {
    "event_id": "e1",
    "shipment_id": "s1",
    "occurred_at": "2026-11-08T12:00:00+02:00",
    "status": "delivered",
    "schema_version": 1,
    "tags": ["a"],
}


def test_entry_point_is_model_validate_json() -> None:
    payload = EventPayload.model_validate_json(json.dumps(VALID))
    assert payload.event_id == "e1"


def test_strict_dates_differ_between_json_and_python() -> None:
    raw = json.dumps(VALID)
    payload = EventPayload.model_validate_json(raw, strict=True)
    assert payload.occurred_at.isoformat() == "2026-11-08T10:00:00+00:00"

    with pytest.raises(ValidationError) as error:
        EventPayload.model_validate(json.loads(raw), strict=True)
    failed_fields = [e["loc"][0] for e in error.value.errors()]
    assert "occurred_at" in failed_fields


def make_line(changes: dict[str, object]) -> str:
    return json.dumps(VALID | changes)


# Example 1
def test_valid_delivered_event_is_accepted_and_utc() -> None:
    event = to_domain(EventPayload.model_validate_json(make_line({})))
    assert event.status is Status.DELIVERED
    assert event.tags == ("a",)
    assert event.occurred_at.isoformat() == "2026-11-08T10:00:00+00:00"


# Example 2
@pytest.mark.parametrize(
    "changes",
    [
        {"color": "red"},
        {"occurred_at": "2026-11-08T10:00:00"},
        {"event_id": ""},
        {"status": "lost"},
    ],
)
def test_bad_lines_are_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        EventPayload.model_validate_json(make_line(changes))


# Example 3
def test_delivered_needs_a_tag_but_in_transit_does_not() -> None:
    with pytest.raises(ValidationError):
        EventPayload.model_validate_json(make_line({"tags": []}))
    payload = EventPayload.model_validate_json(make_line({"status": "in_transit", "tags": []}))
    assert payload.tags == []


# Example 4
def test_changing_payload_tags_does_not_change_event() -> None:
    payload = EventPayload.model_validate_json(make_line({}))
    event = to_domain(payload)
    payload.tags.append("changed")
    assert event.tags == ("a",)
