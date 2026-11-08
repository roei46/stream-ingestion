import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)


class Status(StrEnum):
    CREATED = "created"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
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


@dataclass
class BatchStats:
    read: int = 0
    accepted: int = 0
    rejected: int = 0
    committed: int = 0
    failed_records: int = 0
    failed_batches: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(order=True)
class EventCursor:
    sequence: int
    event_id: str = field(compare=False)
    line_number: int = field(compare=False)


class EventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=64)
    shipment_id: str = Field(min_length=1, max_length=64)
    occurred_at: AwareDatetime
    status: Status
    schema_version: StrictInt = Field(ge=1, le=1)
    tags: list[Annotated[str, Field(max_length=32)]] = Field(
        default_factory=list, max_length=10
    )

    @field_validator("occurred_at")
    @classmethod
    def to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def delivered_needs_tag(self) -> Self:
        if self.status is Status.DELIVERED and not self.tags:
            raise ValueError("delivered event must have at least one tag")
        return self


def to_domain(payload: EventPayload) -> Event:
    return Event(
        event_id=payload.event_id,
        shipment_id=payload.shipment_id,
        occurred_at=payload.occurred_at,
        status=payload.status,
        tags=tuple(payload.tags),
    )


class RejectionCategory(StrEnum):
    LINE_TOO_LONG = "line_too_long"
    INVALID_JSON = "invalid_json"
    VALIDATION = "validation"


MAX_MESSAGE_LENGTH = 200


@dataclass(frozen=True, slots=True, kw_only=True)
class Rejection:
    line_number: int
    category: RejectionCategory
    message: str

    def __post_init__(self) -> None:
        if len(self.message) > MAX_MESSAGE_LENGTH:
            object.__setattr__(self, "message", self.message[:MAX_MESSAGE_LENGTH])
