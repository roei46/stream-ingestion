import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    event_id: str
    shipment_id: str
    occurred_at: datetime
    status: str
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
            f"status={self.status!r}, fingerprint={self.fingerprint[:12]}...)"
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
