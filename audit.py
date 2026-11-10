
from datetime import UTC, datetime
from typing import Self


class AwareTimestamp:
   def __set_name__(self, owner: type, name: str) -> None:
      self.time_stamp_at = "_" + name

   def __set__(self, audit: object, date: datetime) -> None:
      if date.utcoffset() is None:
         raise ValueError("must be timezone-aware")
      
      setattr(audit, self.time_stamp_at, date.astimezone(UTC))

   def __get__(self, audit: object | None, owner: type) -> Self | datetime:
      if audit is None:
         return self

      value: datetime = getattr(audit, self.time_stamp_at)
      return value


class RunAudit:
   started_at = AwareTimestamp()