from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Message:
    """A message delivered by the simulated network."""

    src: str
    dst: str
    payload: Any = None
    tag: str = "message"
    sent_at: float = 0.0
    deliver_at: float = 0.0
    seq: int = field(default=0)

    def delivered_at(self, time: float) -> "Message":
        """Return a copy annotated with its simulated delivery time."""
        return Message(
            src=self.src,
            dst=self.dst,
            payload=self.payload,
            tag=self.tag,
            sent_at=self.sent_at,
            deliver_at=time,
            seq=self.seq,
        )

