from __future__ import annotations

from collections.abc import Iterable

import simpy

from simpy_ds import Message, Network
from simpy_ds.network import Delay


class DuplicatingNetwork(Network):
    """Lossy asynchronous network that may deliver a second copy.

    This is intentionally outside the core package. The base `Network` models
    at-most-once delivery: a message may be dropped, or delivered once. This
    extension models channels where a delivered message may also be duplicated.
    The duplicate is inserted immediately after the original, at the same
    simulated time.
    """

    def __init__(
        self,
        env: simpy.Environment,
        nodes: Iterable[str] = (),
        *,
        delay: Delay = 1.0,
        drop_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        super().__init__(
            env,
            nodes,
            delay=delay,
            drop_rate=drop_rate,
            seed=seed,
        )
        self.duplicate_rate = self._check_probability(
            "duplicate_rate",
            duplicate_rate,
        )

    def _after_delivery(self, message: Message):
        """Optionally insert a second copy after the base delivery."""
        if self.random.random() >= self.duplicate_rate:
            return

        duplicate = Message(
            src=message.src,
            dst=message.dst,
            payload=message.payload,
            tag=message.tag,
            sent_at=message.sent_at,
            deliver_at=self.env.now,
            seq=self._take_seq(),
        )
        yield self._inboxes[message.dst].put(duplicate)
        self._record("duplicate", duplicate)
