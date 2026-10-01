from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from typing import Any

import simpy

from .message import Message

Delay = float | Callable[[Message], float]
Predicate = Callable[[Message], bool]


class Network:
    """Message-passing network built from SimPy events and inboxes.

    Each node has one inbox. Sending a message starts a SimPy process that waits
    for the configured delay and then inserts the message in the destination
    inbox. A message is either dropped or delivered exactly once; extensions
    can add other channel behavior by overriding ``_after_delivery``.
    """

    def __init__(
        self,
        env: simpy.Environment,
        nodes: Iterable[str] = (),
        *,
        delay: Delay = 1.0,
        drop_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.env = env
        self.delay = delay
        self.drop_rate = self._check_probability("drop_rate", drop_rate)
        self.random = random.Random(seed)
        self.trace: list[dict[str, Any]] = []

        self._inboxes: dict[str, simpy.FilterStore] = {}
        self._next_seq = 1

        for node in nodes:
            self.add_node(node)

    @property
    def nodes(self) -> tuple[str, ...]:
        """Return the known node identifiers."""
        return tuple(self._inboxes)

    def add_node(self, node: str) -> None:
        """Add a node and its inbox if it does not already exist."""
        if node not in self._inboxes:
            self._inboxes[node] = simpy.FilterStore(self.env)

    def send(
        self,
        src: str,
        dst: str,
        payload: Any = None,
        *,
        tag: str = "message",
    ) -> simpy.Process:
        """Schedule one message delivery."""
        self._require_node(src)
        self._require_node(dst)

        message = Message(
            src=src,
            dst=dst,
            payload=payload,
            tag=tag,
            sent_at=self.env.now,
            seq=self._take_seq(),
        )
        self._record("send", message)
        return self.env.process(self._deliver(message))

    def broadcast(
        self,
        src: str,
        dsts: Iterable[str] | None = None,
        payload: Any = None,
        *,
        tag: str = "message",
        include_self: bool = False,
    ) -> simpy.AllOf:
        """Send the same payload to several destinations.

        If `dsts` is omitted, the message is sent to all known nodes except
        `src`, unless `include_self` is true.
        """
        self._require_node(src)
        destinations = self._broadcast_destinations(src, dsts, include_self)
        sends = [
            self.send(src, dst, payload, tag=tag)
            for dst in destinations
        ]
        return self.env.all_of(sends)

    def recv(
        self,
        node: str,
        predicate: Predicate | None = None,
    ) -> simpy.FilterStoreGet:
        """Wait for the next message accepted by `predicate`."""
        self._require_node(node)
        if predicate is None:
            predicate = lambda _message: True
        return self._inboxes[node].get(predicate)

    def inbox_size(self, node: str) -> int:
        """Return the number of queued messages in a node inbox."""
        self._require_node(node)
        return len(self._inboxes[node].items)

    def _deliver(self, message: Message):
        if self.random.random() < self.drop_rate:
            self._record("drop", message)
            return

        yield self.env.timeout(self._delay_for(message))

        delivered = message.delivered_at(self.env.now)
        yield self._inboxes[message.dst].put(delivered)
        self._record("deliver", delivered)
        yield from self._after_delivery(delivered)

    def _after_delivery(self, message: Message):
        """Extension hook called once after the normal delivery.

        Override this generator in a network extension to schedule any extra
        behavior. It receives the delivered message and may yield SimPy events.
        The base network deliberately has nothing more to do.
        """
        yield from ()

    def _broadcast_destinations(
        self,
        src: str,
        dsts: Iterable[str] | None,
        include_self: bool,
    ) -> list[str]:
        destinations = list(self.nodes if dsts is None else dsts)
        for dst in destinations:
            self._require_node(dst)
        if include_self:
            return destinations
        return [dst for dst in destinations if dst != src]

    def _delay_for(self, message: Message) -> float:
        delay = self.delay(message) if callable(self.delay) else self.delay
        if delay < 0:
            raise ValueError("network delay must be non-negative")
        return delay

    def _take_seq(self) -> int:
        seq = self._next_seq
        self._next_seq += 1
        return seq

    def _require_node(self, node: str) -> None:
        if node not in self._inboxes:
            raise KeyError(f"unknown node: {node!r}")

    def _record(self, event: str, message: Message) -> None:
        self.trace.append(
            {
                "time": self.env.now,
                "event": event,
                "seq": message.seq,
                "src": message.src,
                "dst": message.dst,
                "tag": message.tag,
                "payload": message.payload,
            }
        )

    @staticmethod
    def _check_probability(name: str, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} must be between 0 and 1")
        return value
