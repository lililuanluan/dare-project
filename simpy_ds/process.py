from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .message import Message
from .network import Network, Predicate


class Process:
    """Convenience wrapper around one simulated distributed process."""

    def __init__(self, pid: str, network: Network) -> None:
        self.pid = pid
        self.network = network
        self.env = network.env
        self.network.add_node(pid)

    def send(
        self,
        dst: str,
        payload: Any = None,
        *,
        tag: str = "message",
    ):
        """Send one message from this process."""
        return self.network.send(self.pid, dst, payload, tag=tag)

    def broadcast(
        self,
        payload: Any = None,
        *,
        tag: str = "message",
        include_self: bool = False,
    ):
        """Broadcast from this process to the network's known nodes."""
        return self.network.broadcast(
            self.pid,
            payload=payload,
            tag=tag,
            include_self=include_self,
        )

    def recv(self, predicate: Predicate | None = None):
        """Receive the next matching message for this process."""
        return self.network.recv(self.pid, predicate)

    def recv_tag(self, tag: str):
        """Receive the next message with the given tag."""
        return self.recv(lambda message: message.tag == tag)

    def timeout(self, delay: float):
        """Wait for simulated time to pass."""
        return self.env.timeout(delay)

    def every(
        self,
        delay: float,
        callback: Callable[["Process"], Any],
    ):
        """Run `callback` periodically as a SimPy process."""
        while True:
            yield self.timeout(delay)
            callback(self)

