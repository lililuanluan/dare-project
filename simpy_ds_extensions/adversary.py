from __future__ import annotations

from collections.abc import Iterable

import simpy

from simpy_ds import Message, Network
from simpy_ds.network import Delay


class Adversary(Network):

    def __init__(
        self,
        env: simpy.Environment,
        d: int,  # at most d correct nodes do not receive the broadcasted message
        nodes: Iterable[str] = (),
        correct_nodes: Iterable[str] = (),
        *,
        delay: Delay = 1.0,
        drop_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        super().__init__(
            env,
            nodes,
            delay=delay,
            drop_rate=drop_rate,
            seed=seed,
        )
        if d < 0 or d >= len(correct_nodes):
            raise ValueError(f"d = {d}, invalid value")
        if not set(correct_nodes).issubset(set(nodes)):
            raise ValueError(
                f"correct nodes {correct_nodes} is not a subset of all nodes"
            )
        self.d = d
        self.correct_nodes = correct_nodes

    def broadcast(
        self,
        src: str,
        dsts: Iterable[str] | None = None,
        payload: Any = None,
        *,
        tag: str = "message",
        include_self: bool = False,
    ) -> simpy.AllOf:
        k = self.random.randint(0, min(self.d, len(dsts)))  # number of omission
        true_dsts = self.random.sample(dsts, len(dsts) - k)
        return super().broadcast(
            src, true_dsts, payload, tag=tag, include_self=include_self
        )
