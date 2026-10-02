from typing import ItemsView
from mbrb import *

from simpy_ds_extensions import Adversary
from simpy_ds import Process, Network
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
import simpy


# 让一个节点发起广播k条消息
def send_app_messages(process: MBRBProcess, messages: list[str], dt=1.0):
    for sn, value in enumerate(messages):
        process.mbrb_broadcast(value, sn)
        # 等待下一个dt再发送
        yield process.env.timeout(dt)


def main():

    n = 10  # number of nodes
    t = 0  # number of byzantine nodes
    d = 2  # power of message adversary

    all_nodes = [f"p{i}" for i in range(n)]
    byzz_nodes = set(all_nodes[-t:]) if t > 0 else set()

    private_keys = {pid: Ed25519PrivateKey.generate() for pid in all_nodes}
    public_keys = {pid: key.public_key() for pid, key in private_keys.items()}
    k = 5
    app_messages = [f"m{i}" for i in range(k)]

    env = simpy.Environment()
    network = Network(env, nodes=all_nodes, delay=1.0, drop_rate=0.0, seed=0)

    processes = {
        pid: MBRBProcess(
            pid=pid,
            network=network,
            private_key=private_keys[pid],
            public_keys=public_keys,
            t=t,
        )
        for pid in all_nodes
    }

    # 对所有节点注册run方法（run会等待接收bundle消息，所以不会在还没发送这个appmsg的时候处理）
    for pid, process in processes.items():
        env.process(process.run())

    # 启动发app-message的进程
    env.process(
        send_app_messages(process=processes["p0"], messages=app_messages, dt=1.0)
    )

    env.run()


if __name__ == "__main__":
    main()
