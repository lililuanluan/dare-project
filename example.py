import simpy
from simpy_ds import Network, Process


def print_message(now, message):
    print(
        f"{now:5.2f}  "
        f"{message.src:<5} -> {message.dst:<5}  "
        f"{message.tag:<5}  {message.payload}"
    )


def alice(process: Process):
    for n in range(1, 4):
        yield process.send("bob", {"n": n}, tag="ping")  # {"n", n} 是一个payload
        reply = yield process.recv_tag("pong")
        print_message(process.env.now, reply)

    yield process.send("bob", {"received": 3}, tag="done")


def bob(process: Process):
    while True:
        message = yield process.recv()
        print_message(process.env.now, message)
        if message.tag == "done":
            break

        yield process.send(message.src, message.payload, tag="pong")


def main():
    env = simpy.Environment()
    network = Network(env, nodes=["alice", "bob"], delay=1.0)
    env.process(alice(Process("alice", network)))
    env.process(bob(Process("bob", network)))

    env.run()


if __name__ == "__main__":
    main()
