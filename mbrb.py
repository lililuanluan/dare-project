from dataclasses import dataclass
import simpy
from simpy_ds_extensions import Adversary
from simpy_ds import Process, Network
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature
from collections import defaultdict  # 初始化后，不存在的key会自动创建一个默认值


n = 10  # number of nodes
t = 0  # number of byzantine nodes
d = 2  # power of message adversary


nodes = [f"p{i}" for i in range(n)]

private_keys = {}
public_keys = {}
for i, pi in enumerate(nodes):
    sk = Ed25519PrivateKey.generate()
    pk = sk.public_key()
    private_keys[pi] = sk
    public_keys[pi] = pk

# print(private_keys)
# print(public_keys)

k = 5
app_messages = [f"m{i}" for i in range(k)]
print(app_messages)


pidT = str
msgT = str
seqT = int
# 这里理论上可以通过自己保存的所有公钥一个一个试，但是这里为了实现方便把pid也放进去
sigT = tuple[pidT, bytes]


@dataclass
class Bundle:
    content: msgT  # app-message
    sequence_number: seqT  # 节点每发送一个app-message，例如 "hello", "world"，这些消息在app层面只广播一次，但是在网络层可能转发多次，每发送一个app-message，需要将sn+1（避免消息内容一样的情况），而sn才是消息的唯一标识符，后面关于签名的逻辑都是 (sn, j) 相关的，消息m只是payload
    emitter: pidT  # 注意这是原始发送者，不是转发者
    signatures: frozenset[sigT]  # 避免引用本地set


class MBRBProcess(Process):

    def __init__(
        self,
        pid: pidT,
        network: Network,
        private_key: Ed25519PrivateKey,
        public_keys: dict[pidT, Ed25519PublicKey],
        t: int,
    ) -> None:
        super().__init__(pid, network)
        # (value, sequence_number, sender) -> signatures
        self.valid_signatures: defaultdict[tuple[msgT, seqT, pidT], set[sigT]] = (
            defaultdict(set)
        )
        # (sender, sequence_number) -> value 注意，对每个(id,seq)只能发送一个m，所以也只能签名一个m，防止拜占庭
        self.signed_values: defaultdict[tuple[pidT, seqT], msgT] = defaultdict(msgT)
        # {(sender, sequence_number)}
        self.delivered: defaultdict[tuple[pidT, seqT], msgT] = defaultdict()
        self.sk = private_key
        self.pks = public_keys
        self.n = len(public_keys)
        self.t = t

    def mbrb_deliver(self, m: msgT, sn: seqT, j: pidT):
        self.delivered[(j, sn)] = m

    def serialize_msg(self, m: msgT, sn: seqT, pid: pidT):

        msg = (m, sn, pid)
        msg_bytes = repr(msg).encode("utf-8")
        return msg_bytes

    def sign(self, m: msgT, sn: seqT, pid: pidT) -> bytes:
        # 注意，要对发送者签名，而不是自己的pid
        msg_bytes = self.serialize_msg(m, sn, pid)
        sig = self.sk.sign(msg_bytes)
        # 更新本地状态
        self.valid_signatures[(m, sn, pid)].add((self.pid, sig))
        self.signed_values[(pid, sn)] = (
            m  # 注意，这里存储的是m，防止对一个原始发送者同一个sn的不同m签名（类似ledger,seq的检查）
        )
        return sig

    def sig_valid(self, m: msgT, sn: seqT, j: pidT, sigpid: sigT):
        # 注意这里检查用的是签名者signer的公钥，而不是原始发送者j的
        signer, sig = sigpid
        try:
            self.pks[signer].verify(sig, self.serialize_msg(m, sn, j))
            return True
        except InvalidSignature:
            return False

    def get_valid_signatures(self, bundle: Bundle) -> list[sigT]:
        sigs = bundle.signatures
        sn, j, m = bundle.sequence_number, bundle.emitter, bundle.content
        valid_sigs = []
        for sig in sigs:
            if self.sig_valid(m, sn, j, sig):
                valid_sigs.append(sig)
        return valid_sigs

    def get_signatures_by_id(self, sigs: list[sigT], pid: pidT) -> list[sigT]:
        return [(id, s) for (id, s) in sigs if id == pid]

    def mbrb_broadcast(self, m: msgT, sn: int):
        sig = self.sign(m, sn, self.pid)
        key = (m, sn, self.pid)
        bundle = Bundle(
            content=m,
            sequence_number=sn,
            emitter=self.pid,
            signatures=frozenset(self.valid_signatures[key]),
        )
        self.broadcast(payload=bundle)

    def on_recv_bundle(self, bundle: Bundle):
        sn, j, m = bundle.sequence_number, bundle.emitter, bundle.content
        valid_sigs = self.get_valid_signatures(bundle)
        if ((j, sn) not in self.delivered) and len(
            self.get_signatures_by_id(valid_sigs, j)  # 注意这里获取的是pj的签名
        ):
            self.valid_signatures[(m, sn, j)].update(valid_sigs)
            if (j, sn) not in self.signed_values:
                self.sign(m, sn, j)
                all_sigs = self.valid_signatures[(m, sn, j)]
                nb = Bundle(
                    content=m,
                    sequence_number=sn,
                    emitter=j,
                    signatures=frozenset(all_sigs),
                )
                self.broadcast(nb)

            # quorum 检查
            all_sigs = self.valid_signatures[(m, sn, j)]
            if len(all_sigs) > (self.n + self.t) / 2:
                nb = Bundle(
                    content=m,
                    sequence_number=sn,
                    emitter=j,
                    signatures=frozenset(all_sigs),
                )
                self.broadcast(nb)
                self.mbrb_deliver(m, sn, j)
