from __future__ import annotations
from dataclasses import dataclass
from .types import DriftQConfig, Message
from .producer import Producer
from .consumer import Consumer
from .admin import Admin

@dataclass
class _State:
    connected: bool = False

class DriftQ:
    def __init__(self, cfg: DriftQConfig) -> None:
        self._cfg = cfg
        self._st = _State(connected=True)

    @classmethod
    async def dial(cls, cfg: DriftQConfig) -> DriftQ:
        return cls(cfg)

    async def close(self) -> None:
        self._st.connected = False

    def producer(self, topic: str) -> Producer:
        return Producer(topic, self)

    def consumer(self, topic: str, group: str) -> Consumer:
        return Consumer(topic, group, self)

    def admin(self) -> Admin:
        return Admin(self)

    async def _send(self, _topic: str, _msg: Message) -> None:
        return None
