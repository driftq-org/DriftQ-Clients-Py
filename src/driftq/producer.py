from __future__ import annotations
from typing import TYPE_CHECKING
from .types import Message


if TYPE_CHECKING:
    from .client import DriftQ

class Producer:
    def __init__(self, topic: str, client: DriftQ) -> None:
        self._topic = topic
        self._client = client

    async def send(self, msg: Message) -> None:
        await self._client._send(self._topic, msg)
