from __future__ import annotations
from typing import Awaitable, Callable
from .types import Message
from .client import DriftQ

Handler = Callable[[Message], Awaitable[None] | None]

class Consumer:
    def __init__(self, topic: str, group: str, client: DriftQ) -> None:
        self._topic = topic
        self._group = group
        self._client = client

    async def start(self, handler: Handler):
        async def stop() -> None:
            return None
        _ = handler
        return stop
