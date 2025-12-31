from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator

from .types import ConsumeOptions, ConsumeMessage

if TYPE_CHECKING:
    from .client import DriftQ


class Consumer:
    def __init__(self, topic: str, group: str, client: "DriftQ") -> None:
        self._topic = topic
        self._group = group
        self._client = client

    async def stream(self, *, owner: str, lease_ms: int = 0) -> AsyncIterator[ConsumeMessage]:
        opt = ConsumeOptions(topic=self._topic, group=self._group, owner=owner, lease_ms=lease_ms)
        async for m in self._client.consume_stream(opt):
            yield m

    async def ack(self, *, owner: str, msg: ConsumeMessage) -> None:
        await self._client.ack(
            topic=self._topic,
            group=self._group,
            owner=owner,
            partition=msg.partition,
            offset=msg.offset,
        )

    async def nack(self, *, owner: str, msg: ConsumeMessage, reason: str = "") -> None:
        await self._client.nack(
            topic=self._topic,
            group=self._group,
            owner=owner,
            partition=msg.partition,
            offset=msg.offset,
            reason=reason,
        )
