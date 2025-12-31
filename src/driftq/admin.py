from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .client import DriftQ

@dataclass
class Topic:
    name: str
    partitions: int
    compacted: bool = False

class Admin:
    def __init__(self, client: DriftQ) -> None:
        self._client = client

    async def list_topics(self) -> list[Topic]:
        return []

    async def create_topic(self, topic: Topic) -> None:
        _ = topic
        return None
