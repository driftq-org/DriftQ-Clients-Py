from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from .admin import Admin
from .consumer import Consumer
from .http_client import HttpClient
from .producer import Producer
from .types import ConsumeOptions, ConsumeMessage, DriftQConfig, Message
import json
from typing import AsyncIterator

@dataclass
class _State:
    connected: bool = False

class DriftQ:
    def __init__(self, cfg: DriftQConfig, http: HttpClient) -> None:
        self._cfg = cfg
        self._http = http
        self._st = _State(connected=True)

    @classmethod
    async def dial(cls, cfg: DriftQConfig) -> "DriftQ":
        http = HttpClient(
            cfg.address,
            timeout_s=cfg.timeout_s,
            retry=cfg.retry,
            tracing=cfg.tracing,
            user_agent=cfg.user_agent,
        )
        return cls(cfg, http)

    async def close(self) -> None:
        if self._st.connected:
            await self._http.aclose()
            self._st.connected = False

    def producer(self, topic: str) -> Producer:
        return Producer(topic, self)

    def consumer(self, topic: str, group: str) -> Consumer:
        return Consumer(topic, group, self)

    def admin(self) -> Admin:
        return Admin(self)

    async def healthz(self) -> Any:
        return await self._http.request_json("GET", "/v1/healthz")

    async def consume_stream(self, opt: ConsumeOptions) -> AsyncIterator[ConsumeMessage]:
        topic = (opt.topic or "").strip()
        group = (opt.group or "").strip()
        owner = (opt.owner or "").strip()

        if not topic or not group or not owner:
            raise ValueError("topic, group, and owner are required")
        if opt.lease_ms < 0:
            raise ValueError("lease_ms must be >= 0")

        params = {"topic": topic, "group": group, "owner": owner}
        if opt.lease_ms > 0:
            params["lease_ms"] = str(int(opt.lease_ms))

        headers = {"Accept": "application/x-ndjson"}

        async for line in self._http.stream_lines("GET", "/v1/consume", params=params, headers=headers):
            d = json.loads(line)
            if isinstance(d, dict):
                yield ConsumeMessage.from_dict(d)

    async def ack(self, *, topic: str, group: str, owner: str, partition: int, offset: int) -> None:
        payload = {
            "topic": topic,
            "group": group,
            "owner": owner,
            "partition": int(partition),
            "offset": int(offset),
        }
        await self._http.request_json("POST", "/v1/ack", json=payload)

    async def nack(
        self,
        *,
        topic: str,
        group: str,
        owner: str,
        partition: int,
        offset: int,
        reason: str = ""
    ) -> None:
        payload = {
            "topic": topic,
            "group": group,
            "owner": owner,
            "partition": int(partition),
            "offset": int(offset),
        }
        if reason:
            payload["reason"] = reason
        await self._http.request_json("POST", "/v1/nack", json=payload)

    async def _send(self, topic: str, msg: Message) -> None:
        # DriftQ-Core expects "value" as a JSON string (same as Go SDK).
        if isinstance(msg.value, (bytes, bytearray, memoryview)):
            value_str = bytes(msg.value).decode("utf-8")  # strict; fail fast if not UTF-8
        else:
            value_str = str(msg.value)

        payload: dict = {"topic": topic, "value": value_str}
        if msg.key:
            payload["key"] = msg.key

        hdrs: dict[str, str] = {}

        if msg.envelope is not None:
            env = msg.envelope.to_dict()
            if env:
                payload["envelope"] = env

            # Make POST safe to retry on transient failures.
            if msg.envelope.idempotency_key:
                hdrs["Idempotency-Key"] = msg.envelope.idempotency_key

        await self._http.request_json("POST", "/v1/produce", json=payload, headers=hdrs)
