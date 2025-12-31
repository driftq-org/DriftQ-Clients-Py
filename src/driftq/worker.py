from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Optional, TYPE_CHECKING
from .types import ConsumeMessage


if TYPE_CHECKING:
    from .client import DriftQ

# User-defined handler:
# - return None => Ack
# - raise/return Exception => Nack
StepHandler = Callable[[asyncio.AbstractEventLoop, asyncio.Future, ConsumeMessage], Awaitable[None]]  # not used
StepFunc = Callable[[ConsumeMessage], Awaitable[None]]


@dataclass
class WorkerConfig:
    client: "DriftQ"
    topic: str
    group: str
    owner: str
    lease_ms: int = 30_000
    concurrency: int = 1

    # Called for unexpected worker-level errors (stream errors, ack/nack failures, etc).
    on_error: Optional[Callable[[Exception], None]] = None

    # Optional: format nack reason from handler error.
    nack_reason: Optional[Callable[[ConsumeMessage, Exception], str]] = None

    # Cap reason size.
    max_nack_reason_bytes: int = 1024


class Worker:
    def __init__(self, cfg: WorkerConfig, handler: StepFunc) -> None:
        if not cfg.client:
            raise ValueError("client is required")
        if not cfg.topic or not cfg.group or not cfg.owner:
            raise ValueError("topic, group, owner are required")
        if cfg.lease_ms < 0:
            raise ValueError("lease_ms must be >= 0")
        if cfg.concurrency <= 0:
            cfg.concurrency = 1

        self._cfg = cfg
        self._handler = handler
        self._sem = asyncio.Semaphore(cfg.concurrency)

    async def run(self) -> None:
        cons = self._cfg.client.consumer(self._cfg.topic, self._cfg.group)

        pending: set[asyncio.Task] = set()

        try:
            async for msg in cons.stream(owner=self._cfg.owner, lease_ms=self._cfg.lease_ms):
                t = asyncio.create_task(self._handle_one(cons, msg))
                pending.add(t)
                t.add_done_callback(lambda x: pending.discard(x))
        except asyncio.CancelledError:
            # stop requested
            raise
        finally:
            # Ensure all in-flight message handlers finish before run() returns.
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _handle_one(self, cons, msg: ConsumeMessage) -> None:
        async with self._sem:
            # Per-message deadline: if envelope.deadline exists, enforce it.
            # We do best-effort: if deadline is in the past, handler will immediately timeout.
            try:
                await self._run_with_deadline(msg)
                await cons.ack(owner=self._cfg.owner, msg=msg)
            except Exception as e:
                reason = self._format_reason(msg, e)
                try:
                    await cons.nack(owner=self._cfg.owner, msg=msg, reason=reason)
                except Exception as ne:
                    self._report(ne)

    async def _run_with_deadline(self, msg: ConsumeMessage) -> None:
        dl = None
        if msg.envelope and msg.envelope.deadline:
            dl = msg.envelope.deadline

        if isinstance(dl, datetime):
            # Convert to seconds remaining
            now = dl.tzinfo and dl.__class__.now(dl.tzinfo) or datetime.utcnow()
            # if tz-aware, now() above matches; if naive, utcnow() approximates
            remaining = (dl - now).total_seconds()
            if remaining <= 0:
                raise TimeoutError("message deadline exceeded")

            await asyncio.wait_for(self._handler(msg), timeout=remaining)
        else:
            await self._handler(msg)

    def _format_reason(self, msg: ConsumeMessage, err: Exception) -> str:
        if self._cfg.nack_reason:
            s = self._cfg.nack_reason(msg, err) or ""
        else:
            s = str(err) if err else ""

        # Python's TimeoutError often stringifies to "" -> make it useful.
        if not s and err:
            s = err.__class__.__name__

        if not s:
            return ""

        b = s.encode("utf-8", errors="ignore")
        if len(b) <= self._cfg.max_nack_reason_bytes:
            return s
        return b[: self._cfg.max_nack_reason_bytes].decode("utf-8", errors="ignore")

    def _report(self, err: Exception) -> None:
        if self._cfg.on_error:
            self._cfg.on_error(err)
