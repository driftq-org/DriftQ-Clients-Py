import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from driftq.types import ConsumeMessage, Envelope
from driftq.worker import Worker, WorkerConfig


class FakeConsumer:
    def __init__(self, msgs):
        self._msgs = list(msgs)
        self.acked = []
        self.nacked = []

    async def stream(self, *, owner: str, lease_ms: int = 0):
        for m in self._msgs:
            yield m

    async def ack(self, *, owner: str, msg: ConsumeMessage) -> None:
        self.acked.append((owner, msg.partition, msg.offset))

    async def nack(self, *, owner: str, msg: ConsumeMessage, reason: str = "") -> None:
        self.nacked.append((owner, msg.partition, msg.offset, reason))


class FakeClient:
    def __init__(self, cons: FakeConsumer):
        self._cons = cons

    def consumer(self, topic: str, group: str):
        return self._cons


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_acks_on_success(self):
        cons = FakeConsumer([
            ConsumeMessage(partition=0, offset=1, attempts=1, key="k", value="v"),
        ])
        client = FakeClient(cons)

        async def handler(msg):
            return None

        w = Worker(
            WorkerConfig(client=client, topic="demo", group="demo", owner="py-1", concurrency=2),
            handler,
        )

        await w.run()

        self.assertEqual(cons.acked, [("py-1", 0, 1)])
        self.assertEqual(cons.nacked, [])

    async def test_worker_nacks_on_handler_error(self):
        cons = FakeConsumer([
            ConsumeMessage(partition=2, offset=99, attempts=1, key="k", value="v"),
        ])
        client = FakeClient(cons)

        async def handler(msg):
            raise ValueError("boom")

        w = Worker(
            WorkerConfig(client=client, topic="demo", group="demo", owner="py-1", concurrency=1),
            handler,
        )

        await w.run()

        self.assertEqual(cons.acked, [])
        self.assertEqual(len(cons.nacked), 1)
        owner, p, off, reason = cons.nacked[0]
        self.assertEqual((owner, p, off), ("py-1", 2, 99))
        self.assertIn("boom", reason)

    async def test_worker_honors_envelope_deadline(self):
        deadline = datetime.now(timezone.utc) + timedelta(milliseconds=100)

        cons = FakeConsumer([
            ConsumeMessage(
                partition=0,
                offset=1,
                attempts=1,
                key="k",
                value="v",
                envelope=Envelope(deadline=deadline),
            ),
        ])
        client = FakeClient(cons)

        async def handler(msg):
            await asyncio.sleep(0.2)  # exceed deadline

        w = Worker(
            WorkerConfig(client=client, topic="demo", group="demo", owner="py-1", concurrency=1),
            handler,
        )

        await w.run()

        self.assertEqual(cons.acked, [])
        self.assertEqual(len(cons.nacked), 1)
        _, _, _, reason = cons.nacked[0]
        self.assertTrue(reason)  # should not be empty
