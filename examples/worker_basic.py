import asyncio
from driftq import DriftQ, DriftQConfig, Worker, WorkerConfig, Message, Envelope


async def main() -> None:
    c = await DriftQ.dial(DriftQConfig(address="http://localhost:8080"))

    async def handler(msg):
        print("handler got:", msg.value)
        # Returning normally => ACK

    w = Worker(
        WorkerConfig(
            client=c,
            topic="demo",
            group="demo",
            owner="py-worker-1",
            concurrency=2,
            lease_ms=30_000,
        ),
        handler,
    )

    # Produce one message so the worker has work.
    await c.producer("demo").send(
        Message(value="hello from python worker", envelope=Envelope(idempotency_key="py-worker-basic-1"))
    )

    task = asyncio.create_task(w.run())
    await asyncio.sleep(2.0)  # let it process
    task.cancel()

    await c.close()


if __name__ == "__main__":
    asyncio.run(main())
