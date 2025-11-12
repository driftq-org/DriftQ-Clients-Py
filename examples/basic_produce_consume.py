import asyncio
from driftq import DriftQ, DriftQConfig, Message

async def main():
    client = await DriftQ.dial(DriftQConfig(address="localhost:9090"))
    stop = await client.consumer("orders", "orders-workers").start(lambda m: print("consumed (stub):", m.key, m.value))
    await client.producer("orders").send(Message(key="k", value=b"hello"))
    await stop()
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
