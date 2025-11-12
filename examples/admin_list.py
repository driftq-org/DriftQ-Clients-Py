import asyncio
from driftq import DriftQ, DriftQConfig

async def main():
    client = await DriftQ.dial(DriftQConfig(address="localhost:9090"))
    topics = await client.admin().list_topics()
    print("topics:", topics)
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
