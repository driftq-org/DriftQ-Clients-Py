# DriftQ Python SDK (driftq)

A small **async** Python SDK for **DriftQ-Core HTTP v1**.

What you can do today:
- **Health check:** `GET /v1/healthz`
- **Produce:** `POST /v1/produce`
- **Consume (stream):** `GET /v1/consume` (NDJSON streaming)
- **Ack / Nack:** `POST /v1/ack`, `POST /v1/nack`
- **Worker loop:** `Worker` = consume → handler → ack/nack (bounded concurrency)

---

## Requirements
- Python **3.10+**
- DriftQ-Core running (default examples assume `http://localhost:8080`)

---

## Install (recommended: virtual env)

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e .
```

`pip install -e .` installs into the **currently active environment**. If you activate `.venv` first, it stays isolated.

---

## Quickstart

### 0) Verify Core is up

```py
import asyncio
from driftq import DriftQ, DriftQConfig

async def main():
    c = await DriftQ.dial(DriftQConfig(address="http://localhost:8080"))
    try:
        print(await c.healthz())
    finally:
        await c.close()

asyncio.run(main())
```

### 1) Create a topic in DriftQ-Core

The SDK can’t create topics yet (admin API is not implemented in this repo).
Create a topic via Core directly, for example:

```powershell
curl -X POST "http://localhost:8080/v1/topics" -H "Content-Type: application/json" -d "{\"name\":\"demo\",\"partitions\":1}"
```

### 2) Produce

```py
import asyncio
from driftq import DriftQ, DriftQConfig, Message, Envelope

async def main():
    c = await DriftQ.dial(DriftQConfig(address="http://localhost:8080"))
    try:
        await c.producer("demo").send(
            Message(value="hello", envelope=Envelope(idempotency_key="py-hello-1"))
        )
        print("produced")
    finally:
        await c.close()

asyncio.run(main())
```

### 3) Consume + Ack

```py
import asyncio
from driftq import DriftQ, DriftQConfig

async def main():
    c = await DriftQ.dial(DriftQConfig(address="http://localhost:8080"))
    try:
        cons = c.consumer("demo", "demo")
        async for msg in cons.stream(owner="py-worker-1", lease_ms=30_000):
            print("got:", msg.value)
            await cons.ack(owner="py-worker-1", msg=msg)
            break
    finally:
        await c.close()

asyncio.run(main())
```

### 4) Worker example

```powershell
python examples/worker_basic.py
```

---

## Retries, timeouts, tracing (what the “middleware” does)

### Retries
The HTTP client retries on transient statuses: **429, 500, 502, 503, 504**.
- **Safe methods** (GET/HEAD/OPTIONS) can retry.
- **POST** retries only happen when an **Idempotency-Key** is present.

In this SDK, you get that for produce by setting:
- `Envelope(idempotency_key="...")`

The client also respects `Retry-After` when a server returns it.

### Timeouts
- Non-streaming calls use `DriftQConfig.timeout_s` by default.
- Streaming (`/v1/consume`) defaults to **no timeout**; the stream lifetime is controlled by cancelling the task / context.

### Tracing context propagation
If OpenTelemetry context exists, the client injects W3C trace headers (like `traceparent`) into outgoing requests.
Disable it via:

```py
from driftq import DriftQConfig
from driftq.http_client import TracingConfig

cfg = DriftQConfig(address="http://localhost:8080", tracing=TracingConfig(disable=True))
```

---

## Tests

```powershell
python -m unittest discover -s tests -p "test_*.py" -q
```

---

## License
Apache-2.0 (see `LICENSE`)
