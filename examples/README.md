# DriftQ Python SDK Examples

These examples assume DriftQ-Core is running locally at:

- `http://localhost:8080`

## 0) Start DriftQ-Core

Verify Core is up:

- `GET http://localhost:8080/v1/healthz`

## 1) Create a topic

Before producing/consuming, create the `demo` topic in DriftQ-Core:

```powershell
curl -X POST "http://localhost:8080/v1/topics" -H "Content-Type: application/json" -d "{\"name\":\"demo\",\"partitions\":1}"
```

## 2) Run the worker example

This demonstrates the StepHandler/Worker loop:

- consume `/v1/consume` (NDJSON)
- handler runs
- ACK on success
- NACK on error

Run:

```powershell
python examples/worker_basic.py
```

### Troubleshooting

If it prints nothing, it usually means your **consumer group already has no pending messages**.

Quick fixes:
- run the producer again (send a new message), or
- change the group name in the example to something new (e.g. `demo-2`).
