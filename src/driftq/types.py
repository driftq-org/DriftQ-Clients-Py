from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Union

from .http_client import RetryConfig, TracingConfig

Headers = Dict[str, str]
Value = Union[str, bytes]


@dataclass
class RetryPolicy:
    max_attempts: int = 0
    backoff_ms: int = 0
    max_backoff_ms: int = 0

    def to_dict(self) -> dict:
        out: dict = {}
        if self.max_attempts:
            out["max_attempts"] = self.max_attempts
        if self.backoff_ms:
            out["backoff_ms"] = self.backoff_ms
        if self.max_backoff_ms:
            out["max_backoff_ms"] = self.max_backoff_ms
        return out

    @classmethod
    def from_dict(cls, d: dict) -> "RetryPolicy":
        return cls(
            max_attempts=int(d.get("max_attempts", 0) or 0),
            backoff_ms=int(d.get("backoff_ms", 0) or 0),
            max_backoff_ms=int(d.get("max_backoff_ms", 0) or 0),
        )


@dataclass
class Envelope:
    run_id: str = ""
    step_id: str = ""
    parent_step_id: str = ""
    tenant_id: str = ""
    idempotency_key: str = ""
    target_topic: str = ""
    deadline: Optional[datetime] = None
    partition_override: Optional[int] = None
    retry_policy: Optional[RetryPolicy] = None

    def to_dict(self) -> dict:
        out: dict = {}

        for k in (
            "run_id",
            "step_id",
            "parent_step_id",
            "tenant_id",
            "idempotency_key",
            "target_topic",
        ):
            v = getattr(self, k)
            if v:
                out[k] = v

        if self.deadline is not None:
            dl = self.deadline
            if dl.tzinfo is None:
                dl = dl.replace(tzinfo=timezone.utc)
            out["deadline"] = dl.isoformat()

        if self.partition_override is not None:
            out["partition_override"] = self.partition_override

        if self.retry_policy is not None:
            rp = self.retry_policy.to_dict()
            if rp:
                out["retry_policy"] = rp

        return out

    @classmethod
    def from_dict(cls, d: dict) -> "Envelope":
        dl = d.get("deadline")
        deadline_dt: Optional[datetime] = None
        if isinstance(dl, str) and dl:
            try:
                deadline_dt = datetime.fromisoformat(dl)
                if deadline_dt.tzinfo is None:
                    deadline_dt = deadline_dt.replace(tzinfo=timezone.utc)
            except Exception:
                deadline_dt = None

        rp = d.get("retry_policy")
        retry_policy = RetryPolicy.from_dict(rp) if isinstance(rp, dict) else None

        po = d.get("partition_override")
        partition_override = int(po) if po is not None else None

        return cls(
            run_id=str(d.get("run_id", "") or ""),
            step_id=str(d.get("step_id", "") or ""),
            parent_step_id=str(d.get("parent_step_id", "") or ""),
            tenant_id=str(d.get("tenant_id", "") or ""),
            idempotency_key=str(d.get("idempotency_key", "") or ""),
            target_topic=str(d.get("target_topic", "") or ""),
            deadline=deadline_dt,
            partition_override=partition_override,
            retry_policy=retry_policy,
        )


@dataclass
class Message:
    key: str = ""
    value: Value = ""
    headers: Optional[Headers] = None
    envelope: Optional[Envelope] = None


@dataclass
class Routing:
    label: str = ""
    meta: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "Routing":
        return cls(
            label=str(d.get("label", "") or ""),
            meta=dict(d.get("meta", {}) or {}),
        )


@dataclass
class ConsumeMessage:
    partition: int
    offset: int
    attempts: int
    key: str
    value: str
    last_error: str = ""
    routing: Optional[Routing] = None
    envelope: Optional[Envelope] = None

    @classmethod
    def from_dict(cls, d: dict) -> "ConsumeMessage":
        routing = Routing.from_dict(d["routing"]) if isinstance(d.get("routing"), dict) else None
        envelope = Envelope.from_dict(d["envelope"]) if isinstance(d.get("envelope"), dict) else None

        return cls(
            partition=int(d.get("partition", 0)),
            offset=int(d.get("offset", 0)),
            attempts=int(d.get("attempts", 0)),
            key=str(d.get("key", "") or ""),
            value=str(d.get("value", "") or ""),
            last_error=str(d.get("last_error", "") or ""),
            routing=routing,
            envelope=envelope,
        )


@dataclass
class ConsumeOptions:
    topic: str
    group: str
    owner: str
    lease_ms: int = 0


@dataclass
class DriftQConfig:
    # HTTP base URL (ex: "http://localhost:8080")
    address: str

    # Default request timeout for non-streaming calls.
    timeout_s: float = 10.0

    # Retry policy for transient failures.
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Trace context propagation (inject trace headers when OTel is present).
    tracing: TracingConfig = field(default_factory=TracingConfig)

    # User-Agent header for all requests.
    user_agent: str = "driftq-py/0.1.0"
