from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Mapping, Optional

import httpx

_DEFAULT = object()

# OpenTelemetry is optional at runtime; if not present, tracing becomes a no-op
try:
    from opentelemetry.propagate import inject as otel_inject
except Exception:
    otel_inject = None


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay_s: float = 0.1
    max_delay_s: float = 2.0

    def normalized(self) -> "RetryConfig":
        ma = self.max_attempts
        if ma <= 0:
            ma = 1
        bd = self.base_delay_s if self.base_delay_s > 0 else 0.1
        md = self.max_delay_s if self.max_delay_s > 0 else 2.0
        return RetryConfig(max_attempts=ma, base_delay_s=bd, max_delay_s=md)


@dataclass
class TracingConfig:
    # If True, do not inject any tracing headers
    disable: bool = False


class DriftQError(Exception):
    pass


class APIError(DriftQError):
    def __init__(self, status: int, code: str = "", message: str = "") -> None:
        super().__init__(f"HTTP {status} {code}: {message}".strip())
        self.status = status
        self.code = code
        self.message = message


def _is_safe_method(method: str) -> bool:
    m = method.upper()
    return m in ("GET", "HEAD", "OPTIONS")


def _retryable_status(status: int) -> bool:
    return status in (429, 500, 502, 503, 504)


def _can_retry(method: str, headers: Mapping[str, str]) -> bool:
    if _is_safe_method(method):
        return True

    return bool(headers.get("Idempotency-Key"))


def _parse_retry_after(h: str) -> Optional[float]:
    if not h:
        return None

    try:
        secs = int(h)
        return max(0.0, float(secs))
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(h)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


def _backoff_s(base: float, cap: float, attempt: int) -> float:
    # attempt=1 means first retry delay
    d = min(cap, base * (2 ** (attempt - 1)))
    # jitter +/-20%
    return d * (0.8 + 0.4 * random.random())


class HttpClient:
    """
    Internal HTTP client with:
      - default timeout (unless overridden per call)
      - retry on transient failures
      - trace context propagation via HTTP headers (traceparent) when OTel is installed
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 10.0,
        retry: RetryConfig | None = None,
        tracing: TracingConfig | None = None,
        user_agent: str = "driftq-py/0.1.0",
        transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.retry = (retry or RetryConfig()).normalized()
        self.tracing = tracing or TracingConfig()
        self.user_agent = user_agent

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, str]] = None,
        json: Any | None = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float | None | object = _DEFAULT
    ) -> Any:
        hdrs: Dict[str, str] = {"Accept": "application/json", "User-Agent": self.user_agent}
        if headers:
            hdrs.update(headers)

        if not self.tracing.disable and otel_inject is not None:
            otel_inject(hdrs)

        # Default timeout unless caller explicitly overrides
        if timeout_s is _DEFAULT:
            effective_timeout = self.timeout_s
        else:
            effective_timeout = timeout_s  # float or None

        cfg = self.retry
        if cfg.max_attempts <= 1 or not _can_retry(method, hdrs):
            resp = await self._client.request(
                method,
                path,
                params=params,
                json=json,
                headers=hdrs,
                timeout=effective_timeout,
            )
            return await self._decode_or_raise(resp)

        last_err: Exception | None = None
        for attempt in range(1, cfg.max_attempts + 1):
            try:
                resp = await self._client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    headers=hdrs,
                    timeout=effective_timeout,
                )

                if not _retryable_status(resp.status_code):
                    return await self._decode_or_raise(resp)

                if attempt == cfg.max_attempts:
                    return await self._decode_or_raise(resp)

                wait = _parse_retry_after(resp.headers.get("Retry-After", ""))  # seconds
                await resp.aclose()

                if wait is None:
                    wait = _backoff_s(cfg.base_delay_s, cfg.max_delay_s, attempt)

                await asyncio.sleep(wait)

            except httpx.TransportError as e:
                last_err = e
                if attempt == cfg.max_attempts:
                    raise
                wait = _backoff_s(cfg.base_delay_s, cfg.max_delay_s, attempt)
                await asyncio.sleep(wait)

        if last_err:
            raise last_err
        raise DriftQError("request failed")

    async def stream_lines(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float | None | object = _DEFAULT
    ):
        """
        Streaming helper used for NDJSON consume

        Default behavior: NO timeout (stream lifetime controlled by caller cancellation)
        - timeout_s is _DEFAULT -> disable timeouts
        - timeout_s is a float -> apply that timeout
        - timeout_s is None -> disable timeouts explicitly
        """
        hdrs: Dict[str, str] = {"User-Agent": self.user_agent}
        if headers:
            hdrs.update(headers)

        if not self.tracing.disable and otel_inject is not None:
            otel_inject(hdrs)

        # For streams: default should be NO timeout unless caller explicitly sets one
        if timeout_s is _DEFAULT:
            effective_timeout = None
        else:
            effective_timeout = timeout_s  # float or None

        async with self._client.stream(
            method,
            path,
            params=params,
            headers=hdrs,
            timeout=effective_timeout,
        ) as resp:
            if resp.status_code >= 400:
                await self._decode_or_raise(resp)
            async for line in resp.aiter_lines():
                if line:
                    yield line

    async def _decode_or_raise(self, resp: httpx.Response) -> Any:
        try:
            if resp.status_code >= 400:
                code = ""
                msg = ""
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        code = str(data.get("error", ""))
                        msg = str(data.get("message", ""))
                except Exception:
                    pass
                raise APIError(resp.status_code, code=code, message=msg)

            if resp.status_code == 204:
                return None
            # best-effort json decode
            return resp.json()
        finally:
            await resp.aclose()
