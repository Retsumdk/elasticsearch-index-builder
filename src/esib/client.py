"""One HTTP chokepoint: retries, error mapping and counters in a single place."""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from .config import Config
from .errors import ESHTTPError, TransportError, error_from_response
from .retry import Retrier, RetryPolicy, is_retryable
from .transport import Response, Transport, UrllibTransport

__all__ = ["Client", "ClientStats"]

JSON = "application/json"


@dataclass(slots=True)
class ClientStats:
    """Cumulative counters, useful in logs and in `esib` summaries."""

    requests: int = 0
    retries: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def _retry_after(response: Response) -> float | None:
    raw = response.header("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _body_json(response: Response) -> Any:
    if not response.body:
        return {}
    try:
        return json.loads(response.body)
    except json.JSONDecodeError:
        return {"raw": response.body.decode("utf-8", "replace")[:500]}


def _encode(body: Any, content_type: str) -> bytes | None:
    if body is None:
        return None
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode()
    if content_type == JSON:
        return json.dumps(body, separators=(",", ":")).encode()
    raise TypeError(f"cannot serialise {type(body).__name__} as {content_type}")


class Client:
    """Elasticsearch client: JSON in, typed errors out, retries where safe.

    Retryable responses (429, 502/503/504, rejected-execution errors) are retried
    with full-jitter backoff, honouring ``Retry-After``. When attempts run out the
    final response is returned rather than raised, so a caller probing for an
    index and a caller loading a document both decide what a 404 or a 429 means.
    """

    def __init__(
        self,
        config: Config | None = None,
        *,
        transport: Transport | None = None,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config if config is not None else Config.load()
        self.stats = ClientStats()
        self._transport: Transport = transport or UrllibTransport(
            self.config.base_url,
            auth_header=self.config.auth_header,
            timeout=self.config.timeout,
            ca_bundle=self.config.ca_bundle,
            user_agent=self.config.user_agent,
        )
        policy = retry_policy or RetryPolicy(attempts=self.config.max_attempts)
        self._retrier = Retrier(
            policy, sleep=sleep, clock=clock, uniform=rng.uniform if rng else None
        )

    def close(self) -> None:
        closer = getattr(self._transport, "close", None)
        if callable(closer):
            closer()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def send(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Any = None,
        content_type: str = JSON,
        attempts: int | None = None,
    ) -> Response:
        """Send one request and return the final response verbatim.

        Non-2xx responses are not raised; :meth:`raise_for` maps them to typed
        errors. Retryable ones are retried first, and the last attempt's response
        is returned once the budget is spent.
        """

        payload = _encode(body, content_type)
        last: list[Response] = []

        def attempt() -> Response:
            self.stats.requests += 1
            if payload:
                self.stats.bytes_sent += len(payload)
            reply = self._transport.request(
                method.upper(),
                path,
                params=dict(params) if params else None,
                body=payload,
                content_type=content_type if payload is not None else None,
            )
            self.stats.bytes_received += len(reply.body)
            if 200 <= reply.status < 300:
                last.clear()
                last.append(reply)
                return reply
            self.stats.errors += 1
            error = error_from_response(
                reply.status, _body_json(reply), retry_after=_retry_after(reply)
            )
            last.clear()
            last.append(reply)
            if is_retryable(error):
                raise error
            return reply

        retrier = self._retrier
        if attempts is not None:
            retrier = Retrier(
                RetryPolicy(
                    attempts=attempts,
                    base_delay=retrier.policy.base_delay,
                    max_delay=retrier.policy.max_delay,
                    jitter=retrier.policy.jitter,
                ),
                sleep=self._retrier.sleep,
                clock=self._retrier.clock,
                uniform=self._retrier.uniform,
            )
        try:
            reply = retrier.run(attempt)
        except ESHTTPError:
            if not last:  # pragma: no cover - defensive
                raise
            reply = last[0]
        self.stats.retries += len(retrier.history)
        return reply

    def raise_for(self, reply: Response) -> Any:
        """Decoded JSON for a successful reply; a typed error otherwise."""

        if 200 <= reply.status < 300:
            return reply.json() if reply.body else {}
        raise error_from_response(reply.status, _body_json(reply), retry_after=_retry_after(reply))

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Any = None,
        content_type: str = JSON,
        attempts: int | None = None,
    ) -> Any:
        reply = self.send(
            method,
            path,
            params=params,
            body=body,
            content_type=content_type,
            attempts=attempts,
        )
        return self.raise_for(reply)

    def get(self, path: str, *, params: Mapping[str, str] | None = None) -> Any:
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        payload: Any = None,
        *,
        params: Mapping[str, str] | None = None,
        raw: bytes | None = None,
        content_type: str = JSON,
    ) -> Any:
        return self.request(
            "POST",
            path,
            params=params,
            body=raw if raw is not None else payload,
            content_type=content_type,
        )

    def put(
        self, path: str, payload: Any = None, *, params: Mapping[str, str] | None = None
    ) -> Any:
        return self.request("PUT", path, params=params, body=payload)

    def delete(self, path: str, *, params: Mapping[str, str] | None = None) -> Any:
        return self.request("DELETE", path, params=params)

    def exists(self, path: str) -> bool:
        """Existence probe: 200 is True, 404 is False, anything else raises."""

        reply = self.send("HEAD", path)
        if reply.status == 404:
            return False
        self.raise_for(reply)
        return True

    def bulk(self, payload: bytes, *, refresh: bool = False) -> dict[str, Any]:
        """POST an NDJSON payload to ``_bulk`` and return the raw reply.

        A bulk request answers 200 even when individual documents failed, so item
        results are read by :mod:`esib.bulk`, not here.
        """

        params = {"refresh": "true"} if refresh else None
        result = self.request(
            "POST",
            "/_bulk",
            params=params,
            body=payload,
            content_type="application/x-ndjson",
        )
        if not isinstance(result, dict):
            raise TransportError(f"unexpected bulk response: {result!r}")
        return result

    def info(self) -> dict[str, Any]:
        result = self.get("/")
        return result if isinstance(result, dict) else {}

    def health(self, *, wait_for_status: str | None = None) -> dict[str, Any]:
        params = {"wait_for_status": wait_for_status} if wait_for_status else None
        result = self.get("/_cluster/health", params=params)
        return result if isinstance(result, dict) else {}
