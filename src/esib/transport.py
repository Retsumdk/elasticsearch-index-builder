"""HTTP transports: a real one over stdlib urllib, and a recording stub."""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

from .errors import TransportError

__all__ = ["CannedResponse", "Request", "Response", "StubTransport", "Transport", "UrllibTransport"]


@dataclass(frozen=True, slots=True)
class Response:
    """A raw HTTP response. Non-2xx is returned, not raised — the client maps it."""

    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        if not self.body:
            return {}
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise TransportError(
                f"response was not JSON (status {self.status}): {self.body[:200]!r}"
            ) from exc

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None


@dataclass(frozen=True, slots=True)
class Request:
    """What a transport was asked to send — recorded by the stub for assertions."""

    method: str
    path: str
    params: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    content_type: str | None = None

    @property
    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body)

    def text(self) -> str:
        return self.body.decode("utf-8", "replace")


@dataclass(frozen=True, slots=True)
class CannedResponse:
    """A scripted reply, optionally consumed only on the Nth matching call."""

    status: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    method: str | None = None
    path_contains: str | None = None
    times: int | None = None

    def to_response(self) -> Response:
        if isinstance(self.body, bytes):
            raw = self.body
        elif isinstance(self.body, str):
            raw = self.body.encode()
        elif self.body is None:
            raw = b"{}"
        else:
            raw = json.dumps(self.body).encode()
        return Response(status=self.status, headers=dict(self.headers), body=raw)


class Transport(Protocol):
    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> Response: ...


class UrllibTransport:
    """Real HTTP with no third-party dependency.

    TLS verification is always on; a private CA is supplied with ``ca_bundle``.
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth_header: str | None = None,
        timeout: float = 30.0,
        ca_bundle: str | None = None,
        user_agent: str = "elasticsearch-index-builder/1.0",
    ) -> None:
        parts = urlsplit(base_url)
        if parts.scheme not in ("http", "https"):
            raise TransportError(f"base_url must be http(s), got {base_url!r}")
        self._base = base_url.rstrip("/")
        self._auth = auth_header
        self._timeout = timeout
        self._user_agent = user_agent
        self._context = ssl.create_default_context()
        if ca_bundle:
            self._context.load_verify_locations(cafile=ca_bundle)

    @property
    def base_url(self) -> str:
        return self._base

    def url_for(self, path: str, *, params: dict[str, str] | None = None) -> str:
        """Absolute URL for a path, percent-encoding each segment and the query."""

        url = f"{self._base}{_quote_path(path)}"
        if params:
            url = f"{url}?{urlencode(params)}"
        return url

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> Response:
        url = self.url_for(path, params=params)
        headers = {"User-Agent": self._user_agent, "Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        if self._auth:
            headers["Authorization"] = self._auth
        request = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(
                request, timeout=self._timeout, context=self._context
            ) as reply:
                return Response(
                    status=reply.status,
                    headers=dict(reply.headers.items()),
                    body=reply.read(),
                )
        except urllib.error.HTTPError as exc:
            return Response(
                status=exc.code,
                headers=dict(exc.headers.items()) if exc.headers else {},
                body=exc.read() or b"",
            )
        except (urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
            reason = getattr(exc, "reason", exc)
            raise TransportError(f"{method.upper()} {path} failed: {reason}") from exc


def _quote_path(path: str) -> str:
    """Percent-encode each path segment but keep separators and underscores."""

    if not path.startswith("/"):
        path = "/" + path
    return "/".join(quote(segment, safe="") for segment in path.split("/"))


class StubTransport:
    """Deterministic transport for tests and demos.

    Replies can be scripted as a flat queue (consumed in order) or as handlers
    keyed by method/path substring. Every request is recorded.
    """

    def __init__(self, responses: list[CannedResponse] | None = None) -> None:
        self._queue: list[tuple[CannedResponse, int | None]] = [
            (item, item.times) for item in (responses or [])
        ]
        self.requests: list[Request] = []
        self.fallback: Response | None = None

    def push(self, response: CannedResponse) -> None:
        self._queue.append((response, response.times))

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> Response:
        self.requests.append(
            Request(
                method=method.upper(),
                path=path,
                params=dict(params or {}),
                body=body or b"",
                content_type=content_type,
            )
        )
        for index, (canned, remaining) in enumerate(self._queue):
            if canned.method and canned.method.upper() != method.upper():
                continue
            if canned.path_contains and canned.path_contains not in path:
                continue
            if remaining is not None:
                if remaining <= 0:
                    continue
                self._queue[index] = (canned, remaining - 1)
            else:
                self._queue.pop(index)
            return canned.to_response()
        if self.fallback is not None:
            return self.fallback
        raise TransportError(f"StubTransport has no reply for {method.upper()} {path}")
