"""End-to-end over real HTTP: urllib client, threaded server, no mocks.

These tests exercise the layer the unit tests cannot: URL building, the
Authorization header, `application/x-ndjson` bodies, HTTP status handling and
Retry-After parsing all travel through a real socket.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import pytest

from esib import Client, Config, FakeCluster, IndexManager, NotFoundError, RateLimited
from esib.search import scan
from esib.transport import UrllibTransport


class _Handler(BaseHTTPRequestHandler):
    cluster: ClassVar[FakeCluster]
    authorizations: ClassVar[list[str | None]] = []

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_DELETE(self) -> None:
        self._handle()

    def do_HEAD(self) -> None:
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else None
        self.authorizations.append(self.headers.get("authorization"))
        reply = self.cluster.request(
            self.command,
            self.path,
            body=body,
            content_type=self.headers.get("content-type"),
        )
        self.send_response(reply.status)
        for name, value in reply.headers.items():
            self.send_header(name, value)
        self.send_header("content-length", str(len(reply.body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(reply.body)

    def log_message(self, format: str, *args: Any) -> None:
        return


@pytest.fixture
def live_cluster() -> Iterator[tuple[str, FakeCluster]]:
    cluster = FakeCluster()
    _Handler.cluster = cluster
    _Handler.authorizations = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host!s}:{port}", cluster
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _client(base_url: str) -> Client:
    return Client(
        Config(base_url=base_url, batch_size=50, max_attempts=3),
        transport=UrllibTransport(base_url, auth_header="ApiKey test-key", timeout=5.0),
    )


def test_build_swap_and_scan_over_real_http(live_cluster: tuple[str, FakeCluster]) -> None:
    base_url, cluster = live_cluster
    client = _client(base_url)
    manager = IndexManager(client)
    documents = [{"id": f"doc-{index}", "title": f"Title {index}"} for index in range(25)]

    report = manager.build(
        "catalogue",
        documents,
        id_field="id",
        settings={"number_of_replicas": 0},
    )

    assert report.swapped is True
    assert report.loaded == 25
    assert report.batches == 1
    assert manager.count("catalogue") == 25
    assert cluster.document_count("catalogue") == 25

    pages = list(scan(client, "catalogue", size=10))
    assert [len(page.hits) for page in pages] == [10, 10, 5]
    assert pages[0].hits[0]["_id"] == "doc-0"

    assert client.stats.requests >= 6
    assert all(value == "ApiKey test-key" for value in _Handler.authorizations)
    client.close()


def test_bulk_content_type_and_framing_reach_the_server(
    live_cluster: tuple[str, FakeCluster],
) -> None:
    base_url, cluster = live_cluster
    client = _client(base_url)
    manager = IndexManager(client)
    manager.create("notes")
    client.post(
        "/notes/_bulk",
        payload=None,
        raw=b'{"index":{"_id":"1"}}\n{"text":"hello"}\n',
        content_type="application/x-ndjson",
    )
    assert cluster.document_count("notes") == 1
    client.close()


def test_missing_index_raises_not_found_over_http(live_cluster: tuple[str, FakeCluster]) -> None:
    base_url, _cluster = live_cluster
    client = _client(base_url)
    with pytest.raises(NotFoundError) as caught:
        client.get("/nope/_count")
    assert caught.value.status == 404
    assert caught.value.hint()
    client.close()


def test_retry_after_header_is_honoured_over_http(live_cluster: tuple[str, FakeCluster]) -> None:
    base_url, cluster = live_cluster
    client = _client(base_url)
    manager = IndexManager(client)
    manager.create("notes")
    cluster.add_fault(
        429,
        reason="queue full",
        error_type="es_rejected_execution_exception",
        retry_after=0.0,
    )
    outcome = manager.load("notes", [{"id": "1", "text": "hello"}], id_field="id")
    assert outcome.ok is True
    assert client.stats.retries >= 1
    client.close()


def test_exhausted_retries_surface_the_rate_limit(live_cluster: tuple[str, FakeCluster]) -> None:
    base_url, cluster = live_cluster
    client = Client(
        Config(base_url=base_url, max_attempts=2),
        transport=UrllibTransport(base_url, timeout=5.0),
    )
    manager = IndexManager(client)
    manager.create("notes")
    cluster.add_fault(
        429,
        reason="queue full",
        error_type="es_rejected_execution_exception",
        retry_after=0.0,
        path_contains="/_bulk",
        times=9,
    )
    with pytest.raises(RateLimited):
        manager.load("notes", [{"id": "1"}], id_field="id")
    assert client.stats.retries >= 1
    client.close()


def test_unreachable_cluster_raises_transport_error() -> None:
    from esib import TransportError

    client = Client(
        Config(base_url="http://127.0.0.1:1", timeout=0.5, max_attempts=1),
        transport=UrllibTransport("http://127.0.0.1:1", timeout=0.5),
    )
    with pytest.raises(TransportError):
        client.info()
    client.close()
