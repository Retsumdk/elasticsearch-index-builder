"""Transports: response helpers, the recording stub, and URL handling."""

from __future__ import annotations

import json

import pytest

from esib import CannedResponse, Response, StubTransport, TransportError, UrllibTransport


def test_response_parses_json_and_headers() -> None:
    reply = Response(status=200, headers={"Retry-After": "3"}, body=b'{"ok": true}')
    assert reply.json() == {"ok": True}
    assert reply.header("retry-after") == "3"
    assert reply.header("missing") is None


def test_response_without_body_is_an_empty_object() -> None:
    assert Response(status=204, headers={}, body=b"").json() == {}


def test_response_that_is_not_json_raises_transport_error() -> None:
    with pytest.raises(TransportError):
        Response(status=502, headers={}, body=b"<html>bad gateway</html>").json()


def test_stub_returns_queued_replies_in_order() -> None:
    stub = StubTransport([CannedResponse(body={"step": 1}), CannedResponse(body={"step": 2})])
    assert stub.request("GET", "/") is not None
    assert stub.request("GET", "/").json() == {"step": 2}
    assert len(stub.requests) == 2


def test_stub_matches_on_method_and_path() -> None:
    stub = StubTransport(
        [
            CannedResponse(body={"kind": "search"}, method="POST", path_contains="_search"),
            CannedResponse(body={"kind": "root"}),
        ]
    )
    assert stub.request("GET", "/").json() == {"kind": "root"}
    assert stub.request("POST", "/idx/_search").json() == {"kind": "search"}


def test_stub_honours_times_and_then_falls_through() -> None:
    stub = StubTransport(
        [
            CannedResponse(status=429, body={"error": "busy"}, times=1),
            CannedResponse(body={"ok": True}),
        ]
    )
    assert stub.request("POST", "/_bulk").status == 429
    assert stub.request("POST", "/_bulk").status == 200


def test_stub_records_request_bodies_and_content_type() -> None:
    stub = StubTransport([CannedResponse(body={})])
    stub.request("POST", "/_bulk", body=b'{"index":{}}\n', content_type="application/x-ndjson")
    recorded = stub.requests[0]
    assert recorded.method == "POST"
    assert recorded.text() == '{"index":{}}\n'
    assert recorded.content_type == "application/x-ndjson"


def test_stub_without_a_reply_is_an_error() -> None:
    with pytest.raises(TransportError):
        StubTransport().request("GET", "/")


def test_stub_fallback_when_the_queue_is_empty() -> None:
    stub = StubTransport()
    stub.fallback = Response(
        status=200, headers={}, body=json.dumps({"version": {"number": "8.15.0"}}).encode()
    )
    assert stub.request("GET", "/").json()["version"]["number"] == "8.15.0"


def test_canned_response_accepts_raw_bytes_and_strings() -> None:
    assert CannedResponse(body=b"raw").to_response().body == b"raw"
    assert CannedResponse(body="text").to_response().body == b"text"


@pytest.mark.parametrize("url", ["ftp://cluster:9200", "cluster:9200", "unix:///sock"])
def test_urllib_transport_rejects_non_http_schemes(url: str) -> None:
    with pytest.raises(TransportError):
        UrllibTransport(url)


def test_urllib_transport_strips_trailing_slash_and_keeps_auth_out_of_the_url() -> None:
    transport = UrllibTransport("https://cluster.example:9243/", auth_header="ApiKey abc")
    assert transport.base_url == "https://cluster.example:9243"
    assert "abc" not in transport.base_url


def test_path_segments_are_quoted_but_separators_are_not() -> None:
    transport = UrllibTransport("http://cluster:9200")
    assert transport.url_for("/my index/_search") == "http://cluster:9200/my%20index/_search"
    assert transport.url_for("_cluster/health") == "http://cluster:9200/_cluster/health"
    assert transport.url_for("/_pit", params={"keep_alive": "1m"}) == (
        "http://cluster:9200/_pit?keep_alive=1m"
    )
