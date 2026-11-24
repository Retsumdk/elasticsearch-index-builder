"""The in-memory cluster is shipped with the package, so it is tested like code.

It is the double every other test (and the demo) stands on; if it drifts away
from Elasticsearch's actual replies, every test above it becomes fiction.
"""

from __future__ import annotations

import json
from typing import Any

from esib import Client, Config, FakeCluster
from esib.bulk import encode_bulk


def _client(cluster: FakeCluster) -> Client:
    return Client(
        Config(base_url="http://cluster.test:9200"),
        transport=cluster,
        sleep=lambda _delay: None,
    )


def test_info_and_health_describe_a_live_cluster() -> None:
    client = _client(FakeCluster())
    info = client.info()
    assert info["version"]["number"]
    assert client.health()["status"] == "green"


def test_create_twice_reports_the_existing_index() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {"mappings": {"properties": {"title": {"type": "text"}}}})
    reply = client.send("PUT", "/books", body={"mappings": {}})
    assert reply.status == 400
    assert reply.json()["error"]["type"] == "resource_already_exists_exception"


def test_creating_an_index_named_like_an_alias_is_rejected() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/_alias", {})  # no-op guard: not a route, but must not crash later
    client.put("/root-index", {})
    client.post("/_aliases", {"actions": [{"add": {"index": "root-index", "alias": "live"}}]})
    reply = client.send("PUT", "/live", body={})
    assert reply.status == 400
    assert reply.json()["error"]["type"] == "invalid_index_name_exception"


def test_head_reports_missing_indices_without_a_body() -> None:
    client = _client(FakeCluster())
    assert client.send("HEAD", "/nope").status == 404
    client.put("/books", {})
    assert client.send("HEAD", "/books").status == 200


def test_delete_removes_the_index_and_the_alias_edges() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.post("/_aliases", {"actions": [{"add": {"index": "books", "alias": "live"}}]})
    client.delete("/books")
    assert cluster.aliases["live"] == set()
    assert client.send("DELETE", "/books").status == 404


def test_deleting_an_alias_is_rejected() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.post("/_aliases", {"actions": [{"add": {"index": "books", "alias": "live"}}]})
    reply = client.send("DELETE", "/live")
    assert reply.status == 400


def test_alias_lookup_lists_targets_and_404s_when_empty() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    assert client.send("GET", "/_alias/live").status == 404
    client.post("/_aliases", {"actions": [{"add": {"index": "books", "alias": "live"}}]})
    body = client.get("/_alias/live")
    assert list(body) == ["books"]


def test_alias_actions_validate_their_input() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    assert client.send("POST", "/_aliases", body={"nope": True}).status == 400
    assert (
        client.send("POST", "/_aliases", body={"actions": [{"add": {"index": "books"}}]}).status
        == 400
    )
    missing = client.send(
        "POST", "/_aliases", body={"actions": [{"add": {"index": "ghost", "alias": "live"}}]}
    )
    assert missing.status == 404
    unknown = client.send(
        "POST", "/_aliases", body={"actions": [{"swap": {"index": "books", "alias": "live"}}]}
    )
    assert unknown.status == 400


def test_bulk_create_update_delete_round_trip() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    created = client.bulk(
        b'{"create":{"_index":"books","_id":"1"}}\n{"title":"Dune"}\n',
    )
    assert created["items"][0]["create"]["result"] == "created"

    updated = client.bulk(
        b'{"update":{"_index":"books","_id":"1"}}\n{"doc":{"title":"Dune Messiah"}}\n'
    )
    assert updated["items"][0]["update"]["result"] == "updated"
    assert cluster.indices["books"].documents["1"]["title"] == "Dune Messiah"

    deleted = client.bulk(b'{"delete":{"_index":"books","_id":"1"}}\n')
    assert deleted["items"][0]["delete"]["result"] == "deleted"
    assert cluster.document_count("books") == 0


def test_updating_a_missing_document_is_a_per_item_404() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    reply = client.bulk(b'{"update":{"_index":"books","_id":"ghost"}}\n{"doc":{"a":1}}\n')
    item = reply["items"][0]["update"]
    assert item["status"] == 404
    assert item["error"]["type"] == "document_missing_exception"


def test_bulk_rejects_a_body_without_a_trailing_newline() -> None:
    client = _client(FakeCluster())
    client.put("/books", {})
    reply = client.send(
        "POST",
        "/_bulk",
        body=b'{"index":{"_index":"books"}}\n{"a":1}',
        content_type="application/x-ndjson",
    )
    assert reply.status == 400
    assert "newline" in reply.json()["error"]["reason"]


def test_bulk_rejects_bad_json_and_unknown_actions() -> None:
    client = _client(FakeCluster())
    client.put("/books", {})
    bad_action = client.send(
        "POST", "/_bulk", body=b"{not json}\n", content_type="application/x-ndjson"
    )
    assert bad_action.status == 400
    assert bad_action.json()["error"]["type"] == "json_parse_exception"
    unknown = client.send(
        "POST",
        "/_bulk",
        body=b'{"upsert":{"_index":"books"}}\n{}\n',
        content_type="application/x-ndjson",
    )
    assert unknown.status == 400
    two_actions = client.send(
        "POST",
        "/_bulk",
        body=b'{"index":{"_index":"books"},"delete":{"_index":"books"}}\n{}\n',
        content_type="application/x-ndjson",
    )
    assert two_actions.status == 400


def test_bulk_accepts_an_index_scoped_path() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    reply = client.bulk(encode_bulk("books", [{"title": "Dune"}]))
    assert reply["errors"] is False
    assert cluster.document_count("books") == 1


def test_strict_mapping_names_the_offending_field() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put(
        "/strict", {"mappings": {"dynamic": "strict", "properties": {"a": {"type": "long"}}}}
    )
    reply = client.bulk(
        b'{"index":{"_index":"strict","_id":"1"}}\n{"a":1,"surprise":true}\n',
    )
    item = reply["items"][0]["index"]
    assert item["error"]["type"] == "strict_dynamic_mapping_exception"
    assert "surprise" in item["error"]["reason"]


def test_mapping_types_are_enforced_like_the_cluster_does() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/typed", {"mappings": {"properties": {"n": {"type": "long"}}}})
    reply = client.bulk(b'{"index":{"_index":"typed","_id":"1"}}\n{"n":"not a number"}\n')
    item = reply["items"][0]["index"]
    assert item["error"]["type"] == "mapper_parsing_exception"


def test_count_mapping_refresh_and_settings_routes() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {"settings": {"number_of_shards": "3", "number_of_replicas": "0"}})
    client.bulk(encode_bulk("books", [{"a": 1}, {"a": 2}]))
    assert client.get("/books/_count")["count"] == 2
    assert client.post("/books/_refresh")["_shards"]["successful"] == 1
    assert client.get("/books/_mapping")["books"]["mappings"] == {}
    assert client.put("/books/_settings", {"refresh_interval": "-1"})["acknowledged"] is True
    assert cluster.indices["books"].settings["number_of_shards"] == "3"
    assert cluster.indices["books"].settings["refresh_interval"] == "-1"


def test_unknown_routes_and_methods_are_reported() -> None:
    client = _client(FakeCluster())
    assert client.send("GET", "/books/_nonsense").status == 400
    assert client.send("POST", "/books").status == 405


def test_search_without_a_pit_reads_an_index_directly() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.bulk(encode_bulk("books", [{"i": index} for index in range(3)]))
    page = client.post("/books/_search", {"size": 2})
    assert page["hits"]["total"]["value"] == 3
    assert len(page["hits"]["hits"]) == 2


def test_search_on_a_missing_index_or_pit_is_a_typed_404() -> None:
    client = _client(FakeCluster())
    assert client.send("POST", "/ghost/_search", body={}).json()["error"]["type"] == (
        "index_not_found_exception"
    )
    reply = client.send("POST", "/_search", body={"pit": {"id": "pit-99"}})
    assert reply.status == 404
    assert reply.json()["error"]["type"] == "search_context_missing_exception"


def test_pit_search_needs_a_pit_id() -> None:
    client = _client(FakeCluster())
    reply = client.send("POST", "/_search", body={})
    assert reply.status == 400


def test_faults_are_consumed_once_and_can_match_a_path() -> None:
    cluster = FakeCluster()
    cluster.add_fault(429, reason="busy", retry_after=0.0, path_contains="/_bulk")
    client = _client(cluster)
    client.put("/books", {})
    reply = client.bulk(encode_bulk("books", [{"a": 1}]))
    assert reply["errors"] is False
    assert client.stats.retries == 1


def test_document_count_follows_aliases() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.bulk(encode_bulk("books", [{"a": 1}]))
    client.post("/_aliases", {"actions": [{"add": {"index": "books", "alias": "live"}}]})
    assert cluster.document_count("live") == 1


def test_cat_indices_lists_what_exists() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.put("/notes", {})
    assert client.get("/_cat/indices")["indices"] == ["books", "notes"]


def test_fake_cluster_requests_are_recorded_for_assertions() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/books", {})
    client.get("/books/_count")
    assert cluster.requests == [("PUT", "/books"), ("GET", "/books/_count")]


def test_bulk_failure_carries_the_document_id() -> None:
    cluster = FakeCluster()
    client = _client(cluster)
    client.put("/strict", {"mappings": {"dynamic": "strict", "properties": {}}})
    body = json.dumps({"index": {"_index": "strict", "_id": "42"}}).encode() + b"\n{}\n"
    reply: Any = client.bulk(body)
    assert reply["items"][0]["index"]["_id"] == "42"
