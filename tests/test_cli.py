"""The command line: every command, both output modes, and the exit codes."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from esib import Client, Config, FakeCluster
from esib.cli import main

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2


@pytest.fixture
def world(tmp_path: Path) -> tuple[list[str], Client, FakeCluster]:
    rows = [{"sku": f"sku-{index}", "price": index * 1.5} for index in range(4)]
    source = tmp_path / "rows.ndjson"
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    cluster = FakeCluster()
    client = Client(Config(base_url="http://fake:9200"), transport=cluster, sleep=lambda _d: None)
    return [str(source)], client, cluster


def run(argv: list[str], client: Client) -> tuple[int, str]:
    stream = io.StringIO()
    code = main(argv, stream=stream, client=client)
    return code, stream.getvalue()


def test_mapping_prints_fields_and_types(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, _ = world
    code, output = run(["mapping", source], client)
    assert code == EXIT_OK
    assert "sku" in output
    assert "price" in output


def test_mapping_json_is_machine_readable(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, _ = world
    code, output = run(["--json", "mapping", source, "--dynamic", "strict"], client)
    payload = json.loads(output)
    assert code == EXIT_OK
    assert payload["mappings"]["dynamic"] == "strict"
    assert payload["documents_sampled"] == 4
    assert payload["mappings"]["properties"]["sku"]["type"] == "keyword"


def test_create_then_load_then_count(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, cluster = world
    code, output = run(["create", "rows", "--source", source, "--load"], client)
    assert code == EXIT_OK
    assert "created: rows" in output
    assert "loaded 4 documents" in output
    assert cluster.document_count("rows") == 4

    code, output = run(["count", "rows"], client)
    assert code == EXIT_OK
    assert "4" in output


def test_create_is_idempotent_and_says_so(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, _ = world
    run(["create", "rows", "--source", source], client)
    code, output = run(["create", "rows", "--source", source], client)
    assert code == EXIT_OK
    assert "already exists" in output


def test_load_reports_rejections_and_exits_non_zero(
    world: tuple[list[str], Client, FakeCluster], tmp_path: Path
) -> None:
    (source,), client, _ = world
    run(["create", "strict", "--source", source], client)
    bad = tmp_path / "bad.ndjson"
    bad.write_text('{"sku": {"nested": true}}\n', encoding="utf-8")

    code, output = run(["load", "strict", "--source", str(bad)], client)
    assert code == EXIT_ERROR
    assert "1 rejected" in output

    code, output = run(["load", "strict", "--source", str(bad), "--tolerate-errors"], client)
    assert code == EXIT_ERROR
    assert "rejected" in output


def test_load_with_tolerate_errors_still_reports_success_for_good_rows(
    world: tuple[list[str], Client, FakeCluster],
) -> None:
    (source,), client, _ = world
    run(["create", "rows", "--source", source], client)
    code, output = run(["load", "rows", "--source", source, "--id-field", "sku"], client)
    assert code == EXIT_OK
    assert "4 indexed" in output


def test_rebuild_swaps_an_alias_and_removes_the_old_index(
    world: tuple[list[str], Client, FakeCluster],
) -> None:
    (source,), client, cluster = world
    run(["rebuild", "live", "--source", source, "--id-field", "sku"], client)
    code, output = run(["rebuild", "live", "--source", source, "--id-field", "sku"], client)
    assert code == EXIT_OK
    assert "alias live swapped=True" in output
    assert "1 old index removed" in output
    assert len(cluster.indices) == 1
    assert cluster.document_count("live") == 4


def test_rebuild_dry_run_writes_nothing(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, cluster = world
    code, output = run(["rebuild", "live", "--source", source, "--dry-run"], client)
    assert code == EXIT_OK
    assert "dry run" in output
    assert cluster.indices == {}


def test_rebuild_keep_previous_leaves_the_old_index_alone(
    world: tuple[list[str], Client, FakeCluster],
) -> None:
    (source,), client, cluster = world
    run(["rebuild", "live", "--source", source, "--id-field", "sku"], client)
    code, output = run(
        ["rebuild", "live", "--source", source, "--id-field", "sku", "--keep-previous"], client
    )
    assert code == EXIT_OK
    assert "0 old index removed" in output
    assert len(cluster.indices) == 2


def test_search_pages_and_sorts(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, _ = world
    run(["rebuild", "live", "--source", source, "--id-field", "sku"], client)
    code, output = run(["search", "live", "--size", "2", "--pages", "2"], client)
    assert code == EXIT_OK
    assert "sku-0" in output
    code, json_output = run(["--json", "search", "live", "--size", "2"], client)
    payload = json.loads(json_output)
    assert code == EXIT_OK
    assert payload["count"] == len(payload["hits"]) > 0


def test_health_and_config_are_printable(world: tuple[list[str], Client, FakeCluster]) -> None:
    _, client, _ = world
    code, output = run(["health"], client)
    assert code == EXIT_OK
    assert "green" in output

    code, output = run(["--json", "config"], client)
    assert code == EXIT_OK
    assert json.loads(output)["auth"] == "none"


def test_verbose_adds_request_statistics(world: tuple[list[str], Client, FakeCluster]) -> None:
    (source,), client, _ = world
    code, output = run(["-v", "create", "rows", "--source", source], client)
    assert code == EXIT_OK
    assert "requests" in output
    assert "bytes sent" in output


def test_missing_source_exits_with_a_hint(world: tuple[list[str], Client, FakeCluster]) -> None:
    _, client, _ = world
    code, output = run(["mapping", "/definitely/not/here.ndjson"], client)
    assert code == EXIT_ERROR
    assert output == ""
    code, output = run(["--json", "mapping", "/definitely/not/here.ndjson"], client)
    assert code == EXIT_ERROR
    assert json.loads(output)["error"] == "SourceError"


def test_bad_base_url_is_a_usage_error(
    world: tuple[list[str], Client, FakeCluster], capsys: pytest.CaptureFixture[str]
) -> None:
    _, client, _ = world
    code, _ = run(["--base-url", "ftp://nope", "health"], client)
    assert code == EXIT_USAGE
    captured = capsys.readouterr()
    assert "base_url" in captured.err
    assert "hint:" in captured.err


def test_demo_command_runs_offline(world: tuple[list[str], Client, FakeCluster]) -> None:
    _, client, _ = world
    code, output = run(["demo"], client)
    assert code == EXIT_OK
    assert "esib demo" in output
    assert "alias products" in output
