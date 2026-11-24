"""Configuration layering: defaults < JSON file < environment, with validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from esib import Config, ConfigError


def test_defaults_target_a_local_cluster() -> None:
    config = Config()
    assert config.base_url == "http://localhost:9200"
    assert config.batch_size == 500
    assert config.max_attempts == 4
    assert config.auth_header is None


def test_no_credentials_and_no_secret_leak_in_redacted() -> None:
    config = Config(api_key="secret-value")
    assert config.redacted()["auth"] == "api_key"
    assert "secret-value" not in json.dumps(config.redacted())


def test_api_key_forms() -> None:
    assert Config(api_key="abc").auth_header == "ApiKey abc"
    assert Config(api_key="ApiKey abc").auth_header == "ApiKey abc"
    assert Config(api_key="Bearer jwt").auth_header == "Bearer jwt"


def test_basic_auth_falls_back_to_username_and_password() -> None:
    assert (
        Config(username="elastic", password="changeme").auth_header
        == "Basic ZWxhc3RpYzpjaGFuZ2VtZQ=="
    )


def test_environment_overrides_defaults() -> None:
    env = {
        "ESIB_BASE_URL": "https://search.example.com:9243",
        "ESIB_BATCH_SIZE": "250",
        "ESIB_TIMEOUT": "2.5",
        "ESIB_BATCH_BYTES": "1048576",
    }
    config = Config.load(environ=env)
    assert config.base_url == "https://search.example.com:9243"
    assert config.batch_size == 250
    assert config.timeout == 2.5
    assert config.batch_bytes == 1_048_576


def test_blank_environment_variables_are_ignored() -> None:
    config = Config.load(environ={"ESIB_BASE_URL": "", "ESIB_BATCH_SIZE": ""})
    assert config.base_url == "http://localhost:9200"
    assert config.batch_size == 500


def test_file_layer_then_environment_layer(tmp_path: Path) -> None:
    path = tmp_path / "esib.json"
    path.write_text(
        json.dumps({"base_url": "http://file:9200", "batch_size": 42}), encoding="utf-8"
    )
    from_file = Config.load(path, environ={})
    assert from_file.base_url == "http://file:9200"
    assert from_file.batch_size == 42
    layered = Config.load(path, environ={"ESIB_BASE_URL": "http://env:9200"})
    assert layered.base_url == "http://env:9200"
    assert layered.batch_size == 42


def test_missing_or_invalid_config_file_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        Config.from_file(tmp_path / "nope.json")
    broken = tmp_path / "broken.json"
    broken.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid JSON"):
        Config.from_file(broken)
    array = tmp_path / "array.json"
    array.write_text("[]", encoding="utf-8")
    with pytest.raises(ConfigError, match="JSON object"):
        Config.from_file(array)


def test_unknown_keys_are_rejected_rather_than_ignored() -> None:
    with pytest.raises(ConfigError, match="unknown config key"):
        Config.from_mapping({"base_urll": "http://typo:9200"})
    with pytest.raises(ConfigError, match="unknown config keys"):
        Config().with_changes(timeuot=5)


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"base_url": "ftp://cluster:21"}, "http:// or https://"),
        ({"base_url": "http://"}, "no host"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_bytes": 12}, "batch_bytes"),
        ({"timeout": 0}, "timeout"),
        ({"max_attempts": 0}, "max_attempts"),
    ],
)
def test_validation_rejects_values_that_would_fail_later(
    values: dict[str, object], message: str
) -> None:
    with pytest.raises(ConfigError, match=message):
        Config.from_mapping(values)


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("batch_size", "many", "must be an integer"),
        ("timeout", "soon", "must be a number"),
    ],
)
def test_bad_environment_values_name_the_variable(key: str, value: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        Config.from_env({f"ESIB_{key.upper()}": value})


def test_with_changes_ignores_none_and_keeps_the_original_immutable() -> None:
    config = Config()
    changed = config.with_changes(batch_size=10, base_url=None)
    assert changed.batch_size == 10
    assert config.batch_size == 500
    assert changed is not config
