import json

import pytest

from collector_to_emulator.scenario_yaml import (
    _is_empty_key,
    _scenario_preamble,
    _yaml_headers_block,
    _yaml_scalar,
    _yaml_send_step,
    _yaml_sleep_step,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "null"),
        (True, "true"),
        (False, "false"),
        (0, "0"),
        (42, "42"),
        (-1, "-1"),
        (3.5, "3.5"),
        ("", '""'),
        ("hello", '"hello"'),
        ("a\nb", json.dumps("a\nb", ensure_ascii=False)),
    ],
)
def test_yaml_scalar_primitives(value: object, expected: str) -> None:
    assert _yaml_scalar(value) == expected


def test_yaml_scalar_bool_before_number_branch() -> None:
    """bool is a subclass of int; bool must be handled first."""
    assert _yaml_scalar(True) == "true"
    assert _yaml_scalar(False) == "false"


def test_yaml_scalar_rejects_unsupported_type() -> None:
    with pytest.raises(ValueError, match="unsupported YAML scalar type"):
        _yaml_scalar([1])
    with pytest.raises(ValueError, match="unsupported YAML scalar type"):
        _yaml_scalar({"a": 1})


def test_scenario_preamble_structure() -> None:
    text = _scenario_preamble("My Scenario", bootstrap_servers="broker:9092")
    assert text.startswith('name: "My Scenario"\n\nkafka:\n')
    assert 'bootstrap_servers: "broker:9092"' in text


def test_is_empty_key() -> None:
    assert _is_empty_key(None) is True
    assert _is_empty_key("") is True
    assert _is_empty_key("k") is False
    assert _is_empty_key(0) is False
    assert _is_empty_key(False) is False


def test_yaml_sleep_step_lines() -> None:
    lines = _yaml_sleep_step(1500)
    assert lines == [
        "  - sleep:",
        '      message: "Waiting 1500ms"',
        '      duration: "1500ms"',
    ]


def test_yaml_headers_block_empty() -> None:
    assert _yaml_headers_block({}, indent=4) == []


def test_yaml_headers_block_plain_and_quoted_keys() -> None:
    lines = _yaml_headers_block(
        {"h1": "v1", "with-dash": "v2", "h space": "v3"},
        indent=6,
    )
    assert lines[0] == "      headers:"
    joined = "\n".join(lines)
    assert 'h1: "v1"' in joined
    assert 'with-dash: "v2"' in joined
    assert '"h space"' in joined and '"v3"' in joined


def test_yaml_headers_block_rejects_non_string_key() -> None:
    with pytest.raises(ValueError, match="header keys must be strings"):
        _yaml_headers_block({1: "x"}, indent=2)  # type: ignore[arg-type]


def test_yaml_send_step_minimal() -> None:
    lines = _yaml_send_step(
        topic="t1",
        key=None,
        headers={},
        body_path="templates/1-t1.json",
    )
    assert lines == [
        "  - send:",
        '      topic: "t1"',
        '      body: "templates/1-t1.json"',
    ]


def test_yaml_send_step_with_key_and_headers() -> None:
    lines = _yaml_send_step(
        topic="alpha",
        key="k1",
        headers={"h1": "v1"},
        body_path="p.json",
    )
    assert 'topic: "alpha"' in lines[1]
    assert 'body: "p.json"' in lines[2]
    assert 'key: "k1"' in lines[3]
    assert "headers:" in lines[4]
    assert 'h1: "v1"' in lines[5]


def test_yaml_send_step_omits_empty_string_key() -> None:
    lines = _yaml_send_step(
        topic="t",
        key="",
        headers={},
        body_path="b.json",
    )
    assert "key:" not in "\n".join(lines)


def test_yaml_send_step_body_json_escapes_path() -> None:
    lines = _yaml_send_step(
        topic="t",
        key=None,
        headers={},
        body_path='say "hi".json',
    )
    body_line = [ln for ln in lines if ln.strip().startswith("body:")][0]
    assert body_line == '      body: "say \\"hi\\".json"'
