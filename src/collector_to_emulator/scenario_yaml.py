"""Minimal YAML-shaped text for kafka-emulator scenario files (no PyYAML)."""

import json
import re

_YAML_PLAIN_HEADER_KEY = re.compile(r"^[a-zA-Z_][\w\-]*$")


def _yaml_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    raise ValueError(f"unsupported YAML scalar type: {type(value).__name__}")


def _scenario_preamble(scenario_name: str, *, bootstrap_servers: str) -> str:
    return (
        f"name: {_yaml_scalar(scenario_name)}\n\n"
        "kafka:\n"
        "  default:\n"
        f"    bootstrap_servers: {_yaml_scalar(bootstrap_servers)}\n\n"
    )


def _is_empty_key(key: object | None) -> bool:
    if key is None:
        return True
    if isinstance(key, str) and not key:
        return True
    return False


def _yaml_sleep_step(sleep_ms: int) -> list[str]:
    msg = f"Waiting {sleep_ms}ms"
    dur = f"{sleep_ms}ms"
    return [
        "  - sleep:",
        f"      message: {_yaml_scalar(msg)}",
        f"      duration: {_yaml_scalar(dur)}",
    ]


def _yaml_headers_block(headers: dict[str, object], indent: int) -> list[str]:
    pad = " " * indent
    if not headers:
        return []
    lines = [f"{pad}headers:"]
    inner = " " * (indent + 2)
    for key, val in headers.items():
        if not isinstance(key, str):
            raise ValueError("header keys must be strings")
        k = key if _YAML_PLAIN_HEADER_KEY.match(key) else json.dumps(key)
        lines.append(f"{inner}{k}: {_yaml_scalar(val)}")
    return lines


def _yaml_send_step(
    *,
    topic: object,
    key: object | None,
    headers: dict[str, object],
    body_path: str,
) -> list[str]:
    """Lines for one ``send:`` step (topic, body template path, optional key,
    headers)."""
    topic_s = _yaml_scalar(topic)
    step_lines = [
        "  - send:",
        f"      topic: {topic_s}",
        f"      body: {json.dumps(body_path)}",
    ]
    if not _is_empty_key(key):
        step_lines.append(f"      key: {_yaml_scalar(key)}")
    step_lines.extend(_yaml_headers_block(headers, indent=6))
    return step_lines
