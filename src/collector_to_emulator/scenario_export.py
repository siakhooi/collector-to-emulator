"""Build kafka-emulator scenario YAML and per-record template JSON files."""

import json
import re
from pathlib import Path
from typing import Any

DEFAULT_SCENARIO_NAME = "Unnamed"
DEFAULT_BOOTSTRAP_SERVERS = "kafka-test:9092"
SLEEP_GAP_THRESHOLD_MS = 500
SLEEP_DURATION_CAP_MS = 5000
SLEEP_ROUND_MS = 1

_UNSAFE_TOPIC_CHARS = re.compile(r"[^\w\-.]+", re.UNICODE)
_YAML_PLAIN_HEADER_KEY = re.compile(r"^[a-zA-Z_][\w\-]*$")


def _index_width(n: int) -> int:
    return max(1, len(str(n)))


def _safe_topic_filename(topic: str) -> str:
    s = _UNSAFE_TOPIC_CHARS.sub("_", str(topic).strip())
    s = s.strip("._")
    return s if s else "topic"


def _value_to_template_body(value: Any) -> str:
    """Parse JSON-encoded string payloads; otherwise serialize as JSON text."""
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        return json.dumps(parsed, indent=2, ensure_ascii=False) + "\n"
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def _template_basename(seq: int, width: int, topic: Any) -> str:
    topic_part = _safe_topic_filename(topic)
    return f"{seq:0{width}d}-{topic_part}.json"


def _body_path_for_template(
    templates_dir: Path,
    basename: str,
    *,
    relative_to: Path | None = None,
) -> str:
    full = templates_dir / basename
    base = Path.cwd() if relative_to is None else relative_to
    try:
        return str(full.relative_to(base))
    except ValueError:
        return full.as_posix()


def _record_headers(record: dict[str, Any]) -> dict[str, Any]:
    if "headers" in record:
        h = record["headers"]
    else:
        h = record.get("header", {})
    if not isinstance(h, dict):
        raise ValueError("'headers' / 'header' must be a JSON object")
    return h


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    raise ValueError(f"unsupported YAML scalar type: {type(value).__name__}")


def _scenario_preamble(scenario_name: str) -> str:
    return (
        f"name: {_yaml_scalar(scenario_name)}\n\n"
        "kafka:\n"
        "  default:\n"
        f"    bootstrap_servers: {_yaml_scalar(DEFAULT_BOOTSTRAP_SERVERS)}\n\n"
    )


def _is_empty_key(key: Any) -> bool:
    if key is None:
        return True
    if isinstance(key, str) and not key:
        return True
    return False


def _record_timestamp_ms(
    record: dict[str, Any], *, line_desc: str
) -> int | None:
    """Return collector epoch milliseconds from ``timestamp``, or None if
    absent."""
    if "timestamp" not in record or record["timestamp"] is None:
        return None
    raw = record["timestamp"]
    if isinstance(raw, bool):
        raise ValueError(
            f"{line_desc}: 'timestamp' must be a number, not bool"
        )
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        try:
            return int(float(stripped))
        except ValueError as e:
            raise ValueError(
                f"{line_desc}: invalid 'timestamp' ({e!s})"
            ) from e
    raise ValueError(
        f"{line_desc}: 'timestamp' must be a number or numeric string, "
        f"not {type(raw).__name__}"
    )


def _quantize_sleep_ms(sleep_ms: int, round_ms: int) -> int:
    """Round sleep duration to the nearest ``round_ms`` (1 = no change)."""
    if round_ms <= 0:
        raise ValueError("sleep round step must be a positive integer")
    if round_ms == 1:
        return sleep_ms
    return int(round(sleep_ms / round_ms) * round_ms)


def _yaml_sleep_step(sleep_ms: int) -> list[str]:
    msg = f"Waiting {sleep_ms}ms"
    dur = f"{sleep_ms}ms"
    return [
        "  - sleep:",
        f"      message: {_yaml_scalar(msg)}",
        f"      duration: {_yaml_scalar(dur)}",
    ]


def _yaml_headers_block(headers: dict[str, Any], indent: int) -> list[str]:
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


def build_scenario_yaml(
    records: list[dict[str, Any]],
    templates_dir: Path,
    *,
    scenario_name: str = DEFAULT_SCENARIO_NAME,
    sleep_gap_threshold_ms: int = SLEEP_GAP_THRESHOLD_MS,
    sleep_duration_cap_ms: int = SLEEP_DURATION_CAP_MS,
    sleep_round_ms: int = SLEEP_ROUND_MS,
    relative_to: Path | None = None,
) -> str:
    preamble = _scenario_preamble(scenario_name).rstrip("\n")
    if not records:
        return preamble + "\nsteps: []\n"
    n = len(records)
    width = _index_width(n)
    lines: list[str] = [preamble, "steps:"]
    base_ms: int | None = None
    for seq, record in enumerate(records, start=1):
        line_desc = f"record {seq}"
        ts_ms = _record_timestamp_ms(record, line_desc=line_desc)
        if ts_ms is not None:
            if base_ms is None:
                base_ms = ts_ms
            else:
                gap_ms = ts_ms - base_ms
                if gap_ms > sleep_gap_threshold_ms:
                    sleep_ms = _quantize_sleep_ms(
                        min(gap_ms, sleep_duration_cap_ms),
                        sleep_round_ms,
                    )
                    lines.append("\n".join(_yaml_sleep_step(sleep_ms)))
                    base_ms = ts_ms
        basename = _template_basename(seq, width, record["topic"])
        body = _body_path_for_template(
            templates_dir, basename, relative_to=relative_to
        )
        topic_s = _yaml_scalar(record["topic"])
        headers = _record_headers(record)
        step_lines = [
            "  - send:",
            f"      topic: {topic_s}",
            f"      body: {json.dumps(body)}",
        ]
        if not _is_empty_key(record.get("key")):
            step_lines.append(f"      key: {_yaml_scalar(record.get('key'))}")
        step_lines.extend(_yaml_headers_block(headers, indent=6))
        lines.append("\n".join(step_lines))
    return "\n".join(lines) + "\n"


def write_templates_from_records(
    records: list[Any], templates_dir: Path
) -> list[dict[str, Any]]:
    n = len(records)
    width = _index_width(n)
    templates_dir.mkdir(parents=True, exist_ok=True)
    dict_records: list[dict[str, Any]] = []
    for seq, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(
                f"record {seq}: expected a JSON object, not "
                f"{type(record).__name__}"
            )
        if "topic" not in record:
            raise ValueError(f"record {seq}: missing required field 'topic'")
        rec = dict(record)
        _record_headers(rec)
        dict_records.append(rec)
        name = _template_basename(seq, width, rec["topic"])
        path = templates_dir / name
        body = _value_to_template_body(rec.get("value"))
        path.write_text(body, encoding="utf-8")
    return dict_records
