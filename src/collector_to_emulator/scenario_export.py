"""Build kafka-emulator scenario YAML and per-record template JSON files."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, TypedDict, cast

from collector_to_emulator.scenario_yaml import (
    _scenario_preamble,
    _yaml_send_step,
    _yaml_sleep_step,
)

DEFAULT_SCENARIO_NAME = "Unnamed"
DEFAULT_BOOTSTRAP_SERVERS = "kafka-test:9092"
SLEEP_GAP_THRESHOLD_MS = 500
SLEEP_DURATION_CAP_MS = 5000
SLEEP_ROUND_MS = 1


@dataclass(frozen=True, slots=True)
class SleepTiming:
    """Threshold, cap, and rounding for timestamp-gap sleep steps."""

    gap_threshold_ms: int = SLEEP_GAP_THRESHOLD_MS
    duration_cap_ms: int = SLEEP_DURATION_CAP_MS
    round_ms: int = SLEEP_ROUND_MS


DEFAULT_SLEEP_TIMING = SleepTiming()


class CollectorRecord(TypedDict, total=False):
    """JSON object shape from collector JSONL.

    ``topic`` is required for template/scenario export; other fields are
    optional. Values match decoded JSON (and may be narrowed at runtime).
    """

    topic: object
    key: object
    headers: dict[str, object]
    header: dict[str, object]
    timestamp: object
    value: object


_UNSAFE_TOPIC_CHARS = re.compile(r"[^\w\-.]+", re.UNICODE)


def _index_width(n: int) -> int:
    return max(1, len(str(n)))


def _safe_topic_filename(topic: str) -> str:
    s = _UNSAFE_TOPIC_CHARS.sub("_", str(topic).strip())
    s = s.strip("._")
    return s if s else "topic"


def _value_to_template_body(value: object) -> str:
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


def _template_basename(seq: int, width: int, topic: object) -> str:
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


def _record_headers(record: Mapping[str, object]) -> dict[str, object]:
    if "headers" in record:
        h = record["headers"]
    else:
        h = record.get("header", {})
    if not isinstance(h, dict):
        raise ValueError("'headers' / 'header' must be a JSON object")
    return cast(dict[str, object], h)


def _record_timestamp_ms(
    record: Mapping[str, object], *, line_desc: str
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


def _sleep_gap_lines(
    *,
    base_ms: int | None,
    ts_ms: int,
    timing: SleepTiming,
) -> tuple[int | None, list[str]]:
    """Advance timestamp baseline and optionally emit one sleep step (as a
    single multi-line string for ``lines.append``)."""
    if base_ms is None:
        return ts_ms, []
    gap_ms = ts_ms - base_ms
    if gap_ms > timing.gap_threshold_ms:
        sleep_ms = _quantize_sleep_ms(
            min(gap_ms, timing.duration_cap_ms),
            timing.round_ms,
        )
        return ts_ms, ["\n".join(_yaml_sleep_step(sleep_ms))]
    return base_ms, []


def build_scenario_yaml(
    records: list[CollectorRecord],
    templates_dir: Path,
    *,
    scenario_name: str = DEFAULT_SCENARIO_NAME,
    sleep_timing: SleepTiming = DEFAULT_SLEEP_TIMING,
    relative_to: Path | None = None,
) -> str:
    preamble = _scenario_preamble(
        scenario_name, bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS
    ).rstrip("\n")
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
            base_ms, sleep_chunks = _sleep_gap_lines(
                base_ms=base_ms,
                ts_ms=ts_ms,
                timing=sleep_timing,
            )
            lines.extend(sleep_chunks)
        basename = _template_basename(seq, width, record["topic"])
        body_path = _body_path_for_template(
            templates_dir, basename, relative_to=relative_to
        )
        lines.append(
            "\n".join(
                _yaml_send_step(
                    topic=record["topic"],
                    key=record.get("key"),
                    headers=_record_headers(record),
                    body_path=body_path,
                )
            )
        )
    return "\n".join(lines) + "\n"


def write_templates_from_records(
    records: list[object], templates_dir: Path
) -> list[CollectorRecord]:
    n = len(records)
    width = _index_width(n)
    templates_dir.mkdir(parents=True, exist_ok=True)
    dict_records: list[CollectorRecord] = []
    for seq, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(
                f"record {seq}: expected a JSON object, not "
                f"{type(record).__name__}"
            )
        if "topic" not in record:
            raise ValueError(f"record {seq}: missing required field 'topic'")
        rec = cast(CollectorRecord, dict(record))
        _record_headers(rec)
        dict_records.append(rec)
        name = _template_basename(seq, width, rec["topic"])
        path = templates_dir / name
        body = _value_to_template_body(rec.get("value"))
        path.write_text(body, encoding="utf-8")
    return dict_records
