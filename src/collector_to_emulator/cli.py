import argparse
import json
import re
import sys
from collections.abc import Iterator
from importlib.metadata import version
from pathlib import Path
from typing import Any, TextIO

_TEMPLATES_DIR = Path("templates")
_SCENARIO_PATH = Path("scenario.yaml")
_DEFAULT_SCENARIO_NAME = "Unnamed"
_UNSAFE_TOPIC_CHARS = re.compile(r"[^\w\-.]+", re.UNICODE)
_SLEEP_GAP_THRESHOLD_MS = 500
_SLEEP_DURATION_CAP_MS = 5000


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def iter_jsonl_records(stream: TextIO) -> Iterator[Any]:
    for line_no, line in enumerate(stream, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError as e:
            raise ValueError(f"line {line_no}: invalid JSON ({e})") from e


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


def _body_path_for_template(templates_dir: Path, basename: str) -> str:
    full = templates_dir / basename
    try:
        return str(full.relative_to(Path.cwd()))
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
        '    bootstrap_servers: "kafka-test:9092"\n\n'
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
        k = key if re.match(r"^[a-zA-Z_][\w\-]*$", key) else json.dumps(key)
        lines.append(f"{inner}{k}: {_yaml_scalar(val)}")
    return lines


def build_scenario_yaml(
    records: list[dict[str, Any]],
    templates_dir: Path,
    *,
    scenario_name: str = _DEFAULT_SCENARIO_NAME,
) -> str:
    preamble = _scenario_preamble(scenario_name).rstrip("\n")
    if not records:
        return preamble + "\nsteps: []\n"
    n = len(records)
    width = max(1, len(str(n)))
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
                if gap_ms > _SLEEP_GAP_THRESHOLD_MS:
                    sleep_ms = min(gap_ms, _SLEEP_DURATION_CAP_MS)
                    lines.append("\n".join(_yaml_sleep_step(sleep_ms)))
                    base_ms = ts_ms
        basename = _template_basename(seq, width, record["topic"])
        body = _body_path_for_template(templates_dir, basename)
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


def write_scenario_output(
    content: str,
    *,
    scenario_path: Path | None,
    stdout_is_tty: bool,
) -> None:
    """Write scenario YAML to stdout when piped/redirected; else to file."""
    if not stdout_is_tty:
        sys.stdout.write(content)
        return
    out = scenario_path if scenario_path is not None else _SCENARIO_PATH
    out.write_text(content, encoding="utf-8")


def write_templates_from_records(
    records: list[Any], templates_dir: Path
) -> list[dict[str, Any]]:
    n = len(records)
    width = max(1, len(str(n))) if n else 1
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


def _scenario_writes_to_file() -> bool:
    """True when scenario YAML should be written to disk; False when piped
    stdout."""
    return sys.stdout.isatty()


def open_jsonl_source(args: argparse.Namespace) -> tuple[TextIO, bool]:
    """Return (stream, must_close).
    Priority: piped stdin, then -i, then positional."""
    if not sys.stdin.isatty():
        return sys.stdin, False
    if args.input_path is not None:
        return open(args.input_path, encoding="utf-8"), True
    if args.jsonl is not None:
        return open(args.jsonl, encoding="utf-8"), True
    raise ValueError(
        "No input: pass a JSONL file, use -i PATH, or pipe JSONL into stdin."
    )


def run() -> None:
    __version__: str = version("collector-to-emulator")

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="collector-to-emulator",
        description="convert kafka-collector output into kafka-emulator "
        "config",
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="input_path",
        metavar="PATH",
        help="JSONL file (ignored when stdin is piped)",
    )
    parser.add_argument(
        "-t",
        "--template-dir",
        dest="template_dir",
        metavar="DIR",
        help="template output directory (default: templates/)",
    )
    parser.add_argument(
        "-n",
        "--name",
        dest="scenario_name",
        metavar="NAME",
        help=(
            "scenario name in generated YAML "
            f"(default: {_DEFAULT_SCENARIO_NAME})"
        ),
    )
    parser.add_argument(
        "-s",
        "--scenario",
        dest="scenario",
        metavar="PATH",
        help=(
            "scenario YAML path when stdout is a TTY "
            "(default: scenario.yaml)"
        ),
    )
    parser.add_argument(
        "jsonl",
        nargs="?",
        metavar="JSONL",
        help="JSONL file (if stdin is a TTY and -i omitted)",
    )

    args = parser.parse_args()

    try:
        stream, must_close = open_jsonl_source(args)
    except OSError as e:
        print_to_stderr_and_exit(e, 1)
        return
    except ValueError as e:
        print_to_stderr_and_exit(e, 2)
        return

    try:
        records = list(iter_jsonl_records(stream))
        templates_dir = (
            Path(args.template_dir)
            if args.template_dir is not None
            else _TEMPLATES_DIR
        )
        dict_records = write_templates_from_records(records, templates_dir)
        scenario_name = args.scenario_name or _DEFAULT_SCENARIO_NAME
        scenario_text = build_scenario_yaml(
            dict_records,
            templates_dir,
            scenario_name=scenario_name,
        )
        scenario_path = Path(args.scenario) if args.scenario else None
        write_scenario_output(
            scenario_text,
            scenario_path=scenario_path,
            stdout_is_tty=_scenario_writes_to_file(),
        )
    except ValueError as e:
        print_to_stderr_and_exit(e, 1)
        return
    finally:
        if must_close:
            stream.close()


if __name__ == "__main__":
    run()
