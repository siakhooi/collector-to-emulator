import argparse
import json
import re
import sys
from collections.abc import Iterator
from importlib.metadata import version
from pathlib import Path
from typing import Any, TextIO

_TEMPLATES_DIR = Path("templates")
_UNSAFE_TOPIC_CHARS = re.compile(r"[^\w\-.]+", re.UNICODE)


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


def write_templates_from_records(
    records: list[Any], templates_dir: Path
) -> None:
    n = len(records)
    width = max(1, len(str(n))) if n else 1
    templates_dir.mkdir(parents=True, exist_ok=True)
    for seq, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(
                f"record {seq}: expected a JSON object, not "
                f"{type(record).__name__}"
            )
        if "topic" not in record:
            raise ValueError(f"record {seq}: missing required field 'topic'")
        topic_part = _safe_topic_filename(record["topic"])
        name = f"{seq:0{width}d}-{topic_part}.json"
        path = templates_dir / name
        body = _value_to_template_body(record.get("value"))
        path.write_text(body, encoding="utf-8")


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
        description="convert kafka-collector output into kafka-emulator config"
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
        write_templates_from_records(records, templates_dir)
    except ValueError as e:
        print_to_stderr_and_exit(e, 1)
        return
    finally:
        if must_close:
            stream.close()


if __name__ == "__main__":
    run()
