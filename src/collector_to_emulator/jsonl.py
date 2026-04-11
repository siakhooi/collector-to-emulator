import json
from collections.abc import Iterator
from typing import Any, TextIO


def iter_jsonl_records(stream: TextIO) -> Iterator[Any]:
    for line_no, line in enumerate(stream, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError as e:
            raise ValueError(f"line {line_no}: invalid JSON ({e})") from e
