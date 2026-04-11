import json
from collections.abc import Iterator
from typing import TextIO


class InvalidJsonlLine(ValueError):
    """A single JSONL line could not be decoded as JSON."""

    def __init__(self, line_no: int, error: json.JSONDecodeError) -> None:
        self.line_no = line_no
        super().__init__(f"line {line_no}: invalid JSON ({error})")


def iter_jsonl_records(stream: TextIO) -> Iterator[object]:
    for line_no, line in enumerate(stream, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError as e:
            raise InvalidJsonlLine(line_no, e) from e
