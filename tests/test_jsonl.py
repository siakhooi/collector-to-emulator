import io
import json

import pytest

from collector_to_emulator.jsonl import (
    InvalidJsonlLine,
    iter_jsonl_records,
    parse_jsonl_line,
)


def test_invalid_jsonl_line_subclasses_value_error():
    assert issubclass(InvalidJsonlLine, ValueError)


def test_parse_jsonl_line_valid_object():
    assert parse_jsonl_line('{"a": 1}', 1) == {"a": 1}


def test_parse_jsonl_line_valid_list():
    assert parse_jsonl_line("[1, 2]", 2) == [1, 2]


def test_parse_jsonl_line_valid_primitives():
    assert parse_jsonl_line("null", 3) is None
    assert parse_jsonl_line("true", 4) is True
    assert parse_jsonl_line("false", 5) is False
    assert parse_jsonl_line("42", 6) == 42
    assert parse_jsonl_line('"x"', 7) == "x"


def test_parse_jsonl_line_invalid_preserves_line_no():
    with pytest.raises(InvalidJsonlLine) as exc_info:
        parse_jsonl_line("not-json", 42)
    err = exc_info.value
    assert err.line_no == 42
    assert "line 42:" in str(err)
    assert "invalid JSON" in str(err)
    assert isinstance(err.__cause__, json.JSONDecodeError)


def test_iter_jsonl_records_empty_stream():
    assert list(iter_jsonl_records(io.StringIO(""))) == []


def test_iter_jsonl_records_only_blank_lines():
    assert list(iter_jsonl_records(io.StringIO("  \n\t\n  \n"))) == []


def test_iter_jsonl_records_single_record():
    stream = io.StringIO('{"k": "v"}\n')
    assert list(iter_jsonl_records(stream)) == [{"k": "v"}]


def test_iter_jsonl_records_multiple_and_skips_blanks():
    stream = io.StringIO('{"a": 1}\n\n  \n[2]\n"hi"\n')
    assert list(iter_jsonl_records(stream)) == [{"a": 1}, [2], "hi"]


def test_iter_jsonl_records_line_numbers_for_invalid_json():
    stream = io.StringIO('{"ok": true}\nnot-json\n')
    it = iter_jsonl_records(stream)
    assert next(it) == {"ok": True}
    with pytest.raises(InvalidJsonlLine) as exc_info:
        next(it)
    assert exc_info.value.line_no == 2


def test_iter_jsonl_records_invalid_after_skipped_empty_lines():
    # Physical line 1 blank, line 2 invalid — enumerate uses physical lines
    stream = io.StringIO("\nnot-json\n")
    with pytest.raises(InvalidJsonlLine) as exc_info:
        list(iter_jsonl_records(stream))
    assert exc_info.value.line_no == 2


def test_iter_jsonl_records_strips_line_before_parse():
    stream = io.StringIO('  {"x": 1}  \n')
    assert list(iter_jsonl_records(stream)) == [{"x": 1}]
