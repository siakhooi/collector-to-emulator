import json
from pathlib import Path

import pytest

from collector_to_emulator.scenario_export import (
    DEFAULT_SCENARIO_NAME,
    SleepTiming,
    _body_path_for_template,
    _index_width,
    _quantize_sleep_ms,
    _record_headers,
    _record_timestamp_ms,
    _safe_topic_filename,
    _sleep_gap_lines,
    _template_basename,
    _value_to_template_body,
    build_scenario_yaml,
    write_templates_from_records,
)


def test_index_width() -> None:
    assert _index_width(0) == 1
    assert _index_width(1) == 1
    assert _index_width(9) == 1
    assert _index_width(10) == 2
    assert _index_width(99) == 2


def test_safe_topic_filename_sanitizes_and_strips() -> None:
    assert _safe_topic_filename("my.topic") == "my.topic"
    assert _safe_topic_filename("a/b*c") == "a_b_c"
    assert _safe_topic_filename("  trim  ") == "trim"
    assert _safe_topic_filename("...") == "topic"
    assert _safe_topic_filename("") == "topic"


def test_safe_topic_filename_non_string_topic() -> None:
    assert _safe_topic_filename(42) == "42"


def test_value_to_template_body() -> None:
    assert _value_to_template_body(None) == ""
    assert _value_to_template_body("plain") == "plain"
    body = _value_to_template_body('{"x": 1}')
    assert json.loads(body) == {"x": 1}
    assert body.endswith("\n")
    body_obj = _value_to_template_body({"y": 2})
    assert json.loads(body_obj) == {"y": 2}


def test_template_basename_zero_padding() -> None:
    assert _template_basename(1, 3, "t") == "001-t.json"
    assert _template_basename(10, 2, "alpha") == "10-alpha.json"


def test_record_headers_prefers_headers_over_header() -> None:
    assert _record_headers({"headers": {"a": "1"}, "header": {"b": "2"}}) == {
        "a": "1"
    }


def test_record_headers_accepts_header_alias() -> None:
    assert _record_headers({"header": {"k": "v"}}) == {"k": "v"}


def test_record_headers_rejects_non_object() -> None:
    with pytest.raises(ValueError, match="must be a JSON object"):
        _record_headers({"headers": []})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="must be a JSON object"):
        _record_headers({"header": "bad"})  # type: ignore[dict-item]


def test_record_timestamp_ms_missing_or_none() -> None:
    assert _record_timestamp_ms({}, line_desc="r1") is None
    assert _record_timestamp_ms({"timestamp": None}, line_desc="r2") is None


def test_record_timestamp_ms_numeric() -> None:
    assert _record_timestamp_ms({"timestamp": 1000}, line_desc="r") == 1000
    assert _record_timestamp_ms({"timestamp": 1000.7}, line_desc="r") == 1000
    assert _record_timestamp_ms({"timestamp": " 1500 "}, line_desc="r") == 1500


def test_record_timestamp_ms_bool_rejected() -> None:
    with pytest.raises(ValueError, match="not bool"):
        _record_timestamp_ms({"timestamp": True}, line_desc="record 1")


def test_record_timestamp_ms_invalid_string() -> None:
    with pytest.raises(ValueError, match="invalid 'timestamp'"):
        _record_timestamp_ms({"timestamp": "nope"}, line_desc="record 2")


def test_record_timestamp_ms_wrong_type() -> None:
    with pytest.raises(ValueError, match="not list"):
        # type: ignore[dict-item]
        _record_timestamp_ms({"timestamp": []}, line_desc="record 3")


def test_quantize_sleep_ms() -> None:
    assert _quantize_sleep_ms(100, 1) == 100
    assert _quantize_sleep_ms(155, 100) == 200
    with pytest.raises(ValueError, match="positive integer"):
        _quantize_sleep_ms(10, 0)


def test_body_path_for_template_relative_to_anchor(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    out = _body_path_for_template(
        templates, "1-topic.json", relative_to=tmp_path
    )
    assert out.replace("\\", "/") == "templates/1-topic.json"


def test_body_path_for_template_default_uses_cwd(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    templates = tmp_path / "templates"
    out = _body_path_for_template(templates, "1-topic.json")
    assert out.replace("\\", "/") == "templates/1-topic.json"


def test_body_path_for_template_falls_back_when_not_under_anchor(
    tmp_path: Path,
) -> None:
    anchor = tmp_path / "anchor"
    anchor.mkdir()
    outside = tmp_path / "elsewhere" / "templates"
    outside.mkdir(parents=True)
    full_posix = (outside / "1-topic.json").as_posix()
    out = _body_path_for_template(outside, "1-topic.json", relative_to=anchor)
    assert out.replace("\\", "/") == full_posix.replace("\\", "/")


def test_sleep_gap_lines_first_timestamp_sets_base_no_chunk() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=None,
        ts_ms=1_000,
        timing=SleepTiming(),
    )
    assert new_base == 1_000
    assert chunks == []


def test_sleep_gap_lines_small_gap_keeps_base() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=1_000,
        ts_ms=1_200,
        timing=SleepTiming(
            gap_threshold_ms=500,
            duration_cap_ms=5_000,
            round_ms=1,
        ),
    )
    assert new_base == 1_000
    assert chunks == []


def test_sleep_gap_lines_large_gap_emits_one_sleep_chunk() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=1_000,
        ts_ms=2_500,
        timing=SleepTiming(
            gap_threshold_ms=500,
            duration_cap_ms=5_000,
            round_ms=1,
        ),
    )
    assert new_base == 2_500
    assert len(chunks) == 1
    assert "  - sleep:" in chunks[0]
    assert "Waiting 1500ms" in chunks[0]
    assert 'duration: "1500ms"' in chunks[0]


def test_sleep_gap_lines_respects_cap() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=0,
        ts_ms=100_000,
        timing=SleepTiming(
            gap_threshold_ms=500,
            duration_cap_ms=2_000,
            round_ms=1,
        ),
    )
    assert new_base == 100_000
    assert len(chunks) == 1
    assert "Waiting 2000ms" in chunks[0]


def test_sleep_gap_lines_respects_round_step() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=0,
        ts_ms=1_550,
        timing=SleepTiming(
            gap_threshold_ms=500,
            duration_cap_ms=5_000,
            round_ms=100,
        ),
    )
    assert new_base == 1_550
    assert len(chunks) == 1
    assert "Waiting 1600ms" in chunks[0]


def test_build_scenario_yaml_empty_records(tmp_path: Path) -> None:
    yaml_text = build_scenario_yaml(
        [],
        tmp_path / "templates",
        relative_to=tmp_path,
    )
    assert "steps: []\n" in yaml_text
    assert f'name: "{DEFAULT_SCENARIO_NAME}"' in yaml_text


def test_build_scenario_yaml_custom_scenario_name(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    yaml_text = build_scenario_yaml(
        [{"topic": "t", "value": "{}"}],
        templates,
        scenario_name="custom",
        relative_to=tmp_path,
    )
    assert yaml_text.startswith('name: "custom"\n')


def test_build_scenario_yaml_inserts_sleep_from_timestamps(
    tmp_path: Path,
) -> None:
    templates = tmp_path / "templates"
    yaml_text = build_scenario_yaml(
        [
            {"topic": "a", "timestamp": 0, "value": "{}"},
            {"topic": "b", "timestamp": 2000, "value": "{}"},
        ],
        templates,
        relative_to=tmp_path,
    )
    assert "  - sleep:" in yaml_text
    assert "Waiting 2000ms" in yaml_text
    assert "1-a.json" in yaml_text


def test_build_scenario_yaml_passes_relative_to(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    yaml_text = build_scenario_yaml(
        [{"topic": "alpha", "value": "{}"}],
        templates,
        relative_to=tmp_path,
    )
    assert 'body: "templates/1-alpha.json"' in yaml_text


def test_write_templates_from_records_writes_files(tmp_path: Path) -> None:
    td = tmp_path / "templates"
    out = write_templates_from_records(
        [{"topic": "alpha", "value": json.dumps({"x": 1})}],
        td,
    )
    assert len(out) == 1
    assert out[0]["topic"] == "alpha"
    body = (td / "1-alpha.json").read_text(encoding="utf-8")
    assert json.loads(body) == {"x": 1}


def test_write_templates_from_records_rejects_non_dict(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="expected a JSON object"):
        write_templates_from_records([123], tmp_path / "out")


def test_write_templates_from_records_requires_topic(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing required field 'topic'"):
        write_templates_from_records([{"value": "{}"}], tmp_path / "t")


def test_write_templates_from_records_invalid_headers(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be a JSON object"):
        write_templates_from_records(
            [{"topic": "t", "headers": "bad"}],
            tmp_path / "t",
        )  # type: ignore[list-item]
