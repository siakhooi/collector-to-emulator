from pathlib import Path

from collector_to_emulator.scenario_export import (
    SLEEP_DURATION_CAP_MS,
    SLEEP_GAP_THRESHOLD_MS,
    SLEEP_ROUND_MS,
    _body_path_for_template,
    _sleep_gap_lines,
    build_scenario_yaml,
)


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
        sleep_gap_threshold_ms=SLEEP_GAP_THRESHOLD_MS,
        sleep_duration_cap_ms=SLEEP_DURATION_CAP_MS,
        sleep_round_ms=SLEEP_ROUND_MS,
    )
    assert new_base == 1_000
    assert chunks == []


def test_sleep_gap_lines_small_gap_keeps_base() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=1_000,
        ts_ms=1_200,
        sleep_gap_threshold_ms=500,
        sleep_duration_cap_ms=5_000,
        sleep_round_ms=1,
    )
    assert new_base == 1_000
    assert chunks == []


def test_sleep_gap_lines_large_gap_emits_one_sleep_chunk() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=1_000,
        ts_ms=2_500,
        sleep_gap_threshold_ms=500,
        sleep_duration_cap_ms=5_000,
        sleep_round_ms=1,
    )
    assert new_base == 2_500
    assert len(chunks) == 1
    assert "  - sleep:" in chunks[0]
    assert "Waiting 1500ms" in chunks[0]
    assert "duration: \"1500ms\"" in chunks[0]


def test_sleep_gap_lines_respects_cap() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=0,
        ts_ms=100_000,
        sleep_gap_threshold_ms=500,
        sleep_duration_cap_ms=2_000,
        sleep_round_ms=1,
    )
    assert new_base == 100_000
    assert len(chunks) == 1
    assert "Waiting 2000ms" in chunks[0]


def test_sleep_gap_lines_respects_round_step() -> None:
    new_base, chunks = _sleep_gap_lines(
        base_ms=0,
        ts_ms=1_550,
        sleep_gap_threshold_ms=500,
        sleep_duration_cap_ms=5_000,
        sleep_round_ms=100,
    )
    assert new_base == 1_550
    assert len(chunks) == 1
    assert "Waiting 1600ms" in chunks[0]


def test_build_scenario_yaml_passes_relative_to(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    yaml_text = build_scenario_yaml(
        [{"topic": "alpha", "value": "{}"}],
        templates,
        relative_to=tmp_path,
    )
    assert 'body: "templates/1-alpha.json"' in yaml_text
