from pathlib import Path

from collector_to_emulator.scenario_export import (
    _body_path_for_template,
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


def test_build_scenario_yaml_passes_relative_to(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    yaml_text = build_scenario_yaml(
        [{"topic": "alpha", "value": "{}"}],
        templates,
        relative_to=tmp_path,
    )
    assert 'body: "templates/1-alpha.json"' in yaml_text
