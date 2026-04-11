import io
import json
import sys
from importlib.metadata import version as pkg_version
from pathlib import Path

from collector_to_emulator.cli import (
    CliStreams,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    build_parser,
    main,
    open_jsonl_source,
    run,
    write_scenario_output,
)

import pytest


def test_build_parser_prog_and_parsed_args() -> None:
    p = build_parser(pkg_version="9.9.9")
    assert p.prog == "collector-to-emulator"
    args = p.parse_args(["-g", "999", "in.jsonl"])
    assert args.sleep_gap_ms == 999
    assert args.jsonl == "in.jsonl"
    assert args.sleep_round_ms == 1


@pytest.mark.parametrize("opt", ["-v", "--version"])
def test_build_parser_version_uses_pkg_version(capsys, opt: str) -> None:
    p = build_parser(pkg_version="0.test.0")
    with pytest.raises(SystemExit) as exc_info:
        p.parse_args([opt])
    assert exc_info.value.code == EXIT_OK
    assert capsys.readouterr().out == "collector-to-emulator 0.test.0\n"


def test_write_scenario_output_writes_to_injected_stdout() -> None:
    buf = io.StringIO()
    write_scenario_output(
        "name: x\nsteps: []\n",
        scenario_path=Path("unused.yaml"),
        stdout=buf,
        stdout_is_tty=False,
    )
    assert buf.getvalue() == "name: x\nsteps: []\n"


def test_write_scenario_output_tty_writes_to_scenario_path(
    tmp_path: Path,
) -> None:
    out_file = tmp_path / "scenario.yaml"
    write_scenario_output(
        "name: x\nsteps: []\n",
        scenario_path=out_file,
        stdout=io.StringIO(),
        stdout_is_tty=True,
    )
    assert out_file.read_text(encoding="utf-8") == "name: x\nsteps: []\n"


def test_parser_rejects_non_positive_sleep_round(capsys) -> None:
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["-r", "0"])
    assert exc_info.value.code == EXIT_USAGE
    assert "at least 1" in capsys.readouterr().err


def test_parser_rejects_non_int_sleep_round(capsys) -> None:
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["-r", "nope"])
    assert exc_info.value.code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "invalid int value" in err


def test_main_returns_exit_ok_without_system_exit(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        json.dumps({"topic": "ok", "value": "{}"}) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )
    args = build_parser().parse_args([str(path)])
    assert main(args) == EXIT_OK


def test_open_jsonl_source_injected_stdin_piped() -> None:
    payload = '{"topic": "t", "value": "{}"}\n'
    stdin = io.StringIO(payload)
    args = build_parser().parse_args([])
    stream, must_close = open_jsonl_source(
        args, stdin=stdin, stdin_is_tty=False
    )
    assert stream is stdin
    assert must_close is False
    assert stream.read() == payload


@pytest.fixture(autouse=True)
def _patch_scenario_to_file(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-TTY stdout under pytest; write scenario to file like a TTY."""
    monkeypatch.setattr(
        "collector_to_emulator.cli._scenario_writes_to_file",
        lambda: True,
    )


@pytest.mark.parametrize("option_help", ["-h", "--help"])
@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="Help output format differs in Python < 3.13",
)
def test_run_help(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[option_help])
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == EXIT_OK

    with open("tests/expected-output/cli-help.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("option_help", ["-h", "--help"])
@pytest.mark.skipif(
    sys.version_info >= (3, 13),
    reason="Help output format differs in Python >= 3.13",
)
def test_run_help312(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[option_help])
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == EXIT_OK

    with open("tests/expected-output/cli-help312.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("option_version", ["-v", "--version"])
def test_run_show_version(monkeypatch, capsys, option_version):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[option_version])
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == EXIT_OK

    captured = capsys.readouterr()
    expected = "collector-to-emulator "
    expected += f"{pkg_version('collector-to-emulator')}\n"
    assert captured.out == expected


@pytest.mark.parametrize("options", [["-p"], ["-p", "-j"]])
def test_run_wrong_options(monkeypatch, capsys, options):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=list(options))
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == EXIT_USAGE

    captured = capsys.readouterr()
    err = captured.err
    assert "usage: collector-to-emulator" in err
    assert "[-n NAME]" in err
    assert "[JSONL]" in err
    assert (
        "collector-to-emulator: error: unrecognized arguments:" in captured.err
    )


def test_run_no_input_when_tty(monkeypatch, capsys):
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty", lambda: True
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[])
    assert pytest_wrapped_e.value.code == EXIT_USAGE

    captured = capsys.readouterr()
    assert "No input:" in captured.err


@pytest.mark.parametrize("flag", ["-n", "--name"])
def test_run_scenario_name_cli(monkeypatch, tmp_path: Path, capsys, flag: str):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "alpha", "value": json.dumps({"x": 1})}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path), flag, "replay-from-collector"])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert scenario.startswith('name: "replay-from-collector"\n')


def test_run_reads_jsonl_positional(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "alpha", "value": json.dumps({"x": 1})}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    out = (tmp_path / "templates" / "1-alpha.json").read_text(encoding="utf-8")
    assert json.loads(out) == {"x": 1}
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert 'body: "templates/1-alpha.json"' in scenario
    assert 'topic: "alpha"' in scenario
    assert "key:" not in scenario
    assert "headers:" not in scenario
    assert 'bootstrap_servers: "kafka-test:9092"' in scenario


def test_run_scenario_includes_key_and_headers_when_set(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {
        "topic": "t1",
        "key": "k1",
        "headers": {"h1": "v1"},
        "value": "{}",
    }
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert 'key: "k1"' in scenario
    assert "headers:" in scenario
    assert 'h1: "v1"' in scenario


def test_run_reads_jsonl_dash_i(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "beta", "value": json.dumps({"y": 2})}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-i", str(path)])
    assert capsys.readouterr().err == ""
    assert (tmp_path / "templates" / "1-beta.json").is_file()


@pytest.mark.parametrize("flag", ["-t", "--template-dir"])
def test_run_template_dir_override(
    monkeypatch, tmp_path: Path, capsys, flag: str
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "x", "value": "{}"}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    custom = tmp_path / "my-templates"
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-i", str(path), flag, str(custom)])
    assert capsys.readouterr().err == ""
    assert (custom / "1-x.json").is_file()
    assert not (tmp_path / "templates").exists()


def test_run_reads_jsonl_from_stdin(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.chdir(tmp_path)
    rec = {"topic": "stdin", "value": json.dumps({"z": 3})}
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin",
        io.StringIO(json.dumps(rec) + "\n"),
    )

    run(argv=[])
    assert capsys.readouterr().err == ""
    assert (tmp_path / "templates" / "1-stdin.json").is_file()


def test_run_stdin_priority_over_dash_i(monkeypatch, tmp_path: Path, capsys):
    good = tmp_path / "good.jsonl"
    good.write_text('{"ok": true}\n', encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin", io.StringIO("not-json\n")
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=["-i", str(good)])
    assert pytest_wrapped_e.value.code == EXIT_ERROR
    assert "invalid JSON" in capsys.readouterr().err


def test_run_dash_i_priority_over_positional(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    good = tmp_path / "good.jsonl"
    rec = {"topic": "ok", "value": "true"}
    good.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    bad = tmp_path / "bad.jsonl"
    bad.write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-i", str(good), str(bad)])
    content = (tmp_path / "templates" / "1-ok.json").read_text(
        encoding="utf-8"
    )
    assert content.strip() == "true"


def test_run_template_padding_ten_records(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    lines = [
        json.dumps({"topic": f"t{i}", "value": json.dumps({"n": i})})
        for i in range(10)
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    assert (tmp_path / "templates" / "01-t0.json").is_file()
    assert (tmp_path / "templates" / "10-t9.json").is_file()


def test_run_missing_topic_exit_code(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(json.dumps({"value": "{}"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[str(path)])
    assert pytest_wrapped_e.value.code == EXIT_ERROR
    assert "topic" in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["-s", "--scenario"])
def test_run_scenario_path_override(
    monkeypatch, tmp_path: Path, capsys, flag: str
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "x", "value": "{}"}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    custom = tmp_path / "custom.yaml"
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[flag, str(custom), str(path)])
    captured = capsys.readouterr()
    assert captured.err == ""
    text = custom.read_text(encoding="utf-8")
    assert "steps:" in text
    assert not (tmp_path / "scenario.yaml").exists()


def test_run_writes_scenario_to_stdout_when_stdout_not_tty(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    rec = {"topic": "piped", "value": "{}"}
    path.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli._scenario_writes_to_file",
        lambda: False,
    )

    run(argv=[str(path)])
    captured = capsys.readouterr()
    assert captured.err == ""
    assert 'topic: "piped"' in captured.out
    assert not (tmp_path / "scenario.yaml").exists()


def test_run_empty_jsonl_writes_empty_scenario(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "empty.jsonl"
    path.write_text("\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    assert (
        (tmp_path / "scenario.yaml")
        .read_text(encoding="utf-8")
        .endswith("steps: []\n")
    )


def test_run_timestamps_sleep_rounded_to_nearest_ms(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 1000, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 2500, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-r", "40", str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert 'message: "Waiting 1520ms"' in scenario
    assert 'duration: "1520ms"' in scenario


def test_run_invalid_sleep_round_exit_code(monkeypatch, capsys):
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=["-r", "0"])
    assert pytest_wrapped_e.value.code == EXIT_USAGE
    assert "at least 1" in capsys.readouterr().err


def test_run_forwards_stderr_to_buffer(monkeypatch) -> None:
    """Errors from ``main`` (after parse) use ``CliStreams.stderr``."""
    err_buf = io.StringIO()
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )
    with pytest.raises(SystemExit) as exc_info:
        run(argv=[], streams=CliStreams(stderr=err_buf))
    assert exc_info.value.code == EXIT_USAGE
    assert "No input:" in err_buf.getvalue()


def test_run_timestamps_emit_sleep_when_gap_over_500ms(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 1000, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 2500, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert "  - sleep:" in scenario
    assert 'message: "Waiting 1500ms"' in scenario
    assert 'duration: "1500ms"' in scenario
    assert scenario.index("  - sleep:") < scenario.index('topic: "b"')


def test_run_timestamps_sleep_when_gap_over_custom_sleep_gap(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 1000, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 1400, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-g", "200", str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert "  - sleep:" in scenario
    assert 'message: "Waiting 400ms"' in scenario


def test_run_timestamps_no_sleep_when_gap_at_most_500ms(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 1000, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 1400, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert "sleep:" not in scenario


def test_run_timestamps_sleep_respects_custom_sleep_cap(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 0, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 12000, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=["-c", "3000", str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert 'message: "Waiting 3000ms"' in scenario
    assert 'duration: "3000ms"' in scenario


def test_run_timestamps_sleep_capped_at_5s(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "in.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"topic": "a", "timestamp": 0, "value": "{}"}),
                json.dumps({"topic": "b", "timestamp": 12000, "value": "{}"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run(argv=[str(path)])
    assert capsys.readouterr().err == ""
    scenario = (tmp_path / "scenario.yaml").read_text(encoding="utf-8")
    assert 'message: "Waiting 5000ms"' in scenario
    assert 'duration: "5000ms"' in scenario


def test_run_invalid_jsonl_exit_code(monkeypatch, tmp_path: Path, capsys):
    path = tmp_path / "bad.jsonl"
    path.write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run(argv=[str(path)])
    assert pytest_wrapped_e.value.code == EXIT_ERROR
    assert "invalid JSON" in capsys.readouterr().err
