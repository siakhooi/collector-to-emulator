import io
import sys
from pathlib import Path

from collector_to_emulator.cli import run

import pytest


@pytest.mark.parametrize("option_help", ["-h", "--help"])
@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="Help output format differs in Python < 3.13",
)
def test_run_help(monkeypatch, capsys, option_help):
    monkeypatch.setattr(
        "sys.argv",
        ["collector-to-emulator", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

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
    monkeypatch.setattr(
        "sys.argv",
        ["collector-to-emulator", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-help312.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("option_version", ["-v", "--version"])
def test_run_show_version(monkeypatch, capsys, option_version):
    monkeypatch.setattr(
        "sys.argv",
        ["collector-to-emulator", option_version],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-version.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("options", [["-p"], ["-p", "-j"]])
def test_run_wrong_options(monkeypatch, capsys, options):
    monkeypatch.setattr(
        "sys.argv",
        ["collector-to-emulator"] + options,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 2

    captured = capsys.readouterr()
    assert (
        "usage: collector-to-emulator [-h] [-v] [-i PATH] [JSONL]"
        in captured.err
    )
    assert (
        "collector-to-emulator: error: unrecognized arguments:" in captured.err
    )


def test_run_no_input_when_tty(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["collector-to-emulator"])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty", lambda: True
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.value.code == 2

    captured = capsys.readouterr()
    assert "No input:" in captured.err


def test_run_reads_jsonl_positional(monkeypatch, tmp_path: Path, capsys):
    path = tmp_path / "in.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["collector-to-emulator", str(path)])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run()
    assert capsys.readouterr().err == ""


def test_run_reads_jsonl_dash_i(monkeypatch, tmp_path: Path, capsys):
    path = tmp_path / "in.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["collector-to-emulator", "-i", str(path)])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run()
    assert capsys.readouterr().err == ""


def test_run_reads_jsonl_from_stdin(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["collector-to-emulator"])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin", io.StringIO('{"x": 2}\n')
    )

    run()
    assert capsys.readouterr().err == ""


def test_run_stdin_priority_over_dash_i(monkeypatch, tmp_path: Path, capsys):
    good = tmp_path / "good.jsonl"
    good.write_text('{"ok": true}\n', encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["collector-to-emulator", "-i", str(good)])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin", io.StringIO("not-json\n")
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.value.code == 1
    assert "invalid JSON" in capsys.readouterr().err


def test_run_dash_i_priority_over_positional(monkeypatch, tmp_path: Path):
    good = tmp_path / "good.jsonl"
    good.write_text('{"ok": true}\n', encoding="utf-8")
    bad = tmp_path / "bad.jsonl"
    bad.write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        ["collector-to-emulator", "-i", str(good), str(bad)],
    )
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    run()


def test_run_invalid_jsonl_exit_code(monkeypatch, tmp_path: Path, capsys):
    path = tmp_path / "bad.jsonl"
    path.write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["collector-to-emulator", str(path)])
    monkeypatch.setattr(
        "collector_to_emulator.cli.sys.stdin.isatty",
        lambda: True,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.value.code == 1
    assert "invalid JSON" in capsys.readouterr().err
