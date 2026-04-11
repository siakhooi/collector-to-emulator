import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import TextIO

from collector_to_emulator.jsonl import iter_jsonl_records
from collector_to_emulator.scenario_export import (
    DEFAULT_SCENARIO_NAME,
    SLEEP_DURATION_CAP_MS,
    SLEEP_GAP_THRESHOLD_MS,
    SLEEP_ROUND_MS,
    SleepTiming,
    build_scenario_yaml,
    write_templates_from_records,
)

__version__: str = version("collector-to-emulator")

# Exit codes returned by ``main`` (and ``run`` via ``sys.exit``).
EXIT_OK = 0
EXIT_ERROR = 1  # I/O failure or invalid record / template data
EXIT_USAGE = 2  # invalid CLI args or unusable input (e.g. no JSONL source)

_TEMPLATES_DIR = Path("templates")
_SCENARIO_PATH = Path("scenario.yaml")


@dataclass(frozen=True, slots=True)
class CliStreams:
    """Optional stdin/stdout/stderr and TTY overrides for
    ``main`` / ``run``."""

    stdin: TextIO | None = None
    stdin_is_tty: bool | None = None
    stdout: TextIO | None = None
    stdout_is_tty: bool | None = None
    stderr: TextIO | None = None


def _print_error(e: Exception, *, stderr: TextIO | None = None) -> None:
    out = sys.stderr if stderr is None else stderr
    print(f"Error: {e}", file=out)


def _main_fail(exc: Exception, *, stderr: TextIO | None, code: int) -> int:
    """Log ``exc`` to stderr and return the exit code."""
    _print_error(exc, stderr=stderr)
    return code


def write_scenario_output(
    content: str,
    *,
    scenario_path: Path,
    stdout: TextIO,
    stdout_is_tty: bool,
) -> None:
    """Write scenario YAML to ``stdout`` when not a TTY; else to
    ``scenario_path``."""
    if not stdout_is_tty:
        stdout.write(content)
        return
    scenario_path.write_text(content, encoding="utf-8")


def _scenario_writes_to_file() -> bool:
    """True when scenario YAML should be written to disk; False when piped
    stdout."""
    return sys.stdout.isatty()


def _resolve_scenario_stdout_tty(
    stdout: TextIO | None,
    stdout_is_tty: bool | None,
) -> tuple[TextIO, bool]:
    out = sys.stdout if stdout is None else stdout
    if stdout_is_tty is not None:
        return out, stdout_is_tty
    if stdout is None:
        return out, _scenario_writes_to_file()
    return out, out.isatty()


def open_jsonl_source(
    args: argparse.Namespace,
    *,
    stdin: TextIO | None = None,
    stdin_is_tty: bool | None = None,
) -> tuple[TextIO, bool]:
    """Return (stream, must_close).
    Priority: piped stdin, then -i, then positional."""
    in_stream = sys.stdin if stdin is None else stdin
    is_tty = in_stream.isatty() if stdin_is_tty is None else stdin_is_tty
    if not is_tty:
        return in_stream, False
    if args.input_path is not None:
        return open(args.input_path, encoding="utf-8"), True
    if args.jsonl is not None:
        return open(args.jsonl, encoding="utf-8"), True
    raise ValueError(
        "No input: pass a JSONL file, use -i PATH, or pipe JSONL into stdin."
    )


def _parse_positive_int_ms(value: str, *, name: str) -> int:
    """Parse a string as an int ``>= 1`` for ``argparse`` ``type=``
    callbacks."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid int value: {value!r}"
        ) from None
    if n < 1:
        raise argparse.ArgumentTypeError(f"{name} must be at least 1")
    return n


def _parse_sleep_gap_ms(value: str) -> int:
    """``argparse`` ``type=`` for ``-g`` / ``--sleep-gap``."""
    return _parse_positive_int_ms(value, name="sleep gap")


def _parse_sleep_cap_ms(value: str) -> int:
    """``argparse`` ``type=`` for ``-c`` / ``--sleep-cap``."""
    return _parse_positive_int_ms(value, name="sleep duration cap")


def _parse_sleep_round_ms(value: str) -> int:
    """``argparse`` ``type=`` for ``-r`` / ``--round``."""
    return _parse_positive_int_ms(value, name="sleep round step")


def build_parser(*, pkg_version: str | None = None) -> argparse.ArgumentParser:
    """CLI argument definitions. ``pkg_version`` defaults to installed package
    metadata."""
    v = pkg_version if pkg_version is not None else __version__
    parser = argparse.ArgumentParser(
        prog="collector-to-emulator",
        description="convert kafka-collector output into kafka-emulator "
        "config",
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {v}"
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
            f"(default: {DEFAULT_SCENARIO_NAME})"
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
        "-g",
        "--sleep-gap",
        dest="sleep_gap_ms",
        metavar="MS",
        type=_parse_sleep_gap_ms,
        default=SLEEP_GAP_THRESHOLD_MS,
        help=(
            "emit a sleep step when the gap between timestamps exceeds "
            f"this many milliseconds (default: {SLEEP_GAP_THRESHOLD_MS})"
        ),
    )
    parser.add_argument(
        "-c",
        "--sleep-cap",
        dest="sleep_cap_ms",
        metavar="MS",
        type=_parse_sleep_cap_ms,
        default=SLEEP_DURATION_CAP_MS,
        help=(
            "maximum sleep duration in milliseconds when inserting a sleep "
            f"step (default: {SLEEP_DURATION_CAP_MS})"
        ),
    )
    parser.add_argument(
        "-r",
        "--round",
        dest="sleep_round_ms",
        metavar="MS",
        type=_parse_sleep_round_ms,
        default=SLEEP_ROUND_MS,
        help=(
            "round each sleep duration to the nearest multiple of this many "
            f"milliseconds (default: {SLEEP_ROUND_MS}, no rounding)"
        ),
    )
    parser.add_argument(
        "jsonl",
        nargs="?",
        metavar="JSONL",
        help="JSONL file (if stdin is a TTY and -i omitted)",
    )
    return parser


def _convert_and_write_scenario(
    args: argparse.Namespace,
    stream: TextIO,
    *,
    streams: CliStreams,
) -> int:
    """Read JSONL from ``stream``, emit templates and scenario; return
    ``EXIT_OK`` or ``EXIT_ERROR``."""
    try:
        records = list(iter_jsonl_records(stream))
        templates_dir = (
            Path(args.template_dir)
            if args.template_dir is not None
            else _TEMPLATES_DIR
        )
        dict_records = write_templates_from_records(records, templates_dir)
        scenario_name = args.scenario_name or DEFAULT_SCENARIO_NAME
        scenario_text = build_scenario_yaml(
            dict_records,
            templates_dir,
            scenario_name=scenario_name,
            sleep_timing=SleepTiming(
                gap_threshold_ms=args.sleep_gap_ms,
                duration_cap_ms=args.sleep_cap_ms,
                round_ms=args.sleep_round_ms,
            ),
        )
        scenario_path = (
            Path(args.scenario)
            if args.scenario is not None
            else _SCENARIO_PATH
        )
        out, scenario_tty = _resolve_scenario_stdout_tty(
            streams.stdout, streams.stdout_is_tty
        )
        write_scenario_output(
            scenario_text,
            scenario_path=scenario_path,
            stdout=out,
            stdout_is_tty=scenario_tty,
        )
    except ValueError as e:
        return _main_fail(e, stderr=streams.stderr, code=EXIT_ERROR)
    return EXIT_OK


def main(
    args: argparse.Namespace,
    *,
    streams: CliStreams | None = None,
) -> int:
    """Run conversion pipeline; return ``EXIT_OK``, ``EXIT_ERROR``, or
    ``EXIT_USAGE``."""
    s = streams if streams is not None else CliStreams()
    try:
        stream, must_close = open_jsonl_source(
            args, stdin=s.stdin, stdin_is_tty=s.stdin_is_tty
        )
    except OSError as e:
        return _main_fail(e, stderr=s.stderr, code=EXIT_ERROR)
    except ValueError as e:
        return _main_fail(e, stderr=s.stderr, code=EXIT_USAGE)

    try:
        return _convert_and_write_scenario(args, stream, streams=s)
    finally:
        if must_close:
            stream.close()


def run(
    *,
    argv: Sequence[str] | None = None,
    streams: CliStreams | None = None,
) -> None:
    """Parse ``argv``, run ``main``, and ``sys.exit`` on non-``EXIT_OK``."""
    args = build_parser().parse_args(argv)
    code = main(args, streams=streams)
    if code != EXIT_OK:
        sys.exit(code)


if __name__ == "__main__":
    run()
