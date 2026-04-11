import argparse
import sys
from importlib.metadata import version
from pathlib import Path
from typing import TextIO

from collector_to_emulator.jsonl import iter_jsonl_records
from collector_to_emulator.scenario_export import (
    DEFAULT_SCENARIO_NAME,
    SLEEP_DURATION_CAP_MS,
    SLEEP_GAP_THRESHOLD_MS,
    SLEEP_ROUND_MS,
    build_scenario_yaml,
    write_templates_from_records,
)

_TEMPLATES_DIR = Path("templates")
_SCENARIO_PATH = Path("scenario.yaml")


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def write_scenario_output(
    content: str,
    *,
    scenario_path: Path | None,
    stdout_is_tty: bool,
) -> None:
    """Write scenario YAML to stdout when piped/redirected; else to file."""
    if not stdout_is_tty:
        sys.stdout.write(content)
        return
    out = scenario_path if scenario_path is not None else _SCENARIO_PATH
    out.write_text(content, encoding="utf-8")


def _scenario_writes_to_file() -> bool:
    """True when scenario YAML should be written to disk; False when piped
    stdout."""
    return sys.stdout.isatty()


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
        description="convert kafka-collector output into kafka-emulator "
        "config",
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
        type=int,
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
        type=int,
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
        type=int,
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

    args = parser.parse_args()

    if args.sleep_round_ms < 1:
        print_to_stderr_and_exit(
            ValueError("sleep round step must be at least 1"),
            2,
        )
        return

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
        dict_records = write_templates_from_records(records, templates_dir)
        scenario_name = args.scenario_name or DEFAULT_SCENARIO_NAME
        scenario_text = build_scenario_yaml(
            dict_records,
            templates_dir,
            scenario_name=scenario_name,
            sleep_gap_threshold_ms=args.sleep_gap_ms,
            sleep_duration_cap_ms=args.sleep_cap_ms,
            sleep_round_ms=args.sleep_round_ms,
        )
        scenario_path = Path(args.scenario) if args.scenario else None
        write_scenario_output(
            scenario_text,
            scenario_path=scenario_path,
            stdout_is_tty=_scenario_writes_to_file(),
        )
    except ValueError as e:
        print_to_stderr_and_exit(e, 1)
        return
    finally:
        if must_close:
            stream.close()


if __name__ == "__main__":
    run()
