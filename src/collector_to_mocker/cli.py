import argparse
import sys
from importlib.metadata import version


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def run() -> None:
    __version__: str = version("collector-to-mocker")

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="convert kafka-collector output into kafka-mocker config"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    parser.parse_args()
