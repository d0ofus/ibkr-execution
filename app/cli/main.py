"""Command-line entrypoint for local operator workflows."""

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""
    parser = argparse.ArgumentParser(prog="ibkr-exec-bot")
    parser.add_argument("--version", action="store_true", help="show version information")
    return parser


def main() -> None:
    """CLI process entrypoint."""
    parser = build_parser()
    _ = parser.parse_args()


if __name__ == "__main__":
    main()
