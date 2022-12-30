# pyright: strict

import os
import sys
import uuid
from typing import Any, Iterable

GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS", "") == "true"
DEBUG = os.environ.get("RUNNER_DEBUG", "") != ""
OUTPUT_FILENAME = os.environ.get("GITHUB_OUTPUT", "/dev/stdout")
OUTPUT_DELIMITER = f"ghadelimiter-{uuid.uuid4()}"


class LogGroup:
    def __init__(self, title: str):
        self.title = title

    def __enter__(self):
        if GITHUB_ACTIONS:
            print(f"::group::{self.title}", file=sys.stderr)

    def __exit__(self, type, value, traceback):  # type: ignore
        if GITHUB_ACTIONS:
            print("::endgroup::", file=sys.stderr)

    def __call__(self, func):  # type: ignore
        def wrapper(*args, **kwargs) -> Any:  # type: ignore
            with self:
                return func(*args, **kwargs)  # type: ignore

        return wrapper  # type: ignore


def log_group(title: str) -> LogGroup:
    return LogGroup(title)


def set_output(key: str, value: str) -> None:
    set_outputs([(key, value)])


def set_outputs(outputs: Iterable[tuple[str, str]]) -> None:
    with open(OUTPUT_FILENAME, "a") as f:
        for key, value in outputs:
            f.write(key)
            f.write("<<")
            f.write(OUTPUT_DELIMITER)
            f.write("\n")
            f.write(value)
            f.write("\n")
            f.write(OUTPUT_DELIMITER)
            f.write("\n")
