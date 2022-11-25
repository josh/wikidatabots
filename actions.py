# pyright: strict

import logging
import os
from typing import Callable, Iterable
import uuid
import inspect

logging.basicConfig(
    level=logging.DEBUG,
    format="::%(levelname)s file=%(pathname)s,line=%(lineno)s::%(message)s",
    datefmt="%Y-%m-%d %H:%M",
)

logging.addLevelName(logging.ERROR, "error")
logging.addLevelName(logging.DEBUG, "debug")
logging.addLevelName(logging.WARNING, "warning")
logging.addLevelName(logging.INFO, "notice")

DEBUG = os.environ.get("RUNNER_DEBUG", "") != ""
OUTPUT_FILENAME = os.environ.get("GITHUB_OUTPUT", "/dev/stdout")
OUTPUT_DELIMITER = f"ghadelimiter-{uuid.uuid4()}"


def get_input(name: str) -> str:
    return os.environ[f"INPUT_{name}"]


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


def run_step(func: Callable[..., dict[str, str] | None]) -> None:
    kwargs: dict[str, str] = {}
    sig = inspect.signature(func)
    for parameter in sig.parameters:
        kwargs[parameter] = get_input(parameter)
    result = func(**kwargs)
    if result:
        set_outputs(result.items())
