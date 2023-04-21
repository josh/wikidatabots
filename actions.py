import sys
import warnings
from contextlib import contextmanager
from threading import Lock
from typing import Generator, Type
from warnings import warn

_GROUP_LOCK = Lock()


@contextmanager
def log_group(name: str) -> Generator[None, None, None]:
    if _GROUP_LOCK.locked():
        raise RuntimeError("Can't nest log groups")

    with _GROUP_LOCK:
        try:
            print(f"::group::{name}", file=sys.stderr)
            yield
        finally:
            print("::endgroup::", file=sys.stderr)


def print_warning(title: str, message: str) -> None:
    print(f"::warning title={title}::{message}", file=sys.stderr)


def _formatwarning(
    message: str,
    category: Type[Warning],
    filename: str,
    lineno: int,
    line: str | None = None,
) -> str:
    return (
        "::warning "
        f"file={filename},line={lineno},title={category.__name__}::"
        f"{message}\n"
    )


warnings.formatwarning = _formatwarning


__all__ = ["log_group", "print_warning", "warn"]
