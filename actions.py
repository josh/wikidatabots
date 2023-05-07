# pyright: strict

import sys
import warnings
from contextlib import contextmanager
from threading import Lock, local
from typing import Generator, Type
from warnings import warn

_GROUP_LOCK = Lock()
_THREAD_LOCAL = local()


@contextmanager
def log_group(name: str | None) -> Generator[None, None, None]:
    if name is None:
        yield
        return

    if hasattr(_THREAD_LOCAL, "actions_log_group"):
        current_name: str = _THREAD_LOCAL.actions_log_group
        raise RuntimeError(f"Can't nest '{name}' log group inside '{current_name}'")

    with _GROUP_LOCK:
        try:
            _THREAD_LOCAL.actions_log_group = name
            print(f"::group::{name}", file=sys.stderr)
            yield
        finally:
            print("::endgroup::", file=sys.stderr)
            del _THREAD_LOCAL.actions_log_group


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
