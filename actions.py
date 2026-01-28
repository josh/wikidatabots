import sys
import warnings
from collections.abc import Generator
from contextlib import contextmanager
from threading import Lock, local
from warnings import warn

from tqdm import tqdm

_GROUP_LOCK = Lock()
_THREAD_LOCAL = local()


@contextmanager
def log_group(name: str) -> Generator[None, None, None]:
    if hasattr(_THREAD_LOCAL, "actions_log_group"):
        current_name: str = _THREAD_LOCAL.actions_log_group
        raise RuntimeError(f"Can't nest '{name}' log group inside '{current_name}'")

    with _GROUP_LOCK:
        try:
            _THREAD_LOCAL.actions_log_group = name
            tqdm.write(f"::group::{name}", file=sys.stderr)
            yield
        finally:
            tqdm.write("::endgroup::", file=sys.stderr)
            del _THREAD_LOCAL.actions_log_group


def _formatwarning(
    message: Warning | str,
    category: type[Warning],
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


__all__ = ["log_group", "warn"]
