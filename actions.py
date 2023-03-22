import sys
import warnings
from typing import Type
from warnings import warn


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


__all__ = ["warn", "print_warning"]
