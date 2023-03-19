import sys
import warnings


def formatwarning(message, category, filename, lineno, line=None) -> str:
    return (
        "::warning "
        f"file={filename},line={lineno},title={category.__name__}::"
        f"{str(message)}\n"
    )


def warn(title: str, message: str) -> None:
    print(f"::warning title={title}::{message}", file=sys.stderr)


def install_warnings_hook():
    warnings.formatwarning = formatwarning
