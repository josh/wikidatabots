import warnings


def formatwarning(message, category, filename, lineno, line=None) -> str:
    return (
        "::warning "
        f"file={filename},line={lineno},title={category.__name__}::"
        f"{str(message)}\n"
    )


def install_warnings_hook():
    warnings.formatwarning = formatwarning
