import subprocess
import sys
import warnings

import actions


def test_formatwarning() -> None:
    expected = "::warning file=example.py,line=42,title=UserWarning::hello\n"
    assert actions._formatwarning("hello", UserWarning, "example.py", 42) == expected
    assert (
        warnings.formatwarning(
            message=UserWarning("hello"),
            category=UserWarning,
            filename="example.py",
            lineno=42,
            line="source line",
        )
        == expected
    )


def test_warning_output() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import actions; import warnings; "
                "warnings.warn_explicit('hello', UserWarning, 'example.py', 42)"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stderr == (
        "::warning file=example.py,line=42,title=UserWarning::hello\n"
    )
