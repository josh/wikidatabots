# Agents Guide

This project uses Python 3.12 and manages dependencies with `uv`.

## Setup

Install Python 3.12 or newer.

Install `uv` with:

```sh
$ curl -LsSf https://astral.sh/uv/install.sh | sh
# or
$ pipx install uv
```

Then install dependencies with:

```sh
$ uv sync
```

## Testing

Check code style with ruff:

```sh
$ uv run ruff format --diff .
$ uv run ruff check .
```

Check type correctness with mypy:

```sh
$ uv run mypy .
```

Run the test suite with:

```sh
$ uv run pytest
```

Avoid [pytest's monkeypatching and mocking features](https://docs.pytest.org/en/stable/how-to/monkeypatch.html). Tests may make real network connections to the API service.

## Formatting

You can automatically fix most formatting issues with:

```sh
$ uv run ruff format .
```

Functions should be sorted in dependency order with:

```sh
$ uv tool run ssort .
```

After making changes to `pyproject.toml`, ensure its formatted with `pyproject-fmt`.

```sh
$ uv tool run pyproject-fmt pyproject.toml
```

## Code Quality

Optionally scan for dead code with vulture:

```sh
$ uv tool run vulture --exclude .venv/ .
```

## Comments and Docstrings

Avoid superfluous comments and Python docstrings. Only include them when they add value or clarify complex logic.
