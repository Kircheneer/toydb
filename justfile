set positional-arguments

test:
    uv run pytest

ruff:
    uv run ruff format .
    uv run ruff check . --fix

shell:
    uv run python

mypy:
    uv run mypy -p toydb

serve:
    uv run fastapi dev ./toydb/app.py
