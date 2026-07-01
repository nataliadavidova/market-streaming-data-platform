.PHONY: install-dev test status

install-dev:
	python -m pip install -e ".[dev]"

test:
	python -m pytest

status:
	git status --short
