SHELL := /bin/bash
PY := python
PIP := pip

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo ""
	@echo "Targets:"
	@echo "  make setup   - create venv + install dev deps"
	@echo "  make lint    - run ruff"
	@echo "  make test    - run pytest"
	@echo "  make run     - run pipeline (default: CLI help)"
	@echo "  make blocks  - run run_block*.sh if present"
	@echo ""

.PHONY: setup
setup:
	$(PY) -m venv .venv
	@source .venv/bin/activate && \
	$(PY) -m pip install --upgrade pip && \
	$(PIP) install -e ".[dev]"
	@echo "OK: venv created and dev deps installed"

.PHONY: lint
lint:
	@ruff check .

.PHONY: test
test:
	@pytest

# Default run: show CLI help so it never fails for new users.
# You can override like: make run ARGS="--mode eu --start 2024-01-01 --end 2024-12-31"
.PHONY: run
run:
	@energy $(ARGS)

.PHONY: blocks
blocks:
	@set -e; \
	if ls run_block*.sh >/dev/null 2>&1; then \
	  for f in run_block*.sh; do echo "Running $$f"; bash "$$f"; done; \
	else \
	  echo "No run_block*.sh scripts found."; \
	fi
