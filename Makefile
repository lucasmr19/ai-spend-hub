.PHONY: setup test lint data data-small contracts reference demo

PY ?= python3

setup:
	$(PY) -m pip install -e ".[dev]"

lint:
	ruff check .

test:
	$(PY) -m pytest -q

contracts:
	$(PY) -m contracts.check_compat contracts/inference_event.v1.schema.json \
		contracts/inference_event.v2.schema.json --migration contracts/migrations/v1_to_v2.json

data:
	$(PY) -m generator --preset default --out out/synthetic

data-small:
	$(PY) -m generator --preset small --out out/synthetic

reference:
	$(PY) -m reference.pipeline out/synthetic

demo: data reference
