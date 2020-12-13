test:
	pip install -e .
	python -m pytest -vv tests/

all: test

.PHONY: all
