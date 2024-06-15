.PHONY: run install-hatch overwrite-pre-commit install test lint json_schema

run:
	export FORMAT_JSON_LOGS="1"

install-hatch:
	pip3 install hatch

# This edits your local pre-commit hook file to use Hatch when executing.
overwrite-pre-commit:
	hatch run dev-env:pre-commit install
	hatch run dev-env:sed -i -e "s/exec /exec hatch run dev-env:/g" .git/hooks/pre-commit

install: install-hatch overwrite-pre-commit

test:
	export FORMAT_JSON_LOGS="1" && hatch -v run dev-env:pytest -n auto tests

lint:
	hatch run dev-env:pre-commit run --color=always --all-files

json_schema:
	hatch run dev-env:python dbt_semantic_interfaces/parsing/generate_json_schema_file.py
