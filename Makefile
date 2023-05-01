.PHONY: run install-hatch overwrite-pre-commit install test


run:
	export FORMAT_JSON_LOGS="1"

install-hatch:
	pip3 install hatch

# Edit your local pre-commit hook file to use Hatch when executing.
overwrite-pre-commit:
	hatch run pre-commit:pre-commit install
	hatch run pre-commit:sed -i -e "s/exec /exec hatch run pre-commit:/g" .git/hooks/pre-commit

install: install-hatch overwrite-pre-commit

test:
	export FORMAT_JSON_LOGS="1" && hatch run pytest:pytest app/tests