.PHONY: run install-hatch overwrite-pre-commit install test

run:
	export FORMAT_JSON_LOGS="1"

install-hatch:
	pip3 install hatch

install:
	install-hatch overwrite-pre-commit

test:
	export FORMAT_JSON_LOGS="1" && hatch run pytest:pytest future_directory

lint:
	hatch run pre-commit:pre-commit run --show-diff-on-failure --color=always --all-files
