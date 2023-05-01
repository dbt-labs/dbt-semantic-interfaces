.PHONY: run install-hatch overwrite-pre-commit install test


run:
	export FORMAT_JSON_LOGS="1"

install-hatch:
	pip3 install hatch

test:
	export FORMAT_JSON_LOGS="1" && hatch run pytest:pytest app/tests