VENV ?= $(shell [ -d "$(CURDIR)/.venv" ] && echo "$(CURDIR)/.venv")

PYTHON  := $(if $(VENV),$(VENV)/bin/python,python3)
DTERM   := $(if $(VENV),$(VENV)/bin/dterm,dterm)

.PHONY: start-router start-worker

start-router:
	@$(DTERM) start-router --rpc-host=localhost --rpc-port=9999 --web-host=localhost --web-port=8888

start-worker:
	@$(DTERM) start-worker --rpc-host=localhost --rpc-port=9999
