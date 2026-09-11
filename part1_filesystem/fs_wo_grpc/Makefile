PYTHON ?= python3

.PHONY: all install-deps run-server run-client test clean help

all: help

install-deps:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install cryptography

run-server:
	$(PYTHON) server.py

run-client:
	$(PYTHON) client.py

clean:
	rm -rf __pycache__
	rm -rf temp_cache_files
	rm -rf server_data

help:
	@echo "Makefile targets:"
	@echo "  make install-deps Install Python dependency (cryptography)"
	@echo "  make run-server   Start the non-gRPC server (blocks)"
	@echo "  make run-client   Run sample client script"
	@echo "  make clean        Remove caches and bytecode"
