REPOS        := $(HOME)/Development
PYTHON_REPO  := $(REPOS)/vgi-rpc
GO_REPO      := $(REPOS)/vgi-rpc-go
TS_REPO      := $(REPOS)/vgi-rpc-typescript

.PHONY: status test-all test-python test-go test-ts describe

status:
	@echo "=== Python ==="
	@grep '^version' $(PYTHON_REPO)/pyproject.toml | head -1
	@cd $(PYTHON_REPO) && git log --oneline -1
	@grep '^DESCRIBE_VERSION = ' $(PYTHON_REPO)/vgi_rpc/introspect.py
	@echo ""
	@echo "=== Go ==="
	@cd $(GO_REPO) && git describe --tags --always
	@cd $(GO_REPO) && git log --oneline -1
	@grep '	DescribeVersion = ' $(GO_REPO)/vgirpc/describe.go
	@echo ""
	@echo "=== TypeScript ==="
	@grep '"version"' $(TS_REPO)/package.json | head -1
	@cd $(TS_REPO) && git log --oneline -1
	@grep '^export const DESCRIBE_VERSION = ' $(TS_REPO)/src/constants.ts

describe:
	uv run python describe_diff.py

test-all: test-python test-go test-ts

test-python:
	@echo "=== Python Tests ==="
	cd $(PYTHON_REPO) && uv run pytest tests/ -q --tb=short

test-go:
	@echo "=== Go Conformance ==="
	cd $(GO_REPO) && make test

test-ts:
	@echo "=== TypeScript Conformance ==="
	cd $(TS_REPO) && make test-conformance
