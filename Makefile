default: test

include version.mk

ALL_SOURCES := $(shell find . -type f -name '*.go')

.PHONY: fmt check test cover coverhtml examples

test:
	go test -timeout=90s -v ./...
	@echo "< ALL TESTS PASS >"

update-deps: go.mod
	GOPROXY=direct go get -u ./...
	go mod tidy

deps: go.mod
	go mod download

fmt:
	go fmt ./...

coverage.out: $(ALL_SOURCES)
	go test -coverprofile=coverage.out ./...

cover: coverage.out
	go tool cover -func=coverage.out

coverhtml: coverage.out
	go tool cover -html=coverage.out

check:
	golangci-lint run ./... || true
	staticcheck -checks all ./...

benchmark:
	./run_benchmarks.sh

examples:
	@for dir in examples/*/; do \
		if [ -f "$$dir"*.go ]; then \
			echo "Running example in $$dir"; \
			(cd "$$dir" && go run *.go > /dev/null); \
		fi; \
	done
