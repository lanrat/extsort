
ALL_SOURCES := $(shell find . -type f -name '*.go')

.PHONY: fmt check test cover coverhtml

test:
	go test 

fmt:
	gofmt -s -w -l .

coverage.out: $(ALL_SOURCES)
	go test -coverprofile=coverage.out ./...

cover: coverage.out
	go tool cover -func=coverage.out

coverhtml: coverage.out
	go tool cover -html=coverage.out

check:
	golangci-lint run || true
	staticcheck -unused.whole-program -checks all ./...
