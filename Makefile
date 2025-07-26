default: test

include version.mk

ALL_SOURCES := $(shell find . -type f -name '*.go')

.PHONY: fmt check test cover coverhtml example

test:
	go test -v ./...
	@echo "< ALL TESTS PASS >"

example:
	go run example/example.go >/dev/null

update-deps: go.mod
	GOPROXY=direct go get -u ./...
	go mod tidy

deps: go.mod
	go mod download

fmt:
	gofmt -s -w -l .

coverage.out: $(ALL_SOURCES)
	go test -coverprofile=coverage.out ./...

cover: coverage.out
	go tool cover -func=coverage.out

coverhtml: coverage.out
	go tool cover -html=coverage.out

check:
	golangci-lint run ./... || true
	staticcheck -checks all ./...
