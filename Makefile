.DEFAULT_GOAL := help
.PHONY: build test lint integration clean help

## Build the kahouse binary
build:
	go build -o kahouse ./cmd/kahouse

## Run unit tests with race detector (verbose)
test:
	go test -v -race ./...

## Run golangci-lint (https://golangci-lint.run/docs/welcome/install/local/)
lint:
	golangci-lint run ./...

## Run the full docker-compose integration test suite
integration:
	docker compose up --build --abort-on-container-exit --exit-code-from test-producer

## Remove build artifacts
clean:
	rm -f kahouse

## Show available targets
help:
	@awk '/^## /{desc=substr($$0,4)} /^[a-zA-Z_-]+:/{if(desc){printf "  \033[36m%-16s\033[0m %s\n",$$1,desc; desc=""}}' $(MAKEFILE_LIST) | sed 's/://'
