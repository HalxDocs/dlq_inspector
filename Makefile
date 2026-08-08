BINARY   := dlq
PKG      := ./...
LD_FLAGS := -s -w -X main.version=$(VERSION)

VERSION  ?= dev

.PHONY: build test vet fmt lint clean install

build:
	go build -ldflags "$(LD_FLAGS)" -o bin/$(BINARY) ./cmd/dlq

test:
	go test $(PKG)

vet:
	go vet $(PKG)

fmt:
	gofmt -l -w .

fmt-check:
	@test -z "$$(gofmt -l . | grep -v '^vendor/')" || (echo "gofmt needed:"; gofmt -l .; exit 1)

lint: vet fmt-check

clean:
	rm -rf bin dist

install:
	go install -ldflags "$(LD_FLAGS)" ./cmd/dlq
