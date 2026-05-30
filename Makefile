.PHONY: build clean

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null)

build:
	go build -ldflags "-X github.com/boringsql/dryrun/internal/buildinfo.Version=$(VERSION)" -o bin/dryrun ./cmd/dryrun

clean:
	rm -rf bin/
