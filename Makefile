.PHONY: build build-static smoke-static docker clean

VERSION := $(or $(shell git describe --tags --always --dirty 2>/dev/null),dev)
LDFLAGS := -X github.com/boringsql/dryrun/internal/buildinfo.Version=$(VERSION)

GO_VERSION := $(shell awk '/^go /{print $$2}' go.mod)
ARCH ?= $(shell docker version -f '{{.Server.Arch}}')
STATIC_BIN = dist/linux/$(ARCH)/dryrun
BASE_IMAGE := $(shell awk '/^FROM /{print $$2}' Dockerfile)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/dryrun ./cmd/dryrun

# static musl build, also used by release.yml
# stack-size: musl threads get 128k by default, pg_query recursion segfaults on big exprs
build-static:
	mkdir -p $(dir $(STATIC_BIN))
	docker run --rm --platform linux/$(ARCH) \
		-v "$(CURDIR):/src" -w /src \
		-e CGO_LDFLAGS=-Wl,-z,stack-size=8388608 \
		-v dryrun-gomod:/go/pkg/mod -v dryrun-gocache:/root/.cache/go-build \
		golang:$(GO_VERSION)-alpine sh -ec '\
			trap "chown -R $(shell id -u):$(shell id -g) dist" EXIT; \
			apk add --no-cache build-base >/dev/null; \
			CGO_ENABLED=1 go build -buildvcs=false -trimpath -tags netgo,osusergo \
				-ldflags "-s -w -linkmode external -extldflags -static $(LDFLAGS)" \
				-o $(STATIC_BIN) ./cmd/dryrun'
	file $(STATIC_BIN) | grep -q 'statically linked'

# deep.sql = catches the stack thing above
smoke-static:
	rm -rf dist/smoke && cp -R examples/demo dist/smoke
	python3 -c "print('ALTER TABLE t ADD COLUMN c int DEFAULT (' + '+'.join(['1'] * 2000) + ');')" \
		> dist/smoke/deep.sql
	docker run --rm --platform linux/$(ARCH) -u $(shell id -u):$(shell id -g) \
		-v "$(CURDIR)/dist/smoke:/work" -w /work \
		-v "$(CURDIR)/$(STATIC_BIN):/dryrun:ro" --entrypoint /dryrun $(BASE_IMAGE) \
		check migration deep.sql >/dev/null

docker: build-static
	docker buildx build --platform linux/$(ARCH) -f Dockerfile -t dryrun:$(VERSION) --load dist

clean:
	rm -rf bin/ dist/
