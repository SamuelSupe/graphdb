# Makefile for kodo
# date: Sun Aug 15 10:10:23 CST 2021

.PHONY: arm64 amd64 dql gofmt install_gcc

DATE := $(shell date -u +'%Y-%m-%d %H:%M:%S')
GIT_VERSION := $(shell git describe --always --tags)
GOVERSION := $(shell go version)
COMMIT := $(shell git rev-parse --short HEAD)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
DEP_IMAGE=golang:latest
GO_MODULE=on
HOST_OS := $(shell go env GOOS)
HOST_ARCH := $(shell go env GOARCH)
AMD64_CC=
ARM64_CC=
AMD64_KODO_CGO_ENABLED=0
ARM64_KODO_CGO_ENABLED=0

ifeq ($(HOST_OS),linux)
     ifneq ($(shell which x86_64-linux-gnu-gcc 2>/dev/null),)
        AMD64_CC=x86_64-linux-gnu-gcc
        AMD64_KODO_CGO_ENABLED=1
     endif

     ifneq ($(shell which aarch64-linux-gnu-gcc 2>/dev/null),)
        ARM64_CC=aarch64-linux-gnu-gcc
        ARM64_KODO_CGO_ENABLED=1
     endif

     ifneq ($(shell which gcc 2>/dev/null),)
     	ifeq ($(HOST_ARCH)_$(AMD64_KODO_CGO_ENABLED),amd64_0)
			AMD64_CC=gcc
			AMD64_KODO_CGO_ENABLED=1
     	endif
     	ifeq ($(HOST_ARCH)_$(ARM64_KODO_CGO_ENABLED),arm64_0)
			ARM64_CC=gcc
			ARM64_KODO_CGO_ENABLED=1
     	endif
     endif
endif

define GIT_INFO
//nolint
package git

const (
	BuildAt  string = "$(DATE)"
	Version  string = "$(GIT_VERSION)"
	Golang   string = "$(GOVERSION)"
	Sha1     string = "$(COMMIT)"
	Branch   string = "$(BRANCH)"
);
endef
export GIT_INFO

default: all

VERSION?=$(shell git describe --always --tags)

define build
	@echo "======  building graphdb for $(1)-$(2) ======"
	@kodo_cgo_enabled=0; kodo_cc=""; \
    if test "$(1)" = "linux" -a "$(2)" = "amd64"; then  \
	  kodo_cgo_enabled=$(AMD64_KODO_CGO_ENABLED); kodo_cc="$(AMD64_CC)"; \
    elif test "$(1)" = "linux" -a "$(2)" = "arm64"; then \
	  kodo_cgo_enabled=$(ARM64_KODO_CGO_ENABLED); kodo_cc="$(ARM64_CC)"; \
    fi; \
	 GO111MODULE=$(GO_MODULE) CC="$${kodo_cc}" CGO_ENABLED="$${kodo_cgo_enabled}" GOOS=$(1) GOARCH=$(2) go build -gcflags="-e" -o ./image/$(1)-$(2)/graphdb -ldflags "-w -s" ./cmd/graphdb
endef

all: linux

build: build-amd64 build-arm64

build-amd64: deps
	@echo "======  building kodo for $(GOOS)-amd64 ======"
	CC="$(AMD64_CC)" CGO_ENABLED="0" GOARCH=amd64 go build -gcflags="-e" -o ./image/cmd/amd64/graphdb -ldflags "-w -s" ./cmd/graphdb
	

build-arm64: deps
	@echo "======  building kodo for $(GOOS)-arm64 ======"
	CC="$(ARM64_CC)" CGO_ENABLED="0" GOARCH=arm64 go build -gcflags="-e" -o ./image/cmd/arm64/graphdb -ldflags "-w -s" ./cmd/graphdb



darwin: deps
	$(call build,darwin,amd64)
	$(call build,darwin,arm64)

linux: deps
	$(call build,linux,amd64)
	$(call build,linux,arm64)

pub_registry_image:
	docker buildx build --platform linux/arm64,linux/amd64 \
	-t registry.jiagouyun.com/cloudcare-forethought/graphdb:$(VERSION) \
	-t pubrepo.jiagouyun.com/cloudcare-forethought/graphdb:$(VERSION) \
	./image --push

pub_pubrepo_image:
	docker buildx build --build-arg DEP_IMAGE=$(DEP_IMAGE) --platform linux/arm64,linux/amd64 -t pubrepo.jiagouyun.com/$(REPO) -f ./image/Dockerfile . --push

pub_pubrepo_uos_image:
	docker buildx build --build-arg DEP_IMAGE=$(DEP_IMAGE) --platform linux/arm64,linux/amd64 -t pubrepo.jiagouyun.com/$(REPO) -f ./image/Dockerfile.uos . --push

gofmt:
	@GO111MODULE=$(GO_MODULE) gofumpt -w -l $(shell find . -type f -name '*.go'| grep -v "/vendor/\|/.git/\|/git/\|.*_y.go")

prepare:
	@mkdir -p git
	@echo "$$GIT_INFO" > git/git.go
	@GO111MODULE=$(GO_MODULE) go fmt ./...

clean:
	rm -rf image/linux-amd64
	rm -rf image/linux-arm64

deps: prepare tidy #lint

tidy:
	go mod tidy

lint: lint_deps
	@truncate -s 0 lint.err
	@golangci-lint --version | tee -a lint.err
	@GO111MODULE=$(GO_MODULE) golangci-lint run | tee -a lint.err # https://golangci-lint.run/usage/install/#local-installation
	#@staticcheck -tests ./...  | tee static-check.err # go get honnef.co/go/tools/...

fix_lint: lint_deps
	@truncate -s 0 lint.err
	@golangci-lint --version | tee -a lint.err
	@GO111MODULE=$(GO_MODULE) golangci-lint run --fix | tee -a lint.err # https://golangci-lint.run/usage/install/#local-installation

lint_deps: dql_disable_line

dql_disable_line:
	@rm -rf dql/parser/dql.y.go
	@goyacc -l -o dql/parser/dql.y.go dql/parser/dql.y

gosec:
	@rm -rf gosec.out
	@GO111MODULE=$(GO_MODULE) gosec -exclude=G104 -out=gosec.out -conf gosec.json ./...

show_metrics:
	@promlinter list . --add-help -o md --with-vendor --add-module | tee kodo-metrics.md
