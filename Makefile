# Makefile for graphdb

GO_MODULE ?= on
VERSION ?= $(shell git describe --always --tags)
COMMIT ?= $(shell git rev-parse --short=12 HEAD)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
REGISTRY_IMAGE ?= registry.jiagouyun.com/cloudcare-forethought/graphdb
PUBREPO_IMAGE ?= pubrepo.jiagouyun.com/cloudcare-forethought/graphdb
RUNTIME_BASE_IMAGE ?= registry.jiagouyun.com/basis/kodo-basis:kodo-basis-2026-03-25
UOS_RUNTIME_BASE_IMAGE ?= registry.jiagouyun.com/basis/uos-kodo-basis:uos-kodo-basis-2025-01-07
IMAGE_CMD_DIR := ./image/cmd
BUILDINFO_PACKAGE := gitlab.jiagouyun.com/guance/graphdb/internal/buildinfo
GO_BUILD_FLAGS := -mod=readonly -gcflags="-e" -ldflags="-w -s -X $(BUILDINFO_PACKAGE).Version=$(VERSION) -X $(BUILDINFO_PACKAGE).Commit=$(COMMIT) -X $(BUILDINFO_PACKAGE).Date=$(BUILD_DATE)"

.PHONY: all build build-amd64 build-arm64 build-linux-amd64 build-linux-arm64 clean deps tidy gofmt lint fix_lint pub_registry_image pub_pubrepo_image pub_pubrepo_uos_image

all: build

build: build-amd64 build-arm64

build-amd64: build-linux-amd64

build-arm64: build-linux-arm64

build-linux-amd64: deps
	@echo "====== building graphdb for linux-amd64 ======"
	@mkdir -p $(IMAGE_CMD_DIR)/amd64
	GO111MODULE=$(GO_MODULE) CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(IMAGE_CMD_DIR)/amd64/graphdb ./cmd/graphdb

build-linux-arm64: deps
	@echo "====== building graphdb for linux-arm64 ======"
	@mkdir -p $(IMAGE_CMD_DIR)/arm64
	GO111MODULE=$(GO_MODULE) CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(GO_BUILD_FLAGS) -o $(IMAGE_CMD_DIR)/arm64/graphdb ./cmd/graphdb

pub_registry_image:
	docker buildx build --platform linux/arm64,linux/amd64 \
		--build-arg BASE_IMAGE=$(RUNTIME_BASE_IMAGE) \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(PUBREPO_IMAGE):$(VERSION) \
		-f ./image/Dockerfile . --push

pub_pubrepo_image:
	docker buildx build --platform linux/arm64,linux/amd64 \
		--build-arg BASE_IMAGE=$(RUNTIME_BASE_IMAGE) \
		-t pubrepo.jiagouyun.com/$(REPO) \
		-f ./image/Dockerfile . --push

pub_pubrepo_uos_image:
	docker buildx build --platform linux/arm64,linux/amd64 \
		--build-arg BASE_IMAGE=$(UOS_RUNTIME_BASE_IMAGE) \
		-t pubrepo.jiagouyun.com/$(REPO) \
		-f ./image/Dockerfile.uos . --push

deps:
	GO111MODULE=$(GO_MODULE) go mod download

tidy:
	GO111MODULE=$(GO_MODULE) go mod tidy

gofmt:
	@gofmt -w $(shell find . -type f -name '*.go' ! -path './vendor/*' ! -path './.git/*')

lint:
	@truncate -s 0 lint.err
	@golangci-lint --version | tee -a lint.err
	@GO111MODULE=$(GO_MODULE) golangci-lint run | tee -a lint.err

fix_lint:
	@truncate -s 0 lint.err
	@golangci-lint --version | tee -a lint.err
	@GO111MODULE=$(GO_MODULE) golangci-lint run --fix | tee -a lint.err

clean:
	rm -rf $(IMAGE_CMD_DIR)
	rm -rf image/linux-amd64
	rm -rf image/linux-arm64
