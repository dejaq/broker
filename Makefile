GOOS ?=
GOARCH ?=
GO111MODULE ?= on

ifeq ($(GOOS),)
  GOOS = $(shell go version | awk -F ' ' '{print $$NF}' | awk -F '/' '{print $$1}')
endif

ifeq ($(GOARCH),)
  GOARCH = $(shell go version | awk -F ' ' '{print $$NF}' | awk -F '/' '{print $$2}')
endif

GO := GO111MODULE=$(GO111MODULE) go

.DEFAULT_GOAL := lint

.PHONY: show-env
show-env:
	@echo ">> show env"
	@echo "   GOOS              = $(GOOS)"
	@echo "   GOARCH            = $(GOARCH)"
	@echo "   GO111MODULE       = $(GO111MODULE)"

.PHONY: lint
lint: show-env
	@echo ">> apply linters"
	cd ./storage && golangci-lint run

#.PHONY: build
#build: show-env
#	@echo ">> building binaries"
#	$(GO) build cmd/main.go

#.PHONY: install
#install: show-env
#	@echo ">> installing binaries"
#	$(GO) install cmd/main.go
