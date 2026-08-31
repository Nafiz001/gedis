BINDIR := bin
GO     ?= go

.PHONY: all build test race bench lint fmt vet clean run

all: build

build:
	$(GO) build -o $(BINDIR)/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

bench:
	$(GO) test -bench=. -benchmem -run=XXX ./...

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

lint: fmt vet

# Start a server and point the benchmark client at it.
run: build
	$(BINDIR)/gedis-server --addr :6380

clean:
	rm -rf $(BINDIR) coverage.out coverage.html
