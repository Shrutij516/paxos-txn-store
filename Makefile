GO ?= go
COVER_PKG := ./internal/paxos
COVER_MIN := 85

.PHONY: test lint race cover

test:
	$(GO) test ./...

lint:
	$(GO) vet ./...
	golangci-lint run ./...

race:
	$(GO) test -race ./...

# Fails if statement coverage of internal/paxos drops below COVER_MIN percent.
cover:
	$(GO) test -race -coverprofile=coverage.out $(COVER_PKG)
	@pct=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "internal/paxos coverage: $$pct% (minimum $(COVER_MIN)%)"; \
	awk -v p="$$pct" -v m="$(COVER_MIN)" 'BEGIN { exit (p+0 < m+0) ? 1 : 0 }' || \
		{ echo "coverage below $(COVER_MIN)%"; exit 1; }
