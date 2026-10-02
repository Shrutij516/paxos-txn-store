GO ?= go
COVER_MIN := 85

.PHONY: test test-full lint race cover bench proto

# Quick local run: seeded suites use 100 seeds instead of 1000.
test:
	$(GO) test -short ./...

# Same seed counts as CI.
test-full:
	$(GO) test ./...

lint:
	$(GO) vet ./...
	golangci-lint run ./...

# Race detector on the short seed counts, as in CI.
race:
	$(GO) test -race -short ./...

# Runs the full test suite (1000 seeds, no -race) once with coverage, then
# fails if statement coverage of internal/paxos drops below COVER_MIN percent.
cover:
	$(GO) test -coverprofile=coverage.out ./...
	@{ head -n 1 coverage.out; grep '/internal/paxos/' coverage.out; } > coverage-paxos.out
	@pct=$$($(GO) tool cover -func=coverage-paxos.out | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "internal/paxos coverage: $$pct% (minimum $(COVER_MIN)%)"; \
	awk -v p="$$pct" -v m="$(COVER_MIN)" 'BEGIN { exit (p+0 < m+0) ? 1 : 0 }' || \
		{ echo "coverage below $(COVER_MIN)%"; exit 1; }

# Persist latency with synchronous=FULL vs NORMAL.
bench:
	$(GO) test -run xxx -bench . ./internal/storage/

# Regenerate Go code from proto/. Needs protoc, protoc-gen-go v1.36.6 and
# protoc-gen-go-grpc v1.5.1 on PATH (see docs/running.md).
PROTOS := proto/paxos/v1/paxos.proto proto/kv/v1/kv.proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative $(PROTOS)
