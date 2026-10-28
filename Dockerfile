# paxosd image: a static binary on distroless (DECISIONS.md entry 30).
#
#   docker build -t paxos-txn-store:dev .
#
# Base images come from mirror.gcr.io (Google's cache of Docker Hub) to
# avoid Docker Hub rate limits in CI. Behind a TLS-intercepting proxy, pass
# its CA as a build secret: --secret id=ca,src=/path/to/ca.pem.
# The base images are written out in the FROM lines (not ARGs) so
# Dependabot can bump them (.github/dependabot.yml). The builder is pinned
# by digest to the Go patch release in go.mod's go directive; the two move
# together (DECISIONS.md entry 38).
FROM mirror.gcr.io/library/golang:1.26.10@sha256:c141012f2b79382f9cb7438104ae0cd324cc548c1a605f393ef7402b87a1bd8b AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export SSL_CERT_FILE=/run/secrets/ca; fi; \
    GOTOOLCHAIN=local go mod download
COPY . .
# CGO_ENABLED=0: the SQLite driver is pure Go, so the binary is fully
# static and needs no libc in the runtime image.
RUN CGO_ENABLED=0 GOTOOLCHAIN=local go build -trimpath -ldflags="-s -w" -o /out/paxosd ./cmd/paxosd \
    && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/paxosd /paxosd
# The data directory belongs to the nonroot user (65532) so a fresh named
# volume mounted there is writable.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
VOLUME ["/data"]
# 7001: peer and client gRPC. 9100: /metrics, /healthz, /readyz.
EXPOSE 7001 9100
# paxosd stops gracefully on SIGTERM (the default stop signal).
STOPSIGNAL SIGTERM
ENTRYPOINT ["/paxosd"]
