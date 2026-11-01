FROM golang:1.24.2-bookworm AS build

ARG MINIO_COMMIT=0d7408fc9969caf07de6a8c3a84f9fbb10a6739e
ENV CGO_ENABLED=0 GOMAXPROCS=2 GOTOOLCHAIN=local MINIO_RELEASE=RELEASE
WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2025-04-22T22-12-26Z \
        https://github.com/minio/minio.git . \
    && test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download \
    && go mod verify \
    && go build -mod=readonly -p 2 -trimpath \
        -ldflags "$(go run buildscripts/gen-ldflags.go 2025-04-22T22:12:26Z)" \
        -o /out/minio . \
    && /out/minio --version

FROM python:3.11-slim-bookworm
LABEL org.opencontainers.image.title="Lab 3 MinIO source build" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="0d7408fc9969caf07de6a8c3a84f9fbb10a6739e" \
      org.opencontainers.image.version="RELEASE.2025-04-22T22-12-26Z" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /src/LICENSE /usr/share/doc/minio/LICENSE
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
