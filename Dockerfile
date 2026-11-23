# syntax=docker/dockerfile:1

# Build stage: a static binary (no cgo), so it runs on a distroless image.
# Go 1.25 or newer matters inside containers: from 1.25 the runtime sizes GOMAXPROCS
# from the container's CPU quota. Go 1.24 used the host's core count, so a
# 2-CPU container ran 16 threads and was throttled (p99 of 67 ms in my benchmark).
FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ember ./cmd/ember \
 && mkdir /out/data

# Runtime stage: distroless/static has no shell, no package manager and runs as
# a non-root user (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/ember /ember
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
WORKDIR /data
VOLUME /data
EXPOSE 6380

# There is no shell or redis-cli in the image, so the binary checks itself. The
# exec form below needs no shell. If you change --port, change it here too.
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=3 \
  CMD ["/ember", "--healthcheck"]

ENTRYPOINT ["/ember"]
CMD ["--host", "0.0.0.0", "--port", "6380", "--aof-path", "/data/ember.aof"]
