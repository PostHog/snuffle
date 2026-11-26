# Images for the kinoko workload; build context is the repo root (see compose.yaml).
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /snuffle ./cmd/snuffle

FROM alpine:3.22 AS snuffle
COPY --from=build /snuffle /usr/local/bin/snuffle
ENTRYPOINT ["/usr/local/bin/snuffle"]

# ClickHouse with Snuffle's log schema created on first start.
FROM clickhouse/clickhouse-server:26.8.3.105 AS clickhouse
COPY scripts/create_logs_snuffle_schema.sql /docker-entrypoint-initdb.d/
COPY kinoko/clickhouse-lean.xml /etc/clickhouse-server/config.d/
