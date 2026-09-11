# Multi-stage build for the snuffle binary. Produces a minimal, non-root image
# the PostHog metrics service can deploy. The build stage needs Go >= the
# version pinned in go.mod.

FROM golang:1.26-bookworm AS build
WORKDIR /src

# Cache module downloads across builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG GIT_SHA=unknown
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X github.com/PostHog/snuffle/internal/snuffle.buildRevision=${GIT_SHA}" \
    -o /out/snuffle ./cmd/snuffle

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/snuffle /snuffle
USER nonroot
EXPOSE 9091
ENTRYPOINT ["/snuffle"]
