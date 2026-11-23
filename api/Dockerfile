# syntax=docker/dockerfile:1
FROM golang:1.27 AS build

ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w" \
      -o /out/attesta-api ./api/cmd/attesta-api

# Static, non-root runtime. The API writes nothing to disk: scaffolds are
# rendered in memory and downloaded as archives, so the root filesystem can
# be mounted read-only.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/attesta-api /usr/local/bin/attesta-api
COPY templates /app/templates
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/attesta-api"]
