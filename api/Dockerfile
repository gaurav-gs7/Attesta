# syntax=docker/dockerfile:1
FROM golang:1.27-bookworm AS build

ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w" \
      -o /out/attesta-api ./api/cmd/attesta-api \
 && mkdir -p /out/generated

# Static, non-root runtime. /app/generated is pre-created and owned by the
# runtime user so a fresh named volume mounted there inherits that ownership.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/attesta-api /usr/local/bin/attesta-api
COPY templates /app/templates
COPY --from=build --chown=65532:65532 /out/generated /app/generated
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/attesta-api"]
