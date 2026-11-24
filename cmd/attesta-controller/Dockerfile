# syntax=docker/dockerfile:1
FROM golang:1.27 AS build

ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/attesta-controller ./cmd/attesta-controller

# Static, non-root runtime with no shell or package manager. UID 65532 matches
# the pod's runAsNonRoot securityContext.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/attesta-controller /attesta-controller
USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/attesta-controller"]
