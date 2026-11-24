.PHONY: fmt test lint build docker-build e2e run-api run-cli run-controller demo demo-full demo-failures validate-service compose-up compose-down clean

fmt:
	go fmt ./...

test:
	go test -race ./...

lint:
	golangci-lint run ./...
	shellcheck scripts/*.sh

build:
	mkdir -p bin
	go build -trimpath -o bin/attesta-api ./cmd/attesta-api
	go build -trimpath -o bin/attesta ./cmd/attesta
	go build -trimpath -o bin/attesta-controller ./cmd/attesta-controller

run-api:
	ATTESTA_API_TOKEN=local-dev-token go run ./cmd/attesta-api

run-cli:
	ATTESTA_API_TOKEN=local-dev-token go run ./cmd/attesta

run-controller:
	go run ./cmd/attesta-controller --leader-elect=false

demo:
	bash scripts/demo-local.sh

demo-full:
	bash scripts/local-ci-cd.sh

demo-failures:
	bash scripts/failure-mode-demo.sh

validate-service:
	bash scripts/validate-service-scaffold.sh generated/services/payments-api

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

docker-build:
	docker build -f cmd/attesta-controller/Dockerfile --build-arg VERSION=$(VERSION) -t attesta-controller:$(VERSION) .
	docker build -f cmd/attesta-api/Dockerfile --build-arg VERSION=$(VERSION) -t attesta-api:$(VERSION) .

KIND_CLUSTER ?= attesta-e2e

# Runs the controller end-to-end test on a throwaway kind cluster.
e2e:
	kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --name $(KIND_CLUSTER) --wait 120s
	docker build -f cmd/attesta-controller/Dockerfile --build-arg VERSION=e2e -t attesta-controller:e2e .
	kind load docker-image attesta-controller:e2e --name $(KIND_CLUSTER)
	kubectl config use-context kind-$(KIND_CLUSTER)
	bash scripts/e2e-rollback.sh

compose-up:
	docker compose up --build

compose-down:
	docker compose down

clean:
	rm -rf generated
