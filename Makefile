VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: all build test lint clean

all: build

build:
	go build $(LDFLAGS) -o bin/nats-tiered-storage ./cmd/nats-tiered-storage
	go build $(LDFLAGS) -o bin/nts-ctl ./cmd/nts-ctl

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

.PHONY: docker
docker:
	docker build -t nats-tiered-storage:$(VERSION) -f deploy/docker/Dockerfile .

.PHONY: dev
dev:
	docker compose -f deploy/docker/docker-compose.yaml up --build
