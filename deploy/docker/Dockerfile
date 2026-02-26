FROM golang:1.25-alpine AS builder

ARG VERSION=dev

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o /nats-tiered-storage ./cmd/nats-tiered-storage
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o /nts-ctl ./cmd/nts-ctl

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -u 1000 nts

COPY --from=builder /nats-tiered-storage /usr/local/bin/nats-tiered-storage
COPY --from=builder /nts-ctl /usr/local/bin/nts-ctl

RUN mkdir -p /var/lib/nts/data && chown -R nts:nts /var/lib/nts

USER nts
ENTRYPOINT ["nats-tiered-storage"]
CMD ["-config", "/etc/nts/config.yaml"]
