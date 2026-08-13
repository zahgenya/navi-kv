FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /kv-api ./cmd/kv-api

FROM alpine:latest

COPY --from=builder /kv-api /usr/local/bin/kv-api

WORKDIR /data

ENTRYPOINT ["kv-api"]
