# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/goagent-lite ./cmd/server

FROM alpine:3.22

RUN adduser -D -H -u 10001 gateway && apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/goagent-lite /app/goagent-lite
COPY configs/agents.yaml /app/configs/agents.yaml

USER gateway
EXPOSE 8080
ENTRYPOINT ["/app/goagent-lite"]
CMD ["-config", "/app/configs/agents.yaml"]
