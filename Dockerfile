FROM docker.io/golang:1.26.1-alpine AS base
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/

FROM base AS build-api
RUN CGO_ENABLED=0 go build -o /api-server ./cmd/api

FROM base AS build-watcher
RUN CGO_ENABLED=0 go build -o /watcher ./cmd/watcher

FROM docker.io/alpine:3.20 AS api
COPY --from=build-api /api-server /usr/local/bin/api-server
EXPOSE 8080
CMD ["api-server"]

FROM docker.io/alpine:3.20 AS watcher
COPY --from=build-watcher /watcher /usr/local/bin/watcher
CMD ["watcher"]
