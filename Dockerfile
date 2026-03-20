FROM --platform=$BUILDPLATFORM docker.io/golang:1.26.1-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/

FROM builder AS build-api
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /api-server ./cmd/api

FROM builder AS build-watcher
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /watcher ./cmd/watcher

FROM docker.io/alpine:3.20 AS runtime

FROM runtime AS api
COPY --from=build-api /api-server /usr/local/bin/api-server
EXPOSE 8080
CMD ["api-server"]

FROM runtime AS watcher
COPY --from=build-watcher /watcher /usr/local/bin/watcher
CMD ["watcher"]
