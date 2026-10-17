# syntax=docker/dockerfile:1
#
# gedis-server in a distroless image: a static binary, no shell, a non-root
# user. Flags go after the image name, for example a public demo that cannot
# be turned against the host:
#
#   docker run -p 6380:6380 gedis \
#     -maxmemory 32mb -maxmemory-policy allkeys-lru -save off \
#     -disable-commands config,debug,replicaof,slaveof,shutdown,save,bgsave

FROM golang:1-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gedis-server ./cmd/gedis-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gedis-server /gedis-server
WORKDIR /data
EXPOSE 6380
ENTRYPOINT ["/gedis-server", "-addr", ":6380", "-dir", "/data"]
