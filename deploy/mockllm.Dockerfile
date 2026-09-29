# Multi-stage build for the mock upstream binary.
# Pinned to the go.mod toolchain: an older cached 1.25-alpine would make
# the build download a toolchain mid-build instead of failing the pin.
FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mockllm ./cmd/mockllm

FROM alpine:3.22
WORKDIR /
COPY --from=build /out/mockllm /mockllm
USER nobody
EXPOSE 8090
ENTRYPOINT ["/mockllm"]
