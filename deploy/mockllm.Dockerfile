# Multi-stage build for the mock upstream binary.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mockllm ./cmd/mockllm

FROM alpine:3.22
WORKDIR /
COPY --from=build /out/mockllm /mockllm
USER nobody
EXPOSE 8090
ENTRYPOINT ["/mockllm"]
