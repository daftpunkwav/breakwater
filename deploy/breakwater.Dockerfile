# Multi-stage build for the gateway binary.
# Pinned to the go.mod toolchain: an older cached 1.27-alpine would make
# the build download a toolchain mid-build instead of failing the pin.
FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/breakwater ./cmd/breakwater

FROM alpine:3.22
WORKDIR /
COPY --from=build /out/breakwater /breakwater
# The access log volume mounts here; the unprivileged user needs it.
RUN mkdir /data && chown nobody:nobody /data
USER nobody
EXPOSE 8080
ENTRYPOINT ["/breakwater"]
