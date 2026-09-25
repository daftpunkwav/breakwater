# Multi-stage build for the gateway binary.
FROM golang:1.25-alpine AS build
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
