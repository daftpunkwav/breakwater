# deploy/ agent rules

## Images

- Build context is the repository root. Dockerfiles:
  `breakwater.Dockerfile`, `mockllm.Dockerfile`.
- Pin the build image to the `go.mod` toolchain
  (`golang:1.27.2-alpine`). Pin the runtime to `alpine:3.22`.
- Build with `CGO_ENABLED=0`. The runtime user is `nobody`.
- The gateway image creates `/data` owned by `nobody` for the access
  log. The process entrypoint is the binary with no flags.
- CI builds both images. A Dockerfile change still has to build there.

## Compose

- `docker-compose.yml` is the local stack: Redis, PostgreSQL, mockllm,
  gateway.
- Publish ports on `127.0.0.1` only.
- PostgreSQL runs `schema.sql`, then `seed.sql`, from
  `docker-entrypoint-initdb.d` on first boot. An existing volume does
  not re-run them.
- `schema.sql` uses `IF NOT EXISTS`. Keep changes additive.
- `seed.sql` is local development data, including the load-test keys.
  Do not commit production credentials.
- The compose gateway uses the static identity document.
  `BREAKWATER_ADMIN_TOKEN` may be overridden from the environment; the
  file's default is the local dev token.
