# Deploy

```sh
cp example.env .env   # fill in required values
set -a; . ./.env; set +a
GOWORK=off go run ./cmd/matchmaking
```

| Variable | Meaning |
| --- | --- |
| `MATCHMAKING_DATABASE_URL` | Postgres database `matchmaking`; the schema migrates at start |
| `MATCHMAKING_HTTP_ADDR` / `_GRPC_ADDR` / `_METRICS_ADDR` | `:20240` / `:20241` / `:20242` |
| `MATCHMAKING_INTERNAL_KEY` | what adapters (gRPC `x-internal-key`) and the erasure finaliser (`X-Internal-Key`) present |

Tests: `MATCHMAKING_TEST_DATABASE_URL=postgres://... GOWORK=off go test ./...` (skipped without it).

## Production (rootless Podman)

1. `podman exec openpak-postgres psql -U postgres -c 'CREATE DATABASE matchmaking'`.
2. Add `MATCHMAKING_INTERNAL_KEY` (random) to the shared `~/openpak/deploy/.env`, and the same key as
   `<ADAPTER>_MATCHMAKING_KEY` for each adapter.
3. Copy `deploy/openpak-matchmaking.container` to `~/.config/containers/systemd/`, run
   `~/openpak/deploy/pin-ips.sh --apply`, `systemctl --user daemon-reload`, then
   `systemctl --user start openpak-matchmaking`.
4. Check: `podman exec openpak-chat wget -qO- http://openpak-matchmaking:20240/healthz` (200).

The HTTP port is on the container network and loopback only; never route it through Traefik.
Logs: one line per HTTP request (method, path, status, ms) and per gRPC call (method, code, ms);
never bodies, keys, secrets, attributes or addresses.

## Erasure

`DELETE /internal/accounts/{account}/data` with `X-Internal-Key` makes the account leave every
session it is in (owners migrate or rooms end) and returns `{"deleted":n}`. Register the URL with the
website's erasure finaliser like the other cores. A repeat is a 200 with 0.
