# matchmaking

OpenPak's console-neutral matchmaking core: rooms, lobbies, members, attributes, search and
peer-to-peer signaling. Console adapters (`np-matching2` first; Demonware, Blaze and NEX later) keep
the wire and call this over gRPC; the core keeps the state, in Postgres.

* [docs/protocol.md](docs/protocol.md): the model and every call
* [docs/adapter-guide.md](docs/adapter-guide.md): how a console maps onto it
* [docs/deploy.md](docs/deploy.md): run and deploy

Ports 20240 HTTP, 20241 gRPC, 20242 metrics. AGPL-3.0-only.
