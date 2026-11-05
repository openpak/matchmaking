# Matchmaking core: model and calls

Why: every online game on every console needs the same five things (a place to create, a list to
search, a way to join and leave, member data, and a path between the players). The consoles differ
only in the wire. The core keeps the five things; an adapter keeps the wire (card #28, plan
`prds/repos/ps3-games-plan.md`). The rule is that an adapter holds protocol state only: ids on the
wire, the ticket, the connection. Room and member state live here.

## Model

* **Session**: `id` (UUID), title, `kind` (`room` or `lobby`), optional `parent_id` (a room's
  lobby), optional `unique_key` (a world id: one open session per `(title, kind, unique_key)`),
  `owner_id`, `max_members` (default 8, max 1024), `open`, `hidden` (never listed, join by id),
  `network_only` (seen only from `title.network`), `migrate_owner`, optional join secret
  (stored as a salted hash), typed attributes, an opaque `data` blob (8 KiB), `revision`, `ttl_seconds`.
* **Member**: `member_no` (1-based, never reused while the session lives), `account_id`, display name,
  attributes, opaque `data` (4 KiB), endpoints, network it joined from.
* **Attribute**: `name` (`[A-Za-z0-9_.:-]{1,64}`), type int / string / bytes, `searchable`. At most 64.
  Strings and bytes up to 1024. Only searchable attributes are visible to non-members and
  filterable; the rest are member-only. The core never interprets a blob.
* **Endpoint**: `kind` (`udp` public address, `relay` TURN, `local`), `host`, `port`, `nat_type`.
  Max 8 per member. Members read each other's endpoints from the session to open a P2P path.
* **Title**: `TitleRef{network, title}`. `title` is the neutral key (`terraria`), the same key
  title-storage uses; the adapter maps its native id. A session is findable from every network
  unless `network_only`.

All state is Postgres (`sessions`, `members`). The only in-memory part is the `Watch` fan-out; a
restart drops the streams and loses nothing, adapters reconnect and `GetSession` to resync.

## Calls (gRPC `openpak.matchmaking.v1.Matchmaking`, port 20251, metadata `x-internal-key`)

| Call | Meaning |
| --- | --- |
| `CreateSession` | new session, creator is member 1 and owner. A taken `unique_key` is `AlreadyExists` |
| `CreateOrJoinSession` | find the session with this `unique_key` and join, else create; a lost race becomes a join |
| `JoinSession` | add a member. A repeat join returns the existing record. `FailedPrecondition` closed or wrong network, `ResourceExhausted` full, `PermissionDenied` wrong secret, `allow_closed` for invited players |
| `LeaveSession` | idempotent. A room ends with its owner unless `migrate_owner` (then the oldest member takes over); a lobby ends when empty |
| `GetSession` | members see everything; others only searchable attributes, no blob, no endpoints |
| `SearchSessions` | filters over searchable attributes and built-ins `$members $max_members $created $kind $unique_key $owner`; ops `eq ne gt ge lt le prefix`; sort by attribute or built-in; paging (default 20, max 100); `hits` before paging; closed rooms hidden unless `include_closed`; `only_with_free_slots`; `owners[]` for friend search; `networks[]` |
| `UpdateSession` | owner only. Attributes, blob, open, hidden, max_members; `expect_revision` for optimistic updates |
| `UpdateMember`, `SetEndpoints` | the member itself (or the owner for `UpdateMember`) |
| `KickMember`, `SetOwner` | owner only |
| `Heartbeat(account)` | keeps all of the account's memberships alive; members unseen for the session `ttl_seconds` (default 300) are removed every 30 s |
| `Signal(session, from, to, kind, payload)` | relay up to 1 KiB between members (`to` empty = everyone else), delivered on the Watch stream; `delivered` counts live streams |
| `Watch(accounts[])` | server stream: created, updated, ended, member joined / left / updated, owner changed, signal. Events are addressed (`to`) per account and are not stored |

HTTP: `GET /healthz`; `DELETE /internal/accounts/{account}/data` (erasure).

## Signaling

Two layers, so a console can use whichever it needs:

1. **Address exchange**: each member publishes endpoints (`SetEndpoints`, or at join). Peers read
   them from the session and hit them directly (STUN-reflexive `udp`, or a `relay` from the
   connectivity / turn cores). NAT type is carried so a console can pick the path itself.
2. **Message relay**: `Signal` carries a short message through the core when no direct path exists
   yet (hole-punch coordination, "please connect to me"). It is a live message, not stored.

The core does no NAT traversal itself. The connectivity core answers STUN (including the classic
change-request form the PS3 needs) and the turn core relays.

## Cross-network

A session is a session: an adapter for any network may search and join it, subject to
`network_only`. Whether a Terraria PS3 client can play with another platform depends on the game, not
the core; the core lets a title opt out by creating `network_only` sessions.

## Privacy and logs

Account ids only; no names are indexed. Logs: method, path, status, duration. Never attributes,
secrets, blobs or addresses. Erasure removes every membership; sessions an erased owner leaves
migrate or end. Join secrets are SHA-256 over `session id + secret`: they are short-lived game codes,
not passwords.

## Not in v0.1

Block / interaction checks against the account core (adapters filter friends' sessions with `owners[]`
and the account core themselves), invites (use the chat core), ranked matchmaking queues (ticket
based "find me a match" is a later `Queue` call over the same sessions), multi-replica Watch fan-out.
