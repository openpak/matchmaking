# Adapter guide

Rule (board #28, #40): the adapter keeps the wire (codecs, ids, signatures, framing); the core keeps
the sessions. An adapter holds no room, member or search state of its own beyond the live connection
and the id maps it needs to talk to the console.

1. Map the console identity (NpId, XUID, PID, profile id) to a core account id through the account
   core's links. The core only takes account ids (UUIDs).
2. Name the title: `TitleRef{network: <your link namespace>, title: <neutral key>}`.
3. Keep the console's own struct next to the neutral fields in `data` / `member_data`; keep
   anything the console searches by as a `searchable` attribute.
4. Heartbeat the accounts that are connected (every ttl/3), and Leave on disconnect.
5. One `Watch` stream per adapter process for all connected accounts; render events as the
   console's notifications. After a reconnect call `GetSession` for each live session.
6. Env: `<SERVICE>_MATCHMAKING_ADDR` (`openpak-matchmaking:20251`) and `<SERVICE>_MATCHMAKING_KEY`.
   Generate the client from `proto/openpak/matchmaking/v1/matchmaking.proto`.

## Per console

### PS3 Matching2 (`np-matching2`)

* Room = session `kind=room`; a Matching2 world/lobby = `kind=lobby` or a `$world` attribute.
  RoomId on the wire is a u64: the adapter keeps a small `id -> session UUID` map per connection
  (not state: it is rebuilt from `GetSession`/`Search`).
* `RoomDataInternal` / `RoomDataExternal` / `RoomMemberDataInternal` go in `data` / `member_data`; the
  external searchable binary and int attributes become `searchable` attributes (`int:<n>`, `bin:<n>`).
* SearchRoom -> `SearchSessions`; CreateJoinRoom -> `CreateSession`; JoinRoom -> `JoinSession`;
  LeaveRoom -> `LeaveSession`; SetRoomDataInternal -> `UpdateSession`; room events -> `Watch`.
* Signaling (`sceNpSignaling`) maps to endpoints plus `Signal`.

### Others (planned)

Demonware (MW3 service 21), Blaze (EA games) and NEX (`MatchmakeSession`) follow the same mapping:
the title's session struct goes in `data`, the match criteria in searchable attributes, the
console's participant slots are `member_no`.
