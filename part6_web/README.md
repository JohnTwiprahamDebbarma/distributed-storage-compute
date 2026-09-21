# Part 6: Web gateway (REST API)

An HTTP/JSON API I built with FastAPI in front of the Raft key-value store (Part 3).
It turns the store's guarantees into standard HTTP:

| The store (gRPC) | The web API (HTTP) |
| --- | --- |
| A key's version | The `ETag` header |
| Compare-and-swap by version | `PUT` with `If-Match: "<version>"`: `412` if someone else wrote first |
| Put-if-absent | `PUT` with `If-None-Match: *`: `201 Created`, or `412` |
| `(client_id, seq_num)` de-duplication | The `Idempotency-Key` header |
| `NOT_LEADER` redirects and elections | Handled inside the gateway; clients only see a slower response |
| `GetStatus` / `SetIsolated` | `GET /v1/cluster`, a live WebSocket, and `PUT /v1/cluster/nodes/{id}/isolation` |

## Run it

```bash
# from the repository root
pip install -r requirements.txt -r part6_web/requirements.txt
cd part2_3_replication && ./setup_raft.sh && ./start_kv_cluster.sh   # 3 nodes on :50051-:50053
cd ../part6_web && fastapi dev gateway/main.py                       # http://127.0.0.1:8000
```

Open <http://127.0.0.1:8000/docs> to try every endpoint from the browser, or use curl:

```bash
curl -i -X PUT localhost:8000/v1/kv/city -H 'Content-Type: application/json' -d '{"value": "Kolkata"}'
curl -i localhost:8000/v1/kv/city                                                  # ETag: "1"
curl -i -X PUT localhost:8000/v1/kv/city -H 'If-Match: "1"' \
     -H 'Content-Type: application/json' -d '{"value": "Delhi"}'                   # 200, now "2"
curl -i -X PUT localhost:8000/v1/kv/city -H 'If-Match: "1"' \
     -H 'Content-Type: application/json' -d '{"value": "Pune"}'                    # 412: stale
curl -s localhost:8000/v1/cluster                                                  # who leads
curl -i -X PUT localhost:8000/v1/cluster/nodes/0/isolation \
     -H 'Content-Type: application/json' -d '{"isolated": true, "heal_after_seconds": 30}'
```

## API

| Method and path | What it does | Success | Errors |
| --- | --- | --- | --- |
| `GET /v1/kv/{key}` | Read; `?consistency=linearizable` adds a read barrier | `200` + `ETag`; `304` if `If-None-Match` is current | 404, 503 |
| `PUT /v1/kv/{key}` | Write `{"value": "..."}` | `200` + `ETag`; `201` + `Location` with `If-None-Match: *` | 400, 412, 413, 422, 503, 504 |
| `DELETE /v1/kv/{key}` | Delete | `204` | 404, 503, 504 |
| `GET /v1/cluster` | Every node's role, term and log progress | `200` | - |
| `WS /v1/cluster/stream` | The same status, pushed every 0.5 s | - | - |
| `PUT /v1/cluster/nodes/{id}/isolation` | Cut a node off, or reconnect it (fault injection) | `200` | 404, 422, 503 |
| `GET /healthz`, `GET /readyz` | Liveness; readiness (a connected leader exists) | `200` | 503 |

Keys may contain slashes (`/v1/kv/ml/model`). Values are UTF-8 text up to 64 KiB; a value
another client wrote as raw bytes comes back base64-encoded.

## Errors

Every error is an RFC 9457 problem-details body (`application/problem+json`):

```json
{"type": "about:blank", "title": "Precondition Failed", "status": 412,
 "detail": "key 'city' is at version 2, not 1", "current_version": 2}
```

| Status | Meaning | What the client should do |
| --- | --- | --- |
| 400 | A malformed or unsupported header, e.g. `If-Match: *` | Fix the request |
| 404 | No such key | - |
| 412 | A conditional write lost: the key changed after it was read | Re-read (the response carries the current `ETag`), then retry |
| 413 | The value is over the size limit | - |
| 422 | The body failed validation | Fix the body |
| 503 | No leader was reachable in time; **nothing was applied** | Retry after `Retry-After` |
| 504 | A write was sent but not confirmed; **it may or may not have been applied** | Retry with the same `Idempotency-Key` |

## Design

### Layers

```text
api/        controllers  HTTP in and out: routes, schemas, error responses
services/   rules        value limits, conditional writes, idempotency keys
clients/    repository   gRPC to the nodes: leader tracking, redirects, retries
domain.py   the types and errors all three share (no HTTP, no gRPC)
```

Each layer calls only the one below it. I split it this way so the hermetic tests can
swap the gRPC client for an in-memory fake and still run every route and rule.

### Finding the leader

`RaftCluster` keeps one gRPC channel per node and remembers the last leader, so a request
normally goes straight there. A `NOT_LEADER` reply redirects it to the leader it names; an
unreachable node sends it on to the next node, pausing with capped exponential backoff and
jitter (50 ms, doubling to 500 ms). It gives up after `KV_REQUEST_DEADLINE`, 12 s by
default. I chose 12 s because that is long enough for a failover even when the first
election is a split vote, given this project's deliberately slow 2-4 s election timeouts.

### Idempotency keys

The store de-duplicates writes by `(client_id, seq_num)` in its replicated session table.
I give every HTTP request its own `client_id` with `seq_num` 1, so requests handled at
the same time never race on one sequence, and the gateway's own retries of a request are
applied at most once.

With an `Idempotency-Key`, I make the `client_id` a hash of the key *and* the request
(key, value, conditions). Retrying the same request returns the original result, even from
a new leader after a failover; reusing a key for a different request counts as a new one.

### 503 or 504?

I return `503` only when every attempt was answered `NOT_LEADER`: then nothing was
appended to any log, so the write was not applied. If any attempt failed in transit (a
timeout, a dropped or refused connection), I can't rule out that a leader appended it and
will still commit it, so I return `504`. For example, with both followers cut off, the
leader appends the write but can't commit it without a majority; once they reconnect, it
may commit after all.

### Browsers and CORS

In development the dashboard is served from another origin (Vite, port 5173), so I allow
that origin (`CORS_ORIGINS`) along with the conditional and idempotency headers. I also
list `ETag`, `Location` and `Retry-After` as exposed headers, because browsers hide all
other response headers from JavaScript.

### Configuration

Environment variables, documented in `gateway/config.py`: `KV_NODES`, `KV_RPC_TIMEOUT`,
`KV_REQUEST_DEADLINE`, `KV_MAX_VALUE_BYTES`, `CORS_ORIGINS`, `STATUS_INTERVAL`.

## Tests

```bash
python test_gateway_api.py    # 28 hermetic tests: every route and rule, on the real state machine
python test_gateway_live.py   # end to end against a live cluster: failover, loss of majority
```

Start `test_gateway_live.py` from fresh cluster state (`rm -rf raft_state_node*` in
`part2_3_replication`).

## Limitations

- `DELETE` doesn't support `If-Match` yet; it answers 400 rather than ignore the condition.
- No authentication or rate limiting yet, so the isolation endpoint must not be exposed to
  the internet unprotected.
- The replicated session table keeps the last 10,000 requests, so an `Idempotency-Key`
  retry is guaranteed to be recognised only within that window.
