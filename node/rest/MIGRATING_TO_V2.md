# Migrating from the v1 REST API to v2

The REST API is served under three prefixes: `/v2/<network>`, `/v1/<network>`, and the unversioned `/<network>`.
The unversioned prefix serves the v1 API.
The v1 API and the unversioned prefix are deprecated.
Use `/v2/<network>`.

The alias routes `/peers/count`, `/peers/all`, `/peers/all/metrics` and `/sync_status` are also deprecated, on every version, `/v2` included.

## Timeline

The deprecation started on 2026-11-08.

| Release | `/v1/<network>/…`, `/<network>/…` and the alias routes |
|---|---|
| The first release with the deprecation (N) | Served as before, plus the deprecation headers |
| N+1 | Served as before, plus the deprecation headers |
| N+2 | **`410 Gone`**, with a body that points to `/v2` |

## What to change

1. **Put `/v2` in front of the network name** in every request URL:

   ```text
   https://node.example/mainnet/block/latest     →  https://node.example/v2/mainnet/block/latest
   https://node.example/v1/mainnet/block/latest  →  https://node.example/v2/mainnet/block/latest
   ```

   The routes, query parameters and request bodies are the same on v2.

   Also replace the alias routes:

   | Deprecated | Use |
   |---|---|
   | `/peers/count` | `/connections/p2p/count` |
   | `/peers/all` | `/connections/p2p/all` |
   | `/peers/all/metrics` | `/connections/p2p/all/metrics` |
   | `/sync_status` | `/sync/status` |

2. **Successful responses do not change.**
   Every 2xx response, including `203`, has the same status and body on v2 as on v1.

3. **Error responses change. You must handle the new status codes and the JSON body.**

   - **Status:** v1 answers every error with `500`.
     v2 uses the status that describes the error:

     | Status | Meaning |
     |---|---|
     | `400 Bad Request` | The request is malformed: bad parameters, a body that does not parse, or a body over a size limit |
     | `404 Not Found` | The resource does not exist on this node, for example a block height it does not have yet |
     | `422 Unprocessable Entity` | The request is well formed but invalid, for example a transaction that fails verification |
     | `429 Too Many Requests` | Rate limited. A rate-limit rejection carries `Retry-After` |
     | `503 Service Unavailable` | The node cannot serve this now, for example because it is syncing or the route needs a validator |
     | `500 Internal Server Error` | A fault on the node |

     Branch on the status code, not on the error text.
     Handle any other 4xx or 5xx status by its class: v2 can add more specific codes.

   - **Body:** v1 sends plain text, `message — cause — …`, with an `(HTTP NNN)` suffix when the error did not come from a route.
     v2 sends JSON with `Content-Type: application/json`:

     ```json
     { "message": "…", "error_type": "not_found", "chain": ["cause", "…"] }
     ```

     If you display or parse error text, read `message` and `chain` from the JSON.

## How v2 changes

v2 is the stable API.

These can be added to v2: status codes, JSON fields in error and success bodies, response headers, routes, and query parameters.
For example, `transaction/broadcast` will use `409 Conflict`, and error bodies will get a machine-readable `code` field.

These do not change in v2: the status and body of an existing success response, and the meaning of an existing field.

Your client must ignore JSON fields and headers that it does not know.
It must handle an unknown status by its class (4xx or 5xx).
A change that breaks these rules gets a new API version.

## Detecting a deprecated route

Every response from a deprecated route, success or error, carries these headers ([RFC 9745](https://www.rfc-editor.org/rfc/rfc9745)):

```text
Deprecation: @1794096000
Link: <https://github.com/aleo-labs-protocol/snarkOS/blob/staging/node/rest/MIGRATING_TO_V2.md>; rel="deprecation"
```

`Deprecation` is the date of deprecation, 2026-11-08, as a Unix timestamp.
CORS exposes both headers, so browser clients can read them.

HTTP libraries do not act on these headers by themselves.
Check for `Deprecation` in your client and log a warning.
`snarkos developer` prints a warning for each deprecated response.
