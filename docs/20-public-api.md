# Public API

The public v1 API is read-only and cache-friendly. It exposes lookup,
coordinate-definition discovery, health, and service version endpoints.

No public endpoint exposes provenance, entity IDs, write operations, imports,
derivation internals, confidence scores, candidate clusters, or batching.

## Coordinate representation in JSON

Coordinates in JSON responses are structured arrays:

```json
["C17", "tt1234", "6"]
```

They are not emitted as path strings. This avoids ambiguity around URL encoding
inside JSON.

## Lookup

```http
GET /v1/lookup/{coordinate_definition_id}/{part...}
```

Example:

```http
GET /v1/lookup/C17/tt1234/6
```

Lookup treats the path as an addressable coordinate resource. The server decodes
the path segments, constructs the coordinate key, and looks it up directly.

The lookup hot path must be schema-unaware:

- do not load coordinate definitions;
- do not validate arity;
- do not run regex validation;
- do not check whether the coordinate definition ID exists.

Unknown definition IDs, wrong arity, and malformed-looking coordinate parts all
behave like absent lookup keys as long as the request path itself is syntactically
valid.

### Successful response

Status: `200 OK`

```json
{
  "input": {
    "coordinate": ["C17", "tt1234", "6"]
  },
  "matches": [
    {
      "coordinate": ["C17", "tt1234", "6"]
    },
    {
      "coordinate": ["C42", "tmdb-abc"]
    },
    {
      "coordinate": ["C51", "Q123"]
    }
  ]
}
```

`matches` is the complete coordinate set visible from the lookup resolution
context. The context may be a stored remap entity, a derived context produced by
an applicable scoped derivation rule, or both. `matches` includes the input
coordinate and may include derived coordinates produced by applicable scoped
derivation rules.

`matches` is semantically an unordered set. The API guarantees no duplicate
coordinates in the array, but it does not guarantee response order.

Public lookup does not distinguish asserted coordinates from derived
coordinates. Derivation provenance and rule details are internal in v1.

### No match

Status: `404 Not Found`

A syntactically valid lookup path returns `404` when the decoded coordinate key
is absent from both asserted lookup storage and applicable derivation scopes.

This supersedes the earlier idea of returning `200` with `matches: []`. With a
path-shaped lookup, absence is resource absence.

### Malformed request

Status: `400 Bad Request`

Use `400` only for malformed request syntax, such as:

- invalid percent-encoding;
- invalid UTF-8;
- missing coordinate definition ID;
- empty path segment / empty coordinate part.

Do not use `400` for unknown coordinate definition IDs, wrong arity, or lookup
misses.

### Query parameters

Lookup v1 does not define query parameters.

In particular, there is no `include=provenance`, no target filtering, no
pagination, no asserted-vs-derived filter, and no batch mode. Those can be
designed later without changing the basic lookup path.

## Coordinate definition discovery

```http
GET /v1/coordinates
GET /v1/coordinates/{coordinate_definition_id}
```

These endpoints expose coordinate definitions, not coordinate values.

The exact response payload shape is intentionally not pinned yet. It should be
defined before implementation. Expected fields may include ID, friendly name,
ordered part labels, deprecation status, and import-time validation metadata,
but v1 has not committed to that schema.

Discovery v1 supports list/get only. It does not include text search, filtering,
or alias lookup.

## Health

```http
GET /healthz
```

Operational health endpoint. Intended for load balancers, deployment checks,
and orchestration systems.

The exact response shape is not pinned yet.

## Service version

```http
GET /v1/version
```

Returns service/API version information only.

It does not report current data freshness, import timestamps, or source
versions. Public v1 does not expose provenance or data-version metadata.
