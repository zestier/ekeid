# Technology and serving architecture

This document records the current implementation recommendation. It is not a
constraint from the existing repository; it follows from the new v1 design goal
of solid single-coordinate lookup performance.

## Recommended starting stack

- **API/runtime:** Rust with `axum` and `tokio`.
- **Authoritative storage:** PostgreSQL.
- **Database access:** direct SQL, preferably through `sqlx`.
- **Lookup projection:** initially PostgreSQL tables or indexes shaped like
  key-value lookups.
- **Cache:** optional in-process exact-response cache after measurement.
- **Future read-optimized store:** optional generated projection in RocksDB,
  LMDB, SQLite, or another embedded read store if PostgreSQL lookup performance
  becomes insufficient.

PostgreSQL should be the source of truth for ingestion, entity repair, source
replacement, derivation rules, scope bindings, exceptions, and transactional
updates. The public lookup path should not be implemented as open-ended
relational reasoning over that authoritative model.

## Serving shape

The desired serving shape is:

```text
coordinate_key -> asserted lookup projection
coordinate_key -> derivation binding index
coordinate_key -> exception index
```

Normal lookup should be a bounded set of indexed reads plus small declarative
template application. It should not require loading coordinate definitions,
validating arity, computing provenance, or walking an unbounded graph.

## Projection strategy

Materialize:

- asserted coordinate-to-entity lookup;
- entity-to-asserted-coordinate-set lookup;
- derivation binding indexes by fixed prefix;
- derivation exception indexes by exact derived coordinate or prefix.

Do not require materializing every possible derived coordinate. A rule such as:

```text
C_tvdb_episode(series, season, episode)
  -> C_tmdb_episode(target_series, season, episode)
```

with a binding:

```text
series = "123"
target_series = "456"
```

should not force prewriting rows for every episode. Lookup can match the fixed
series prefix, copy the remaining parts through the template, check exceptions,
and return the derived coordinate.

Exact-response rows or cache entries may be added for hot coordinates, but they
are optimizations. Correctness comes from the asserted projection plus
derivation binding and exception indexes.

## Pushback on alternatives

- Do not start with a graph database. The public query shape is key-to-set, not
  graph exploration.
- Do not start with Elasticsearch or another search engine. Fuzzy search and
  metadata lookup are non-goals.
- Do not start with Redis as the primary read model. It adds invalidation and
  operational complexity before there is evidence PostgreSQL cannot serve the
  projection.
- Do not make arbitrary rule execution part of lookup. Derivations should remain
  constrained declarative templates.

