# ekeid redesign overview

ekeid v1 is a clean-break service for remapping opaque identifiers and
coordinate tuples between external coordinate systems.

The service answers one primary public question:

> Given this coordinate, what other coordinates are currently attached to the
> same remap entity or derivable through applicable scoped rules?

Examples:

- Movie IDs: IMDb title ID to TMDB, TVDB, Wikidata, Freebase, or other movie
  database IDs.
- Song coordinates: a direct recording ID or a composite coordinate such as
  release ID plus track number.
- TV episode coordinates: a series ID plus season and episode values, or a
  direct TVMaze episode ID.

The service deliberately does not try to decide what a movie, song, recording,
release, episode, edition, work, or other media concept "really is". It only
stores and serves remappable coordinates.

## Design posture

The v1 design optimizes for:

- fast single-coordinate lookups;
- cache-friendly read APIs;
- opaque coordinate values;
- simple operational ingestion primitives;
- avoiding ontology curation;
- avoiding false precision when source data is messy.

The core lookup path is:

```text
coordinate key -> remap entity -> asserted coordinates on that entity
                         -> derived coordinates from applicable scoped rules
```

Entities are operational resolution nodes. They are not public claims that all
attached coordinates identify the exact same ontological thing. In messy media
data, some upstream IDs may mean exact recordings, others may mean loose
recording equivalence, and some may be inconsistently applied. ekeid v1 does not
model those distinctions.

v1 also supports derived coordinates for common tuple-shaped remaps. For
example, if a TV series coordinate in one database is attached to the same remap
entity as a TV series coordinate in another database, an explicitly scoped rule
may derive matching episode coordinates by copying season and episode parts.
Derivation rules are intentionally scoped and declarative; coordinate-definition
inheritance never implies derivation.

## Public v1 surface

Public v1 is read-only:

- `GET /v1/lookup/{coordinate_definition_id}/{part...}`
- `GET /v1/coordinates`
- `GET /v1/coordinates/{coordinate_definition_id}`
- `GET /healthz`
- `GET /v1/version`

Public v1 does not expose:

- public writes;
- provenance;
- entity IDs;
- derivation internals;
- confidence scores;
- candidate clusters;
- batching;
- fuzzy search;
- metadata lookup;
- editorial correction UI;
- application-level authentication or authorization.

Authentication and authorization are deployment concerns, handled outside the
process by a reverse proxy or equivalent boundary.

## Non-goals

ekeid v1 does not provide:

- fuzzy title/name matching;
- media metadata search;
- canonical title selection;
- source trust scoring;
- confidence ranking;
- public correction workflows;
- public import APIs;
- ontology classification by domain, kind, granularity, work, edition, season,
  recording, or episode;
- in-process authn/authz or TLS termination.

## Important v1 tradeoff

Earlier design discussion favored candidate-oriented responses for ambiguity.
The entity-centered model chosen for v1 intentionally does not expose multiple
candidate clusters. A coordinate belongs to at most one remap entity.

This is a real tradeoff:

- It makes lookup simple and fast.
- It avoids public cluster semantics that the service cannot reliably justify.
- It pushes ambiguity handling into ingestion/admin policy.

If future use cases require representing a single coordinate as belonging to
multiple possible remap entities, that is a model expansion, not a small response
shape change.

Derived coordinates add a second tradeoff. A scoped derivation rule may be true
for nearly every item in a source pair while still producing rare bogus
coordinates for unusual cases, such as TV shows split into separate season-like
series in one database. v1 accepts that risk and fixes those cases with explicit
derivation exceptions instead of refusing broad useful derivations.
