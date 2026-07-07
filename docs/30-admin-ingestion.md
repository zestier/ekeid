# Internal admin and ingestion API

The admin API is non-public. It exists to support ingestion, curation, and
operational repair. It is included in the spec because the public lookup model
depends on clear mutation semantics.

v1 does not implement in-process authentication or authorization. Admin API
access control is a deployment concern.

## Core concepts

### Entity

An entity is an operational remap node. It has an opaque admin ID such as `E123`.

Entities collect coordinates that should be returned together by public lookup.
They are not public ontology claims.

### Coordinate membership

A coordinate belongs to at most one entity in v1.

Adding a coordinate that already belongs to a different entity is an error. The
caller must explicitly merge entities or remove/move the coordinate.

### Source dataset

A source dataset identifies an internal contribution source or subset. It has an
opaque ID such as `S17`.

Human-readable source labels or slugs are metadata, not identity.

Examples of possible source datasets:

- one imported Wikidata property set;
- one external dump subset;
- one curated internal import stream.

The exact taxonomy is intentionally not public API.

### Derivation rule

A derivation rule is a constrained declarative transform from one coordinate
definition to another. It describes how to construct an output coordinate from an
input coordinate by copying selected parts and filling other parts from a scoped
binding.

Derivation rules are not scripts. They cannot inspect media metadata, run fuzzy
matching, perform arithmetic, or execute source-specific code.

Example rule:

```text
C_tvdb_episode(series, season, episode)
  -> C_tmdb_episode(target_series, season, episode)
```

### Derivation scope binding

A scope binding says a derivation rule applies for a specific relationship
between fixed coordinate prefixes.

Example:

```text
rule R applies with:
  series = C_tvdb_series(123)
  target_series = C_tmdb_series(456)
```

Together, the rule and binding allow lookup to derive:

```text
C_tvdb_episode(123, "2", "5")
  -> C_tmdb_episode(456, "2", "5")
```

Scope bindings may be more specific than a whole series pair. For split-series
cases, a binding can map one source season to a target show entry:

```text
C_source_episode(series, season, episode)
  -> C_target_episode(target_show_for_season, episode)
```

### Derivation exception

A derivation exception suppresses a derived coordinate that would otherwise be
produced by a rule and scope binding.

Exceptions are the repair mechanism for rare cases where a broad scoped rule is
wrong. They may block one derived coordinate or a deeper prefix such as one
season within a series pair.

### Import run

Each import run should be tracked internally with:

- import run ID;
- source dataset ID;
- `fetched_at`;
- `imported_at`;
- success/failure status.

Public v1 does not expose import runs.

## Required admin primitives

The exact endpoint paths and payloads are not pinned yet, but v1 must support
these operations.

### Create entity

Create a new entity, optionally with initial coordinates.

If any provided coordinate already belongs to an entity, the operation fails
unless the operation is explicitly defined as a merge.

### Add coordinate to entity

Attach a coordinate to an existing entity.

Failure cases:

- entity does not exist;
- coordinate already belongs to another entity;
- coordinate has invalid structural shape for storage.

Import-time coordinate-definition validation may also reject the coordinate, but
lookup-time validation does not exist.

### Remove coordinate from entity

Detach a coordinate from an entity.

If the entity becomes empty, whether it is deleted automatically or retained as
an empty admin object is not yet specified.

### Merge entities

Merge one or more entities into a caller-chosen survivor.

Coordinates move to the surviving entity. Non-surviving entity IDs disappear.
There are no redirects or aliases in v1.

### Create derivation rule

Create a declarative derivation template between coordinate definitions.

The exact endpoint and payload shape are not pinned yet, but the operation must
record:

- input coordinate definition ID;
- output coordinate definition ID;
- input parts used as fixed scope;
- output parts supplied by scope bindings;
- input parts copied into output positions;
- whether the inverse direction is also valid.

### Add derivation scope binding

Record that a derivation rule applies for a specific coordinate scope.

Failure cases:

- rule does not exist;
- scope binding shape does not match the rule;
- binding coordinates have invalid structural shape for storage.

Import-time coordinate-definition validation may also reject binding
coordinates, but lookup-time validation does not exist.

### Add derivation exception

Record that a rule and scope binding must not produce a specific derived
coordinate or prefix of coordinates.

Exceptions are explicit. Directly asserting the correct coordinate membership
does not automatically delete or override a derived coordinate; ingestion should
also add an exception for the bad derivation.

### Replace source dataset contribution

Replace all coordinate memberships contributed by a source dataset or named
source subset.

Replacement is atomic:

- public lookups keep serving the previous committed state while replacement is
  in progress;
- on success, the new state becomes visible as one committed state;
- on failure, no lookup-visible changes occur.

Source replacement removes the replaced source's prior contributions. A
coordinate membership is removed only if no other source dataset still supports
that same coordinate membership.

Source contribution is tracked at the coordinate-membership level:

```text
source dataset S17 contributed coordinate C42/x/y to entity E123
```

not merely:

```text
source dataset S17 contributed coordinate C42/x/y globally
```

This distinction matters when the same coordinate value appears in operational
repair workflows or source replacements.

Source contribution must also be tracked for derivation scope bindings and
exceptions if those records are imported from replaceable sources. Replacing a
source dataset removes only that source's support for those derivation records.
The record remains active if another source still supports it.

## No split primitive in v1

There is no explicit split-entity operation in v1.

Over-merge correction can be expressed by removing coordinates and creating or
adding them to another entity.

## Challenge: source replacement and entity edits

Source replacement plus manual entity mutation can get subtle.

Example:

1. Source `S17` contributes coordinate `A` to entity `E1`.
2. An admin manually moves `A` to entity `E2`.
3. A later `S17` replacement omits `A`.

The spec still needs to decide whether source replacement should remove only the
source contribution record, detach `A` from `E2`, or notice that the contribution
no longer matches the current entity membership.

Before implementation, v1 should define one of:

- source contributions are invalidated when membership moves;
- manual moves transfer or clear contribution ownership;
- source replacement only touches memberships that still match the recorded
  entity ID.

The conservative default is the last option: replacement only removes source
contributions for the exact entity-coordinate membership it previously
contributed.
