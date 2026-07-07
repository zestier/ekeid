# Storage model

This document describes the logical storage model. It is not yet a physical
schema.

## Logical tables

### coordinate_definitions

Stores immutable coordinate definition records.

Expected logical fields:

- `id`, such as `C17`;
- display metadata;
- ordered part metadata;
- deprecation metadata;
- optional import-time validation metadata.

The exact public payload shape is not pinned yet.

### entities

Stores operational remap entities.

Expected logical fields:

- `id`, such as `E123`;
- lifecycle timestamps;
- optional admin metadata.

Entity IDs are admin/internal handles. They are not returned by public lookup in
v1.

### coordinates

Stores decoded coordinate tuples.

Expected logical fields:

- coordinate definition ID;
- ordered string parts;
- normalized internal lookup key;
- owning entity ID.

The internal lookup key is an implementation detail. It may be a normalized path
string, tuple encoding, hash, or other representation. The key must preserve the
exact decoded Unicode strings and must not depend on URL spelling.

Coordinate ownership is unique in v1: one coordinate belongs to at most one
entity.

### source_datasets

Stores source dataset identities such as `S17`.

Human-friendly names are metadata.

### import_runs

Stores internal import audit records:

- import run ID;
- source dataset ID;
- `fetched_at`;
- `imported_at`;
- status;
- diagnostic metadata.

### source_contributions

Stores source support for coordinate membership on an entity.

Logical key:

```text
source_dataset_id + entity_id + coordinate_key
```

Multiple source datasets may support the same entity-coordinate membership.
Source replacement removes only the contribution records for the replaced source
dataset/subset, then detaches coordinates whose membership is no longer
supported.

### derivation_rules

Stores constrained declarative derivation templates.

Expected logical fields:

- rule ID;
- input coordinate definition ID;
- output coordinate definition ID;
- fixed input part positions used for scope matching;
- output part positions supplied by scope bindings;
- copied input-to-output part positions;
- inverse-rule metadata, if the rule is bidirectional.

Rules are schema-level configuration. They do not by themselves derive any
coordinate. A rule is active only where a scope binding applies.

### derivation_scope_bindings

Stores scoped applicability assertions for derivation rules.

Logical fields:

- binding ID;
- rule ID;
- fixed input coordinate prefix or parts;
- bound output coordinate prefix or parts;
- lifecycle timestamps;
- optional admin metadata.

Example:

```text
rule R:
  C_tvdb_episode(series, season, episode)
    -> C_tmdb_episode(target_series, season, episode)

binding B:
  series = "123"
  target_series = "456"
```

Scope bindings may represent broad common cases, such as whole-series episode
layout compatibility, or narrower exceptional cases, such as one season mapping
to a separate target show entry.

### derivation_exceptions

Stores explicit suppressions for derived outputs.

Logical fields:

- exception ID;
- rule ID;
- binding ID or matching scope;
- blocked derived coordinate key or blocked derived coordinate prefix;
- lifecycle timestamps;
- optional admin metadata.

Exceptions are used for rare cases where a broad scoped derivation would produce
bogus coordinates. They are also the mechanism by which asserted corrective data
prevents a bad derived coordinate from appearing in public lookup.

### derivation_source_contributions

Stores source support for derivation scope bindings and exceptions.

Logical key:

```text
source_dataset_id + derivation_record_kind + derivation_record_id
```

This mirrors coordinate membership contribution tracking so source replacement
can remove derivation support without deleting a binding or exception still
supported by another source.

## Lookup index

The hot lookup path should be implemented as a read-optimized projection rather
than live relational reasoning.

At the logical level, lookup should be equivalent to:

```text
decoded coordinate key -> entity id -> asserted coordinates owned by entity
decoded coordinate key -> applicable derivation rules -> derived resolution context
```

It must not require:

- joining coordinate definitions;
- checking definition existence;
- checking arity;
- running validation patterns;
- computing source provenance.

## Read model strategy

The authoritative model and the lookup model are separate concerns.

The authoritative model stores entities, asserted coordinates, source
contributions, derivation rules, scope bindings, and exceptions. It is optimized
for correctness, replacement, repair, and transactional updates.

The lookup read model is optimized for request latency. It should include:

- an asserted lookup projection:

  ```text
  coordinate_key -> entity_id or encoded asserted result set
  entity_id -> encoded asserted coordinate set
  ```

- a derivation binding index:

  ```text
  input_definition_id + fixed input prefix -> derivation bindings
  output_definition_id + fixed output prefix -> inverse derivation bindings
  ```

- an exception index:

  ```text
  rule/binding + derived coordinate key or prefix -> blocked
  ```

- optionally, an exact-response cache:

  ```text
  coordinate_key -> final encoded response
  ```

The asserted projection should be materialized because asserted coordinates are
finite known records. The derivation binding and exception indexes should be
materialized because they are compact and make derivation lookup bounded.

The full set of derived child coordinates should not be required to be
materialized. For example, a scoped rule from one TV series to another should not
require prewriting every possible episode coordinate. Lookup can instead match
the series prefix in the derivation binding index, copy the remaining path parts
through the declarative template, check exceptions, and produce the derived
coordinate on demand.

Exact-response caching is an optimization only. It may be useful for hot
coordinates, but correctness must come from the asserted projection plus
derivation and exception indexes. Cache entries must be invalidated or versioned
when the asserted projection, derivation rules, scope bindings, or exceptions
change.

Lookup must consider both asserted and derived coordinates:

1. If the decoded coordinate key is asserted, find its owning entity.
2. If the decoded coordinate key is derivable by an applicable rule, construct
   the derived resolution context implied by that rule and scope binding. This is
   required for reverse lookups of coordinates that were never asserted.
3. Return the union of asserted coordinates on any visible entity plus derived
   coordinates reachable through applicable non-excepted derivations.

The same public response shape is used whether a coordinate was asserted,
derived, or both.

Derived coordinates are semantic computed facts. They do not need to be fully
hydrated into the `coordinates` table. Implementations may materialize derived
lookup rows or derived result caches for performance, but materialization is an
optimization and must be invalidated when rules, scope bindings, exceptions, or
supporting asserted coordinates change.

Lookup should be bounded by configured derivation templates. v1 must not
recursively execute arbitrary rule chains in a way that can become an unbounded
graph search.

## Public consistency

Public lookups must not observe mixed old/new state during atomic source
replacement.

Implementation may use database transactions, staging tables, versioned lookup
indexes, or another mechanism. The public guarantee is what matters.

## Deletion and empty entities

The exact behavior for empty entities is not yet specified.

Before implementation, decide whether an entity with zero coordinates:

- is deleted automatically;
- is retained for admin audit;
- is retained only if it has source/import history.

Public lookup is unaffected because empty entities are unreachable by coordinate.
