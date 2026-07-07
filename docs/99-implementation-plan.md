# Implementation plan

This plan is provisional and should be revised after the open questions are
closed.

## Phase 1: Model and storage

- [ ] Define physical schema for coordinate definitions, entities, coordinates,
      source datasets, import runs, and source contributions.
- [ ] Implement exact coordinate tuple encoding for storage lookup keys.
- [ ] Enforce one-coordinate-one-entity ownership.
- [ ] Implement entity create, coordinate add/remove, and merge primitives.
- [ ] Define empty entity lifecycle.
- [ ] Define physical schema for derivation rules, scope bindings, exceptions,
      and derivation source contributions.
- [ ] Implement derivation template validation without arbitrary code execution.

## Phase 2: Public read API

- [ ] Implement `GET /v1/lookup/{coordinate_definition_id}/{part...}`.
- [ ] Implement `404` lookup miss semantics.
- [ ] Implement `400` malformed path semantics.
- [ ] Implement coordinate JSON response envelope.
- [ ] Implement on-demand derived coordinate evaluation for applicable scoped
      rules.
- [ ] Ensure derived-only coordinates are addressable by lookup when an inverse
      or reverse binding is defined.
- [ ] Suppress derived coordinates blocked by derivation exceptions.
- [ ] Implement `GET /v1/coordinates`.
- [ ] Implement `GET /v1/coordinates/{id}`.
- [ ] Implement `GET /healthz`.
- [ ] Implement `GET /v1/version`.

## Phase 3: Internal ingestion

- [ ] Define admin endpoint paths and payloads.
- [ ] Implement source dataset and import run records.
- [ ] Implement atomic source dataset replacement.
- [ ] Ensure public reads observe old state until replacement commits.
- [ ] Define manual edit interaction with source contribution records.
- [ ] Define admin endpoint paths and payloads for derivation rule creation,
      scope binding creation, and exception creation.
- [ ] Implement source replacement for derivation scope bindings and exceptions.

## Phase 4: Hardening

- [ ] Add tests for URL decoding, encoded slash handling, invalid encodings, and
      empty path segments.
- [ ] Add tests for opaque Unicode coordinate parts without normalization.
- [ ] Add tests for lookup miss, successful lookup, deduplication, and unordered
      response semantics.
- [ ] Add tests for merge conflict behavior.
- [ ] Add tests for source replacement preserving memberships supported by other
      sources.
- [ ] Add tests for scoped derivation in both lookup directions.
- [ ] Add tests for split-series derivation bindings.
- [ ] Add tests that derivation exceptions suppress bogus derived coordinates.
- [ ] Add tests that derived coordinates are not exposed as provenance-bearing
      public internals.
