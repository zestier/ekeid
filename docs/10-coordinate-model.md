# Coordinate model

## Coordinate definition

A coordinate definition describes the shape of a coordinate tuple.

Coordinate definitions are similar in spirit to Wikidata properties, except
that they define ordered tuples instead of single scalar values.

Coordinate definitions:

- have stable opaque IDs such as `C17`;
- may have friendly display names;
- may describe ordered part labels for humans and tooling;
- may include import-time validation patterns;
- may be marked deprecated;
- are immutable once created.

If a coordinate definition needs a different shape, create a new coordinate
definition ID. Do not change the meaning or tuple arity of an existing
definition.

The public API uses only coordinate definition IDs as identity. Friendly names,
labels, aliases, and descriptions are display/discovery metadata, not alternate
request identifiers.

## Coordinate value

A coordinate is a flat tuple:

```json
["C17", "tt1234", "6", "97"]
```

The first item is the coordinate definition ID. Remaining items are ordered
coordinate parts.

Coordinate parts are:

- Unicode strings;
- opaque;
- exact after URL decoding;
- allowed to contain characters such as `/` when properly percent-encoded in
  HTTP paths;
- never normalized by the service.

Coordinate parts are not:

- integers, even if they look numeric;
- dates, booleans, typed IDs, or nested coordinate objects;
- Unicode-normalized;
- interpreted according to coordinate definition semantics during lookup.

Empty coordinate parts are invalid.

## Flat coordinates only

Runtime coordinates are flat in v1. Nested coordinates are not part of lookup
identity.

Use a dedicated coordinate definition for composite external shapes:

```text
C17 = musicbrainz release id + track number
```

rather than:

```text
album_track(musicbrainz_release(id), track_number)
```

Coordinate definitions may later expose purely informational UI metadata that
says one definition extends the parts of another definition. That metadata does
not imply lookup inheritance, prefix matching, containment, or automatic
inference.

## Derived coordinates

Some coordinates can be derived from other coordinates by applying an explicit,
scoped rule. This is useful when external databases generally use the same tuple
layout for child items.

Example:

```text
C_tvdb_episode(tvdb_series_id, season, episode)
  -> C_tmdb_episode(tmdb_series_id, season, episode)
```

This rule must not apply globally. It only applies when an admin or importer has
recorded a scope binding such as:

```text
C_tvdb_series(123) maps to C_tmdb_series(456)
```

Then lookup may derive:

```text
C_tvdb_episode(123, "2", "5")
  -> C_tmdb_episode(456, "2", "5")
```

Coordinate derivation is separate from coordinate-definition UI inheritance.
Inheritance metadata can help humans understand that one coordinate definition
extends another, but it has no runtime meaning unless a derivation rule and
applicable scope binding also exist.

### Derivation templates

v1 derivations are declarative templates, not arbitrary code.

Allowed template shape:

- select fixed input parts that define the scope;
- bind replacement output parts from a scoped assertion;
- copy selected remaining input parts to selected output positions.

Examples:

```text
(series, season, episode) -> (target_series, season, episode)
(series, season, episode) -> (target_season_series, episode)
```

The second shape handles rare split-series cases, such as a database that models
each season-like subtitle as its own show entry.

v1 derivations must not include:

- scripts or plugin code;
- arithmetic;
- fuzzy matching;
- metadata/title matching;
- conditionals outside explicit scope and exception records.

### Derivation exceptions

Broad scoped derivations may be true for nearly all coordinates in a scope but
wrong for rare items. v1 handles that with explicit exceptions.

An exception suppresses a derived output for a rule and scope. Exceptions may be
as narrow as one derived coordinate or as broad as a deeper prefix, such as one
season within one series pair.

Asserted coordinates do not silently override derived coordinates. If an asserted
coordinate proves a derived coordinate is wrong, ingestion should add the
asserted membership and add an exception that blocks the bogus derived result.

## No ontology labels

Coordinate definitions must not require domain, kind, entity type, or
granularity labels such as `movie`, `recording`, `release`, `episode`, `work`,
or `edition`.

Those labels are tempting, but they invite ontology curation and create edge
cases where the service has to decide what external IDs "really mean". v1 avoids
that problem entirely.

## Validation

Coordinate definitions may include optional validation patterns for import-time
schema checks. For example, an importer may verify that a coordinate part looks
like an IMDb title ID before loading it.

Lookup does not run coordinate-definition validation. The hot path should not
load definition metadata, regexes, arity declarations, or friendly names. A
malformed but syntactically parseable coordinate simply fails to resolve unless
it happens to match asserted storage or an applicable derivation scope.

## HTTP encoding

Percent-encoding in HTTP paths is transport only. The lookup key is based on
decoded path segments, not on the exact spelling of the incoming URL.

For example, if a coordinate part contains `/`, clients must percent-encode that
slash so it is decoded as data rather than treated as a path separator.

Invalid percent-encoding or invalid UTF-8 is a malformed request.
