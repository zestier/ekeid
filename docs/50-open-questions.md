# Open questions

These are deliberate unresolved questions discovered while drafting the v1
specs.

## Coordinate definition response shape

Question: what exactly do `GET /v1/coordinates` and
`GET /v1/coordinates/{id}` return?

Current position: discovery endpoints are required, but the payload is not
pinned. Likely fields include ID, friendly name, part labels, deprecation state,
and maybe import-time validation metadata.

Why it matters: once public, field names are compatibility commitments.

## Admin endpoint shapes

Question: what are the concrete paths and payloads for non-public admin
operations?

Current position: required primitives are specified, but endpoint shapes are not.

Why it matters: ingestion tooling needs stable contracts, even if non-public.

## Empty entity lifecycle

Question: should entities with no coordinates be automatically deleted or kept
for admin audit?

Current position: unspecified.

Why it matters: affects admin repair workflows and source replacement cleanup.

## Manual edits versus source contributions

Question: how exactly should source contribution records behave when admins move
coordinates between entities?

Current position: source replacement should probably only remove contribution
records that still match the original entity-coordinate membership, but this
needs to be pinned.

Why it matters: source replacement must not accidentally undo manual repairs.

## Future ambiguity model

Question: will v1's one-coordinate-one-entity model be enough?

Current position: use one owning entity for v1. Ambiguity is handled during
ingestion/admin policy rather than by returning public candidate clusters.

Why it matters: if real data frequently requires one coordinate to map to
multiple plausible entities, this becomes a major model change.

## Provenance exposure

Question: should public lookup eventually expose source provenance?

Current position: no public provenance in v1. Source datasets and import runs
exist internally.

Why it matters: clients may later need to explain or debug why a coordinate is
matched.

## Derivation response transparency

Question: should public lookup eventually distinguish asserted matches from
derived matches?

Current position: no. Public v1 returns one `matches` set and keeps derivation
internals hidden.

Why it matters: clients may eventually want to explain why a coordinate appeared
or filter out derived coordinates. Adding that later would require an explicit
response-shape expansion.

## Derivation template language

Question: exactly how should derivation templates be represented in storage and
admin payloads?

Current position: v1 allows only declarative fixed-part binding and part-copying
templates. It must not support arbitrary code, arithmetic, fuzzy matching,
metadata/title matching, or unbounded graph traversal.

Why it matters: this is the boundary between a maintainable remap system and an
accidental rule engine.

## Derived lookup reachability

Question: how should lookup find an entity when the input coordinate exists only
as a derived coordinate and is not asserted?

Current position: derivation rules that are intended for reverse lookup must be
explicitly bidirectional or have paired inverse rules. Lookup can then resolve
the derived input through the applicable reverse binding.

Why it matters: without reverse reachability, derived coordinates would appear in
responses but `GET /v1/lookup/...` for that same coordinate could return `404`.

## Derivation cache strategy

Question: should derived coordinates be evaluated on demand, cached, or
materialized into lookup rows?

Current position: derived coordinates are semantic computed facts. Full
hydration is not required. Implementations may cache or materialize for
performance if invalidation is correct.

Why it matters: fully hydrating common TV episode derivations could waste space
and make exception updates expensive, but pure on-demand evaluation may need
careful indexing.

## Ordering

Question: should lookup responses eventually provide a stable order?

Current position: no ordering guarantee in v1.

Why it matters: unordered responses preserve optimization freedom, but stable
ordering can help clients diff and cache.

## Data freshness

Question: should there be a public data freshness or import status endpoint?

Current position: `/v1/version` reports only service/API version. Public v1 does
not expose data freshness.

Why it matters: clients may need to understand staleness when imports are
infrequent or cached for long periods.
