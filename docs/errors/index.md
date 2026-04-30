---
applies_to: v0.2.x
status: generated
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# Error Index

| Code | Title | Hint |
|---|---|---|
| `CDB-6020` | operator shape mismatch | check that the operator input table has the expected columns and row shape |
| `CDB-6040` | hash join key mismatch | align join key names and compatible value types before planning the join |
| `CDB-7001` | column segment read failed | verify the bundle path and rebuild the affected column segment from source data |
| `CDB-7081` | CSR checksum mismatch | rebuild the CSR or CSC file; the stored footer checksum does not match the body |
| `CDB-8002` | transaction conflict | another transaction committed a conflicting write; retry on a fresh snapshot |
| `CDB-9001` | bundle already exists | choose a new path or open the existing bundle instead of creating it again |
| `TF-1001` | invalid character | remove the unsupported character or quote it inside a string literal |
| `TF-1002` | unterminated string | add the closing quote or escape an embedded quote with a backslash |
| `TF-2001` | unexpected token | check the token near the highlighted span against the Tuft grammar |
| `TF-2015` | missing pattern after MATCH | add a node or relationship pattern immediately after MATCH |
| `TF-3001` | undefined prefix | declare the namespace prefix before using it in an IRI or qualified name |
| `TF-3004` | unknown class | register the class in the catalog or use an existing class local name |
| `TF-3005` | unknown property | check the property name against the catalog for the matched class |
| `TF-4001` | type mismatch | compare operands with compatible types or cast explicitly where supported |
| `TF-4010` | implicit cast forbidden | rewrite the expression so both sides have the same expected type |
| `TF-5003` | aggregate not allowed in WHERE | move aggregate predicates to a grouped or post-aggregation query stage |
| `TF-6012` | graph function limit exceeded | lower the traversal fanout, depth, or row budget before retrying |
| `TF-7004` | index corruption detected | rebuild the affected index from trusted source data |
| `TF-8002` | transaction conflict | retry the transaction from a fresh snapshot |
| `TF-9501` | ontology constraint violated | fix the catalog or data so it satisfies the declared ontology constraint |
