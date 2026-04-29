---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CSR / CSC

CaracalDB stores forward and reverse adjacency as standalone files under the bundle path. CSR and CSC share the same byte layout; CSC is built over swapped `(src, dst)` columns.

## Wire Layout

```text
[ CRCL header (24 B) ]            # struct.pack("<8sIIII", magic, version, page_size, flags, crc32)
[ u64 num_vertices    ]
[ u64 num_edges       ]
[ u32 flags           ]           # bit 0: HAS_EIDS
[ u32 reserved        ]           # zero
[ u64 offsets[num_vertices + 1] ]
[ u64 neighbors[num_edges]      ]
[ u64 eids[num_edges]           ] # only when flags & HAS_EIDS
[ u32 footer_crc32              ] # CRC32 over the body after the CRCL header
```

`offsets[i + 1] - offsets[i]` is the out-degree of vertex `i` for CSR and the in-degree for CSC. `neighbors[offsets[i]:offsets[i + 1]]` is the adjacency slice in stable insertion order, so building twice over the same edge stream produces byte-identical files.

## Flags

| Bit | Name | Meaning |
|---|---|---|
| 0 | `HAS_EIDS` | The `eids` block is present. |

The remaining flag bits and the reserved word are zero in v0.1.x. Future readers should reject incompatible versions and ignore only fields explicitly marked as forward-compatible.

## CRC

`footer_crc32` covers the bytes `[HEADER_SIZE, len(file) - 4)`, meaning every byte after the CRCL header up to, but not including, the trailer itself. The shared header protects format identity; the footer protects adjacency arrays.

## Loader API

The reference loader is `caracaldb.graph.csr_format.read_csr(path, mmap=True)`. With memory mapping enabled, offsets, neighbors, and edge ids are returned as zero-copy array views. Builders write through a temporary file and rename on success so interrupted writes do not publish a partial index.

## Compatibility

Versioning is delegated to the shared CRCL header. Adding a new flag bit is backward-compatible only when older readers can ignore it safely; adding new array sections requires a version bump. CSC files reuse the CSR layout as-is; directionality is implicit in the filename or manifest entry, not the bytes.
