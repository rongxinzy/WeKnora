# Knowledge lifecycle and chunk-image APIs

This document describes the additive API and storage changes for knowledge
enable/disable and authenticated chunk-image reads.

## Knowledge enable state

`PUT /api/v1/knowledge/:id/enable-status`

Request body:

```json
{"enabled": false}
```

`enabled` is required and must be a JSON boolean. The route requires the
existing knowledge-owner/admin write guards and the knowledge base write
access check; it does not grant access to a different tenant or knowledge
base. The response is the normal knowledge DTO.

Disabling is accepted while parsing and atomically stores
`manual_disabled=true` and `enable_status=disabled`. Parser and enrichment
updates evaluate `manual_disabled` in the database write, so stale worker
snapshots and parse finalizers cannot re-enable the document. Reparse and file
replacement retain this explicit preference. Enabling is accepted only when
`parse_status=completed`; it atomically clears `manual_disabled` and sets
`enable_status=enabled`. If parsing/reparse wins the concurrent write first,
enable returns a conflict and the caller may retry after completion.

The additive `manual_disabled` column is migration `000104` for PostgreSQL
and `000024` for SQLite. Existing rows default to `false`, preserving their
current `enable_status`, except completed rows already marked disabled are
backfilled to `manual_disabled=true` so that an existing user choice survives
the next parse. Existing clients and the older retrieval API remain compatible.

## Chunk image read

`GET /api/v1/knowledge/:knowledge_id/chunks/:chunk_id/images/:index`

This is a viewer/read operation under the existing knowledge-base access
guards. `index` is zero-based and indexes the JSON array persisted in that
chunk's `image_info`. The API only serves a registered `resource://` handle
whose resource is active, has image kind, belongs to the caller's tenant, and
has an explicit binding with:

- owner type `knowledge_chunk`;
- owner ID equal to the requested chunk ID;
- relation `image`;
- the chunk, knowledge document, and KB all matching the request and tenant.

The caller cannot supply a storage path or URL. `http(s)://`, protocol-relative
references, provider paths, and unregistered handles are never proxied. The
response is a private, non-cacheable streamed file response with a sanitized
filename. Cross-tenant, cross-KB, cross-document, cross-chunk, deleted-row,
documents already marked `deleting`, inactive-resource, and missing-binding
requests fail closed, including the asynchronous window before the delete
worker removes the document and its chunks.

Normal parser output binds each stored image handle to the exact chunk after
the chunk rows are created. Deletion, KB deletion, and reparse release those
chunk claims before removing chunks; the blob is removed only after the
resource catalog reports no remaining owners. Shared bindings keep the bytes.
Cloned document/FAQ chunks copy image objects into the destination storage and
bind those copies to their new chunk IDs; failed clone cleanup removes bindings
only after the destination rows are removed. Legacy external/provider-path
metadata that survives cloning is preserved but is not registered as an
authorized image resource; only canonical `resource://` handles receive the
new chunk-image binding.
Legacy `image_info` values without a matching chunk binding are not authorized
by this route; reparse the document to establish verifiable ownership. No
automatic legacy backfill is performed because a URL string alone cannot prove
which document/chunk owns the underlying bytes. If a replacement reparse fails
after the old chunks have been cleaned, the restored source is marked failed
and its images remain unavailable until a successful reparse rebuilds chunks
and bindings. Cleanup likewise deletes only catalog resources after releasing
their exact chunk binding. Raw legacy `local://`, `s3://`, or external URLs are
preserved and logged as unverified orphans; they are never passed to
`DeleteFile` based only on a caller-writable metadata string.

The API is additive. Existing `/knowledge/:id/download` source-file behavior
and existing `/knowledge/:id/preview` behavior are unchanged.
