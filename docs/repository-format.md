# Repository format

Format version: `2`

## Layout

```text
<repo>/
  VERSION                 # ASCII integer, currently "2\n"
  index.redb              # completed backup and tombstone index
  .lock                   # exclusive OS flock (survives crash without manual cleanup)
  chunks/<aa>/<rest>      # immutable compressed chunk files
  manifests/<id>.manifest # published manifests
  tmp/                    # temporary files, never listed as backups
```

`<aa>` is the first two hex characters of the content ID. `<rest>` is the
remaining 62 hex characters.

## Magic and versions

- Repository `VERSION` file: `REPO_FORMAT_VERSION = 2`
- Manifest bytes: 8-byte magic `ZSTNMFST` + little-endian `u32` format version + JSON body
- Unknown major versions are rejected

## Content ID

```text
content_id = BLAKE3(
    "ZeroStun/ChunkContent/v1" ||
    little-endian u64 original_length ||
    original_bytes
)
```

The content ID is independent of compression codec and compressor version.

## Root hash

```text
root_hash = BLAKE3(
    "ZeroStun/RootManifest/v1" ||
    format_version_le_u32 ||
    total_logical_bytes_le_u64 ||
    chunk_count_le_u64 ||
    fastcdc_min_le_u32 ||
    fastcdc_avg_le_u32 ||
    fastcdc_max_le_u32 ||
    for each chunk in order:
        index_le_u64 ||
        logical_offset_le_u64 ||
        original_length_le_u64 ||
        stored_length_le_u64 ||
        content_id_32_bytes ||
        codec_tag_u8
)
```

Codec tags: `none=0`, `zstd=1`, `lz4=2`.

Golden vectors live in `tests/core_pipeline.rs`.

## Stored chunk file

Each file under `chunks/` is:

```text
8 bytes  magic "ZSTNCHNK"
4 bytes  little-endian version (1)
1 byte   codec tag (none=0, zstd=1, lz4=2)
3 bytes  reserved zeros
8 bytes  little-endian compressed payload length
N bytes  compressed payload
```

Dedupe keys on the original-byte content ID. If a later backup requests a
different codec for the same original bytes, the existing on-disk codec and
payload length are recorded in that backup's manifest.

## Atomic commit

1. Write chunk payload to `tmp/chunk-<id>-<rand>`.
2. `flush` + `sync_all`.
3. `rename` into `chunks/<aa>/<rest>`.
4. If the destination already exists, keep the existing chunk.
5. Write the encoded manifest to `tmp/manifest-<id>-<rand>`.
6. `flush` + `sync_all`.
7. Insert the encoded bytes into `index.redb` and commit.
8. `rename` into `manifests/<id>.manifest` as a human-readable copy.

`list`, `inspect`, `verify`, and `restore` load completed backups from
`index.redb` only and hide any backup ID present in the `tombstones` table.
Tombstoning never modifies completed backup bytes or chunk files. A crash after
the file rename but before the database commit cannot expose an unfinished
backup. A crash after the database commit but before the file rename still
leaves the backup visible, because the authoritative copy is in redb.

## Compatibility

Readers must reject an unknown `VERSION` or unknown manifest magic/version.
Version 1 readers do not understand tombstones, so version 2 repositories are
not readable by them. `Repository::open` rejects version 1 without mutation.
`Repository::init` explicitly migrates version 1 by creating the tombstone table
and only then atomically replacing `VERSION` with `2\n`.
