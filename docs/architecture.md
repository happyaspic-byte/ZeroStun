# Architecture

ZeroStun is a single CLI binary with library modules that have one responsibility each.

## Modules

| Module | Responsibility |
| --- | --- |
| `cli` (`src/main.rs`) | clap parsing, human/JSON output, exit codes |
| `config` | byte-size parsing, worker/queue validation, in-flight payload bound |
| `engine` | backup / verify / restore / inspect lifecycle |
| `source` | regular-file sequential input and start/end fingerprint |
| `chunking` | FastCDC v2020 min/avg/max validation and streaming chunker |
| `hash` | domain-separated BLAKE3 content IDs and root hashes |
| `codec` | none / zstd / lz4 compress and bounded decompress |
| `repository` | local layout, exclusive writer lock, atomic chunk/manifest publish |
| `manifest` | versioned encoding of backup metadata |
| `rate_limit` | deficit token bucket for bytes/sec and optional IOPS |
| `telemetry` | progress mode and job stats |

## Backup pipeline

1. Acquire exclusive writer lock.
2. Open the source file and record size + mtime.
3. Stream FastCDC chunks from a blocking reader one chunk at a time.
4. Apply the read token bucket before enqueueing each bounded chunk.
5. Feed chunks through a bounded `mpsc` channel of `queue_depth`.
6. Hash original bytes (`ZeroStun/ChunkContent/v1`).
7. Compress with the requested codec.
8. Consume write tokens, then write the chunk if it is new.
9. If the content ID already exists, reuse the on-disk codec and length.
10. Append ordered descriptors to the in-memory manifest.
11. Recheck source fingerprint.
12. Compute root hash and atomically publish the manifest.

Chunk order in the manifest is source order. Parallel CPU work must not reorder
logical offsets.

## Backpressure

The channel capacity is `queue_depth`. When the channel is full, the feeder
stops sending. Combined with the token bucket, this keeps memory bounded.

Maximum in-flight original payload is:

```text
max_chunk * (queue_depth + workers)
```

Configurations where `max_chunk * queue_depth > 1 GiB` are rejected.

## Resource limits

- Default FastCDC: 8 KiB / 64 KiB / 256 KiB
- Default workers: `min(available_parallelism, 4)`
- Default queue depth: 8
- Default codec: zstd level 3
- Decompress allocates at most the declared original length
