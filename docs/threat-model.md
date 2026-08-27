# Threat model

## Trust boundary

MVP assumes a local operator who already can read the source file and write the
repository directory. The binary does not authenticate callers.

## Integrity coverage

An attacker or bit-rot event that mutates:

- a chunk file
- a published manifest
- chunk order or lengths inside a published manifest

is detected by `verify` and by restore-time rehashing.

Content IDs and the root hash use domain-separated BLAKE3 inputs so a chunk
hash cannot be confused with a root hash.

## Malicious repository input

Readers treat repository files as untrusted structured data:

- unknown format versions are rejected
- backup IDs must be `[A-Za-z0-9_-]{1,64}`
- content IDs must be 64 hex characters
- decompress size is capped at the manifest original length
- extra decompressed bytes after the expected length are rejected
- path traversal via backup IDs is rejected by identifier validation

## Out of scope

- Confidentiality of stored bytes (no encryption)
- Authenticity of backups against a remote attacker (no signatures)
- Availability against a local denial-of-service on disk
- Malicious operator with write access to the repository who replaces both
  chunks and the matching hashes in lockstep
