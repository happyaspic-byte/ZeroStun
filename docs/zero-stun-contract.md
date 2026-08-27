# Zero-Stun contract

## Guaranteed in this MVP

When `--read-rate` / `--write-rate` / `--read-iops` are set:

- Source reads wait until the deficit token bucket has capacity.
- Repository writes wait on the write bucket when configured.
- Burst capacity equals one second of the configured rate.
- A request larger than the burst is allowed, then the bucket goes into debt
  and subsequent I/O waits for that debt to refill.
- Tests allow scheduler jitter. A 100 KiB/s limiter consuming 200 KiB must take
  at least 800 ms and less than 4 s on CI.

Internal queues are bounded. The source is never fully loaded into memory.

## Not guaranteed

- Application p99 latency of 0 seconds on the host
- No stun between FT nodes
- Fairness against other processes without cgroup / ionice isolation
- Exact bytes/sec over very short windows

Host latency depends on kernel I/O scheduler, storage, CPU contention, and the
workload sharing the device. Those measurements belong to a later platform
validation cycle, not this portable core.

## Why platform validation is separate

Token buckets control this process. They cannot observe another VM's disk
queue. Proving stun-free behavior requires a defined host, a defined competing
workload, and p99 instrumentation on that host.
