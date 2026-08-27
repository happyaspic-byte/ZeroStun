use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use zerostun::codec::{CompressionCodec, Compressor};
use zerostun::hash::content_id_from_bytes;

fn bench_hashing_and_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_throughput");
    let size = 1024 * 1024; // 1 MiB
    let mut data = Vec::with_capacity(size);
    for i in 0..(size / 4) {
        data.extend_from_slice(&(i as u32).to_le_bytes());
    }

    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("blake3_1mb", |b| {
        b.iter(|| content_id_from_bytes(black_box(&data)))
    });

    group.bench_function("zstd_l3_1mb", |b| {
        b.iter(|| {
            Compressor::compress(CompressionCodec::Zstd { level: 3 }, black_box(&data)).unwrap()
        })
    });

    group.bench_function("lz4_1mb", |b| {
        b.iter(|| Compressor::compress(CompressionCodec::Lz4, black_box(&data)).unwrap())
    });

    group.finish();
}

criterion_group!(benches, bench_hashing_and_compression);
criterion_main!(benches);
