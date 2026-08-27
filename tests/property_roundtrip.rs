use proptest::prelude::*;
use zerostun::codec::{CompressionCodec, Compressor};
use zerostun::hash::content_id_from_bytes;

proptest! {
    #[test]
    fn codec_round_trip_preserves_arbitrary_bytes(data in proptest::collection::vec(any::<u8>(), 0..65_536)) {
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Zstd { level: 3 },
            CompressionCodec::Lz4,
        ] {
            let compressed = Compressor::compress(codec, &data).unwrap();
            let restored = Compressor::decompress(codec, &compressed, data.len()).unwrap();
            prop_assert_eq!(&restored, &data);
            prop_assert_eq!(content_id_from_bytes(&restored), content_id_from_bytes(&data));
        }
    }
}

#[test]
fn truncated_compressed_payloads_are_rejected() {
    let mut data = Vec::with_capacity(128 * 1024);
    for i in 0..32_768u32 {
        data.extend_from_slice(&i.to_le_bytes());
    }

    for codec in [CompressionCodec::Zstd { level: 3 }, CompressionCodec::Lz4] {
        let compressed = Compressor::compress(codec, &data).unwrap();
        assert!(compressed.len() > 32);
        // Truncate half or quarter of the compressed payload
        let truncated = &compressed[..compressed.len() / 2];
        assert!(Compressor::decompress(codec, truncated, data.len()).is_err());
    }
}
